/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/epoch_store/ZookeeperEpochStore.h"

#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/MockZookeeperClient.h"
#include "logdevice/common/test/TestNodeSetSelector.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/epoch_store/LogMetaDataCodec.h"
#include "logdevice/server/epoch_store/SetLastCleanEpochZRQ.h"

using namespace facebook::logdevice;
using testing::_;

#define TEST_CLUSTER "epochstore_test" // fake LD cluster name to use

static const std::vector<logid_t> VALID_LOG_IDS{logid_t(1), logid_t(2)};
static const logid_t UNPROVISIONED_LOG_ID(3); // must not be provisioned in
                                              // Zookeeper for TEST_CLUSTER

class ZookeeperEpochStoreTestBase : public ::testing::TestWithParam<bool> {
 public:
  static std::string testSuffixName(::testing::TestParamInfo<bool> param_info) {
    return param_info.param ? "WithDoubleWrites" : "WithoutDoubleWrites";
  }
  /**
   * Create a Processor with TEST_CONFIG_FILE(TEST_CLUSTER.conf) config.
   * Create a ZookeeperEpochStore with the same config.
   * The config must list an up and running Zookeeper quorum and logs 1 and 2.
   */
  void SetUp() override {
    static Settings settings = create_default_settings<Settings>();

    settings.server = true; // ZookeeperEpochStore requires this
    settings.epoch_store_double_write_new_serialization_format =
        enableDoubleWrite();

    auto cfg_in =
        Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"))
            ->withNodesConfiguration(createSimpleNodesConfig(2));

    if (!cfg_in)
      return;

    config = std::make_shared<UpdateableConfig>(std::move(cfg_in));

    processor = make_test_processor(settings, config, nullptr, NodeID(0, 1));
    other_node_processor =
        make_test_processor(settings, config, nullptr, NodeID(1, 1));

    auto znodes = getPrefillZnodes();

    zkclient = std::make_shared<ZookeeperClientInMemory>(
        config->getZookeeperConfig()->getQuorumString(), std::move(znodes));
    epochstore = std::make_unique<ZookeeperEpochStore>(
        TEST_CLUSTER,
        processor->getRequestExecutor(),
        zkclient,
        config->updateableNodesConfiguration(),
        processor->updateableSettings(),
        processor->getOptionalMyNodeID(),
        processor->stats_);

    dbg::assertOnData = true;
  }

  virtual bool enableDoubleWrite() const {
    return false;
  }

  virtual ZookeeperClientInMemory::state_map_t getPrefillZnodes() {
    ZookeeperClientInMemory::state_map_t map;
    map["/logdevice/epochstore_test/logs"] = {"", {}};

    map["/logdevice/epochstore_test/logs/1"] = {"", {}};
    map["/logdevice/epochstore_test/logs/1/lce"] = {
        "3559930028@18446744073709551615@0@0", {}};
    map["/logdevice/epochstore_test/logs/1/metadatalog_lce"] = {
        "3522255020@18446744073709551615@0@0", {}};
    map["/logdevice/epochstore_test/logs/1/sequencer"] = {
        std::string("\x31\x35\x35\x31\x39\x35\x40\x4e\x30\x3a\x31\x23"
                    "\x02\x00\x00\x00\x3b\x5e\x02\x00\x3a\x5e\x02\x00"
                    "\x01\x00\x01\x00\x00\x00\x00\x00\x01\x00",
                    34),
        {}};

    map["/logdevice/epochstore_test/logs/2"] = {"", {}};
    map["/logdevice/epochstore_test/logs/2/lce"] = {
        "3502237871@18446744073709551615@0@0", {}};
    map["/logdevice/epochstore_test/logs/2/metadatalog_lce"] = {
        "4244623534@18446744073709551615@0@0", {}};
    map["/logdevice/epochstore_test/logs/2/sequencer"] = {
        std::string("\x31\x35\x35\x32\x31\x33\x40\x4e\x30\x3a\x31\x23"
                    "\x02\x00\x00\x00\x4d\x5e\x02\x00\x4c\x5e\x02\x00"
                    "\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00",
                    34),
        {}};
    return map;
  }

  std::shared_ptr<Processor> processor;
  std::shared_ptr<Processor> other_node_processor;
  std::shared_ptr<UpdateableConfig> config;
  std::shared_ptr<ZookeeperClientInMemory> zkclient;
  std::unique_ptr<ZookeeperEpochStore> epochstore;

 private:
  Alarm alarm_{std::chrono::seconds(30)};
};

class ZookeeperEpochStoreTest : public ZookeeperEpochStoreTestBase {
  bool enableDoubleWrite() const override {
    return GetParam();
  }
};

class ZookeeperEpochStoreTestEmpty : public ZookeeperEpochStoreTest {
 public:
  ZookeeperClientInMemory::state_map_t getPrefillZnodes() override {
    return {};
  }
};

class ZookeeperEpochStoreMigrationTest : public ZookeeperEpochStoreTestBase {
  ZookeeperClientInMemory::state_map_t getPrefillZnodes() override {
    return {};
  }
};

namespace {
TailRecord gen_tail_record(logid_t logid,
                           lsn_t lsn,
                           uint64_t timestamp,
                           OffsetMap offsets) {
  return TailRecord(
      {logid,
       lsn,
       timestamp,
       {BYTE_OFFSET_INVALID /* deprecated use OffsetMap instead */},
       0,
       {}},
      std::move(offsets),
      PayloadHolder());
}

TailRecord gen_tail_record_with_payload(logid_t logid,
                                        lsn_t lsn,
                                        uint64_t timestamp,
                                        OffsetMap offsets) {
  TailRecordHeader::flags_t flags = TailRecordHeader::HAS_PAYLOAD;
  std::array<char, 333> payload_flat{};
  std::strncpy(&payload_flat[0], "Tail Record Test Payload", 50);
  return TailRecord(
      {logid,
       lsn,
       timestamp,
       {BYTE_OFFSET_INVALID /* deprecated use byte_offset instead */},
       0,
       {}},
      std::move(offsets),
      PayloadHolder::copyBuffer(&payload_flat[0], payload_flat.size()));
}
} // namespace

/*
 * This is a test request for [get|set]LastCleanEpoch operations.
 * The request does the following:
 *   - calls ZookeeperEpochStore::getLastCleanEpoch() for a log.
 *   - verifies that the call succeeds, records the epoch number
 *   - calls ZookeeperEpochStore::setLastCleanEpoch() for the log with
 *     epoch number that's one greater than the one read
 *   - verifies that the call succeed
 *   - calls ZookeeperEpochStore::setLastCleanEpoch() for the log with
 *     epoch number that's smaller than the last one set
 *   -verifies that the call fails with E::STALE
 */
class LastCleanEpochTestRequest : public Request {
 public:
  LastCleanEpochTestRequest(ZookeeperEpochStore* epochstore, logid_t logid)
      : Request(RequestType::TEST_ZK_LCE_REQUEST),
        logid_(logid),
        epochstore_(epochstore) {}

  Execution execute() override {
    posted_to_ = Worker::onThisThread()->idx_;
    // get current LCE for logid_
    int rv_outer = epochstore_->getLastCleanEpoch(
        logid_,
        [this](Status st1, logid_t lid1, epoch_t read_epoch, TailRecord) {
          if (logid_ == UNPROVISIONED_LOG_ID ||
              logid_ == MetaDataLog::metaDataLogID(UNPROVISIONED_LOG_ID)) {
            EXPECT_EQ(E::NOTFOUND, st1);
          } else {
            EXPECT_EQ(E::OK, st1);
          }
          EXPECT_EQ(logid_, lid1);
          EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);

          if (logid_ == UNPROVISIONED_LOG_ID ||
              logid_ == MetaDataLog::metaDataLogID(UNPROVISIONED_LOG_ID)) {
            terminateRequest();
            return;
          }

          // log id is valid, set LCE to current+1
          epoch_t next_epoch(read_epoch.val_ + 1);
          cb_ = [next_epoch, read_epoch, this](
                    Status st2, logid_t lid2, epoch_t set_epoch, TailRecord) {
            if (st2 == E::AGAIN) {
              // another sequencer updated LCE at the same time and we lost
              // the race. try again.
              int rv = epochstore_->setLastCleanEpoch(
                  logid_,
                  next_epoch,
                  gen_tail_record(logid_, LSN_INVALID, 0, OffsetMap()),
                  cb_);
              EXPECT_EQ(0, rv);
              return;
            } else if (st2 != E::STALE) {
              // for the E::STALE case here, we ignore it because it is
              // explictily tested below. the reason we might get this errro
              // code is because we lost the race with another sequencer. in
              // that case we don't know which lce was actually set.
              EXPECT_EQ(E::OK, st2);
              EXPECT_EQ(next_epoch, set_epoch);
            }
            EXPECT_EQ(logid_, lid2);
            EXPECT_NE(EPOCH_INVALID, set_epoch);
            EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);

            // try to set LCE to its previous value, expect E::STALE
            int rv = epochstore_->setLastCleanEpoch(
                logid_,
                read_epoch,
                gen_tail_record(logid_, LSN_INVALID, 0, OffsetMap()),
                [next_epoch, this](
                    Status st3, logid_t lid3, epoch_t lce, TailRecord) {
                  EXPECT_EQ(E::STALE, st3);
                  EXPECT_EQ(logid_, lid3);
                  EXPECT_LE(next_epoch, lce);
                  EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);
                  terminateRequest();
                });
            EXPECT_EQ(0, rv);
          };

          int rv = epochstore_->setLastCleanEpoch(
              logid_,
              next_epoch,
              gen_tail_record(logid_, LSN_INVALID, 0, OffsetMap()),
              cb_);
          EXPECT_EQ(0, rv);
        });
    EXPECT_EQ(0, rv_outer);

    return Execution::CONTINUE;
  }

  // counts the number of completed Requests of this class
  static std::atomic<int> completedRequestCnt;
  EpochStore::CompletionLCE cb_;

 private:
  void terminateRequest() {
    ++completedRequestCnt;
    delete this;
  }

  const logid_t logid_;
  EpochStore* const epochstore_;
  worker_id_t posted_to_; // Worker to which this Request was posted
};

std::atomic<int> LastCleanEpochTestRequest::completedRequestCnt;

/*
 * This is a test request for the EpochMetaDataUpdateToNextEpoc hoperation. The
 * request does the following:
 *
 * - calls ZookeeperEpochStore::createOrUpdateMetaData() for a log with the
 *   EpochMetaDataUpdateToNextEpoch updater, expects it to succeed if the log id
 *   is valid (provisioned in the Zookeeper cluster),
 *   otherwise expects it to fail. On success, saves the epoch reported by the
 *   CF.
 * - if the log was valid, calls createOrUpdateMetaData() with
 *   EpochMetaDataUpdateToNextEpoch on the same log one more time. Expects the
 *   call to succeed and the reported epoch number to be one greater than the
 *   last one.
 */
class NextEpochTestRequest : public Request {
 public:
  explicit NextEpochTestRequest(
      ZookeeperEpochStore* epochstore,
      logid_t lid,
      EpochStore::WriteNodeID write_node_id = EpochStore::WriteNodeID::MY)
      : Request(RequestType::TEST_ZK_NEXT_EPOCH_REQUEST),
        logid_(lid),
        write_node_id_(write_node_id),
        epochstore_(epochstore) {}

  Execution execute() override {
    posted_to_ = Worker::onThisThread()->idx_;

    // increment next epoch the first time to get its current value
    int rv_outer = epochstore_->createOrUpdateMetaData(
        logid_,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            EpochMetaData::Updater::Options().setProvisionIfEmpty()),
        [this](Status st,
               logid_t logid,
               std::unique_ptr<EpochMetaData> info,
               std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
          if (logid_ == UNPROVISIONED_LOG_ID) {
            EXPECT_EQ(E::NOTFOUND, st);
          } else {
            EXPECT_EQ(E::OK, st);
          }
          EXPECT_EQ(logid_, logid);
          EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);

          if (logid_ == UNPROVISIONED_LOG_ID) {
            terminateRequest();
            return;
          }

          ASSERT_NE(nullptr, info);
          EXPECT_TRUE(info->isValid());
          EpochMetaData base_info(*info);
          int rv = epochstore_->createOrUpdateMetaData(
              logid_,
              std::make_shared<EpochMetaDataUpdateToNextEpoch>(
                  EpochMetaData::Updater::Options().setProvisionIfEmpty()),
              [base_info, this](
                  Status st1,
                  logid_t lid,
                  std::unique_ptr<EpochMetaData> next_info,
                  std::unique_ptr<
                      EpochStoreMetaProperties> /*next_meta_props*/) {
                EXPECT_EQ(E::OK, st1);
                EXPECT_EQ(logid_, lid);
                EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);

                ASSERT_NE(nullptr, next_info);
                EXPECT_TRUE(next_info->isValid());
                EXPECT_EQ(base_info.h.epoch.val_ + 1, next_info->h.epoch.val_);
                // other parts of metadata should remain the same
                EXPECT_EQ(base_info.replication.toString(),
                          next_info->replication.toString());
                EXPECT_EQ(
                    base_info.h.effective_since, next_info->h.effective_since);
                EXPECT_EQ(base_info.shards, next_info->shards);
                terminateRequest();
              },
              MetaDataTracer(),
              write_node_id_);
          EXPECT_EQ(0, rv);
        },
        MetaDataTracer(),
        write_node_id_);
    EXPECT_EQ(0, rv_outer);
    return Execution::CONTINUE;
  }

  // counts the number of completed Requests of this class
  static std::atomic<int> completedRequestCnt;

 private:
  void terminateRequest() {
    ++completedRequestCnt;
    delete this;
  }

  const logid_t logid_;
  EpochStore::WriteNodeID write_node_id_;
  EpochStore* const epochstore_;
  worker_id_t posted_to_; // Worker to which this Request was posted
};

std::atomic<int> NextEpochTestRequest::completedRequestCnt;

/*
 * This is a test request for the updateMetaData operation. The request does
 * the following:
 *
 * - calls ZookeeperEpochStore::createOrUpdateMetaData() for a log with
 *   EpochMetaDataUpdateToNextEpoch, gets the current metadata
 * - calls ZookeeperEpochStore::createOrUpdateMetaData() for a log with an
 *   updater to a different nodeset, expects it to succeed
 * - calls ZookeeperEpochStore::createOrUpdateMetaData() for the log again with
 *   an identical updater, expects it to return E::UPTODATE
 */
class UpdateMetaDataRequest : public Request {
 public:
  explicit UpdateMetaDataRequest(ZookeeperEpochStore* epochstore,
                                 logid_t lid,
                                 std::shared_ptr<Configuration> cfg)
      : Request(RequestType::TEST_ZK_UPDATE_METADATA_REQUEST),
        logid_(lid),
        epochstore_(epochstore),
        config_(std::move(cfg)),
        nodeset_selector_(std::make_shared<TestNodeSetSelector>()) {}

  Execution execute() override {
    posted_to_ = Worker::onThisThread()->idx_;
    int rv1 = epochstore_->createOrUpdateMetaData(
        logid_,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            EpochMetaData::Updater::Options().setProvisionIfEmpty(),
            config_,
            config_->getNodesConfiguration()),
        [this](Status st1,
               logid_t lid,
               std::unique_ptr<EpochMetaData> info,
               std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
          if (logid_ == UNPROVISIONED_LOG_ID) {
            EXPECT_EQ(E::NOTFOUND, st1);
          } else {
            EXPECT_EQ(E::OK, st1);
          }
          EXPECT_EQ(logid_, lid);
          EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);
          if (logid_ == UNPROVISIONED_LOG_ID) {
            terminateRequest();
            return;
          }
          ASSERT_NE(nullptr, info);
          EXPECT_TRUE(info->isValid());
          EpochMetaData base_info(*info);
          // change nodeset
          ld_check(base_info.shards.size() == 1);
          const ShardID shard = base_info.shards[0];
          StorageSet next_storage_set;
          next_storage_set.push_back(ShardID(1 - shard.node(), shard.shard()));
          nodeset_selector_->setStorageSet(next_storage_set);
          int rv2 = epochstore_->createOrUpdateMetaData(
              logid_,
              std::make_shared<CustomEpochMetaDataUpdater>(
                  config_,
                  config_->getNodesConfiguration(),
                  nodeset_selector_,
                  EpochMetaData::Updater::Options()
                      .setUseStorageSetFormat()
                      .setProvisionIfEmpty()
                      .setUpdateIfExists()),
              [base_info, next_storage_set, this](
                  Status st2,
                  logid_t lid2,
                  std::unique_ptr<EpochMetaData> next_info,
                  std::unique_ptr<
                      EpochStoreMetaProperties> /*next_meta_props*/) {
                EXPECT_EQ(E::OK, st2);
                EXPECT_EQ(logid_, lid2);
                EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);
                ASSERT_NE(nullptr, next_info);
                EXPECT_TRUE(next_info->isValid());
                EXPECT_EQ(base_info.h.epoch.val_, next_info->h.epoch.val_);
                EXPECT_EQ(base_info.replication.toString(),
                          next_info->replication.toString());
                EXPECT_EQ(next_info->h.epoch.val(),
                          next_info->h.effective_since.val());
                EXPECT_EQ(next_storage_set, next_info->shards);
                // same nodes
                EpochMetaData prev_info(*next_info);
                int rv3 = epochstore_->createOrUpdateMetaData(
                    logid_,
                    std::make_shared<CustomEpochMetaDataUpdater>(
                        config_,
                        config_->getNodesConfiguration(),
                        nodeset_selector_,
                        EpochMetaData::Updater::Options()
                            .setUseStorageSetFormat()
                            .setUpdateIfExists()),
                    [prev_info, this](Status st3,
                                      logid_t /* logid */,
                                      std::unique_ptr<EpochMetaData> next_info2,
                                      std::unique_ptr<EpochStoreMetaProperties>
                                      /*next_meta_props2*/) {
                      EXPECT_EQ(E::UPTODATE, st3);
                      ASSERT_NE(next_info2, nullptr);
                      EXPECT_TRUE(next_info2->isValid());
                      EXPECT_EQ(prev_info, *next_info2);
                      terminateRequest();
                    },
                    MetaDataTracer());
                EXPECT_EQ(0, rv3);
              },
              MetaDataTracer());
          EXPECT_EQ(0, rv2);
        },
        MetaDataTracer());
    EXPECT_EQ(0, rv1);
    return Execution::CONTINUE;
  }

  // counts the number of completed Requests of this class
  static std::atomic<int> completedRequestCnt;

 private:
  void terminateRequest() {
    ++completedRequestCnt;
    delete this;
  }

  const logid_t logid_;
  EpochStore* const epochstore_;
  worker_id_t posted_to_; // Worker to which this Request was posted
  std::shared_ptr<Configuration> config_;
  std::shared_ptr<TestNodeSetSelector> nodeset_selector_;
};

std::atomic<int> UpdateMetaDataRequest::completedRequestCnt;

// make sure that datalog and its metadata log are stored in different znodes
TEST_P(ZookeeperEpochStoreTest, LastCleanEpochMetaDataZnodePath) {
  ASSERT_NE(std::string(LastCleanEpochZRQ::znodeNameDataLog),
            std::string(LastCleanEpochZRQ::znodeNameMetaDataLog));

  const logid_t logid(2);
  const logid_t meta_logid(MetaDataLog::metaDataLogID(logid));
  SetLastCleanEpochZRQ lce_data_zrq(
      logid,
      epoch_t(1),
      gen_tail_record(logid, LSN_INVALID, 0, OffsetMap()),
      [](Status, logid_t, epoch_t, TailRecord) {});
  SetLastCleanEpochZRQ lce_metadata_zrq(
      meta_logid,
      epoch_t(1),
      gen_tail_record(meta_logid, LSN_INVALID, 0, OffsetMap()),
      [](Status, logid_t, epoch_t, TailRecord) {});
  ASSERT_NE(lce_data_zrq.getZnodePath(epochstore->rootPath()),
            lce_metadata_zrq.getZnodePath(epochstore->rootPath()));
}

/**
 *  Post LastCleanEpochTestRequests for logs 1 and 2, then post
 *  another one for log 3, which does not exist. Wait for replies.
 */

TEST_P(ZookeeperEpochStoreTest, LastCleanEpoch) {
  int rv;

  for (logid_t logid : VALID_LOG_IDS) {
    std::unique_ptr<Request> rq =
        std::make_unique<LastCleanEpochTestRequest>(epochstore.get(), logid);

    std::unique_ptr<Request> meta_rq =
        std::make_unique<LastCleanEpochTestRequest>(
            epochstore.get(), MetaDataLog::metaDataLogID(logid));

    rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
    rv = processor->postRequest(meta_rq);
    ASSERT_EQ(0, rv);
  }

  std::unique_ptr<Request> invalid_logid_rq =
      std::make_unique<LastCleanEpochTestRequest>(
          epochstore.get(), UNPROVISIONED_LOG_ID);

  std::unique_ptr<Request> invalid_metalogid_rq =
      std::make_unique<LastCleanEpochTestRequest>(
          epochstore.get(), MetaDataLog::metaDataLogID(UNPROVISIONED_LOG_ID));

  rv = processor->postRequest(invalid_logid_rq);
  ASSERT_EQ(0, rv);

  rv = processor->postRequest(invalid_metalogid_rq);
  ASSERT_EQ(0, rv);

  // post data log rq and metadata rq for each data log
  const int n_requests_posted = (VALID_LOG_IDS.size() + 1) * 2;

  while (LastCleanEpochTestRequest::completedRequestCnt < n_requests_posted) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

/**
 *  Post NextEpochTestRequests for logs 1 and 2, then post another one
 *  for log 3, which does not exist. Wait for replies.
 *
 *  Then, do the same thing for UpdateMetaDataRequest test. Noted that
 *  we perform these two tests in a single test body and serialize them
 *  because they are operating on the same zookeeper key.
 */
TEST_P(ZookeeperEpochStoreTest, EpochMetaDataTest) {
  int rv;

  NextEpochTestRequest::completedRequestCnt.store(0);
  for (logid_t logid : VALID_LOG_IDS) {
    std::unique_ptr<Request> rq =
        std::make_unique<NextEpochTestRequest>(epochstore.get(), logid);

    rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
  }

  std::unique_ptr<Request> invalid_logid_rq =
      std::make_unique<NextEpochTestRequest>(
          epochstore.get(), UNPROVISIONED_LOG_ID);

  rv = processor->postRequest(invalid_logid_rq);
  ASSERT_EQ(0, rv);

  const int n_requests_posted = VALID_LOG_IDS.size() + 1;

  while (NextEpochTestRequest::completedRequestCnt < n_requests_posted) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // UpdateMetaData test
  for (logid_t logid : VALID_LOG_IDS) {
    std::unique_ptr<Request> rq = std::make_unique<UpdateMetaDataRequest>(
        epochstore.get(), logid, config->get());

    rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
  }

  std::unique_ptr<Request> invalid_logid_rq_update =
      std::make_unique<UpdateMetaDataRequest>(
          epochstore.get(), UNPROVISIONED_LOG_ID, config->get());

  rv = processor->postRequest(invalid_logid_rq_update);
  ASSERT_EQ(0, rv);

  while (UpdateMetaDataRequest::completedRequestCnt < n_requests_posted) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

/**
 *  Try to create a znode when the root does not exist.
 */
TEST_P(ZookeeperEpochStoreTestEmpty, NoRootNodeEpochMetaDataTest) {
  int rv;

  int n_requests_posted = 0;

  // UpdateMetaData test
  for (logid_t logid : VALID_LOG_IDS) {
    std::unique_ptr<Request> rq = std::make_unique<UpdateMetaDataRequest>(
        epochstore.get(), logid, config->get());

    rv = processor->postRequest(rq);
    ASSERT_EQ(0, rv);
    ++n_requests_posted;
  }

  while (UpdateMetaDataRequest::completedRequestCnt < n_requests_posted) {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

/**
 *  Try to create a znode when the root does not exist and creation of root
 *  znodes is disabled
 */
TEST_P(ZookeeperEpochStoreTestEmpty, NoRootNodeEpochMetaDataTestNoCreation) {
  int rv;

  int n_operations_pending = 0;

  SettingsUpdater updater;
  updater.registerSettings(processor->updateableSettings());
  updater.setFromAdminCmd("zk-create-root-znodes", "false");

  ASSERT_FALSE(processor->updateableSettings()->zk_create_root_znodes);

  Semaphore sem;

  for (logid_t logid : VALID_LOG_IDS) {
    rv = epochstore->createOrUpdateMetaData(
        logid,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            EpochMetaData::Updater::Options().setProvisionIfEmpty(),
            config->get(),
            config->get()->getNodesConfiguration()),
        [&sem](Status st,
               logid_t,
               std::unique_ptr<EpochMetaData>,
               std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
          EXPECT_EQ(E::NOTFOUND, st);
          sem.post();
        },
        MetaDataTracer());
    ++n_operations_pending;
    ASSERT_EQ(0, rv);
  }

  while (n_operations_pending--) {
    sem.wait();
  }
}

/*
 * This is a test request that verifies that the NodeID read from
 * ZookeeperEpochStore is the one of the last writer.
 */
class CheckNodeIDRequest : public Request {
 public:
  explicit CheckNodeIDRequest(ZookeeperEpochStore* epochstore,
                              logid_t lid,
                              std::shared_ptr<Configuration> cfg)
      : Request(RequestType::TEST_ZK_UPDATE_METADATA_REQUEST),
        logid_(lid),
        epochstore_(epochstore),
        config_(std::move(cfg)) {}

  Execution execute() override {
    posted_to_ = Worker::onThisThread()->idx_;
    int rv = epochstore_->readMetaData(
        logid_,
        [this](Status st,
               logid_t lid,
               std::unique_ptr<EpochMetaData> info,
               std::unique_ptr<EpochStoreMetaProperties> meta_props) {
          EXPECT_EQ(E::OK, st);
          EXPECT_EQ(logid_, lid);
          EXPECT_EQ(posted_to_, Worker::onThisThread()->idx_);
          ASSERT_NE(nullptr, info);
          EXPECT_TRUE(info->isValid());
          ASSERT_NE(nullptr, meta_props);
          ASSERT_TRUE(meta_props->last_writer_node_id.has_value());
          ASSERT_EQ(Worker::onThisThread()->processor_->getMyNodeID(),
                    meta_props->last_writer_node_id.value());
          ++completedRequestCnt;
          delete this;
        });
    EXPECT_EQ(0, rv);
    return Execution::CONTINUE;
  }

  // counts the number of completed Requests of this class
  static std::atomic<int> completedRequestCnt;

 private:
  const logid_t logid_;
  EpochStore* const epochstore_;
  worker_id_t posted_to_; // Worker to which this Request was posted
  std::shared_ptr<Configuration> config_;
};

std::atomic<int> CheckNodeIDRequest::completedRequestCnt{0};

TEST_P(ZookeeperEpochStoreTest, MetaProperties) {
  int rv;

  NextEpochTestRequest::completedRequestCnt.store(0);
  int n_requests_posted = 0;
  for (int i = 0; i <= 1; ++i) {
    EpochStore::WriteNodeID write_node_id =
        i ? EpochStore::WriteNodeID::KEEP_LAST : EpochStore::WriteNodeID::MY;
    for (logid_t logid : VALID_LOG_IDS) {
      std::unique_ptr<Request> rq = std::make_unique<NextEpochTestRequest>(
          epochstore.get(), logid, write_node_id);
      if (i == 1) {
        // On the second iteration, simulate a different node update. However,
        // it should still keep writing the previous NodeID, as now the writes
        // are being down with KEEP_LAST
        rv = other_node_processor->postRequest(rq);
      } else {
        rv = processor->postRequest(rq);
      }
      ASSERT_EQ(0, rv);
      ++n_requests_posted;
    }

    while (NextEpochTestRequest::completedRequestCnt < n_requests_posted) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    for (logid_t logid : VALID_LOG_IDS) {
      std::unique_ptr<Request> rq = std::make_unique<CheckNodeIDRequest>(
          epochstore.get(), logid, config->get());

      rv = processor->postRequest(rq);
      ASSERT_EQ(0, rv);
    }

    while (CheckNodeIDRequest::completedRequestCnt < n_requests_posted) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

TEST_P(ZookeeperEpochStoreTest, LastCleanEpochWithTailRecord) {
  Semaphore sem;

  // initial value
  // map["/logdevice/epochstore_test/logs/1/lce"] =
  //   "3559930028@18446744073709551615@0@0";
  int rv = epochstore->getLastCleanEpoch(
      logid_t(1), [&](Status st, logid_t logid, epoch_t lce, TailRecord tail) {
        ld_info("get LCE completed with %s", error_description(st));
        ASSERT_EQ(E::OK, st);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(epoch_t(3559930028), lce);
        ASSERT_TRUE(tail.isValid());
        ASSERT_EQ(LSN_INVALID, tail.header.lsn);
        ASSERT_EQ(0, tail.header.timestamp);
        ASSERT_EQ(0, tail.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH);
        ASSERT_EQ(0, tail.offsets_map_.getCounter(BYTE_OFFSET));

        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();

  // now update LCE to the new format
  const epoch_t new_lce = epoch_t(3559930033);
  const TailRecord new_tail = gen_tail_record_with_payload(
      logid_t(1),
      compose_lsn(epoch_t(3429107), esn_t(43940088)),
      224433115,
      OffsetMap({{BYTE_OFFSET, 1103428925893352348}}));

  rv = epochstore->setLastCleanEpoch(
      logid_t(1),
      new_lce,
      new_tail,
      [&](Status status, logid_t logid, epoch_t lce, TailRecord tail) {
        ASSERT_EQ(E::OK, status);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(new_lce, lce);
        ASSERT_TRUE(new_tail.sameContent(tail));
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();

  // verify the content using getLastCleanEpoch
  rv = epochstore->getLastCleanEpoch(
      logid_t(1),
      [&](Status status, logid_t logid, epoch_t lce, TailRecord tail) {
        ASSERT_EQ(E::OK, status);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(new_lce, lce);
        ASSERT_TRUE(new_tail.sameContent(tail));
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();

  // test for stale LCE update
  const TailRecord new_tail2 = gen_tail_record_with_payload(
      logid_t(1),
      compose_lsn(epoch_t(3429102), esn_t(88423423)),
      11243115,
      OffsetMap({{BYTE_OFFSET, 3289245847740}}));
  rv = epochstore->setLastCleanEpoch(
      logid_t(1),
      epoch_t(new_lce.val_ - 1),
      new_tail2,
      [&](Status status, logid_t logid, epoch_t lce, TailRecord tail) {
        ASSERT_EQ(E::STALE, status);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(new_lce, lce);
        ASSERT_FALSE(new_tail2.sameContent(tail));
        ASSERT_TRUE(new_tail.sameContent(tail));
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();

  // test for adding record with BYTE_OFFSET_INVALID, expect to preseve previous
  // byte_offset
  const epoch_t new_lce3 = epoch_t(new_lce.val_ + 10);
  const TailRecord new_tail3 = gen_tail_record_with_payload(
      logid_t(1),
      compose_lsn(epoch_t(3429112), esn_t(894623233)),
      224439115,
      OffsetMap());

  rv = epochstore->setLastCleanEpoch(
      logid_t(1),
      new_lce3,
      new_tail3,
      [&](Status status, logid_t logid, epoch_t lce, TailRecord tail) {
        ASSERT_EQ(E::OK, status);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(new_lce3, lce);
        ASSERT_EQ(new_tail3.header.lsn, tail.header.lsn);
        ASSERT_NE(new_tail3.offsets_map_, tail.offsets_map_);
        ASSERT_EQ(new_tail.offsets_map_, tail.offsets_map_);
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();

  rv = epochstore->getLastCleanEpoch(
      logid_t(1),
      [&](Status status, logid_t logid, epoch_t lce, TailRecord tail) {
        ASSERT_EQ(E::OK, status);
        ASSERT_EQ(logid_t(1), logid);
        ASSERT_EQ(new_lce3, lce);
        ASSERT_EQ(new_tail3.header.lsn, tail.header.lsn);
        ASSERT_EQ(new_tail3.header.timestamp, tail.header.timestamp);
        ASSERT_NE(new_tail3.offsets_map_, tail.offsets_map_);
        ASSERT_EQ(new_tail.offsets_map_, tail.offsets_map_);
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();
}

/**
 *  Provision a new log and make sure that epoch store creates its znodes.
 */
TEST_P(ZookeeperEpochStoreTest, ProvisionNewLog) {
  auto get_znode = [&](std::string znode) {
    Status status;
    Semaphore sem;
    auto cb = [&](int rc, std::string, zk::Stat) {
      status = ZookeeperClientBase::toStatus(rc);
      sem.post();
    };
    zkclient->getData(znode, std::move(cb));
    sem.wait();
    return status;
  };

  const std::array<std::string, 4> log4_znodes{
      "/logdevice/epochstore_test/logs/4",
      "/logdevice/epochstore_test/logs/4/sequencer",
      "/logdevice/epochstore_test/logs/4/lce",
      "/logdevice/epochstore_test/logs/4/metadatalog_lce",
  };

  // Make sure that the znodes don't exist.
  for (const auto& znode : log4_znodes) {
    const auto status = get_znode(znode);
    ASSERT_EQ(E::NOTFOUND, status)
        << "Initially znode: " << znode
        << " shouldn't exist for the test correctness";
  }

  // Modify the config to add a new log.
  {
    auto logs_cfg = config->getLocalLogsConfig()->copyLocal();
    logs_cfg->insert(boost::icl::right_open_interval<logid_t::raw_type>(4, 5),
                     "logs4",
                     logsconfig::LogAttributes().with_replicateAcross(
                         {{NodeLocationScope::NODE, 1}}));
    config->updateableLogsConfig()->update(std::move(logs_cfg));
  }

  // Now try to provision the log
  Semaphore sem;
  auto rq = FuncRequest::make(
      worker_id_t(-1), WorkerType::GENERAL, RequestType::MISC, [&]() {
        int rv = epochstore->createOrUpdateMetaData(
            logid_t(4),
            std::make_shared<EpochMetaDataUpdateToNextEpoch>(
                EpochMetaData::Updater::Options().setProvisionIfEmpty(),
                config->get(),
                config->get()->getNodesConfiguration()),
            [&sem](Status st,
                   logid_t,
                   std::unique_ptr<EpochMetaData> info,
                   std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
              EXPECT_EQ(Status::OK, st);
              EXPECT_EQ(epoch_t(2), info->h.epoch);
              sem.post();
            },
            MetaDataTracer());
        ASSERT_EQ(0, rv);
      });
  ASSERT_EQ(0, processor->blockingRequest(rq));
  sem.wait();

  // Znodes should have been created by now
  for (const auto& znode : log4_znodes) {
    const auto status = get_znode(znode);
    ASSERT_EQ(E::OK, status) << " znode: " << znode
                             << " should exist after provisioning the metadata";
  }
}

/**
 * Creates a mock zookeeper client and checks how epoch store handle different
 * failure scenarios.
 */
TEST_F(ZookeeperEpochStoreTestBase, ZookeeperFailures) {
  // The epoch metadata that the mock zookeeper client will return
  std::string epoch_metadata_payload(
      "\x31\x35\x35\x32\x31\x33\x40\x4e\x30\x3a\x31\x23"
      "\x02\x00\x00\x00\x4d\x5e\x02\x00\x4c\x5e\x02\x00"
      "\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00",
      34);

  // Runs an EpochMetadDataUpdateToNextEpoch and validates the expected
  const auto run_epoch_update = [&](Status expected_status) {
    Semaphore sem;
    auto rq = FuncRequest::make(
        worker_id_t(-1), WorkerType::GENERAL, RequestType::MISC, [&]() {
          int rv = epochstore->createOrUpdateMetaData(
              logid_t(1),
              std::make_shared<EpochMetaDataUpdateToNextEpoch>(
                  EpochMetaData::Updater::Options().setProvisionIfEmpty(),
                  config->get(),
                  config->get()->getNodesConfiguration()),
              [&sem, expected_status](
                  Status st,
                  logid_t,
                  std::unique_ptr<EpochMetaData> /* info */,
                  std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
                EXPECT_EQ(expected_status, st);
                sem.post();
              },
              MetaDataTracer());
          ASSERT_EQ(0, rv);
        });
    ASSERT_EQ(0, processor->blockingRequest(rq));
    sem.wait();
  };

  // A helper function to easily create a zookeeper callback.
  const auto build_read_cb =
      [](int rc, std::string data = "", zk::Stat stat = zk::Stat{}) {
        return [=](std::string, ZookeeperClientBase::data_callback_t* cb) {
          (*cb)(rc, data, stat);
        };
      };

  auto mock_zk = std::make_shared<MockZookeeperClient>();
  epochstore = std::make_unique<ZookeeperEpochStore>(
      TEST_CLUSTER,
      processor->getRequestExecutor(),
      mock_zk,
      config->updateableNodesConfiguration(),
      processor->updateableSettings(),
      processor->getOptionalMyNodeID(),
      processor->stats_);

  {
    // When epoch store fails auth, epoch update should fail with E::ACCESS
    EXPECT_CALL(
        *mock_zk, getData("/logdevice/epochstore_test/logs/1/sequencer", _))
        .WillOnce(testing::Invoke(build_read_cb(ZAUTHFAILED)));
    run_epoch_update(E::ACCESS);
  }

  {
    // When epoch store connection drops, epoch update should fail with
    // E::CONNFAILED
    EXPECT_CALL(
        *mock_zk, getData("/logdevice/epochstore_test/logs/1/sequencer", _))
        .WillOnce(testing::Invoke(build_read_cb(ZCONNECTIONLOSS)));
    run_epoch_update(E::CONNFAILED);
  }

  {
    // When epoch store tries to update the epoch and there's a version mismatch
    // it should return E::AGAIN.
    EXPECT_CALL(
        *mock_zk, getData("/logdevice/epochstore_test/logs/1/sequencer", _))
        .WillOnce(testing::Invoke(
            build_read_cb(ZOK, epoch_metadata_payload, zk::Stat{123})));
    EXPECT_CALL(
        *mock_zk,
        setData("/logdevice/epochstore_test/logs/1/sequencer", _, _, 123))
        .WillOnce(testing::Invoke(
            [](std::string,
               std::string,
               ZookeeperClientBase::stat_callback_t* cb,
               zk::version_t) { (*cb)(ZBADVERSION, zk::Stat{}); }));
    run_epoch_update(E::AGAIN);
  }
}

INSTANTIATE_TEST_CASE_P(ZookeeperEpochStoreTest,
                        ZookeeperEpochStoreTest,
                        ::testing::Bool(),
                        ZookeeperEpochStoreTest::testSuffixName);

// Tests that enabling double writes will work with existing znodes.
TEST_F(ZookeeperEpochStoreMigrationTest, testMigration) {
  // For the correctness of the test, double writes needs to be disabled
  // initialy.
  ASSERT_FALSE(
      processor->settings()->epoch_store_double_write_new_serialization_format);

  const logid_t logid = logid_t(1);
  const logid_t metadata_logid = MetaDataLog::metaDataLogID(logid);

  // A helper function to bump a new epoch
  const auto do_epoch_bump = [&]() {
    Semaphore sem;
    int rv = epochstore->createOrUpdateMetaData(
        logid,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            EpochMetaData::Updater::Options().setProvisionIfEmpty(),
            config->get(),
            config->getNodesConfiguration()),
        [&](Status st,
            logid_t,
            std::unique_ptr<EpochMetaData> info,
            std::unique_ptr<EpochStoreMetaProperties> /*meta_props*/) {
          EXPECT_EQ(E::OK, st);
          sem.post();
        },
        MetaDataTracer());
    EXPECT_EQ(0, rv);

    sem.wait();
  };

  // Test starts here.
  Semaphore sem;
  do_epoch_bump();
  do_epoch_bump();

  epochstore->setLastCleanEpoch(
      logid,
      epoch_t(1),
      gen_tail_record(logid, LSN_INVALID, 0, OffsetMap{}),
      [&](Status st, logid_t, epoch_t, TailRecord) {
        EXPECT_EQ(E::OK, st);
        sem.post();
      });
  sem.wait();

  epochstore->setLastCleanEpoch(
      metadata_logid,
      epoch_t(1),
      gen_tail_record(metadata_logid, LSN_INVALID, 0, OffsetMap{}),
      [&](Status st, logid_t, epoch_t, TailRecord) {
        EXPECT_EQ(E::OK, st);
        sem.post();
      });
  sem.wait();

  // Given that the double writes are disabled, the migration znode value
  // should be still empty.
  zkclient->getData("/logdevice/epochstore_test/logs/1",
                    [&](int rc, std::string value, zk::Stat) {
                      ASSERT_EQ(E::OK, ZookeeperClientBase::toStatus(rc));
                      EXPECT_EQ("", value);
                      sem.post();
                    });
  sem.wait();

  // Now enable double writes
  {
    auto settings = processor->updateableSettings();
    SettingsUpdater updater{};
    updater.registerSettings(settings);
    updater.setFromAdminCmd(
        "epoch-store-double-write-new-serialization-format", "true");
  }

  // Only an EpochMetaData change is allowed to trigger the migration. So the
  // following LCE change won't trigger the migration.
  epochstore->setLastCleanEpoch(
      logid,
      epoch_t(3),
      gen_tail_record(logid, LSN_INVALID, 0, OffsetMap{}),
      [&](Status st, logid_t, epoch_t, TailRecord) {
        EXPECT_EQ(E::OK, st);
        sem.post();
      });
  sem.wait();
  zkclient->getData("/logdevice/epochstore_test/logs/1",
                    [&](int rc, std::string value, zk::Stat) {
                      ASSERT_EQ(E::OK, ZookeeperClientBase::toStatus(rc));
                      EXPECT_EQ("", value);
                      sem.post();
                    });
  sem.wait();

  // Now trigger the migration.
  do_epoch_bump();
  do_epoch_bump();
  epochstore->setLastCleanEpoch(
      logid,
      epoch_t(4),
      gen_tail_record(logid, LSN_INVALID, 0, OffsetMap{}),
      [&](Status st, logid_t, epoch_t, TailRecord) {
        EXPECT_EQ(E::OK, st);
        sem.post();
      });
  sem.wait();
  epochstore->setLastCleanEpoch(
      metadata_logid,
      epoch_t(4),
      gen_tail_record(metadata_logid, LSN_INVALID, 0, OffsetMap{}),
      [&](Status st, logid_t, epoch_t, TailRecord) {
        EXPECT_EQ(E::OK, st);
        sem.post();
      });
  sem.wait();

  // With double writes enabled, we should be able to read a correct LogMetaData
  // from the migration znode.
  zkclient->getData(
      "/logdevice/epochstore_test/logs/1",
      [&](int rc, std::string value, zk::Stat) {
        ASSERT_EQ(E::OK, ZookeeperClientBase::toStatus(rc));
        EXPECT_NE("", value);
        auto log_metadata = LogMetaDataCodec::deserialize(std::move(value));
        ASSERT_NE(nullptr, log_metadata);
        EXPECT_EQ(epoch_t(5), log_metadata->current_epoch_metadata.h.epoch);
        EXPECT_EQ(epoch_t(4), log_metadata->data_last_clean_epoch);
        EXPECT_TRUE(log_metadata->data_tail_record.sameContent(
            gen_tail_record(logid, LSN_INVALID, 0, OffsetMap{})));
        EXPECT_EQ(epoch_t(4), log_metadata->metadata_last_clean_epoch);
        EXPECT_TRUE(log_metadata->metadata_tail_record.sameContent(
            gen_tail_record(metadata_logid, LSN_INVALID, 0, OffsetMap{})));
        EXPECT_EQ(LogMetaData::Version(4), log_metadata->version);
        sem.post();
      });
  sem.wait();
}
