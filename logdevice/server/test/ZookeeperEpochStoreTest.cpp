/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ZookeeperEpochStore.h"

#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/TestNodeSetSelector.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/SetLastCleanEpochZRQ.h"

using namespace facebook::logdevice;

#define TEST_CLUSTER "epochstore_test" // fake LD cluster name to use

static const std::vector<logid_t> VALID_LOG_IDS{logid_t(1), logid_t(2)};
static const logid_t UNPROVISIONED_LOG_ID(3); // must not be provisioned in
                                              // Zookeeper for TEST_CLUSTER

class ZookeeperEpochStoreTest : public ::testing::Test {
 public:
  /**
   * Create a Processor with TEST_CONFIG_FILE(TEST_CLUSTER.conf) config.
   * Create a ZookeeperEpochStore with the same config.
   * The config must list an up and running Zookeeper quorum and logs 1 and 2.
   */
  void SetUp() override {
    static Settings settings = create_default_settings<Settings>();

    settings.server = true; // ZookeeperEpochStore requires this

    auto cfg_in =
        Configuration::fromJsonFile(TEST_CONFIG_FILE(TEST_CLUSTER ".conf"));

    if (!cfg_in)
      return;

    config = std::make_shared<UpdateableConfig>(std::move(cfg_in));

    processor = make_test_processor(settings, config, nullptr, NodeID(0, 1));
    other_node_processor =
        make_test_processor(settings, config, nullptr, NodeID(1, 1));

    auto znodes = getPrefillZnodes();

    std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory =
        std::make_shared<ZookeeperClientInMemoryFactory>(znodes);
    epochstore = std::make_unique<ZookeeperEpochStore>(
        TEST_CLUSTER,
        processor.get(),
        config->updateableZookeeperConfig(),
        config->updateableNodesConfiguration(),
        processor->updateableSettings(),
        zookeeper_client_factory);

    dbg::assertOnData = true;
  }

  virtual ZookeeperClientInMemory::state_map_t getPrefillZnodes() {
    ZookeeperClientInMemory::state_map_t map;
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
  std::unique_ptr<ZookeeperEpochStore> epochstore;

 private:
  Alarm alarm_{std::chrono::seconds(30)};
};

class ZookeeperEpochStoreTestEmpty : public ZookeeperEpochStoreTest {
 public:
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
      std::shared_ptr<PayloadHolder>());
}

TailRecord gen_tail_record_with_payload(logid_t logid,
                                        lsn_t lsn,
                                        uint64_t timestamp,
                                        OffsetMap offsets) {
  TailRecordHeader::flags_t flags = TailRecordHeader::HAS_PAYLOAD;
  void* payload_flat = malloc(333);
  std::strncpy((char*)payload_flat, "Tail Record Test Payload", 50);
  return TailRecord(
      {logid,
       lsn,
       timestamp,
       {BYTE_OFFSET_INVALID /* deprecated use byte_offset instead */},
       0,
       {}},
      std::move(offsets),
      std::make_shared<PayloadHolder>(payload_flat, 333));
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
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
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
              std::make_shared<EpochMetaDataUpdateToNextEpoch>(),
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
            config_, config_->getNodesConfigurationFromServerConfigSource()),
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
                  config_->getNodesConfigurationFromServerConfigSource(),
                  nodeset_selector_,
                  /* use_storage_set_format */ true,
                  /* provision_if_empty */ true),
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
                        config_->getNodesConfigurationFromServerConfigSource(),
                        nodeset_selector_,
                        true),
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
TEST_F(ZookeeperEpochStoreTest, LastCleanEpochMetaDataZnodePath) {
  ASSERT_NE(std::string(LastCleanEpochZRQ::znodeNameDataLog),
            std::string(LastCleanEpochZRQ::znodeNameMetaDataLog));

  const logid_t logid(2);
  const logid_t meta_logid(MetaDataLog::metaDataLogID(logid));
  SetLastCleanEpochZRQ lce_data_zrq(
      logid,
      epoch_t(1),
      gen_tail_record(logid, LSN_INVALID, 0, OffsetMap()),
      [](Status, logid_t, epoch_t, TailRecord) {},
      epochstore.get());
  SetLastCleanEpochZRQ lce_metadata_zrq(
      meta_logid,
      epoch_t(1),
      gen_tail_record(meta_logid, LSN_INVALID, 0, OffsetMap()),
      [](Status, logid_t, epoch_t, TailRecord) {},
      epochstore.get());
  ASSERT_NE(lce_data_zrq.getZnodePath(), lce_metadata_zrq.getZnodePath());
}

/**
 *  Post LastCleanEpochTestRequests for logs 1 and 2, then post
 *  another one for log 3, which does not exist. Wait for replies.
 */

TEST_F(ZookeeperEpochStoreTest, LastCleanEpoch) {
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
TEST_F(ZookeeperEpochStoreTest, EpochMetaDataTest) {
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
TEST_F(ZookeeperEpochStoreTestEmpty, NoRootNodeEpochMetaDataTest) {
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
TEST_F(ZookeeperEpochStoreTestEmpty, NoRootNodeEpochMetaDataTestNoCreation) {
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
            config->get(),
            config->get()->getNodesConfigurationFromServerConfigSource()),
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

TEST_F(ZookeeperEpochStoreTest, QuorumChangeTest) {
  // Change the quorum to one that points somewhere else
  auto orig_zk_config = config->getZookeeperConfig();
  ASSERT_NE(orig_zk_config, nullptr);
  const std::vector<Sockaddr> new_quorum = {
      Sockaddr("2401:db00:21:3:face:0:43:0", 2183),
      Sockaddr("2401:db00:21:3:face:0:45:0", 2183),
      Sockaddr("2401:db00:2030:6103:face:0:27:0", 2183),
      Sockaddr("2401:db00:2030:6103:face:0:1d:0", 2183),
      Sockaddr("2401:db00:2030:6103:face:0:17:0", 2183)};
  config->updateableZookeeperConfig()->update(
      std::make_shared<configuration::ZookeeperConfig>(
          new_quorum, orig_zk_config->getSessionTimeout()));

  // Run a ZK request
  Semaphore sem;
  Status zk_req_st = E::OK;

  int rv = epochstore->getLastCleanEpoch(
      logid_t(1),
      [&](Status st, logid_t /*logid*/, epoch_t /*read_epoch*/, TailRecord) {
        ld_info("get LCE completed with %s", error_description(st));
        zk_req_st = st;
        sem.post();
      });
  ASSERT_EQ(0, rv);
  ld_info("Posted request");
  // Change the quorum back to the original
  config->updateableZookeeperConfig()->update(orig_zk_config);
  ld_info("Changed quorum");

  ld_info("Waiting for request to complete");
  sem.wait();
  ASSERT_EQ(E::CONNFAILED, zk_req_st);
  ld_info("Request completed");

  // Try again
  rv = epochstore->getLastCleanEpoch(
      logid_t(1),
      [&](Status st, logid_t /*logid*/, epoch_t /*read_epoch*/, TailRecord) {
        zk_req_st = st;
        sem.post();
      });

  ASSERT_EQ(0, rv);
  sem.wait();
  ASSERT_NE(E::SHUTDOWN, zk_req_st);
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
          ASSERT_TRUE(meta_props->last_writer_node_id.hasValue());
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

TEST_F(ZookeeperEpochStoreTest, MetaProperties) {
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

TEST_F(ZookeeperEpochStoreTest, LastCleanEpochWithTailRecord) {
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
