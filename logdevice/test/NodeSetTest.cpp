/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <thread>
#include <unistd.h>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/Digest.h"
#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestNodeSetSelector.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

using namespace facebook::logdevice;
using IntegrationTestUtils::markShardUnrecoverable;
using IntegrationTestUtils::requestShardRebuilding;

/*
 * @file: A set of Integration tests that involves changing epoch metadata
 * (i.e., nodeset, replication factor).
 */

const logid_t LOG_ID{1};
const int SHARD_IDX{0}; // only one shard
const logid_t META_LOGID{MetaDataLog::metaDataLogID(LOG_ID)};
const int NLOGS{1};

class NodeSetTest : public IntegrationTestBase {
 public:
  // initializes a Cluster object with the desired log config
  void init();

  void updateMetaDataInEpochStore(logid_t log = LOG_ID);

  // writes metadata of all epochs that were updated in epoch store since
  // last call to this method
  void writeMetaDataLog();

  void markMetaDataWrittenInEpochStore(logid_t log = LOG_ID);

  void waitSequencerActivation();

  // Log properties
  size_t nodes_ = 5; // 0 sequencer, 1 - 4 valid storage nodes
  size_t replication_ = 2;
  size_t extra_ = 0;
  size_t synced_ = 0;

  // current nodeset
  StorageSet storage_set_{N1, N2, N3, N4};

  // insert bridge record into emtpy epoch
  bool bridge_empty_epoch_{true};

  // Epoch metadata that was written to epoch store but not to metadata log.
  std::map<std::pair<logid_t, epoch_t>, std::unique_ptr<EpochMetaData>>
      metadata_map_;

  // keeping track of all historical metadata
  std::shared_ptr<const EpochMetaDataMap> historical_metadata_;

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;

 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();
  }
};

void NodeSetTest::init() {
  ld_check(replication_ <= nodes_);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_singleWriter(false);
  log_attrs.set_replicationFactor(replication_);
  log_attrs.set_extraCopies(extra_);
  log_attrs.set_syncedCopies(synced_);

  auto factory =
      IntegrationTestUtils::ClusterFactory()
          .enableMessageErrorInjection()
          .setLogGroupName("test_log")
          .setLogAttributes(log_attrs)
          .setNumLogs(NLOGS) // only need 1 log
          .deferStart()
          .doNotLetSequencersProvisionEpochMetaData()
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setParam("--disable-rebuilding", "false")
          .setParam("--bridge-record-in-empty-epoch",
                    bridge_empty_epoch_ ? "true" : "false")
          .setNumDBShards(1);

  cluster_ = factory.create(nodes_);

  // nodeset for the internal logs
  storage_set_ = StorageSet{N1, N2, N3, N4};
  updateMetaDataInEpochStore(configuration::InternalLogs::EVENT_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::EVENT_LOG_DELTAS);
  updateMetaDataInEpochStore(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::CONFIG_LOG_DELTAS);
  updateMetaDataInEpochStore(configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  updateMetaDataInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS);
  updateMetaDataInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS);
  updateMetaDataInEpochStore();
}

void NodeSetTest::updateMetaDataInEpochStore(logid_t log) {
  std::shared_ptr<Configuration> config = cluster_->getConfig()->get();
  auto selector = std::make_shared<TestNodeSetSelector>();
  selector->setStorageSet(storage_set_);
  auto epoch_store = cluster_->createEpochStore();

  ld_debug("Updating metadata in epoch store for log %lu.", log.val_);
  int rv =
      static_cast<FileEpochStore*>(epoch_store.get())
          ->createOrUpdateMetaData(
              log,
              std::make_shared<CustomEpochMetaDataUpdater>(
                  config,
                  config->getNodesConfigurationFromServerConfigSource(),
                  selector,
                  true,
                  true /* provision_if_empty */,
                  true /* update_if_exists */),
              [this](Status st,
                     logid_t logid,
                     std::unique_ptr<EpochMetaData> info,
                     std::unique_ptr<EpochStoreMetaProperties>) {
                if (st == E::UPTODATE) {
                  // metadata not changed
                  return;
                }
                ASSERT_EQ(E::OK, st);
                ASSERT_NE(info, nullptr);
                ASSERT_TRUE(info->isValid());
                ASSERT_EQ(info->h.effective_since, info->h.epoch);
                epoch_t epoch = info->h.epoch;
                auto info_copy = *info;
                auto result = metadata_map_.insert(std::make_pair(
                    std::make_pair(logid, epoch), std::move(info)));
                ASSERT_TRUE(result.second);

                // update historical metadata for LOG_ID
                if (logid == LOG_ID) {
                  if (historical_metadata_ == nullptr) {
                    EpochMetaDataMap::Map hmap;
                    hmap[info_copy.h.effective_since] = info_copy;
                    historical_metadata_ = EpochMetaDataMap::create(
                        std::make_shared<const EpochMetaDataMap::Map>(hmap),
                        info_copy.h.epoch);
                  } else {
                    historical_metadata_ = historical_metadata_->withNewEntry(
                        info_copy, info_copy.h.epoch);
                  }

                  ASSERT_NE(nullptr, historical_metadata_);
                }
              },
              MetaDataTracer());
  ASSERT_EQ(0, rv);
}

void NodeSetTest::writeMetaDataLog() {
  auto client_orig = cluster_->createClient();
  // cast the Client object back to ClientImpl and enable metadata log
  // writes
  auto client = std::dynamic_pointer_cast<ClientImpl>(client_orig);
  ASSERT_NE(nullptr, client);
  client->allowWriteMetaDataLog();

  auto count_activations = [&]() {
    size_t count = 0;
    for (size_t i = 0; i < nodes_; ++i) {
      auto config = cluster_->getConfig()->get()->serverConfig();
      if (!config->getNode(i) || !config->getNode(i)->isSequencingEnabled()) {
        continue;
      }
      auto stats = cluster_->getNode(i).stats();
      count += stats["sequencer_activations"];
    }
    return count;
  };

  // write all pending metadata to metadata log, wait until it can be read
  while (!metadata_map_.empty()) {
    logid_t log = metadata_map_.begin()->first.first;
    logid_t meta_log = MetaDataLog::metaDataLogID(log);
    std::unique_ptr<EpochMetaData> metadata =
        std::move(metadata_map_.begin()->second);
    metadata_map_.erase(metadata_map_.begin());

    Semaphore sem;
    std::string data = metadata->toStringPayload();
    ASSERT_FALSE(data.empty());

    size_t current_activations = count_activations();
    ld_debug("Writing metadata for log %lu", log.val_);
    wait_until([&]() {
      lsn_t lsn =
          client->appendSync(meta_log, Payload(data.data(), data.size()));
      if (lsn == LSN_INVALID) {
        EXPECT_EQ(E::SEQNOBUFS, err);
        return false;
      }
      return true;
    });

    ld_debug("Reading metadata for log %lu", log.val_);
    auto reader = client->createAsyncReader(-1);
    epoch_t expected = metadata->h.epoch;
    reader->setRecordCallback(
        [expected, &sem, this, log](std::unique_ptr<DataRecord>& record) {
          EpochMetaData info;
          int rv = info.fromPayload(
              record->payload,
              log,
              *cluster_->getConfig()
                   ->getServerConfig()
                   ->getNodesConfigurationFromServerConfigSource());
          EXPECT_EQ(0, rv);
          EXPECT_EQ(info.h.epoch, info.h.effective_since);
          if (info.h.epoch == expected) {
            sem.post();
          }
          return true;
        });
    reader->startReading(meta_log, LSN_OLDEST);
    sem.wait();

    Semaphore stop_sem;
    reader->stopReading(meta_log, [&]() { stop_sem.post(); });
    stop_sem.wait();

    // wait for the data sequencer reactivate to a new epoch
    // so that future writes can always go to the updated nodeset
    wait_until([&]() { return count_activations() > current_activations; });
  }
}

void NodeSetTest::markMetaDataWrittenInEpochStore(logid_t log) {
  auto epoch_store = cluster_->createEpochStore();
  ld_debug("Marking metadata in epoch store for log %lu as written "
           "in the metadata log.",
           log.val_);
  Semaphore sem;
  int rv = static_cast<FileEpochStore*>(epoch_store.get())
               ->createOrUpdateMetaData(
                   log,
                   std::make_shared<EpochMetaDataUpdateToWritten>(),
                   [this, &sem](Status st,
                                logid_t /*logid*/,
                                std::unique_ptr<EpochMetaData> info,
                                std::unique_ptr<EpochStoreMetaProperties>) {
                     if (st == E::UPTODATE) {
                       // metadata not changed
                       sem.post();
                       return;
                     }
                     ASSERT_EQ(E::OK, st);
                     ASSERT_NE(info, nullptr);
                     ASSERT_TRUE(info->isValid());
                     ASSERT_TRUE(info->writtenInMetaDataLog());
                     sem.post();
                   },
                   MetaDataTracer());
  ASSERT_EQ(0, rv);
  sem.wait();
}

#define START_CLUSTER()                \
  do {                                 \
    init();                            \
    ASSERT_EQ(0, cluster_->start());   \
    writeMetaDataLog();                \
    markMetaDataWrittenInEpochStore(); \
  } while (0)

// change nodeset only
#define CHANGE_STORAGE_SET(...)             \
  do {                                      \
    storage_set_ = StorageSet{__VA_ARGS__}; \
    updateMetaDataInEpochStore();           \
    writeMetaDataLog();                     \
    markMetaDataWrittenInEpochStore();      \
  } while (0)

// change nodeset and replication factor
#define CHANGE_STORAGE_SET_REPLICATION(replication, ...)                      \
  do {                                                                        \
    replication_ = (replication);                                             \
    auto logs_config =                                                        \
        cluster_->getConfig()->getLocalLogsConfig()->copyLocal();             \
    auto iter = logs_config->getLogMap().find(LOG_ID.val_);                   \
    ASSERT_NE(logs_config->getLogMap().end(), iter);                          \
    const logsconfig::LogGroupNode* log = iter->second.log_group.get();       \
    logsconfig::LogGroupNode new_node = log->withLogAttributes(               \
        log->attrs().with_replicationFactor(replication));                    \
    bool result = logs_config->replaceLogGroup(                               \
        iter->second.getFullyQualifiedName(), new_node);                      \
    ld_check(result);                                                         \
    cluster_->writeLogsConfig(logs_config.get());                             \
    cluster_->waitForConfigUpdate();                                          \
    auto attrs =                                                              \
        cluster_->getConfig()->get()->getLogGroupByIDShared(LOG_ID)->attrs(); \
    ASSERT_EQ(replication_, attrs.replicationFactor().value());               \
    storage_set_ = StorageSet{__VA_ARGS__};                                   \
    updateMetaDataInEpochStore();                                             \
    writeMetaDataLog();                                                       \
    markMetaDataWrittenInEpochStore();                                        \
  } while (0)

#define ASSERT_READ_RESULT(lsn_map, records)         \
  do {                                               \
    ASSERT_EQ(lsn_map.size(), records.size());       \
    auto it = lsn_map.cbegin();                      \
    for (int i = 0; i < records.size(); ++i, ++it) { \
      const DataRecord& r = *records[i];             \
      EXPECT_EQ(LOG_ID, r.logid);                    \
      ASSERT_NE(lsn_map.cend(), it);                 \
      EXPECT_EQ(it->first, r.attrs.lsn);             \
      const Payload& p = r.payload;                  \
      EXPECT_NE(nullptr, p.data());                  \
      EXPECT_EQ(it->second.size(), p.size());        \
      EXPECT_EQ(it->second, p.toString());           \
    }                                                \
    ASSERT_EQ(lsn_map.cend(), it);                   \
  } while (0)

std::shared_ptr<const EpochMetaDataMap>
clearUpdateTimestampFromEpochMetaDataMap(const EpochMetaDataMap* map_with_ts) {
  auto map = *map_with_ts->getMetaDataMap();
  std::for_each(map.begin(),
                map.end(),
                [](std::pair<const epoch_t, EpochMetaData>& record) {
                  record.second.epoch_incremented_at = RecordTimestamp();
                });
  return EpochMetaDataMap::create(
      std::make_shared<const EpochMetaDataMap::Map>(std::move(map)),
      map_with_ts->getEffectiveUntil());
}

#define CHECK_HISTORICAL_METADATA(_client_, _until_)                        \
  do {                                                                      \
    auto* client_impl = static_cast<ClientImpl*>((_client_).get());         \
    auto result = client_impl->getHistoricalMetaDataSync(LOG_ID);           \
    ASSERT_NE(nullptr, result);                                             \
    auto expected = historical_metadata_->withNewEffectiveUntil((_until_)); \
    auto result_no_ts =                                                     \
        clearUpdateTimestampFromEpochMetaDataMap(result.get());             \
    auto expected_no_ts =                                                   \
        clearUpdateTimestampFromEpochMetaDataMap(expected.get());           \
    ld_info("Historical metadata: expecting: %s, got %s.",                  \
            expected_no_ts->toString().c_str(),                             \
            result_no_ts->toString().c_str());                              \
    ASSERT_EQ(*expected_no_ts, *result_no_ts);                              \
  } while (0)

static lsn_t write_test_records(std::shared_ptr<Client> client,
                                logid_t logid,
                                size_t num_records,
                                std::map<lsn_t, std::string>& lsn_map) {
  lsn_t first_lsn = LSN_INVALID;
  static size_t counter = 0;
  for (size_t i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(++counter));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn)
        << "Append failed (E::" << error_name(err) << ")";
    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }
    EXPECT_EQ(lsn_map.end(), lsn_map.find(lsn));
    lsn_map[lsn] = data;
  }
  return first_lsn;
}

TEST_F(NodeSetTest, BasicMetaDataUpdate) {
  START_CLUSTER();
  CHANGE_STORAGE_SET(N3, N4);
  CHANGE_STORAGE_SET(N1, N2, N3);
  // attempt to change using the same one
  CHANGE_STORAGE_SET(N1, N2, N3);
  // change replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N1, N2, N3);
  // change nodeset and replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N2, N3);
}

TEST_F(NodeSetTest, ReadWithNodeSet) {
  bridge_empty_epoch_ = false;
  START_CLUSTER();
  std::shared_ptr<Client> client = cluster_->createClient();

  // a map <lsn, data> for appended records
  std::map<lsn_t, std::string> lsn_map;
  const size_t records_first = 10, records_second = 14, records_third = 19;

  lsn_t first_lsn = write_test_records(client, LOG_ID, 10, lsn_map);
  // change to a new nodeset with disjoint nodes
  CHANGE_STORAGE_SET(N1, N4);
  lsn_t second_lsn = write_test_records(client, LOG_ID, 14, lsn_map);
  // change to a new nodeset with common nodes
  CHANGE_STORAGE_SET(N1, N3, N4);
  lsn_t third_lsn = write_test_records(client, LOG_ID, 19, lsn_map);

  // epoch bump must happen
  ASSERT_GT(lsn_to_epoch(second_lsn), lsn_to_epoch(first_lsn));
  ASSERT_GT(lsn_to_epoch(third_lsn), lsn_to_epoch(second_lsn));

  // wait recovery to finish
  cluster_->waitForRecovery();

  // Test reading these records
  const size_t total_records = records_first + records_second + records_third;
  const size_t max_logs = 1;
  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  ssize_t nread;
  std::unique_ptr<Reader> reader(client->createReader(max_logs));

  // now start reading from LSN 1
  reader->startReading(LOG_ID, 1);

  // expect a bridge gap to skip epoch 0
  nread = reader->read(total_records, &records, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::BRIDGE, gap.type);
  ASSERT_EQ(1, gap.lo);

  auto prev_hi = gap.hi;

  nread = reader->read(total_records, &records, &gap);
  // expect another gap
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_GT(gap.lo, prev_hi);
  ASSERT_EQ(first_lsn - 1, gap.hi);
  // reading the first epoch
  nread = reader->read(total_records, &records, &gap);
  EXPECT_EQ(records_first, nread);
  // try to read once more record there should be epoch bump gaps
  nread = reader->read(total_records, &records, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::BRIDGE, gap.type);
  ASSERT_EQ(lsn_to_epoch(first_lsn), lsn_to_epoch(gap.lo));
  ASSERT_EQ(lsn_to_epoch(first_lsn), lsn_to_epoch(gap.hi));
  ASSERT_EQ(ESN_MAX, lsn_to_esn(gap.hi));
  nread = reader->read(total_records, &records, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::BRIDGE, gap.type);

  ASSERT_EQ(lsn_to_epoch(second_lsn), lsn_to_epoch(gap.lo));
  ASSERT_EQ(ESN_INVALID, lsn_to_esn(gap.lo));
  ASSERT_EQ(second_lsn - 1, gap.hi);

  // reading the second epoch
  nread = reader->read(total_records, &records, &gap);
  EXPECT_EQ(records_second, nread);
  // try to read one more record there should be an epoch bump gap to
  // the end of epoch
  nread = reader->read(total_records, &records, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::BRIDGE, gap.type);
  ASSERT_EQ(lsn_to_epoch(second_lsn), lsn_to_epoch(gap.lo));
  ASSERT_EQ(compose_lsn(lsn_to_epoch(second_lsn), ESN_MAX), gap.hi);
  // keep reading there is another bridge gap at the beginning of epoch
  nread = reader->read(total_records, &records, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(GapType::BRIDGE, gap.type);
  ASSERT_EQ(compose_lsn(lsn_to_epoch(third_lsn), ESN_INVALID), gap.lo);
  ASSERT_EQ(third_lsn - 1, gap.hi);
  // reading the third epoch
  nread = reader->read(records_third, &records, &gap);
  EXPECT_EQ(records_third, nread);

  int rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);

  // verify the results
  ASSERT_READ_RESULT(lsn_map, records);
}

TEST_F(NodeSetTest, AsyncReadWithNodeSet) {
  START_CLUSTER();
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  const size_t records_first = 10, records_second = 14, records_third = 19;

  lsn_t first_lsn = write_test_records(client, LOG_ID, 10, lsn_map);
  CHANGE_STORAGE_SET(N1, N4);
  lsn_t second_lsn = write_test_records(client, LOG_ID, 14, lsn_map);
  CHANGE_STORAGE_SET(N1, N3, N4);
  lsn_t third_lsn = write_test_records(client, LOG_ID, 19, lsn_map);

  ASSERT_GT(lsn_to_epoch(second_lsn), lsn_to_epoch(first_lsn));
  ASSERT_GT(lsn_to_epoch(third_lsn), lsn_to_epoch(second_lsn));

  cluster_->waitForRecovery();

  std::vector<std::unique_ptr<DataRecord>> records;
  auto it = lsn_map.cbegin();
  Semaphore sem;
  auto gap_cb = [&](const GapRecord& gap) {
    EXPECT_EQ(GapType::BRIDGE, gap.type);
    EXPECT_NE(lsn_map.cend(), it);

    // expect two bridge gaps for each epoch with record:
    if (lsn_to_esn(gap.lo) == ESN_INVALID) {
      EXPECT_EQ(lsn_to_epoch(it->first), lsn_to_epoch(gap.lo));
      EXPECT_EQ(it->first - 1, gap.hi);
    } else {
      EXPECT_EQ(std::prev(it, 1)->first + 1, gap.lo);
      EXPECT_EQ(compose_lsn(lsn_to_epoch(gap.lo), ESN_MAX), gap.hi);
    }

    return true;
  };

  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    records.push_back(std::move(r));
    if (++it == lsn_map.cend()) {
      sem.post();
    }
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(LOG_ID, first_lsn);
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(LOG_ID, [&]() { stop_sem.post(); });
  ASSERT_EQ(0, rv);
  stop_sem.wait();

  // verify the results
  ASSERT_READ_RESULT(lsn_map, records);
}

// test class for a separate reading thread, it runs until it receives
// `total_records' records.
class TestReaderThread {
 public:
  TestReaderThread(Reader* reader,
                   std::vector<std::unique_ptr<DataRecord>>* records,
                   Semaphore* sem,
                   size_t total_records)
      : reader_(reader),
        records_(records),
        sem_(sem),
        total_records_(total_records) {}

  void operator()() {
    GapRecord gap;
    ssize_t nread;
    size_t total_read = 0;
    do {
      int remaining = total_records_ - total_read;
      nread = reader_->read(remaining, records_, &gap);
      if (nread < 0) {
        ASSERT_EQ(E::GAP, err);
      } else {
        total_read += nread;
      }
    } while (total_read < total_records_);
    sem_->post();
  }

 private:
  Reader* const reader_;
  std::vector<std::unique_ptr<DataRecord>>* records_;
  Semaphore* sem_;
  size_t total_records_;
};

// test a reader tailing read the log while we are writing to it
TEST_F(NodeSetTest, TailingReader) {
  START_CLUSTER();
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  Semaphore sem;
  const size_t total_records = 60;

  // start a reader tailing the log in another thread
  std::vector<std::unique_ptr<DataRecord>> records;
  std::unique_ptr<Reader> reader(client->createReader(1));
  reader->startReading(LOG_ID, 0);

  TestReaderThread read_thread(reader.get(), &records, &sem, total_records);
  std::thread reader_thread(std::ref(read_thread));

  // let reader thread starts first
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // write 60 records in 6 different epochs with different metadata
  write_test_records(client, LOG_ID, 10, lsn_map);
  CHANGE_STORAGE_SET(N1, N4);
  write_test_records(client, LOG_ID, 10, lsn_map);
  CHANGE_STORAGE_SET(N1, N3, N4);
  write_test_records(client, LOG_ID, 10, lsn_map);
  // change replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/3, /*nodeset=*/N1, N3, N4);
  write_test_records(client, LOG_ID, 10, lsn_map);
  // change nodeset and replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N2);
  write_test_records(client, LOG_ID, 10, lsn_map);
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N2, N3);
  write_test_records(client, LOG_ID, 10, lsn_map);

  ASSERT_EQ(total_records, lsn_map.size());

  // wait for reader finally got all records
  sem.wait();
  reader_thread.join();

  int rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);

  // verify the results
  ASSERT_READ_RESULT(lsn_map, records);
  // the first record and the last record should span across at least 6 epochs
  epoch_t start_epoch = lsn_to_epoch(records.front()->attrs.lsn);
  epoch_t end_epoch = lsn_to_epoch(records.back()->attrs.lsn);
  ASSERT_LE(start_epoch.val_ + 5, end_epoch.val_);
}

// Test when a reader starts reading from an epoch whose metadata (and data)
// is not yet released
TEST_F(NodeSetTest, TailingFutureEpoch) {
  START_CLUSTER();
  // initial nodeset is {2, 3}, initial epoch is 2
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  Semaphore sem;
  const size_t total_records = 20;

  // start a reader tailing the log in another thread
  std::vector<std::unique_ptr<DataRecord>> records;
  std::unique_ptr<Reader> reader(client->createReader(1));
  // start reading from epoch 3
  reader->startReading(LOG_ID, compose_lsn(epoch_t(3), esn_t(0)));

  TestReaderThread read_thread(reader.get(), &records, &sem, total_records);
  std::thread reader_thread(std::ref(read_thread));

  // let reader thread starts first
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // write 30 records in 3 different epochs with different metadata
  write_test_records(client, LOG_ID, 10, lsn_map); // epoch 2
  // don't care records in epoch 2
  lsn_map.clear();

  // real nodeset for epoch 3 is actualy {2, 4}, if the reader reading
  // epoch 3 from {2, 3}, it won't get all records
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N2, N4);
  write_test_records(client, LOG_ID, 10, lsn_map); // epoch 3
  CHANGE_STORAGE_SET(N1, N3, N4);
  write_test_records(client, LOG_ID, 10, lsn_map); // epoch 4

  ASSERT_EQ(total_records, lsn_map.size());

  // wait for reader finally got all records
  sem.wait();
  reader_thread.join();

  int rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);

  // verify the results
  ASSERT_READ_RESULT(lsn_map, records);
  // the first record and the last record should span across 2 epochs
  epoch_t start_epoch = lsn_to_epoch(records.front()->attrs.lsn);
  epoch_t end_epoch = lsn_to_epoch(records.back()->attrs.lsn);
  ASSERT_EQ(start_epoch.val_ + 1, end_epoch.val_);
}

// Sequencer must defer releases of records in data log whose epoch >= e,
// which is the effective_since of the epoch metadata used to activate the
// current epoch, until it confirms that it can read epoch metadata.
TEST_F(NodeSetTest, DeferReleasesUntilMetaDataRead) {
  // initial nodeset is {2, 3}
  START_CLUSTER();
  cluster_->waitForRecovery();
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  lsn_t first_lsn = write_test_records(client, LOG_ID, 10, lsn_map);

  // Wait until these records are readable
  auto reader = client->createReader(1);
  int rv = reader->startReading(LOG_ID, first_lsn);
  ASSERT_EQ(0, rv);
  // reader should be able to read epoch of first_lsn
  read_records_no_gaps(*reader, 10);
  reader->stopReading(LOG_ID);

  // choose a new nodeset which includes the old one
  storage_set_ = StorageSet{N1, N2, N3};
  // write the new epoch metadata only to epochstore but not metadata log
  updateMetaDataInEpochStore();
  // restart the sequencer to pick up the new epoch metadata, write some data
  cluster_->replace(0);
  lsn_t second_lsn = write_test_records(client, LOG_ID, 10, lsn_map);

  // restart the sequencer again and write some data in the third epoch
  cluster_->replace(0);
  lsn_t third_lsn = write_test_records(client, LOG_ID, 10, lsn_map);

  ASSERT_GT(lsn_to_epoch(second_lsn), lsn_to_epoch(first_lsn));
  ASSERT_GT(lsn_to_epoch(third_lsn), lsn_to_epoch(second_lsn));

  // give some time to the sequencer to attempt recovery (we expect it never
  // completes)
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(800));

  rv = reader->startReading(LOG_ID, first_lsn);
  ASSERT_EQ(0, rv);
  // reader should be able to read epoch of first_lsn
  read_records_no_gaps(*reader, 10);
  reader->stopReading(LOG_ID);
  rv = reader->startReading(LOG_ID, second_lsn);
  ASSERT_EQ(0, rv);
  // reader should not be able to read epoch of second_lsn or third_lsn
  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  int nread;
  reader->setTimeout(std::chrono::seconds(1));
  nread = reader->read(10, &data, &gap);
  ASSERT_EQ(0, nread);
  reader->stopReading(LOG_ID);
  rv = reader->startReading(LOG_ID, third_lsn);
  ASSERT_EQ(0, rv);
  nread = reader->read(10, &data, &gap);
  ASSERT_EQ(0, nread);

  // write epoch metadata to the metadata log
  writeMetaDataLog();
  markMetaDataWrittenInEpochStore();

  rv = reader->startReading(LOG_ID, first_lsn);
  ASSERT_EQ(0, rv);
  // now that we should be able to read everything
  data.clear();
  read_records_swallow_gaps(*reader, lsn_map.size(), &data);

  // verify the results
  ASSERT_READ_RESULT(lsn_map, data);
}

// test rebuilding with multiple epochs
TEST_F(NodeSetTest, RebuildMultipleEpochs) {
  START_CLUSTER();
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  const size_t records_per_epoch = 10;

  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N1, N3, N4);
  write_test_records(client, LOG_ID, records_per_epoch, lsn_map); // epoch 3
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N1, N2, N4);
  write_test_records(client, LOG_ID, records_per_epoch, lsn_map); // epoch 4
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N1, N2, N3);
  write_test_records(client, LOG_ID, records_per_epoch, lsn_map); // epoch 5
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N2, N3, N4);
  write_test_records(client, LOG_ID, records_per_epoch, lsn_map); // epoch 6

  ASSERT_EQ(40, lsn_map.size());

  // do a rolling replacement of storage nodes
  for (int i = 0; i < nodes_; ++i) {
    if (cluster_->getConfig()
            ->get()
            ->serverConfig()
            ->getNode(i)
            ->isReadableStorageNode()) {
      ASSERT_EQ(0, cluster_->replace(i));
      ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, i, SHARD_IDX));
      // Wait until all shards finish rebuilding.
      cluster_->getNode(i).waitUntilAllShardsFullyAuthoritative(client);
    }
  }

  cluster_->waitForRecovery();
  std::vector<std::unique_ptr<DataRecord>> records;
  auto it = lsn_map.cbegin();
  Semaphore sem;
  auto gap_cb = [&](const GapRecord& gap) {
    EXPECT_EQ(GapType::BRIDGE, gap.type);
    EXPECT_NE(lsn_map.cend(), it);

    // expect two bridge gaps for each epoch with record:
    if (lsn_to_esn(gap.lo) == ESN_INVALID) {
      EXPECT_EQ(lsn_to_epoch(it->first), lsn_to_epoch(gap.lo));
      EXPECT_EQ(it->first - 1, gap.hi);
    } else {
      EXPECT_EQ(std::prev(it, 1)->first + 1, gap.lo);

      ld_check(std::prev(it, 1)->first + 1 == gap.lo);
      EXPECT_EQ(compose_lsn(lsn_to_epoch(gap.lo), ESN_MAX), gap.hi);
    }

    // Unblock the main thread if any of the checks failed
    if (HasFailure()) {
      sem.post();
    }
    return true;
  };

  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    records.push_back(std::move(r));
    if (++it == lsn_map.cend()) {
      sem.post();
    }
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(LOG_ID, lsn_map.begin()->first);
  sem.wait();
  if (HasFailure()) {
    return;
  }

  Semaphore stop_sem;
  int rv = reader->stopReading(LOG_ID, [&]() { stop_sem.post(); });
  ASSERT_EQ(0, rv);
  stop_sem.wait();
  // verify the results
  ASSERT_READ_RESULT(lsn_map, records);
}

TEST_F(NodeSetTest, EpochMetaDataCache) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_singleWriter(false);
  log_attrs.set_replicationFactor(replication_);
  log_attrs.set_extraCopies(extra_);
  log_attrs.set_syncedCopies(synced_);

  // we have a specific setup:
  // node 0: sequencer
  // node 1-4: storage nodes that store data logs
  // node 5: metadata storage node
  size_t nodes_ = 6;
  // current nodeset
  storage_set_ = StorageSet{N2, N3};

  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{5}, /*replication=*/1, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto factory = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     .setMetaDataLogsConfig(meta_config)
                     .setNumLogs(NLOGS) // only need 1 log
                     .deferStart()
                     .doNotLetSequencersProvisionEpochMetaData()
                     .setNumDBShards(1);

  ld_info("Updating epoch store.");
  cluster_ = factory.create(nodes_);
  updateMetaDataInEpochStore(configuration::InternalLogs::EVENT_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::EVENT_LOG_DELTAS);
  updateMetaDataInEpochStore(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::CONFIG_LOG_DELTAS);
  updateMetaDataInEpochStore(configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS);
  updateMetaDataInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS);
  updateMetaDataInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS);
  markMetaDataWrittenInEpochStore(
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS);
  updateMetaDataInEpochStore();

  ld_info("Starting cluster.");
  ASSERT_EQ(0, cluster_->start());

  ld_info("Writing metadata log.");
  writeMetaDataLog();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("num-workers", 10));
  ASSERT_EQ(0, client_settings->set("client-epoch-metadata-cache-size", 10));
  auto client =
      cluster_->createClient(this->testTimeout(), std::move(client_settings));

  std::map<lsn_t, std::string> lsn_map;
  std::set<epoch_t> all_epochs;

  // write 30 records in 3 different epochs with different metadata
  ld_info("Writing records.");
  lsn_t lsn = write_test_records(client, LOG_ID, 10, lsn_map); // epoch 2
  all_epochs.insert(lsn_to_epoch(lsn));
  CHANGE_STORAGE_SET(N1, N4);
  lsn = write_test_records(client, LOG_ID, 10, lsn_map); // epoch 3
  all_epochs.insert(lsn_to_epoch(lsn));
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N2, N4);
  lsn = write_test_records(client, LOG_ID, 10, lsn_map); // epoch 4
  all_epochs.insert(lsn_to_epoch(lsn));
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N1, N3);
  lsn = write_test_records(client, LOG_ID, 10, lsn_map); // epoch 5
  all_epochs.insert(lsn_to_epoch(lsn));
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/3, /*nodeset=*/N1, N2, N3, N4);
  lsn = write_test_records(client, LOG_ID, 10, lsn_map); // epoch 6
  all_epochs.insert(lsn_to_epoch(lsn));

  ld_info("Waiting for recovery.");
  cluster_->waitForRecovery();
  Semaphore sem;

  auto read_thread_func = [client, &sem, &all_epochs, this] {
    auto reader = client->createReader(1);
    std::vector<epoch_t> epochs(all_epochs.begin(), all_epochs.end());
    std::shuffle(epochs.begin(), epochs.end(), folly::ThreadLocalPRNG());
    for (epoch_t e : epochs) {
      int rv = reader->startReading(LOG_ID, compose_lsn(e, esn_t(1)));
      ASSERT_EQ(0, rv);
      std::vector<std::unique_ptr<DataRecord>> data;
      GapRecord gap;
      size_t records_read = 0;
      while (records_read < 10) {
        ssize_t nread = reader->read(10 - records_read, &data, &gap);
        if (nread < 0) {
          ASSERT_EQ(E::GAP, err);
        } else {
          records_read += nread;
        }
      }
      reader->stopReading(LOG_ID);
    }
    sem.post();
  };

  // read with metadata storage node, reading should populate the cache
  ld_info("Reading with metadata storage node.");
  std::vector<std::thread> reader_threads;
  for (int i = 0; i < 10; ++i) {
    reader_threads.emplace_back(read_thread_func);
    sem.wait();
  }
  for (std::thread& t : reader_threads) {
    t.join();
  }

  reader_threads.clear();

  // disable the metadata storage node, this time we should still finish
  // reading since we should get all metadata from the cache
  ld_info("Stopping metadata storage node.");
  cluster_->getNode(5).suspend();
  ld_info("Reading without metadata storage node.");
  for (int i = 0; i < 10; ++i) {
    reader_threads.emplace_back(read_thread_func);
    sem.wait();
  }
  for (std::thread& t : reader_threads) {
    t.join();
  }
}

// check the functionality of getting historical epoch metadata from the
// sequencer
TEST_F(NodeSetTest, GettingHistoricalMetaData) {
  START_CLUSTER();
  std::shared_ptr<Client> client = cluster_->createClient();
  std::map<lsn_t, std::string> lsn_map;
  lsn_t first_lsn = write_test_records(client, LOG_ID, 1, lsn_map);
  ASSERT_NE(LSN_INVALID, first_lsn);
  epoch_t first = lsn_to_epoch(first_lsn);

  CHECK_HISTORICAL_METADATA(client, first);

  CHANGE_STORAGE_SET(N3, N4);
  CHECK_HISTORICAL_METADATA(client, epoch_t(first.val_ + 1));

  CHANGE_STORAGE_SET(N1, N2, N3);
  CHECK_HISTORICAL_METADATA(client, epoch_t(first.val_ + 2));

  // attempt to change using the same one
  CHANGE_STORAGE_SET(N1, N2, N3);
  CHECK_HISTORICAL_METADATA(client, epoch_t(first.val_ + 2));

  // change replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/1, /*nodeset=*/N1, N2, N3);
  CHECK_HISTORICAL_METADATA(client, epoch_t(first.val_ + 3));

  // change nodeset and replication factor
  CHANGE_STORAGE_SET_REPLICATION(/*replication=*/2, /*nodeset=*/N2, N3);
  CHECK_HISTORICAL_METADATA(client, epoch_t(first.val_ + 4));
}
