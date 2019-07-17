/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"
#include "logdevice/server/rebuilding/RebuildingReadStorageTaskV2.h"

using namespace facebook::logdevice;

static const std::chrono::milliseconds MINUTE = std::chrono::minutes(1);
static const std::chrono::milliseconds PARTITION_DURATION = MINUTE * 15;
static const RecordTimestamp BASE_TIME{
    TemporaryPartitionedStore::baseTime().toMilliseconds()};
static const std::string BIG_PAYLOAD(10000, '.');

static const ShardID N0{0, 0};
static const ShardID N1{1, 0};
static const ShardID N2{2, 0};
static const ShardID N3{3, 0};
static const ShardID N4{4, 0};
static const ShardID N5{5, 0};
static const ShardID N6{6, 0};
static const ShardID N7{7, 0};
static const ShardID N8{8, 0};
static const ShardID N9{9, 0};

// Ain't nobody got time to spell "compose_lsn(epoch_t(1), esn_t(1))".
lsn_t mklsn(epoch_t::raw_type epoch, esn_t::raw_type esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

class RebuildingReadStorageTaskTest : public ::testing::Test {
 public:
  class MockRebuildingReadStorageTaskV2 : public RebuildingReadStorageTaskV2 {
   public:
    MockRebuildingReadStorageTaskV2(RebuildingReadStorageTaskTest* test,
                                    std::weak_ptr<Context> context)
        : RebuildingReadStorageTaskV2(context), test(test) {}

    RebuildingReadStorageTaskTest* test;

   protected:
    UpdateableSettings<Settings> getSettings() override {
      return test->settings;
    }
    std::shared_ptr<UpdateableConfig> getConfig() override {
      return test->config;
    }
    folly::Optional<NodeID> getMyNodeID() override {
      return folly::none;
    }
    std::unique_ptr<LocalLogStore::AllLogsIterator> createIterator(
        const LocalLogStore::ReadOptions& opts,
        const std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>& logs)
        override {
      return test->store->readAllLogs(opts, logs);
    }
    bool fetchTrimPoints(Context* context) override {
      return true;
    }
    void updateTrimPoint(logid_t log,
                         Context* context,
                         Context::LogState* log_state) override {}
    StatsHolder* getStats() override {
      return &test->stats;
    }
  };

  RebuildingReadStorageTaskTest() {
    // Create nodes and logs config. This part is super boilerplaty, just skip
    // the code and read comments.

    configuration::Nodes nodes;
    // Nodes 0..9.
    NodeSetTestUtil::addNodes(&nodes, /* nodes */ 10, /* shards */ 2, "....");

    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logsconfig::LogAttributes log_attrs;
    // Logs 1..10, replication factor 3.
    log_attrs.set_backlogDuration(std::chrono::seconds(3600 * 24 * 1));
    log_attrs.set_replicationFactor(3);
    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, 10),
        "lg1",
        log_attrs);
    // Event log, replication factor 3.
    log_attrs.set_backlogDuration(folly::none);
    log_attrs.set_replicationFactor(3);
    configuration::InternalLogs internal_logs;
    ld_check(internal_logs.insert("event_log_deltas", log_attrs));
    logs_config->setInternalLogsConfig(internal_logs);
    logs_config->markAsFullyLoaded();
    // Metadata logs, nodeset 5..9, replication factor 3.
    configuration::MetaDataLogsConfig meta_logs_config;
    meta_logs_config.metadata_nodes = {5, 6, 7, 8, 9};
    meta_logs_config.setMetadataLogGroup(
        logsconfig::LogGroupNode("metadata logs",
                                 log_attrs,
                                 logid_range_t(LOGID_INVALID, LOGID_INVALID)));

    auto server_config = std::shared_ptr<ServerConfig>(
        ServerConfig::fromDataTest("RebuildingReadStorageTaskTest",
                                   configuration::NodesConfig(nodes),
                                   meta_logs_config));

    auto cfg = std::make_shared<Configuration>(server_config, logs_config);
    config = std::make_shared<UpdateableConfig>(cfg);
    config->updateableNodesConfiguration()->update(
        config->getNodesConfigurationFromServerConfigSource());

    // Create local log store (logsdb) and 6 partitions, 15 minutes apart.
    store = std::make_unique<TemporaryPartitionedStore>();
    for (size_t i = 0; i < 6; ++i) {
      RecordTimestamp t = BASE_TIME + PARTITION_DURATION * i;
      store->setTime(SystemTimestamp(t.toMilliseconds()));
      store->createPartition();
      partition_start.push_back(t);
    }
  }

  void
  setRebuildingSettings(std::unordered_map<std::string, std::string> settings) {
    SettingsUpdater u;
    u.registerSettings(rebuildingSettings);
    u.setFromConfig(settings);
  }

  // The caller needs to assign `onDone` and `logs` afterwards.
  std::shared_ptr<RebuildingReadStorageTaskV2::Context>
  createContext(std::shared_ptr<const RebuildingSet> rebuilding_set,
                ShardID my_shard_id = ShardID(1, 0)) {
    auto c = std::make_shared<RebuildingReadStorageTaskV2::Context>();
    c->rebuildingSet = rebuilding_set;
    c->rebuildingSettings = rebuildingSettings;
    c->myShardID = my_shard_id;
    c->onDone = [&](std::vector<std::unique_ptr<ChunkData>> c) {
      chunks = std::move(c);
    };
    return c;
  }

  UpdateableSettings<Settings> settings;
  UpdateableSettings<RebuildingSettings> rebuildingSettings;
  std::shared_ptr<UpdateableConfig> config;
  StatsHolder stats{StatsParams().setIsServer(true)};
  std::unique_ptr<TemporaryPartitionedStore> store;

  std::vector<RecordTimestamp> partition_start;

  std::vector<std::unique_ptr<ChunkData>> chunks;
};

struct ChunkDescription {
  logid_t log;
  lsn_t min_lsn;
  size_t num_records = 1;
  bool big_payload = false;

  bool operator==(const ChunkDescription& rhs) const {
    return std::tie(log, min_lsn, num_records, big_payload) ==
        std::tie(rhs.log, rhs.min_lsn, rhs.num_records, rhs.big_payload);
  }
};

static std::vector<ChunkDescription>
convertChunks(const std::vector<std::unique_ptr<ChunkData>>& chunks) {
  std::vector<ChunkDescription> res;
  for (auto& c : chunks) {
    EXPECT_EQ(c->numRecords(), c->address.max_lsn - c->address.min_lsn + 1);

    // Expect payload to be either BIG_PAYLOAD or log_id concatenated with lsn.
    bool big = c->totalBytes() > BIG_PAYLOAD.size();
    if (big) {
      EXPECT_EQ(1, c->numRecords());
    }

    // Verify payloads.
    for (size_t i = 0; i < c->numRecords(); ++i) {
      EXPECT_EQ(c->address.min_lsn + i, c->getLSN(i));
      Payload payload;
      int rv = LocalLogStoreRecordFormat::parse(c->getRecordBlob(i),
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                0,
                                                nullptr,
                                                nullptr,
                                                &payload,
                                                shard_index_t(0));
      EXPECT_EQ(0, rv);
      std::string expected_payload;
      if (big) {
        expected_payload = BIG_PAYLOAD;
      } else {
        expected_payload =
            toString(c->address.log.val()) + lsn_to_string(c->getLSN(i));
      }
      EXPECT_EQ(expected_payload, payload.toString());
    }

    res.push_back(ChunkDescription{c->address.log,
                                   c->address.min_lsn,
                                   c->address.max_lsn - c->address.min_lsn + 1,
                                   big});
  }
  return res;
}

std::ostream& operator<<(std::ostream& out, const ChunkDescription& c) {
  out << c.log.val() << lsn_to_string(c.min_lsn);
  if (c.num_records != 1) {
    out << ":" << c.num_records;
  }
  if (c.big_payload) {
    out << ":big";
  }
  return out;
}

TEST_F(RebuildingReadStorageTaskTest, Basic) {
  logid_t L1(1), L2(2), L3(3), L4(4);
  logid_t M1 = MetaDataLog::metaDataLogID(L1);
  logid_t I1 = configuration::InternalLogs::EVENT_LOG_DELTAS;
  auto& P = partition_start;
  ReplicationProperty R({{NodeLocationScope::NODE, 3}});
  StorageSet all_nodes{N0, N1, N2, N3, N4, N5, N6, N7, N8, N9};
  Slice big_payload = Slice::fromString(BIG_PAYLOAD);

  // Read 10 KB per storage task.
  setRebuildingSettings({{"rebuilding-max-batch-bytes", "10000"}});

  // Rebuilding set is {N2}.
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->shards.emplace(
      ShardID(2, 0), RebuildingNodeInfo(RebuildingMode::RESTORE));
  auto c = createContext(rebuilding_set);

  // Fill out rebuilding plan.
  // For internal and metadata log read all epochs.
  c->logs[I1].plan.untilLSN = LSN_MAX;
  c->logs[I1].plan.addEpochRange(
      EPOCH_INVALID, EPOCH_MAX, std::make_shared<EpochMetaData>(all_nodes, R));
  c->logs[M1].plan.untilLSN = LSN_MAX;
  c->logs[M1].plan.addEpochRange(
      EPOCH_INVALID, EPOCH_MAX, std::make_shared<EpochMetaData>(all_nodes, R));
  // For log 1 read epochs 3-4 and 7-8, and stop at e8n50.
  c->logs[L1].plan.untilLSN = mklsn(8, 50);
  c->logs[L1].plan.addEpochRange(
      epoch_t(3), epoch_t(4), std::make_shared<EpochMetaData>(all_nodes, R));
  c->logs[L1].plan.addEpochRange(
      epoch_t(7), epoch_t(8), std::make_shared<EpochMetaData>(all_nodes, R));
  // For log 2 read all epochs and stop at e10n50.
  c->logs[L2].plan.untilLSN = mklsn(10, 50);
  c->logs[L2].plan.addEpochRange(
      EPOCH_INVALID, EPOCH_MAX, std::make_shared<EpochMetaData>(all_nodes, R));
  // Don't read log 3.
  // For log 4 read only epoch 1.
  c->logs[L4].plan.untilLSN = LSN_MAX;
  c->logs[L4].plan.addEpochRange(
      epoch_t(1), epoch_t(1), std::make_shared<EpochMetaData>(all_nodes, R));

  // Write some records. Commment '-' means the record will be filtered out.
  // Remember that we're N1, and rebuilding set is {N2}.

  // Internal log.
  store->putRecord(I1, mklsn(1, 10), BASE_TIME, {N1, N0, N2}); // +
  // Metadata log.
  store->putRecord(M1, mklsn(2, 1), BASE_TIME - MINUTE, {N2, N1, N0}); // +
  store->putRecord(M1, mklsn(2, 2), BASE_TIME - MINUTE, {N2, N1, N0}); // +
  store->putRecord(M1, mklsn(3, 1), BASE_TIME - MINUTE, {N1, N3, N4}); // -
  store->putRecord(M1, mklsn(3, 2), BASE_TIME - MINUTE, {N1, N2, N3}); // +
  // Data logs in partition 0.
  store->putRecord(L1, mklsn(1, 10), P[0] + MINUTE, {N2, N1, N3});     // -
  store->putRecord(L1, mklsn(2, 10), P[0] + MINUTE, {N2, N1, N3});     // -
  store->putRecord(L1, mklsn(3, 10), P[0] + MINUTE, {N3, N1, N2});     // -
  store->putRecord(L1, mklsn(3, 11), P[0] + MINUTE, {N1, N3, N2});     // +
  store->putRecord(L1, mklsn(3, 12), P[0] + MINUTE * 2, {N1, N3, N2}); // +
  store->putRecord(
      L1, mklsn(4, 1), P[0] + MINUTE * 2, {N1, N3, N2}, 0, big_payload); // +
  store->putRecord(L2, mklsn(1, 1), P[0] + MINUTE, {N3, N1, N2});        // -
  store->putRecord(L3, mklsn(1, 1), P[0] + MINUTE, {N2, N1, N3});        // -
  store->putRecord(
      L4, mklsn(1, 1), P[0] + MINUTE, {N2, N1, N5}, 0, big_payload); // +
  // Data logs in partition 1.
  store->putRecord(L4,
                   mklsn(1, 2),
                   P[1] + MINUTE,
                   {N2, N1, N5},
                   LocalLogStoreRecordFormat::FLAG_DRAINED); // -
  // Keep partition 2 empty.
  // Data logs in partition 3.
  store->putRecord(L1, mklsn(4, 2), P[3] + MINUTE, {N1, N3, N2});       // +
  store->putRecord(L1, mklsn(5, 1), P[3] + MINUTE, {N1, N3, N2});       // -
  store->putRecord(L1, mklsn(8, 50), P[3] + MINUTE, {N1, N3, N2});      // +
  store->putRecord(L1, mklsn(8, 51), P[3] + MINUTE, {N1, N3, N2});      // -
  store->putRecord(L2, mklsn(10, 40), P[3] + MINUTE * 2, {N1, N3, N2}); // +
  store->putRecord(L2, mklsn(10, 60), P[3] + MINUTE * 2, {N1, N3, N2}); // -
  store->putRecord(L3, mklsn(10, 10), P[3] + MINUTE, {N2, N1, N3});     // -
  store->putRecord(L4, mklsn(1, 3), P[3] + MINUTE * 2, {N1, N3, N2});   // +
  // Write lots of CSI entries that'll be filtered out. This should get iterator
  // into LIMIT_REACHED state.
  for (size_t i = 0; i < 2000; ++i) {
    store->putRecord(L4, mklsn(1, i + 4), P[3] + MINUTE * 2, {N1, N3, N4}); // -
  }
  store->putRecord(L4, mklsn(1, 100000), P[3] + MINUTE * 2, {N1, N3, N2}); // +
  store->putRecord(L4, mklsn(2, 1), P[3] + MINUTE * 2, {N1, N3, N2});      // -

  // Sanity check that the records didn't end up in totally wrong partitions.
  EXPECT_EQ(std::vector<size_t>({4, 1, 0, 4}), store->getNumLogsPerPartition());

  {
    // Read up to the big record that exceeds the byte limit.
    MockRebuildingReadStorageTaskV2 task(this, c);
    task.execute();
    task.onDone();
    EXPECT_FALSE(c->reachedEnd);
    EXPECT_FALSE(c->persistentError);
    EXPECT_EQ(std::vector<ChunkDescription>({{I1, mklsn(1, 10)},
                                             {M1, mklsn(2, 1), 2},
                                             {M1, mklsn(3, 2)},
                                             {L1, mklsn(3, 11), 2}}),
              convertChunks(chunks));
  }
  size_t block_id;
  {
    // Read up to the next big record. Note that the first big record doesn't
    // count towards the limit for this batch because it was counted by the
    // previous batch.
    MockRebuildingReadStorageTaskV2 task(this, c);
    task.execute();
    task.onDone();
    EXPECT_FALSE(c->reachedEnd);
    EXPECT_FALSE(c->persistentError);
    EXPECT_EQ(
        std::vector<ChunkDescription>({{L1, mklsn(4, 1), 1, /* big */ true}}),
        convertChunks(chunks));
    block_id = chunks.at(0)->blockID;
  }
  {
    MockRebuildingReadStorageTaskV2 task(this, c);
    task.execute();
    task.onDone();
    EXPECT_FALSE(c->reachedEnd);
    EXPECT_FALSE(c->persistentError);
    EXPECT_EQ(
        std::vector<ChunkDescription>({{L4, mklsn(1, 1), 1, /* big */ true},
                                       {L1, mklsn(4, 2)},
                                       {L1, mklsn(8, 50)},
                                       {L2, mklsn(10, 40)},
                                       {L4, mklsn(1, 3)}}),
        convertChunks(chunks));
    // Check that chunk e4n2 from this batch has the same block ID as chunk
    // e4n1 from previous batch.
    EXPECT_EQ(block_id, chunks.at(1)->blockID);
    // Check that next chunk for same log has next block ID.
    EXPECT_EQ(block_id + 1, chunks.at(2)->blockID);
  }
  // Read through a long sequence of CSI that doesn't pass filter.
  int empty_batches = 0;
  while (true) {
    MockRebuildingReadStorageTaskV2 task(this, c);
    task.execute();
    task.onDone();
    EXPECT_FALSE(c->persistentError);
    if (c->reachedEnd) {
      break;
    }
    EXPECT_EQ(0, chunks.size());
    ++empty_batches;
    // Invalidate iterator after first batch.
    if (empty_batches == 1) {
      c->iterator->invalidate();
    }
  }
  EXPECT_GE(empty_batches, 2);
  EXPECT_EQ(std::vector<ChunkDescription>({{L4, mklsn(1, 100000)}}),
            convertChunks(chunks));

  // Run a task after Context is destroyed. Check that callback is not called.
  {
    chunks.clear();
    chunks.push_back(std::make_unique<ChunkData>());
    chunks.back()->address.min_lsn = 42;

    MockRebuildingReadStorageTaskV2 task(this, c);
    c.reset();
    task.execute();
    task.onDone();
    ASSERT_EQ(1, chunks.size());
    EXPECT_EQ(42, chunks.at(0)->address.min_lsn);
  }
}

TEST_F(RebuildingReadStorageTaskTest, TimeRanges) {
  // N1 is us.
  // N2 needs to be rebuilt for partitions 0-2.
  // N3 needs to be rebuilt for partitions 1 and 4.
  // N4 needs to be rebuilt for partition 5.
  // Log 1 epoch 3 has nodeset {N1, N2, N3,  N5, N6}.
  // Log 1 epoch 4 has nodeset {N1, N3,  N5, N6}.
  // Log 2 epoch 2 has nodeset {N1, N4,  N5, N6}.

  logid_t L1(1), L2(2);
  auto& P = partition_start;
  ReplicationProperty R({{NodeLocationScope::NODE, 3}});

  // Fill out rebuilding set according to comment above.
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  RecordTimeIntervals n2_dirty_ranges;
  n2_dirty_ranges.insert(
      RecordTimeInterval(P[0] + MINUTE * 2, P[3] - MINUTE * 2));
  rebuilding_set->shards.emplace(
      N2,
      RebuildingNodeInfo(
          {{DataClass::APPEND, n2_dirty_ranges}}, RebuildingMode::RESTORE));
  RecordTimeIntervals n3_dirty_ranges;
  n3_dirty_ranges.insert(
      RecordTimeInterval(P[1] + MINUTE * 2, P[2] - MINUTE * 2));
  n3_dirty_ranges.insert(
      RecordTimeInterval(P[4] + MINUTE * 2, P[5] - MINUTE * 2));
  rebuilding_set->shards.emplace(
      N3,
      RebuildingNodeInfo(
          {{DataClass::APPEND, n3_dirty_ranges}}, RebuildingMode::RESTORE));
  RecordTimeIntervals n4_dirty_ranges;
  n4_dirty_ranges.insert(RecordTimeInterval(
      P[5] + MINUTE * 2, P[5] + PARTITION_DURATION - MINUTE * 2));
  rebuilding_set->shards.emplace(
      N4,
      RebuildingNodeInfo(
          {{DataClass::APPEND, n4_dirty_ranges}}, RebuildingMode::RESTORE));

  auto c = createContext(rebuilding_set);

  // Fill out rebuilding plan according to comment above.
  c->logs[L1].plan.untilLSN = LSN_MAX;
  c->logs[L1].plan.addEpochRange(
      epoch_t(3),
      epoch_t(3),
      std::make_shared<EpochMetaData>(StorageSet{N1, N2, N3, N5, N6}, R));
  c->logs[L1].plan.addEpochRange(
      epoch_t(4),
      epoch_t(4),
      std::make_shared<EpochMetaData>(StorageSet{N1, N3, N5, N6}, R));
  c->logs[L2].plan.untilLSN = LSN_MAX;
  c->logs[L2].plan.addEpochRange(
      epoch_t(2),
      epoch_t(2),
      std::make_shared<EpochMetaData>(StorageSet{N1, N4, N5, N6}, R));

  // Write some records, trying to hit as many different cases as possible.

  // Partition 0.
  store->putRecord(L1, mklsn(2, 1), P[0] + MINUTE * 3, {N1, N2, N3}); // -
  store->putRecord(L1, mklsn(3, 1), P[0] + MINUTE * 1, {N1, N2, N3}); // -
  store->putRecord(L1, mklsn(3, 2), P[0] + MINUTE * 3, {N1, N2, N3}); // +
  store->putRecord(L1, mklsn(3, 3), P[0] + MINUTE * 3, {N1, N3, N5}); // -
  store->putRecord(L1, mklsn(3, 4), P[0] + MINUTE * 3, {N1, N5, N6}); // -
  store->putRecord(L1, mklsn(3, 5), P[0] + MINUTE * 3, {N1, N2, N6}); // +
  store->putRecord(L1, mklsn(3, 6), P[1] - MINUTE * 1, {N1, N2, N6}); // +

  store->putRecord(L2, mklsn(1, 1), P[0] + MINUTE * 3, {N1, N7, N8}); // -
  store->putRecord(L2, mklsn(2, 1), P[0] + MINUTE * 3, {N1, N4, N5}); // -

  // Partition 1.
  store->putRecord(L1, mklsn(3, 7), P[1] + MINUTE * 1, {N1, N2, N5});  // +
  store->putRecord(L1, mklsn(3, 8), P[1] + MINUTE * 1, {N1, N3, N5});  // -
  store->putRecord(L1, mklsn(3, 9), P[1] + MINUTE * 3, {N1, N3, N5});  // +
  store->putRecord(L1, mklsn(3, 10), P[1] + MINUTE * 3, {N1, N2, N3}); // +
  store->putRecord(L1, mklsn(4, 1), P[1] + MINUTE * 4, {N1, N3, N5});  // +

  // Partition 2.
  store->putRecord(L1, mklsn(4, 2), P[2] + MINUTE * 4, {N1, N3, N5}); // -
  store->putRecord(L1, mklsn(4, 3), P[2] + MINUTE * 4, {N1, N5, N6}); // -

  // Partition 3.
  store->putRecord(L1, mklsn(4, 4), P[3] + MINUTE * 4, {N1, N3, N4}); // -
  store->putRecord(L2, mklsn(2, 2), P[3] + MINUTE * 4, {N1, N4, N5}); // -

  // Partition 4.
  store->putRecord(L1, mklsn(4, 5), P[4] + MINUTE * 4, {N1, N3, N5}); // +
  store->putRecord(L1, mklsn(4, 6), P[4] + MINUTE * 4, {N1, N5, N6}); // -
  store->putRecord(L1, mklsn(5, 1), P[4] + MINUTE * 4, {N1, N5, N6}); // -
  store->putRecord(
      L1, mklsn(2000000000, 1), P[4] + MINUTE * 4, {N1, N5, N6}); // -

  // Partition 5.
  store->putRecord(
      L1, mklsn(2000000000, 2), P[5] + MINUTE * 4, {N1, N5, N6});     // -
  store->putRecord(L2, mklsn(2, 3), P[5] + MINUTE * 4, {N1, N4, N5}); // +

  EXPECT_EQ(
      std::vector<size_t>({2, 1, 1, 2, 1, 2}), store->getNumLogsPerPartition());

  {
    MockRebuildingReadStorageTaskV2 task(this, c);
    task.execute();
    task.onDone();
    EXPECT_TRUE(c->reachedEnd);
    EXPECT_FALSE(c->persistentError);
    EXPECT_EQ(std::vector<ChunkDescription>({{L1, mklsn(3, 2)},
                                             {L1, mklsn(3, 5), 2},
                                             {L1, mklsn(3, 7)},
                                             {L1, mklsn(3, 9)},
                                             {L1, mklsn(3, 10)},
                                             {L1, mklsn(4, 1)},
                                             {L1, mklsn(4, 5)},
                                             {L2, mklsn(2, 3)}}),
              convertChunks(chunks));
  }
}
