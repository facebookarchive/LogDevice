/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <algorithm>
#include <numeric>
#include <thread>
#include <unistd.h>

#include <boost/algorithm/string/predicate.hpp>
#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/Digest.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/RecordCacheDependencies.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

// TODO 11866467: convert all tests to support immutable consensus

using namespace facebook::logdevice;
using IntegrationTestUtils::markShardUnrecoverable;
using IntegrationTestUtils::requestShardRebuilding;

const logid_t LOG_ID{1};
const int SHARD_IDX{1}; // log 1 maps to shard 1

// Handy macros for writing ShardIDs
#define N0 ShardID(0, SHARD_IDX)
#define N1 ShardID(1, SHARD_IDX)
#define N2 ShardID(2, SHARD_IDX)
#define N3 ShardID(3, SHARD_IDX)
#define N4 ShardID(4, SHARD_IDX)
#define N5 ShardID(5, SHARD_IDX)
#define N6 ShardID(6, SHARD_IDX)
#define N7 ShardID(7, SHARD_IDX)
#define N8 ShardID(8, SHARD_IDX)
#define N9 ShardID(9, SHARD_IDX)

namespace {

enum class PopulateRecordCache { NO, YES };

class RecoveryTest : public ::testing::TestWithParam<PopulateRecordCache> {
 public:
  const std::chrono::milliseconds timeout_ = DEFAULT_TEST_TIMEOUT;
  Alarm alarm_;

  std::chrono::milliseconds testTimeout() const {
    return timeout_;
  }
  void SetUp() override {
    dbg::currentLevel = dbg::Level::DEBUG;
    dbg::assertOnData = true;
  }

  explicit RecoveryTest() : alarm_(timeout_) {}

 public:
  typedef std::tuple<GapType, lsn_t, lsn_t> gap_record_t;

  // initializes a Cluster object with the desired log config
  void init();

  using PerEpochReleaseMap = std::map<epoch_t, std::pair<esn_t, OffsetMap>>;

  // writes given data records to the specified node's local log store,
  // may also populate per-epoch releases if given. Applies to both local log
  // store and record cache
  void prepopulateData(node_index_t node,
                       const std::vector<TestRecord>& records,
                       const PerEpochReleaseMap& releases = {});

  // writes given data records to the nodes's record cache snapshot so that
  // the next time node starts, these records will be populated into the record
  // cache of the node.
  // Note: populate all logs in the config, all logs are empty expect LOGID in
  //       shard SHARD_IDX, which has records in @param records
  void prepopulateRecordCache(ShardedLocalLogStore* store,
                              const std::vector<TestRecord>& records,
                              const PerEpochReleaseMap& releases);

  // populate record cache for a single log
  void prepopulateRecordCacheForLog(LocalLogStore* store,
                                    logid_t log_id,
                                    const std::vector<TestRecord>& records,
                                    const PerEpochReleaseMap& releases);

  // writes given log metadata values to the specified node's local log store
  void
  prepopulateMetadata(node_index_t node,
                      const std::vector<std::unique_ptr<LogMetadata>>& meta);

  // sets the soft seal metadata to a certain epoch on each node
  void populateSoftSeals(epoch_t epoch);

  // constructs a Client and reads all records in the specified range from the
  // log (populates records_ and gaps_)
  void read(lsn_t start_lsn = LSN_OLDEST,
            lsn_t end_lsn = LSN_MAX,
            bool use_scd = true);

  // constructs a Digest from records (in a given epoch) contained in storage
  // nodes' local log stores whose ESN is larger than _lng_ as if it is
  // collected by an epoch recovery instance running with
  // _expected_seal_epoch_. for a nodeset specified by _nodes_,
  // replication factor specified by _replication_, and synchronous replication
  // scope specified by _sync_replication_scope_. Use the _expected_seal_epoch_
  // for determining whether the digest still needs to be mutated.
  std::unique_ptr<Digest> buildDigest(epoch_t epoch,
                                      esn_t lng,
                                      const StorageSet& shards,
                                      int replication,
                                      NodeLocationScope sync_replication_scope,
                                      epoch_t expected_seal_epoch);

  // convenient override for building digeset using all nodes and replication
  // from the public member of this class
  std::unique_ptr<Digest> buildDigest(epoch_t epoch,
                                      esn_t lng,
                                      epoch_t expected_seal_epoch);

  // checks that the given digest doesn't contain any conflicts and that each
  // record is stored on enough nodes. Note that @param expected_plugs should
  // not contain LSN of the bridge record, which should be in
  // @param expected_bridges.
  void verifyDigest(Digest& digest,
                    const std::vector<esn_t>& expected_records,
                    const std::vector<esn_t>& expected_plugs,
                    const std::vector<esn_t>& expected_bridges,
                    bool expect_fully_replicated = true);

  void verifyEpochRecoveryMetadata(epoch_t epoch,
                                   const EpochRecoveryMetadata& md);

  size_t summarize_stats(const char* stats_name) {
    ld_check(stats_name != nullptr);
    size_t num_stats = 0;
    for (const auto& it : cluster_->getNodes()) {
      node_index_t idx = it.first;
      auto stats = cluster_->getNode(idx).stats();
      num_stats += stats[stats_name];
    }
    return num_stats;
  }

  // summarize the number of hole plugs reported by each node in the cluster
  // used to verify the behavior of recoverying epoch and hole plug counters
  size_t summarize_num_hole_plugs() {
    return summarize_stats("num_hole_plugs");
  }

  size_t summarize_mutations_started() {
    return summarize_stats("mutation_started");
  }

  void expectAuthoritativeOnly() {
    auto stats = cluster_->getNode(0).stats();
    ASSERT_EQ(0, stats["non_auth_recovery_epochs"]);
    ASSERT_EQ(0, stats["num_holes_not_plugged"]);
  }

  void printRecordCacheStats();

  // used by tests w/o on-demand provisioning
  void provisionInternalLogs(StorageSet shards, ReplicationProperty rep);

  // Log properties
  size_t nodes_ = 3;
  size_t replication_ = 2;
  size_t extra_ = 0;
  size_t synced_ = 0;
  size_t max_writes_in_flight_ = 256;
  bool tail_optimized_{false};
  bool bridge_empty_epoch_{false};

  NodeLocationScope sync_replication_scope_{NodeLocationScope::NODE};
  folly::Optional<Configuration::Nodes> node_configs_;
  folly::Optional<Configuration::MetaDataLogsConfig> metadata_config_;

  // number of data logs in the config
  folly::Optional<size_t> num_logs_{1};

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::vector<lsn_t> records_;
  std::vector<std::unique_ptr<DataRecord>> data_;
  std::vector<gap_record_t> gaps_;

  std::unique_ptr<RecordCacheDependencies> cache_deps_;

  // provision epoch metadata before cluster startup, defaults to true
  bool pre_provision_epoch_metadata_ = false;
  // metadata logs and epoch store metadata written by sequencers
  bool let_sequencers_provision_metadata_ = true;
  // Pass --disable-rebuilding parameter to nodes.
  bool enable_rebuilding_ = false;

  // if true, storage node will have record cache enabled
  bool use_record_cache_ = true;

  bool single_empty_erm_ = false;

  folly::Optional<std::chrono::milliseconds> recovery_timeout_;
  folly::Optional<std::chrono::milliseconds> recovery_grace_period_;

  // if true, prepopulateData() will also populate the record cache besides
  // local log store
  PopulateRecordCache populate_record_cache_ = PopulateRecordCache::NO;

  // sequencer reactivation limit setting string. optional
  folly::Optional<std::string> seq_reactivation_limit_;

  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
};

class MockRecordCacheDependencies : public RecordCacheDependencies {
 public:
  explicit MockRecordCacheDependencies(RecoveryTest* test) : test_(test) {}

  void
  disposeOfCacheEntry(std::unique_ptr<EpochRecordCacheEntry> entry) override {
    entry.reset();
  }

  void onRecordsReleased(const EpochRecordCache&,
                         lsn_t /* begin */,
                         lsn_t /* end */,
                         const ReleasedVector& /* entries */) override {}

  folly::Optional<Seal> getSeal(logid_t /* unused */,
                                shard_index_t /* unused */,
                                bool /* unused */) const override {
    const NodeID nid(0, 1);
    // node never stored anything at this time. set the Seal to epoch 0.
    return Seal(EPOCH_INVALID, nid);
  }

  int getHighestInsertedLSN(logid_t /* unused */,
                            shard_index_t /* unused */,
                            lsn_t* highest_lsn) const override {
    ld_check(highest_lsn != nullptr);
    err = E::NOTSUPPORTED;
    return -1;
  }

  size_t getEpochRecordCacheSize(logid_t /* unused */) const override {
    return test_->max_writes_in_flight_;
  }

  bool tailOptimized(logid_t log_id) const override {
    if (MetaDataLog::isMetaDataLog(log_id)) {
      return true;
    }
    return test_->tail_optimized_;
  }

 private:
  RecoveryTest* const test_;
};

static lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

void RecoveryTest::init() {
  ld_check(replication_ <= nodes_);

  // randomly choose if the log is tail optimized or not
  tail_optimized_ = (folly::Random::rand64(2) == 0);

  ld_info("Log %lu tail optimized: %s.",
          LOG_ID.val_,
          tail_optimized_ ? "YES" : "NO");

  cache_deps_ = std::make_unique<MockRecordCacheDependencies>(this);
  populate_record_cache_ = GetParam();

  if (populate_record_cache_ == PopulateRecordCache::NO) {
    // do not use record cache at all if we don't populate data
    use_record_cache_ = false;
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_maxWritesInFlight(max_writes_in_flight_);
  log_attrs.set_singleWriter(false);
  log_attrs.set_replicationFactor(replication_);
  log_attrs.set_extraCopies(extra_);
  log_attrs.set_syncedCopies(synced_);
  log_attrs.set_syncReplicationScope(sync_replication_scope_);
  log_attrs.set_stickyCopySets(true);
  log_attrs.set_tailOptimized(tail_optimized_);

  // Use replication factor 3 for event log if there are enough nodes.
  logsconfig::LogAttributes event_log_attrs;
  event_log_attrs.set_maxWritesInFlight(256);
  event_log_attrs.set_singleWriter(false);
  event_log_attrs.set_replicationFactor(std::min(nodes_ - (nodes_ > 1), 3ul));
  event_log_attrs.set_extraCopies(0);
  event_log_attrs.set_syncedCopies(0);
  event_log_attrs.set_syncReplicationScope(sync_replication_scope_);

  // Use replication factor 3 for config-log-deltas
  // log if there are enough nodes.
  logsconfig::LogAttributes config_attrs;
  config_attrs.set_maxWritesInFlight(256);
  config_attrs.set_singleWriter(false);
  config_attrs.set_replicationFactor(std::min(nodes_ - (nodes_ > 1), 3ul));
  config_attrs.set_extraCopies(0);
  config_attrs.set_syncedCopies(0);
  config_attrs.set_syncReplicationScope(sync_replication_scope_);

  logsconfig::LogAttributes maint_attrs;
  maint_attrs.set_maxWritesInFlight(256);
  maint_attrs.set_singleWriter(false);
  maint_attrs.set_replicationFactor(std::min(nodes_ - (nodes_ > 1), 3ul));
  maint_attrs.set_extraCopies(0);
  maint_attrs.set_syncedCopies(0);
  maint_attrs.set_syncReplicationScope(sync_replication_scope_);

  auto factory =
      IntegrationTestUtils::ClusterFactory()
          // use logsdb to support record cache persistence
          .enableMessageErrorInjection()
          .setLogGroupName("my-log-group")
          .setLogAttributes(log_attrs)
          .setEventLogDeltaAttributes(event_log_attrs)
          .setConfigLogAttributes(config_attrs)
          .setMaintenanceLogAttributes(maint_attrs)
          .deferStart()
          // if some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setParam("--byte-offsets")
          // we'll be using fake unrealistic timestamps;
          // tell logsdb to not worry about it
          .setParam("--rocksdb-partition-duration", "0s")
          // purge quickly when nodes are down
          .setParam("--gap-grace-period", "10ms")
          // allow 1000 reactivations per seconds
          .setParam("--reactivation-limit", "1000/1s")
          // fall back to non-authoritative quickly
          .setParam("--event-log-grace-period", "10ms");

  if (enable_rebuilding_) {
    factory.setParam("--disable-rebuilding", "false");
  }

  factory.setParam(
      "--bridge-record-in-empty-epoch", bridge_empty_epoch_ ? "true" : "false");

  factory.setParam(
      "--enable-record-cache", use_record_cache_ ? "true" : "false");

  if (seq_reactivation_limit_.hasValue()) {
    factory.setParam("--reactivation-limit", seq_reactivation_limit_.value());
  }

  if (recovery_timeout_.hasValue()) {
    factory.setParam("--recovery-timeout",
                     toString(recovery_timeout_.value().count()) + "ms");
  }

  if (recovery_grace_period_.hasValue()) {
    factory.setParam("--recovery-grace-period",
                     toString(recovery_grace_period_.value().count()) + "ms");
  }

  factory.setParam("--single-empty-erm", single_empty_erm_ ? "true" : "false");
  factory.setParam("--get-erm-for-empty-epoch", "true");

  factory.setParam("--purging-use-metadata-log-only", "false");

  if (pre_provision_epoch_metadata_) {
    factory.doPreProvisionEpochMetaData();
  }

  if (num_logs_.hasValue()) {
    factory.setNumLogs(num_logs_.value());
  }

  if (node_configs_.hasValue()) {
    ASSERT_EQ(nodes_, node_configs_.value().size());
    factory.setNodes(node_configs_.value());
  }

  if (metadata_config_.hasValue()) {
    factory.setMetaDataLogsConfig(metadata_config_.value());
  } else {
    // Place metadata logs on the first 3 nodes in the cluster.
    const size_t nodeset_size = std::min(5lu, nodes_ - (nodes_ > 1));
    std::vector<node_index_t> nodeset(nodeset_size);
    std::iota(nodeset.begin(), nodeset.end(), nodes_ > 1 ? 1 : 0);

    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodeset, std::min(nodes_ - (nodes_ > 1), 3ul), NodeLocationScope::NODE);

    meta_config.sequencers_write_metadata_logs =
        let_sequencers_provision_metadata_;
    meta_config.sequencers_provision_epoch_store =
        let_sequencers_provision_metadata_;

    factory.setMetaDataLogsConfig(meta_config);
  }

  cluster_ = factory.create(nodes_);

  dbg::currentLevel = dbg::Level::INFO;
  dbg::assertOnData = true;
}

void RecoveryTest::prepopulateData(node_index_t node,
                                   const std::vector<TestRecord>& records,
                                   const PerEpochReleaseMap& releases) {
  auto sharded_store = cluster_->getNode(node).createLocalLogStore();
  LocalLogStore* store = sharded_store->getByIndex(SHARD_IDX);
  ASSERT_NE(nullptr, store);
  store_fill(*store, records);

  // write mutable per-epoch log metadata which contains per-epoch releases
  for (const auto& kv : releases) {
    MutablePerEpochLogMetadata metadata(0, kv.second.first, kv.second.second);
    int rv =
        store->updatePerEpochLogMetadata(LOG_ID,
                                         kv.first,
                                         metadata,
                                         LocalLogStore::SealPreemption::DISABLE,
                                         LocalLogStore::WriteOptions());
    ASSERT_EQ(0, rv);
  }

  if (populate_record_cache_ == PopulateRecordCache::YES) {
    prepopulateRecordCache(sharded_store.get(), records, releases);
  }
}

void RecoveryTest::prepopulateRecordCache(
    ShardedLocalLogStore* sharded_store,
    const std::vector<TestRecord>& records,
    const PerEpochReleaseMap& releases) {
  std::shared_ptr<const Configuration> config = cluster_->getConfig()->get();
  for (auto it = config->localLogsConfig()->logsBegin();
       it != config->localLogsConfig()->logsEnd();
       ++it) {
    const logid_t log_id(it->first);
    // TODO(T15517759): if we make RecoveryTest use a nodeset selector that uses
    // more than one shard per node, this code needs to be changed.
    shard_index_t shard =
        getLegacyShardIndexForLog(log_id, sharded_store->numShards());
    auto* store = sharded_store->getByIndex(shard);
    prepopulateRecordCacheForLog(store,
                                 log_id,
                                 log_id == LOG_ID && shard == SHARD_IDX
                                     ? records
                                     : std::vector<TestRecord>(),
                                 releases);
  }
}

void RecoveryTest::prepopulateRecordCacheForLog(
    LocalLogStore* store,
    logid_t log_id,
    const std::vector<TestRecord>& records,
    const PerEpochReleaseMap& releases) {
  auto record_cache =
      std::make_unique<RecordCache>(log_id, SHARD_IDX, cache_deps_.get());

  // the node is empty, update last non-authoritative epoch to reflect that to
  // ensure we get cache hit later
  record_cache->neverStored();
  for (const TestRecord& tr : records) {
    Slice payload_slice =
        tr.payload_.hasValue() ? Slice(tr.payload_.value()) : Slice("blah", 4);
    auto ph = (tr.payload_.hasValue()
                   ? std::make_shared<PayloadHolder>(
                         tr.payload_.value(), PayloadHolder::UNOWNED)
                   : std::make_shared<PayloadHolder>(nullptr, 0));

    STORE_flags_t store_flags =
        STORE_flags_t(tr.flags_ & LocalLogStoreRecordFormat::FLAG_MASK);

    if (tr.flags_ & LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH) {
      store_flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
    }

    int rv = record_cache->putRecord(
        RecordID(tr.lsn_, tr.log_id_),
        tr.timestamp_.count(),
        tr.last_known_good_,
        tr.wave_,
        copyset_t(tr.copyset_.cbegin(), tr.copyset_.cend()),
        store_flags,
        {},
        payload_slice,
        ph,
        tr.offsets_within_epoch_);

    ASSERT_EQ(0, rv);
  }

  for (const auto& kv : releases) {
    auto r = record_cache->getEpochRecordCache(kv.first);
    if (r.first == RecordCache::Result::HIT) {
      r.second->advanceLNG(kv.second.first);
      r.second->setOffsetsWithinEpoch(kv.second.second);
    }
  }

  ssize_t size = record_cache->sizeInLinearBuffer();
  ASSERT_GE(size, 0);

  auto buffer = std::make_unique<uint8_t[]>(size);
  ssize_t linear_size =
      record_cache->toLinearBuffer(reinterpret_cast<char*>(buffer.get()), size);
  ASSERT_EQ(size, linear_size);

  std::vector<std::pair<logid_t, Slice>> snapshots;
  snapshots.emplace_back(log_id, Slice(buffer.get(), linear_size));

  int rv = store->writeLogSnapshotBlobs(
      LocalLogStore::LogSnapshotBlobType::RECORD_CACHE, snapshots);

  ASSERT_EQ(0, rv);
}

void RecoveryTest::prepopulateMetadata(
    node_index_t node,
    const std::vector<std::unique_ptr<LogMetadata>>& meta) {
  auto sharded_store = cluster_->getNode(node).createLocalLogStore();
  LocalLogStore* store = sharded_store->getByIndex(SHARD_IDX);

  ld_check(store);

  for (const auto& md : meta) {
    int rv =
        store->writeLogMetadata(LOG_ID, *md, LocalLogStore::WriteOptions());
    ASSERT_EQ(0, rv);
  }
}

void RecoveryTest::populateSoftSeals(epoch_t epoch) {
  std::vector<std::unique_ptr<LogMetadata>> metadata;
  metadata.emplace_back(new SoftSealMetadata(Seal(epoch, NodeID(0, 1))));

  for (node_index_t i = 0; i < nodes_; i++) {
    prepopulateMetadata(i, metadata);
  }
}

void RecoveryTest::read(lsn_t start_lsn, lsn_t end_lsn, bool use_scd) {
  ld_check(cluster_ != nullptr);

  records_.clear();
  data_.clear();
  gaps_.clear();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  // using a small gap grace period to speed us reading when nodes are down
  ASSERT_EQ(0, client_settings->set("gap-grace-period", "10ms"));

  auto client = cluster_->createClient(
      std::chrono::seconds(10), std::move(client_settings));
  auto reader = client->createReader(1);
  if (!use_scd) {
    reader->forceNoSingleCopyDelivery();
  }
  reader->includeByteOffset();

  int rv = reader->startReading(LOG_ID, start_lsn, end_lsn);
  ASSERT_EQ(0, rv);

  // reading will block until recovery completes, so set a big-enough timeout
  reader->setTimeout(this->testTimeout());
  lsn_t last_lsn = LSN_INVALID;
  for (;;) {
    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;

    ssize_t nread = reader->read(1, &data, &gap);
    if (nread == 0) {
      // Should finish reading and not timeout.
      ASSERT_EQ(last_lsn, end_lsn);
      break;
    }

    if (nread > 0) {
      ASSERT_EQ(1, nread);
      last_lsn = std::max(last_lsn, data[0]->attrs.lsn);
      records_.push_back(data[0]->attrs.lsn);
      data_.push_back(std::move(data[0]));
    } else {
      gaps_.push_back(gap_record_t(gap.type, gap.lo, gap.hi));
      last_lsn = std::max(last_lsn, gap.hi);
    }
  }
}

void RecoveryTest::verifyEpochRecoveryMetadata(
    epoch_t epoch,
    const EpochRecoveryMetadata& expected) {
  // examine the local log store for the per-epoch log metadata
  for (int i = 0; i < nodes_; ++i) {
    if (!cluster_->getConfig()
             ->get()
             ->serverConfig()
             ->getNode(i)
             ->isReadableStorageNode()) {
      continue;
    }

    auto store = cluster_->getNode(i).createLocalLogStore();
    EpochRecoveryMetadata metadata;
    int rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch, &metadata);

    ASSERT_EQ(0, rv);
    ASSERT_TRUE(metadata.valid());
    ASSERT_EQ(expected, metadata);
  }
}

void RecoveryTest::printRecordCacheStats() {
  if (!use_record_cache_) {
    return;
  }

  for (node_index_t i = 0; i < nodes_; i++) {
    auto stats = cluster_->getNode(i).stats();
    auto seal_hit = stats["record_cache_seal_hit"];
    auto seal_miss = stats["record_cache_seal_miss"];
    auto digest_hit = stats["record_cache_digest_hit"];
    auto digest_miss = stats["record_cache_digest_miss"];

    auto hit_rate = [](size_t hit, size_t miss) {
      if (hit + miss == 0) {
        return 1.0;
      }
      return (double)hit / (double)(hit + miss);
    };

    ld_info("Node %hu: seal hit: %lu, seal miss: %lu, hit rate: %f. "
            "digest hit: %lu, digest miss: %lu, hit rate: %f.",
            i,
            seal_hit,
            seal_miss,
            hit_rate(seal_hit, seal_miss),
            digest_hit,
            digest_miss,
            hit_rate(digest_hit, digest_miss));
  }
}

void RecoveryTest::provisionInternalLogs(StorageSet storage_set,
                                         ReplicationProperty rep) {
  std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
  epoch_metadata.emplace_back(
      new EpochMetaData(storage_set, rep, epoch_t(1), epoch_t(1)));
  auto meta_provisioner = cluster_->createMetaDataProvisioner();
  int rv = meta_provisioner->prepopulateMetaDataLog(
      configuration::InternalLogs::EVENT_LOG_DELTAS, epoch_metadata);
  ASSERT_EQ(0, rv);
  auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
    if (!info) {
      info = std::make_unique<EpochMetaData>();
    }
    *info = *epoch_metadata[0];
    info->h.epoch = epoch_t(1);
    return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
  };
  rv = meta_provisioner->provisionEpochMetaDataForLog(
      configuration::InternalLogs::EVENT_LOG_DELTAS,
      std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
      true /* write_metadata_logs */);
  ASSERT_EQ(0, rv);
  rv = meta_provisioner->provisionEpochMetaDataForLog(
      configuration::InternalLogs::CONFIG_LOG_DELTAS,
      std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
      true /* write_metadata_logs */);
  ASSERT_EQ(0, rv);
  rv = meta_provisioner->provisionEpochMetaDataForLog(
      configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
      std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
      true /* write_metadata_logs */);
  ASSERT_EQ(0, rv);
  rv = meta_provisioner->provisionEpochMetaDataForLog(
      configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
      std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
      true /* write_metadata_logs */);
  ASSERT_EQ(0, rv);
  rv = meta_provisioner->provisionEpochMetaDataForLog(
      configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
      std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
      true /* write_metadata_logs */);
  ASSERT_EQ(0, rv);
}

std::unique_ptr<Digest> RecoveryTest::buildDigest(epoch_t epoch,
                                                  esn_t lng,
                                                  epoch_t expected_seal_epoch) {
  ld_check(nodes_ > 0);
  std::vector<ShardID> shards;
  for (node_index_t nid = 0; nid < nodes_; ++nid) {
    shards.push_back(ShardID(nid, SHARD_IDX));
  }
  return buildDigest(epoch,
                     lng,
                     shards,
                     replication_,
                     sync_replication_scope_,
                     expected_seal_epoch);
}

std::unique_ptr<Digest>
RecoveryTest::buildDigest(epoch_t epoch,
                          esn_t lng,
                          const StorageSet& shards,
                          int replication,
                          NodeLocationScope sync_replication_scope,
                          epoch_t expect_seal_epoch) {
  ld_check(cluster_ != nullptr);

  std::shared_ptr<Configuration> config = cluster_->getConfig()->get();
  auto digest = std::make_unique<Digest>(
      LOG_ID,
      epoch,
      EpochMetaData(
          shards, ReplicationProperty(replication, sync_replication_scope)),
      expect_seal_epoch,
      config->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      Digest::Options({bridge_empty_epoch_}));

  for (ShardID shard : shards) {
    auto* n = config->serverConfig()->getNode(shard.node());
    if (!n || !n->isReadableStorageNode()) {
      continue;
    }

    auto store = cluster_->getNode(shard.node()).createLocalLogStore();
    auto it = store->getByIndex(shard.shard())
                  ->read(LOG_ID, LocalLogStore::ReadOptions("buildDigest"));

    it->seek(compose_lsn(epoch, esn_t(0)));

    lsn_t last_lsn = LSN_INVALID;
    for (; it->state() == IteratorState::AT_RECORD; it->next()) {
      lsn_t lsn = it->getLSN();

      if (lsn <= last_lsn) {
        continue;
      }

      if (lsn_to_epoch(lsn) != epoch) {
        break;
      }

      // filter out records less or equal than the lng, as they are not touched
      // by epoch recovery
      if (lsn_to_esn(lsn) <= lng) {
        continue;
      }

      // construct DataRecordOwnsPayload from the raw record read
      Slice blob = it->getRecord();
      Payload payload;
      std::chrono::milliseconds timestamp;
      LocalLogStoreRecordFormat::flags_t disk_flags;
      uint32_t wave_or_recovery_epoch;
      esn_t last_known_good;

      copyset_t copyset;
      copyset_size_t copyset_size;
      copyset.resize(15);
      OffsetMap offsets_within_epoch;
      int rv = LocalLogStoreRecordFormat::parse(blob,
                                                &timestamp,
                                                &last_known_good,
                                                &disk_flags,
                                                &wave_or_recovery_epoch,
                                                &copyset_size,
                                                &copyset[0],
                                                15,
                                                &offsets_within_epoch,
                                                nullptr,
                                                &payload,
                                                0 /* shard_index_t */);
      ld_check(rv == 0);
      copyset.resize(copyset_size);

      void* data = nullptr;
      if (payload.size() > 0) {
        data = malloc(payload.size());
        ld_check(data != nullptr);
        memcpy(data, payload.data(), payload.size());
      }

      auto extra_metadata = std::make_unique<ExtraMetadata>();
      extra_metadata->header = {
          last_known_good, wave_or_recovery_epoch, copyset_size};
      extra_metadata->copyset = std::move(copyset);
      extra_metadata->offsets_within_epoch = offsets_within_epoch;

      RECORD_flags_t record_flags =
          disk_flags & LocalLogStoreRecordFormat::FLAG_MASK;
      record_flags |= RECORD_Header::INCLUDES_EXTRA_METADATA;
      if (extra_metadata->offsets_within_epoch.isValid()) {
        record_flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
      }

      auto record =
          std::make_unique<DataRecordOwnsPayload>(LOG_ID,
                                                  Payload(data, payload.size()),
                                                  lsn,
                                                  timestamp,
                                                  record_flags,
                                                  std::move(extra_metadata));

      digest->onRecord(shard, std::move(record));

      last_lsn = lsn;
    }
  }

  return digest;
}

void RecoveryTest::verifyDigest(Digest& digest,
                                const std::vector<esn_t>& expected_records,
                                const std::vector<esn_t>& expected_plugs,
                                const std::vector<esn_t>& expected_bridges,
                                bool expect_fully_replicated) {
  std::vector<esn_t> records;
  std::vector<esn_t> plugs;
  std::vector<esn_t> bridges;

  for (Digest::iterator it = digest.begin(); it != digest.end(); ++it) {
    std::set<ShardID> successfully_stored;
    std::set<ShardID> amend_metadata;
    std::set<ShardID> conflict_copies;

    bool needs_mutation = digest.needsMutation(
        it->second,
        &successfully_stored,
        &amend_metadata,
        &conflict_copies,
        /*complete_digest*/ true); // we have contacted all the nodes
    EXPECT_NE(expect_fully_replicated, needs_mutation);

    if (expect_fully_replicated) {
      ASSERT_TRUE(conflict_copies.empty());
    }

    if (it->second.isBridgeRecord()) {
      bridges.push_back(it->first);
    } else {
      (it->second.isHolePlug() ? plugs : records).push_back(it->first);
    }
  }

  EXPECT_EQ(expected_records, records);
  EXPECT_EQ(expected_plugs, plugs);
  EXPECT_EQ(expected_bridges, bridges);
}

INSTANTIATE_TEST_CASE_P(RecoveryTest,
                        RecoveryTest,
                        ::testing::Values(PopulateRecordCache::NO,
                                          PopulateRecordCache::YES));

// test the new Digest and Mutation mechanism that supports
// immutatble consensus
TEST_P(RecoveryTest, MutationsWithImmutableConsensus) {
  nodes_ = 4;
  replication_ = 3;
  extra_ = 0;

  init();

  const copyset_t copyset = {N1, N2, N3};

  // EpochRecovery instance is running with seal epoch of 7.
  // Storage nodes contain the following records (x, x_w or x(e)),
  // plugs o(e) and bridge b(e) (x(e), o(e), and b(e) mean that the record
  // is written by recovery with seal epoch of e, x means that the record was
  // written by the sequencer with wave 1, while x_w means that the record was
  // written by the sequencer with wave w):
  //
  //      (1,1) (1,2)  (1,3) (1,4) (2,1) (2,2) (2,3)  (2,4)  (2,5)  (2,6)
  // ---------------------------------------------------------------------
  // #1  |  x     x          o(2)         x(4)         x(2)   o(7)   x(3)
  // #2  |  x    o(3)                           x(5)   x(2)   o(7)   x(4)
  // #3  |  x            x                 x    o(3)   x(2)   o(7)   o(6)
  //
  // (cont.)
  //
  //      (2,7)  (2,8)
  // -----------------
  // #1  |  x_3   x_1
  // #2  |  x_3   x_2
  // #3  |  x_3   x_3
  //
  // expected outcome:
  // (1,1)  no mutation: since its esn == lng
  // (1,2)  o(7): overwrite N1, amend N2, store N3
  // (1,3)  x(7): store N1, N2, amend N3
  // (1,4)  b(7): store N2, N3, amend N1. This becomes bridge record for e1.
  // (2,1)  o(7): store N1, N2, N3
  // (2,2)  x(7): store N2, amend N1, N3
  // (2,3)  x(7): overwrite N2, amend N2, store N1
  // (2,4)  no mutation: fully replicated + a complete digest
  // (2,5)  no mutation: all records are up-to-date
  // (2,6)  o(7): overwrite N1, N2, amend N3
  // (2,7)  no mutation: fully replicated + a complete digest
  // (2,8)  x(7): amend N1, N2, N3
  // (2,9)  b(7): will be stored on N1, N2, and N3. marks the end of e2.

  prepopulateData(
      node_index_t(1),
      {
          TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(1))
              .offsetsWithinEpoch(OffsetMap(
                  {{BYTE_OFFSET, 10}})), // since lsn(1, 1) is already someone's
                                         // last konwn good, it should have the
                                         // correct byteoffset
          TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(2)),
          TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(4))
              .writtenByRecovery(2)
              .hole(),
          TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(11))
              .writtenByRecovery(4),
          TestRecord(LOG_ID, lsn(2, 4), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(14))
              .writtenByRecovery(2),
          TestRecord(LOG_ID, lsn(2, 5), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(15))
              .writtenByRecovery(7)
              .hole(),
          TestRecord(LOG_ID, lsn(2, 6), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(16))
              .writtenByRecovery(3),
          TestRecord(LOG_ID, lsn(2, 7), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(17))
              .wave(3),
          TestRecord(LOG_ID, lsn(2, 8), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(18))
              .wave(1),
      });

  prepopulateData(
      node_index_t(2),
      {
          TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(1))
              .offsetsWithinEpoch(OffsetMap(
                  {{BYTE_OFFSET, 10}})), // since lsn(1, 1) is already someone's
                                         // last konwn good, it should have the
                                         // correct byteoffset
          TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(2))
              .writtenByRecovery(3)
              .hole(),
          TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(12))
              .writtenByRecovery(5),
          TestRecord(LOG_ID, lsn(2, 4), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(14))
              .writtenByRecovery(2),
          TestRecord(LOG_ID, lsn(2, 5), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(15))
              .writtenByRecovery(7)
              .hole(),
          TestRecord(LOG_ID, lsn(2, 6), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(16))
              .writtenByRecovery(4),
          TestRecord(LOG_ID, lsn(2, 7), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(17))
              .wave(3),
          TestRecord(LOG_ID, lsn(2, 8), ESN_INVALID)
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(18))
              .wave(2),
      });

  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 10}})),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(3))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 12}})),
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(11)),
                      TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(12))
                          .writtenByRecovery(3)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 4), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(14))
                          .writtenByRecovery(2),
                      TestRecord(LOG_ID, lsn(2, 5), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(15))
                          .writtenByRecovery(7)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 6), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(16))
                          .writtenByRecovery(6)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 7), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(17))
                          .wave(3),
                      TestRecord(LOG_ID, lsn(2, 8), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(18))
                          .wave(3),
                  });

  // latest epoch of store is 2, soft seal 1
  populateSoftSeals(epoch_t(1));

  cluster_->setStartingEpoch(LOG_ID, epoch_t(8));
  const epoch_t seal_epoch = epoch_t(7); // recovery seals epoch up to 2

  ASSERT_EQ(0, cluster_->start());

  read(lsn(1, 1), lsn(2, 2233));
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1),
                                lsn(1, 3),
                                lsn(2, 2),
                                lsn(2, 3),
                                lsn(2, 4),
                                lsn(2, 7),
                                lsn(2, 8)}),
            records_);

  for (auto i = 0; i < data_.size(); ++i) {
    // each data payload is 4 bytes,
    // e1n1 as the last know good has the byteoffset 10, which we will start
    // rebuild from
    EXPECT_EQ(
        RecordOffset({{BYTE_OFFSET, 4 * i + 10}}), data_[i]->attrs.offsets);
  }

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
                gap_record_t(GapType::HOLE, lsn(2, 1), lsn(2, 1)),
                gap_record_t(GapType::HOLE, lsn(2, 5), lsn(2, 5)),
                gap_record_t(GapType::HOLE, lsn(2, 6), lsn(2, 6)),
                gap_record_t(GapType::BRIDGE, lsn(2, 9), lsn(2, 2233)),
            }),
            gaps_);

  // verify the num of new holes plugged by EpochRecovery
  EXPECT_EQ(1, summarize_num_hole_plugs());

  expectAuthoritativeOnly();
  printRecordCacheStats();
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               std::vector<esn_t>({esn_t(3)}), // records
               std::vector<esn_t>({esn_t(2)}), // plugs
               std::vector<esn_t>({esn_t(4)})  // bridge
  );
  verifyDigest(
      *buildDigest(epoch_t(2), ESN_INVALID, seal_epoch),
      // records
      std::vector<esn_t>({esn_t(2), esn_t(3), esn_t(4), esn_t(7), esn_t(8)}),
      // plugs
      std::vector<esn_t>({esn_t(1), esn_t(5), esn_t(6)}),
      // bridge
      std::vector<esn_t>({esn_t(9)}));
}

// There are too many nodes rebuilding, recovery is non authoritative and does
// not plug holes.
TEST_P(RecoveryTest, NonAuthoritativeRecovery) {
  nodes_ = 6;
  replication_ = 2;
  extra_ = 0;
  enable_rebuilding_ = true;

  // pre-provisioning metadata not to break the "num_holes_plugged" math with
  // spontaneous sequencer activations
  pre_provision_epoch_metadata_ = true;
  let_sequencers_provision_metadata_ = false;

  init();

  const copyset_t copyset12 = {N1, N2};
  const copyset_t copyset13 = {N1, N3};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4) (2,1) (2,2) (2,3)
  // --------------------------------------------------
  // #1  |  x     x           o           x          <- authoritative
  // #2  |  x     o                             x    <- authoritative
  // #3  |                                x          <- authoritative
  // #4  |                                           <- down and rebuilding
  // #5  |                                           <- down and rebuilding
  //
  // nodes #4 and #5 are non-authoritative. This results in non-authoritative
  // recovery. The holes at 1.3 and 2.1 remain unplugged and are expected to be
  // reported as data loss to readers. In addition the recovery is expected to
  // replace (1,2) with a hole plug, replicate (1,4), and replicate (2,3).

  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(4))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset13)
                          .timestamp(std::chrono::milliseconds(11)),
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(2))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(12)),
                  });

  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset13)
                          .timestamp(std::chrono::milliseconds(11)),
                  });

  populateSoftSeals(epoch_t(1));
  // recover everything up to epoch 3
  cluster_->setStartingEpoch(LOG_ID, epoch_t(3));
  const epoch_t seal_epoch = epoch_t(2); // recovery seals epoch up to 2

  // this test expects all storage nodes to be up, so make sure they're running
  // before starting the sequencer
  ld_info("Starting nodes");
  ASSERT_EQ(0, cluster_->start({1, 2, 3}));
  ASSERT_EQ(0, cluster_->start({0}));

  ld_info("Requesting shard rebuildings.");
  auto client = cluster_->createClient();
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 4, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 5, SHARD_IDX));

  ld_info("Waiting for recovery.");
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  ld_info("Starting more nodes.");
  // Need an f-majority to read.
  ASSERT_EQ(0, cluster_->start({4}));

  ld_info("Reading.");
  read(lsn(1, 1), lsn(2, 3));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(2, 2),
                lsn(2, 3),
            }),
            records_);
  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::DATALOSS, lsn(1, 3), lsn(1, 3)),
                gap_record_t(GapType::HOLE, lsn(1, 4), lsn(1, 4)),
                gap_record_t(GapType::BRIDGE, lsn(1, 5), lsn(2, 1)),
            }),
            gaps_);

  ld_info("Getting stats.");
  auto stats = cluster_->getNode(0).stats();
  EXPECT_GT(stats["non_auth_recovery_epochs"], 0);

  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               std::vector<esn_t>({}),                   // records
               std::vector<esn_t>({esn_t(2), esn_t(4)}), // plugs
               std::vector<esn_t>({})                    // bridges
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({esn_t(2), esn_t(3)}), // records
               std::vector<esn_t>({}),                   // plugs
               std::vector<esn_t>({})                    // bridge
  );
}

// This test simulates a sequencer failure after storing only one copy of a
// record (on node 1), followed by node 1 crashing and not being present during
// recovery. Purging on release should delete the record.
TEST_P(RecoveryTest, Purging) {
  nodes_ = 5; // four storage nodes
  replication_ = 2;
  extra_ = 0;
  init();

  const copyset_t copyset = {N1, N2};

  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(2)),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1)),
                  });

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));
  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 1

  ASSERT_EQ(0, cluster_->start({0, 2, 3, 4})); // node 1 is inaccessible
  // until we have a better mechanism to wait until recovery is complete, we'll
  // rely on the property that reading blocks until recovery sends a final
  // release
  // Note: not using scd here because otherwise the test would be too slow.
  // TODO(#6228960) use scd when #6228960 is implemented.
  read(lsn(1, 1), lsn(1, 1), false /* use_scd */);
  cluster_->stop();

  // start all nodes this time; after sequencer sends a RELEASE for e3n0, node 1
  // should delete record (1, 2)
  ASSERT_EQ(0, cluster_->start());
  read(lsn(1, 1), lsn(3, 0));

  ld_info("Waiting for N1 to finish purge");
  cluster_->getNode(1).waitForPurge(LOG_ID, epoch_t(2));

  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1)}), records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::BRIDGE, lsn(1, 2), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(3, 0)),
            }),
            gaps_);
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               {},                              // records
               {},                              // plugs
               std::vector<esn_t>({esn_t(2)})); // bridge
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch), {}, {}, {});
}

// In this test, all nodes run sequencers and race to perform recovery. Log
// should be in a consistent state after they are all done.
TEST_P(RecoveryTest, MultipleSequencers) {
  // four nodes, all of them store records and run sequencers
  Configuration::Nodes node_configs;
  for (node_index_t i = 0; i < 4; ++i) {
    auto& node = node_configs[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
  }

  node_configs_ = node_configs;
  nodes_ = node_configs_.value().size();
  replication_ = 2;
  // prepopulating metadata to avoid throwing off the epoch math below
  pre_provision_epoch_metadata_ = true;
  let_sequencers_provision_metadata_ = false;
  init();

  // (1, 1) is on two nodes, (1, 2) is missing, while there's a gap/record
  // conflict for (1, 3)
  prepopulateData(node_index_t(0),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N0, N1})
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N0, N3})
                          .timestamp(std::chrono::milliseconds(3)),
                  });
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N0, N1})
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N1, N2, N3})
                          .timestamp(std::chrono::milliseconds(3))
                          .wave(0)
                          .hole(),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N1, N2, N3})
                          .timestamp(std::chrono::milliseconds(3))
                          .wave(0)
                          .hole(),
                  });
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N1, N2, N3})
                          .timestamp(std::chrono::milliseconds(3))
                          .wave(0)
                          .hole(),
                  });

  const int first_epoch = 2;
  cluster_->setStartingEpoch(LOG_ID, epoch_t(first_epoch));

  ASSERT_EQ(0, cluster_->start());
  cluster_->waitForRecovery();

  // write a record
  auto client = cluster_->createClient();

  lsn_t last_lsn = LSN_INVALID;

  do {
    // we might get append failures with E::CANCELLED because of the
    // silent duplicate prevention logic.
    last_lsn = client->appendSync(LOG_ID, Payload("test", 4));
  } while (last_lsn == LSN_INVALID && err == E::CANCELLED);

  ASSERT_NE(LSN_INVALID, last_lsn);
  EXPECT_EQ(epoch_t(first_epoch + (int)nodes_ - 1), lsn_to_epoch(last_lsn));

  read(lsn(1, 1), last_lsn);
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1), last_lsn}), records_);

  // bridge record is inserted at lsn(1,2), so we will get a bridge gap
  // of [e1n2, e1nESN_MAX], and bridge gaps covering the range of
  // [e2n0, last_lsn-1]
  ASSERT_LE(2, gaps_.size());
  EXPECT_EQ(
      gap_record_t(GapType::BRIDGE, lsn(1, 2), lsn(1, ESN_MAX.val_)), gaps_[0]);

  // bridge gap from lsn(1, 4) to last_lsn-1 may be split into multiple gaps
  EXPECT_EQ(GapType::BRIDGE, std::get<0>(gaps_[1]));
  lsn_t gap_lo = std::get<1>(gaps_[1]), gap_hi = std::get<2>(gaps_[1]);
  for (size_t i = 3; i < gaps_.size(); ++i) {
    EXPECT_EQ(GapType::BRIDGE, std::get<0>(gaps_[i]));
    EXPECT_EQ(gap_hi + 1, std::get<1>(gaps_[i]));
    gap_hi = std::get<2>(gaps_[i]);
  }
  EXPECT_EQ(lsn(2, 0), gap_lo);
  EXPECT_EQ(last_lsn - 1, gap_hi);

  // latest seal_epoch should be one less than the epoch of last_lsn
  const epoch_t seal_epoch = epoch_t(lsn_to_epoch(last_lsn).val_ - 1);

  client.reset();
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               {},         // records
               {esn_t(3)}, // plugs
               {esn_t(2)}  // bridge
  );

  // all other epochs except lsn_to_epoch(last_lsn) should be empty
  verifyDigest(*buildDigest(lsn_to_epoch(last_lsn), ESN_INVALID, seal_epoch),
               {lsn_to_esn(last_lsn)}, // records
               {},                     // plugs
               {}                      // bridge
  );

  for (epoch_t epoch(2); epoch < lsn_to_epoch(last_lsn); ++epoch.val_) {
    verifyDigest(*buildDigest(epoch, ESN_INVALID, seal_epoch), {}, {}, {});
  }
}

// Tests that recovery successfully detects when it's unable to seal due to
// preemption and updates preempted_by.
TEST_P(RecoveryTest, RecoveryPreempted) {
  nodes_ = 1;
  replication_ = 1;
  init();

  // Make sure recovery actually runs.
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  // Write a seal record with the higher epoch to simulate preemption by another
  // node running recovery.
  const NodeID sealed_by(1, 1);
  const epoch_t epoch(100);
  {
    auto store = cluster_->getNode(0).createLocalLogStore();
    SealMetadata meta(Seal(epoch, sealed_by));
    ASSERT_EQ(0,
              store->getByIndex(SHARD_IDX)->writeLogMetadata(
                  LOG_ID, meta, LocalLogStore::WriteOptions()));
  }

  // Start the cluster and wait for recovery to finish.
  ld_info("Starting cluster.");
  ASSERT_EQ(0, cluster_->start());
  ld_info("Waiting for recovery.");
  cluster_->waitForRecovery();

  ld_info("Getting stats and sequencer info.");
  auto stats = cluster_->getNode(0).stats();
  auto seq_info = cluster_->getNode(0).sequencerInfo(LOG_ID);

  ASSERT_GE(stats["recovery_preempted"], 1);
  ASSERT_EQ(std::to_string(epoch.val_), seq_info["Preempted epoch"]);
  ASSERT_EQ(std::to_string(sealed_by.index()), seq_info["Preempted by"]);
}

// Tests that recovery successfully detects when it's unable to seal due to
// preemption and updates preempted_by.
TEST_P(RecoveryTest, MetadatalogPreempted) {
  nodes_ = 1;
  replication_ = 1;
  init();

  // Make sure recovery actually runs.
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  // Write a seal record with the higher epoch to simulate preemption by another
  // node running recovery.
  const NodeID sealed_by(1, 1);
  const epoch_t seal_epoch(8);
  {
    auto store = cluster_->getNode(0).createLocalLogStore();
    SealMetadata meta(Seal(seal_epoch, sealed_by));
    ASSERT_EQ(0,
              store->getByIndex(SHARD_IDX)->writeLogMetadata(
                  MetaDataLog::metaDataLogID(LOG_ID),
                  meta,
                  LocalLogStore::WriteOptions()));
  }

  // Start the cluster and wait for recovery to finish.
  ld_info("Starting cluster.");
  ASSERT_EQ(0, cluster_->start());
  ld_info("Waiting for recovery.");
  cluster_->waitForRecovery();

  ld_info("Getting stats and sequencer info.");
  auto stats = cluster_->getNode(0).stats();
  auto seq_info = cluster_->getNode(0).sequencerInfo(LOG_ID);

  EXPECT_EQ(std::to_string(sealed_by.index()), seq_info.at("Preempted by"));
  int preempted_epoch = folly::to<int>(seq_info.at("Preempted epoch"));
  EXPECT_LE(2, preempted_epoch);
  EXPECT_GE(8, preempted_epoch);
  int sequencer_epoch = folly::to<int>(seq_info.at("Epoch"));
  std::string state = seq_info.at("State");

  // There are two possible situations:
  if (sequencer_epoch > seal_epoch.val()) {
    // 1. Sequencer was reactivated so many times that it got to an epoch
    //    above our seal. Then it's not preempted. This case is unlikely.
    EXPECT_EQ(9, sequencer_epoch);
    EXPECT_EQ(8, preempted_epoch);
    EXPECT_EQ("ACTIVE", state);
  } else {
    // 2. Sequencer is preempted. It may still be in ACTIVE state because
    //    of how things work: metadata log append+recovery notice preemption
    //    and transition the data sequencer to PREEMPTED, but if the sequencer
    //    gets activated again (e.g. because of a GET_SEQ_STATE message),
    //    it'll become ACTIVE, at least until another metadata log write
    //    attempt notices the preemption again (and even when it does, there's
    //    at lease one code path that doesn't transition sequencer into
    //    PREEMPTED state: when AppenderPrep calls sequencer->checkIfPreempted).
    EXPECT_GE(preempted_epoch, sequencer_epoch);
    EXPECT_TRUE(state == "ACTIVE" || state == "PREEMPTED") << state;
  }
}

// Test the scenario that recovery performs on multiple epochs, each of them
// has different epoch metadata
TEST_P(RecoveryTest, AuthoritativeRecoveryWithNodeSet) {
  nodes_ = 5;
  replication_ = 3; // replication factor of the starting epoch
  extra_ = 0;
  pre_provision_epoch_metadata_ = false; // do not provision metadata
  let_sequencers_provision_metadata_ = false;
  init();

  // NodeSet and replication factor for recovering epochs
  // epoch 1: nodeset {1, 2}     replication factor: 2
  // epoch 2: nodeset {3, 4}     replication factor: 2
  // epoch 3: nodeset {1, 3, 4}  replication factor: 3
  std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N1, N2},
                        ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                        epoch_t(1),
                        epoch_t(1)));
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N3, N4},
                        ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                        epoch_t(2),
                        epoch_t(2)));
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N1, N3, N4},
                        ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                        epoch_t(3),
                        epoch_t(3)));

  {
    auto meta_provisioner = cluster_->createMetaDataProvisioner();

    // By default, metadata log records are stored on all storage
    // nodes with replication factor set to 3
    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);
  }

  provisionInternalLogs(StorageSet{N1, N2, N3},
                        ReplicationProperty{{NodeLocationScope::NODE, 2}});

  const copyset_t copyset_e1 = {N1, N2};
  const copyset_t copyset_e2 = {N3, N4};
  const copyset_t copyset_e3 = {N1, N3, N4};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4) (2,1) (2,2) (2,3) (3,1) (3,2) (3,3)
  // ----------------------------------------------------------------
  // #1  |  x     x           o                            x     o
  // #2  |  x     o
  // #3  |              x                 x     o
  // #4  |                          x           o                x
  //
  // The recovery is expected to replace (1,2), (3,3) with hole plugs, insert
  // a plug at (1,3) and (3,1) replicate (1,4), (2,1), (2,2), and (3,2)
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(4))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(3, 2), ESN_INVALID)
                          .copyset(copyset_e3)
                          .timestamp(std::chrono::milliseconds(22)),
                      TestRecord(LOG_ID, lsn(3, 3), ESN_INVALID)
                          .copyset(copyset_e3)
                          .timestamp(std::chrono::milliseconds(23))
                          .wave(0)
                          .hole(),
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(2))
                          .wave(0)
                          .hole(),
                  });

  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset_e2)
                          .timestamp(std::chrono::milliseconds(12)),
                      TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
                          .copyset(copyset_e2)
                          .timestamp(std::chrono::milliseconds(13))
                          .wave(0)
                          .hole(),
                  });

  prepopulateData(node_index_t(4),
                  {
                      TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                          .copyset(copyset_e2)
                          .timestamp(std::chrono::milliseconds(11)),
                      TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
                          .copyset(copyset_e2)
                          .timestamp(std::chrono::milliseconds(13))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(3, 3), ESN_INVALID)
                          .copyset(copyset_e3)
                          .timestamp(std::chrono::milliseconds(23)),
                  });

  // latest epoch is 3
  populateSoftSeals(epoch_t(2));

  // starting epoch of the cluster is 4, epoch metadata for epoch 4 is
  // the same as in epoch 3
  auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
    if (!info) {
      info = std::make_unique<EpochMetaData>();
    }
    *info = *epoch_metadata[2];
    info->h.epoch = epoch_t(4);
    return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
  };

  {
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  const epoch_t seal_epoch = epoch_t(3); // recovery seals epoch up to 3
  ASSERT_EQ(0, cluster_->start());

  read(lsn(1, 1), lsn(3, 3));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(2, 1),
                lsn(2, 2),
                lsn(3, 2),
            }),
            records_);

  EXPECT_EQ(std::vector<gap_record_t>(
                {// e1n1 is the first and last data record in its epoch
                 gap_record_t(GapType::BRIDGE, lsn(1, 2), lsn(1, ESN_MAX.val_)),
                 gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
                 gap_record_t(GapType::BRIDGE, lsn(2, 3), lsn(2, ESN_MAX.val_)),
                 gap_record_t(GapType::BRIDGE, lsn(3, 0), lsn(3, 0)),
                 gap_record_t(GapType::HOLE, lsn(3, 1), lsn(3, 1)),
                 // e3n3 is the bridge record of epoch 3, deliver a bridge gap
                 // to until_lsn
                 gap_record_t(GapType::BRIDGE, lsn(3, 3), lsn(3, 3))}),
            gaps_);

  // verify the num of log holes stored in EpochRecovery
  EXPECT_EQ(2, summarize_num_hole_plugs());
  printRecordCacheStats();
  cluster_->stop();

  // build and verify digest from nodes in the nodeset of each epoch.
  // mutation shouldn't be needed for each of them
  verifyDigest(*buildDigest(epoch_t(1),
                            esn_t(1),
                            /*shards=*/{N1, N2},
                            /*replication=*/2,
                            sync_replication_scope_,
                            seal_epoch),
               std::vector<esn_t>({}),                   // records
               std::vector<esn_t>({esn_t(3), esn_t(4)}), // plugs
               std::vector<esn_t>({esn_t(2)})            // bridge
  );
  verifyDigest(*buildDigest(epoch_t(2),
                            ESN_INVALID,
                            /*shards=*/{N3, N4},
                            /*replication=*/2,
                            sync_replication_scope_,
                            seal_epoch),
               std::vector<esn_t>({esn_t(1), esn_t(2)}), // records
               std::vector<esn_t>({}),                   // plugs
               std::vector<esn_t>({esn_t(3)})            // bridge
  );
  verifyDigest(*buildDigest(epoch_t(3),
                            ESN_INVALID,
                            /*shards=*/{N1, N3, N4},
                            /*replication=*/3,
                            sync_replication_scope_,
                            seal_epoch),
               std::vector<esn_t>({esn_t(2)}), // records
               std::vector<esn_t>({esn_t(1)}), // plugs
               std::vector<esn_t>({esn_t(3)})  // bridge
  );
}

// Test that recovery should be able to finish in case all nodes (>r-1)
// from a single failure domain are down
TEST_P(RecoveryTest, FailureDomainAuthoritative) {
  nodes_ = 6;
  replication_ = 3;
  extra_ = 0;
  sync_replication_scope_ = NodeLocationScope::REGION;

  // Region 1:  {0, 1, 2}
  // Region 2:  {3, 4}
  // Region 3:  {5}
  Configuration::Nodes node_configs;
  for (int i = 0; i < nodes_; ++i) {
    auto& node = node_configs[i];
    if (i == 5) {
      node.addSequencerRole();
    }
    node.addStorageRole(/*num_shards*/ 2);
    std::string domain_string;
    if (i < 3) {
      domain_string = "region1....";
    } else if (i < 5) {
      domain_string = "region2....";
    } else {
      domain_string = "region3....";
    }
    NodeLocation location;
    location.fromDomainString(domain_string);
    node_configs[i].location = location;
  }
  node_configs_ = node_configs;

  // metadata logs are replicated cross-region
  // use a smaller, cross-region nodeset to ensure all records must be
  // cross-region no matter how their copysets are selected. this is
  // a workaround since we just select copyset randomly in MetaDataProvisioner.
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{0, 3, 5}, /*replication=*/2, NodeLocationScope::REGION);
  metadata_config_ = meta_config;

  pre_provision_epoch_metadata_ = true;

  init();

  const copyset_t copyset = {N0, N1, N2, N3, N4, N5};

  //
  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4) (2,1) (2,2)
  // -----------------------------------------
  // #0  |                                     <- down
  // #1  |                                     <- down
  // #2  |                                     <- down
  // #3  |       x           o            o
  // #4  |       o                        x
  // #5  |  x           x                 o
  //
  // lng = (1, 1) because (1, 1) is fully stored on nodes from {0, 1, 2,}.
  // Recovery should replace (1,2), (2, 2) with hole plugs, replicate (1,3),
  // (1,4) insert a plug at (2,1)
  prepopulateData(node_index_t(0),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1))});

  prepopulateData(node_index_t(2),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1))});

  prepopulateData(node_index_t(3),
                  {TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2)),
                   TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(4))
                       .wave(0)
                       .hole(),
                   TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(11))
                       .wave(0)
                       .hole()});

  prepopulateData(node_index_t(4),
                  {TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .wave(0)
                       .hole(),
                   TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(11))});

  prepopulateData(node_index_t(5),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(3)),
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(11))
                          .wave(0)
                          .hole(),
                  });

  populateSoftSeals(epoch_t(1));
  // recover everything up to epoch 3
  cluster_->setStartingEpoch(LOG_ID, epoch_t(3));
  const epoch_t seal_epoch = epoch_t(2); // recovery seals epoch up to 2

  // only start node {3, 4, 5}
  ASSERT_EQ(0, cluster_->start({3, 4, 5}));

  cluster_->waitForRecovery();

  // start a reader to read the records just got recovered. reading should
  // not stall despite that half of the cluster (node 0, 1, 2) are down
  read(lsn(1, 1), lsn(2, 2));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(1, 3),
            }),
            records_);
  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
                gap_record_t(GapType::BRIDGE, lsn(2, 1), lsn(2, 2)),
            }),
            gaps_);

  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               std::vector<esn_t>({esn_t(3)}), // records
               std::vector<esn_t>({esn_t(2)}), // plugs
               std::vector<esn_t>({esn_t(4)})  // bridge
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({}),         // records
               std::vector<esn_t>({esn_t(2)}), // plugs
               std::vector<esn_t>({esn_t(1)})  // bridge
  );
}

// Like Purging test but 2 nodes are down and being rebuilt. Purging should
// finish without them.
TEST_P(RecoveryTest, NonAuthoritativePurging) {
  nodes_ = 6;
  replication_ = 2;
  extra_ = 0;
  enable_rebuilding_ = true;
  // two logs are used in this test
  num_logs_ = 2;
  single_empty_erm_ = true;
  init();

  const copyset_t copyset = {N1, N2};

  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(2)),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1)),
                  });

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  ld_info("Starting nodes for the first time.");
  ASSERT_EQ(0, cluster_->start({0, 2, 3, 4, 5})); // node 1 is inaccessible
  ld_info("Waiting for recovery.");
  cluster_->waitForRecovery();
  ld_info("Stopping nodes for the first time.");
  cluster_->stop();

  ld_info("Starting nodes for the second time.");
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3})); // nodes 4 and 5 are down

  // epoch should be bumped to 3
  const epoch_t seal_epoch = epoch_t(2); // recovery seals epoch up to 2

  ld_info("Requesting shard rebuildings.");
  auto client = cluster_->createClient();
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 4, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 5, SHARD_IDX));

  // after sequencer sends a RELEASE for e3n0,
  // node 1 should delete record (1, 2)
  ld_info("Waiting for recovery of log 1.");
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  // Check that recovery of the other log is stuck because its shard is not
  // rebuilding.
  ld_info("Grace period for recovery of log 2.");
  int rv = cluster_->getNode(0).waitForRecovery(
      logid_t(2), std::chrono::steady_clock::now() + std::chrono::seconds(1));
  EXPECT_EQ(-1, rv);

  ld_info("Starting more nodes.");
  // need an f-majority to read
  ASSERT_EQ(0, cluster_->start({4}));

  // Now log 2 should be recovered.
  ld_info("Waiting for recovery of log 2.");
  cluster_->getNode(0).waitForRecovery(logid_t(2));

  ld_info("Reading.");
  read(lsn(1, 1), lsn(3, 0));
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1)}), records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::BRIDGE, lsn(1, 2), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(3, 0)),
            }),
            gaps_);

  ld_info("Getting stats.");
  auto stats = cluster_->getNode(1).stats();

  ld_info("Done. Stopping nodes.");
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               {},
               {},
               std::vector<esn_t>({esn_t(2)}) // bridge
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch), {}, {}, {});
}

// Test to verify that per-epoch log metadata is stored for nodes participated
// in epoch recovery
TEST_P(RecoveryTest, PerEpochLogMetadata) {
  nodes_ = 5; // four storage nodes
  replication_ = 2;
  extra_ = 0;
  // prepopulate metadata logs to be able to do exact epoch math
  pre_provision_epoch_metadata_ = true;
  init();

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4)
  // ----------------------------
  // #1  |  x     x           x
  // #2  |  x           x
  // #3  |              x
  // #4  |
  const copyset_t copyset = {N1, N2};
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset({N1, N4})
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset({N1, N3})
                          .timestamp(std::chrono::milliseconds(4)),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(3)),
                  });
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(3)),
                  });

  epoch_size_map.setCounter(BYTE_OFFSET, 8);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 8);
  tail_record.reset();
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 8);
  const EpochRecoveryMetadata expected_metadata_epoch_1(
      epoch_t(2), // sequencer epoch
      esn_t(1),   // last known good
      esn_t(3),   // last digest record
      0,          // flags
      tail_record,
      epoch_size_map,
      epoch_end_offsets);

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  ASSERT_EQ(0, cluster_->start({0, 2, 3, 4})); // node 1 is inaccessible
  cluster_->waitForRecovery();
  cluster_->stop();

  // examine the local log store for the per-epoch log metadata
  for (int i = 0; i < nodes_; ++i) {
    if (!cluster_->getConfig()
             ->get()
             ->serverConfig()
             ->getNode(i)
             ->isReadableStorageNode()) {
      continue;
    }

    auto store = cluster_->getNode(i).createLocalLogStore();
    EpochRecoveryMetadata metadata;
    int rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(1), &metadata);
    if (i == 1) {
      // for node 1, there should be no such metadata since it is down
      ASSERT_EQ(-1, rv);
      ASSERT_EQ(E::NOTFOUND, err);
    } else {
      ASSERT_EQ(0, rv);
      ASSERT_TRUE(metadata.valid());
      ASSERT_EQ(expected_metadata_epoch_1, metadata);
    }
  }

  // start all nodes this time; after sequencer sends a RELEASE for e3n0, node 1
  // should delete record (1, 2) and (1, 4)
  ASSERT_EQ(0, cluster_->start());

  // epoch should be bumped to 3
  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 1

  ld_info("Waiting for N1 to purge");
  {
    // start reader to trigger RELEASEs to be sent because of the initial
    // log storage state recover, which triggers purging
    auto client = cluster_->createClient();
    auto reader = client->createReader(1);
    cluster_->getNode(1).waitForPurge(LOG_ID, epoch_t(3));
  }

  ld_info("Purging done");
  read(lsn(1, 1), lsn(3, 0));
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1), lsn(1, 3)}), records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(3, 0)),
            }),
            gaps_);
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               {esn_t(3)}, // records
               {esn_t(2)}, // hole plugs
               {esn_t(4)}  // bridge
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch), {}, {}, {});

  {
    // examine Node 1, it should advance its local LCE to epoch 2 and it will
    // get epoch recovery metadata from other nodes and store it in its local
    // log store.
    //
    // Moreover, Node 1 will not have epoch recovery metadata for epoch 2 since
    // the epoch is empty.
    auto store = cluster_->getNode(1).createLocalLogStore();
    LastCleanMetadata lce_metadata;
    int rv =
        store->getByIndex(SHARD_IDX)->readLogMetadata(LOG_ID, &lce_metadata);
    ASSERT_EQ(epoch_t(2), lce_metadata.epoch_);
    EpochRecoveryMetadata recovery_metadata;
    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(1), &recovery_metadata);

    ASSERT_EQ(0, rv);
    ASSERT_TRUE(recovery_metadata.valid());
    ASSERT_EQ(expected_metadata_epoch_1, recovery_metadata);

    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(2), &recovery_metadata);
    ASSERT_EQ(-1, rv);
    ASSERT_EQ(E::NOTFOUND, err);
  }
}

// When R=2 and there are 2 nodes empty (in repair) while another node is down,
// recovery and purging should be able to make progress. Recovery should be
// authoritative (it should plug holes).
TEST_P(RecoveryTest, AuthoritativeRecoveryAndPurgingWithRNodesEmpty) {
  nodes_ = 10;
  replication_ = 2;
  extra_ = 0;
  enable_rebuilding_ = true;
  pre_provision_epoch_metadata_ = false; // do not provision metadata
  let_sequencers_provision_metadata_ = false;
  init();

  // Set the nodeset of LOGID to {5, 6, 7, 8, 9}
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N5, N6, N7, N8, N9},
                          ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }
  // Set the nodeset of the event log to {0, 1, 2, 3}. These nodes will all
  // remain available during the test.
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N0, N1, N2, N3},
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  // Start the cluster a first time without nodes 8, 9 and start their
  // rebuilding (nothing will be rebuilt).
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3, 4, 5, 6, 7}));
  ld_info("Requesting shard rebuildings.");
  auto client = cluster_->createClient();
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 8, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 9, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, 8, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, 9, SHARD_IDX));

  ld_info("Waiting for rebuilding to complete");
  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, {N8, N9}, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);

  cluster_->stop();

  const copyset_t copyset56 = {N5, N6};
  const copyset_t copyset57 = {N5, N7};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (2,1) (2,2) (2,3) (2,4) (3,1) (3,2) (3,3)
  // --------------------------------------------------
  // #5  |  x     x           o           x          <- authoritative
  // #6  |  x     o                             x    <- authoritative
  // #7  |        x           x                      <- down
  // #8  |                                           <- empty and down
  // #9  |                                           <- empty and down
  //
  // Nodes #8 and #9 are down but known as empty. Node #7 is down.
  // This results in an authoritative recovery.
  // Recovery will plug the holes at (2,3) and (3,1), replicate (2,1), (2,4),
  // (3,3), and replace (2, 2) with a hole plug.
  // Purging on N7 will remove (2,2) and (2,4)

  prepopulateData(node_index_t(5),
                  {
                      TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(2, 2), esn_t(1))
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(2, 4), esn_t(1))
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(4))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(3, 2), ESN_INVALID)
                          .copyset(copyset57)
                          .timestamp(std::chrono::milliseconds(11)),
                  });

  prepopulateData(node_index_t(6),
                  {
                      TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(2, 2), esn_t(1))
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(2))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(3, 3), ESN_INVALID)
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(12)),
                  });

  prepopulateData(node_index_t(7),
                  {
                      TestRecord(LOG_ID, lsn(2, 2), esn_t(1))
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(2, 4), esn_t(1))
                          .copyset(copyset56)
                          .timestamp(std::chrono::milliseconds(4)),
                  });

  populateSoftSeals(epoch_t(2));
  // recover everything up to epoch 4
  cluster_->setStartingEpoch(LOG_ID, epoch_t(4), epoch_t(2));

  const epoch_t seal_epoch = epoch_t(3); // recovery seals epoch up to 3

  ld_info("Starting nodes"); // N7 remains down, N8,N9 are down and empty.
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3, 4, 5, 6}));
  ld_info("Waiting for recovery");
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  ld_info("Starting N7");
  ASSERT_EQ(0, cluster_->start({7}));

  // Wait until N7 finishes purging, ie log storage state for the log reports a
  // LSN >= e4n0.
  ld_info("Waiting for N7 to finish purging");
  {
    auto client = cluster_->createClient();
    auto reader = client->createReader(1);
    reader->startReading(LOG_ID, LSN_OLDEST);
    cluster_->getNode(7).waitForPurge(LOG_ID, epoch_t(4));
  }

  // Purging should have remove (2,2) and (2,4)
  auto stats = cluster_->getNode(7).stats();

  ld_info("Done. Stopping nodes.");
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(2), esn_t(1), seal_epoch),
               std::vector<esn_t>({}),                   // records
               std::vector<esn_t>({esn_t(3), esn_t(4)}), // plugs
               std::vector<esn_t>({esn_t(2)})            // bridges
  );
  verifyDigest(*buildDigest(epoch_t(3), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({esn_t(2), esn_t(3)}), // records
               std::vector<esn_t>({esn_t(1)}),           // plugs
               std::vector<esn_t>({esn_t(4)})            // bridges
  );
}

// Too many nodes are rebuilding, causing recovery to be non authoritative (and
// thus not plug holes) but also to not be able to fully replicate
// holes/records.
TEST_P(RecoveryTest, RecoveryCannotFullyReplicate) {
  nodes_ = 10;
  replication_ = 3;
  extra_ = 0;
  enable_rebuilding_ = true;
  pre_provision_epoch_metadata_ = false; // do not provision metadata
  let_sequencers_provision_metadata_ = false;
  init();

  // Set the nodeset of LOGID to {5, 6, 7, 8, 9}
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N5, N6, N7, N8, N9},
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }
  // Set the nodeset of the event log to {0, 1, 2, 3}. These nodes will all
  // remain available during the test.
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N0, N1, N2, N3},
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  const copyset_t copyset568 = {N5, N6, N8};
  const copyset_t copyset579 = {N5, N7, N9};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4) (2,1) (2,2) (2,3)
  // --------------------------------------------------
  // #5  |  x     x           o           x          <- authoritative
  // #6  |  x     o                             x    <- authoritative
  // #7  |                                           <- down and rebuilding
  // #8  |                                           <- down and rebuilding
  // #9  |                                           <- down and rebuilding
  //
  // nodes #7, #8 and #9 are non-authoritative. This results in
  // non-authoritative recovery. The holes at 1.3 and 2.1 remain unplugged and
  // are expected to be reported as data loss to readers. In addition the
  // recovery is expected to replace (1,2) with a hole plug, replicate (1,4),
  // and replicate (2,2) and (2,3). R=3 but only 2 nodes are available which
  // means recovery will not be able to fully replicate records.

  prepopulateData(node_index_t(5),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(4))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                          .copyset(copyset579)
                          .timestamp(std::chrono::milliseconds(11)),
                  });

  prepopulateData(node_index_t(6),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(1)),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(2))
                          .wave(0)
                          .hole(),
                      TestRecord(LOG_ID, lsn(2, 3), ESN_INVALID)
                          .copyset(copyset568)
                          .timestamp(std::chrono::milliseconds(12)),
                  });

  populateSoftSeals(epoch_t(1));
  // recover everything up to epoch 3
  cluster_->setStartingEpoch(LOG_ID, epoch_t(3));

  const epoch_t seal_epoch = epoch_t(2); // recovery seals epoch up to 2

  // Start all nodes but  7,8,9
  ld_info("Starting nodes");
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3, 4, 5, 6}));

  ld_info("Requesting shard rebuildings.");
  auto client = cluster_->createClient();
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 7, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 8, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 9, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, 7, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, 8, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, markShardUnrecoverable(*client, 9, SHARD_IDX));

  ld_info("Waiting for recovery.");
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  // Reading should be able to make progress with a non authoritative majority.
  ld_info("Reading.");
  read(lsn(1, 1), lsn(2, 3));

  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(2, 2),
                lsn(2, 3),
            }),
            records_);
  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::DATALOSS, lsn(1, 3), lsn(1, 3)),
                gap_record_t(GapType::HOLE, lsn(1, 4), lsn(1, 4)),
                gap_record_t(GapType::BRIDGE, lsn(1, 5), lsn(2, 1)),
            }),
            gaps_);

  // verify the num of log holes stored in EpochRecovery
  EXPECT_EQ(0, summarize_num_hole_plugs());

  ld_info("Getting stats.");
  auto stats = cluster_->getNode(0).stats();
  EXPECT_GT(stats["non_auth_recovery_epochs"], 0);

  // 4 records are mutated as best effort but stays underreplication
  EXPECT_EQ(4, stats["epoch_recovery_record_underreplication_datalog"]);

  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               std::vector<esn_t>({}),                   // records
               std::vector<esn_t>({esn_t(2), esn_t(4)}), // plugs
               // brdige records not inserted for non-authoritative recovery
               std::vector<esn_t>({}),
               false /* expect_fully_replicated */
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({esn_t(2), esn_t(3)}), // records
               std::vector<esn_t>({}),                   // plugs
               // brdige records not inserted for non-authoritative recovery
               std::vector<esn_t>({}),
               false /* expect_fully_replicated */
  );
}

// Purging should be able to complete for just one node available that has
// participated in epoch recovery (except the condition that the epoch is not
// empty locally and it requires R E::EMPTY replies, see notes in
// PurgeSingleEpoch.h).
// In this test, N1 will be absent for recovery, and N2, N3, N4, N5 paticipated
// in recovery. After recovery is done, N2, N3, N4 becomes unavailable. However,
// N1 should still be able to finish purging for all recovered epochs given N5
// is up
TEST_P(RecoveryTest, PurgingAvailabilityTest) {
  nodes_ = 6;
  replication_ = 2; // replication factor of the starting epoch
  extra_ = 0;
  pre_provision_epoch_metadata_ = false; // do not provision metadata
  let_sequencers_provision_metadata_ = false;
  single_empty_erm_ = true;

  // metadata log is only stored in N5, which is the only clean node available
  // later in the test

  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{5}, /*max_replication=*/1, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;
  metadata_config_ = meta_config;

  init();

  // NodeSet and replication factor for recovering epochs
  // epoch 1: nodeset {1, 2, 5}     replication factor: 2
  // epoch 2: nodeset {1, 4, 5}     replication factor: 2
  // epoch 3: nodeset {1, 3, 4, 5}  replication factor: 3
  std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N1, N2, N5},
                        ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                        epoch_t(1),
                        epoch_t(1)));
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N1, N4, N5},
                        ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                        epoch_t(2),
                        epoch_t(2)));
  epoch_metadata.emplace_back(
      new EpochMetaData(StorageSet{N1, N3, N4, N5},
                        ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                        epoch_t(3),
                        epoch_t(3)));

  {
    auto meta_provisioner = cluster_->createMetaDataProvisioner();

    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);

    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[2];
      info->h.epoch = epoch_t(3);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };

    rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  provisionInternalLogs(StorageSet{N3, N4, N5},
                        ReplicationProperty{{NodeLocationScope::NODE, 2}});

  // currently copyset is not used in recovery, so it is OK to have inconsistent
  // values
  const copyset_t copyset_e1 = {N1, N2};
  const copyset_t copyset_e2 = {N4, N5};
  const copyset_t copyset_e3 = {N1, N3, N4, N5};
  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4) (3,1)
  // ------------------------------------
  // #1  |  x     o     x     x              <--- absent during recovery
  // #2  |        x
  // #3  |
  // #4  |                          x
  // #5  |  x           x
  //
  // The recovery is expected to replicate (1,2), (1,3), (3, 1)
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(2))
                          .hole(),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(4)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(2)),
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(2)),
                  });

  prepopulateData(node_index_t(4),
                  {
                      TestRecord(LOG_ID, lsn(3, 1), ESN_INVALID)
                          .copyset(copyset_e3)
                          .timestamp(std::chrono::milliseconds(21)),
                  });

  prepopulateData(node_index_t(5),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset(copyset_e1)
                          .timestamp(std::chrono::milliseconds(4)),
                  });

  // latest epoch is 3
  populateSoftSeals(epoch_t(2));
  const epoch_t seal_epoch = epoch_t(3); // recovery seals epoch up to 3

  // N1 did not participated in recovery
  ASSERT_EQ(0, cluster_->start({0, 2, 3, 4, 5}));

  read(lsn(1, 1), lsn(3, 1));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(1, 2),
                lsn(1, 3),
                lsn(3, 1),
            }),
            records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(3, 0)),
            }),
            gaps_);

  // verify the num of log holes stored in EpochRecovery
  EXPECT_EQ(0, summarize_num_hole_plugs());

  // stop N2, N3, N4, start N1
  cluster_->getNode(2).suspend();
  cluster_->getNode(3).suspend();
  cluster_->getNode(4).suspend();
  cluster_->getNode(1).start();
  cluster_->getNode(1).waitUntilStarted();

  // read one record to trigger logstoragestate recover on N1 and eventually
  // on purging on N1
  ld_info("Waiting for N1 to finish purging");
  {
    auto client = cluster_->createClient();
    // Start reading, to make the node recover its log state, which should
    // trigger purging.
    auto reader = client->createReader(1);
    reader->startReading(LOG_ID, lsn(1, 1));
    cluster_->getNode(1).waitForPurge(LOG_ID, epoch_t(4));
  }
  cluster_->stop();

  // build and verify digest from nodes in the nodeset of each epoch.
  // mutation shouldn't be needed for each of them
  verifyDigest(*buildDigest(epoch_t(1),
                            esn_t(1),
                            /*shards=*/{N1, N2, N5},
                            /*replication=*/2,
                            NodeLocationScope::NODE,
                            seal_epoch),
               std::vector<esn_t>({esn_t(2), esn_t(3)}), // records
               std::vector<esn_t>({}),                   // plugs
               std::vector<esn_t>({esn_t(4)})            // bridges
  );
  verifyDigest(*buildDigest(epoch_t(2),
                            ESN_INVALID,
                            /*shards=*/{N1, N4, N5},
                            /*replication=*/2,
                            NodeLocationScope::NODE,
                            seal_epoch),
               std::vector<esn_t>({}), // records
               std::vector<esn_t>({}), // plugs
               std::vector<esn_t>({})  // bridges
  );
  verifyDigest(*buildDigest(epoch_t(3),
                            ESN_INVALID,
                            /*shards=*/{N1, N3, N4, N5},
                            /*replication=*/3,
                            NodeLocationScope::NODE,
                            seal_epoch),
               std::vector<esn_t>({esn_t(1)}), // records
               std::vector<esn_t>({}),         // plugs
               std::vector<esn_t>({esn_t(2)})  // bridges
  );
  {
    epoch_size_map.setCounter(BYTE_OFFSET, 12);
    epoch_end_offsets.setCounter(BYTE_OFFSET, 12);
    tail_record.offsets_map_.setCounter(BYTE_OFFSET, 12);
    const EpochRecoveryMetadata expected_metadata_epoch_1(
        epoch_t(4), // sequencer epoch
        esn_t(1),   // last known good
        esn_t(3),   // last digest record
        0,          // flags
        tail_record,
        epoch_size_map,
        epoch_end_offsets);

    tail_record.offsets_map_.setCounter(BYTE_OFFSET, 16);
    epoch_end_offsets.setCounter(BYTE_OFFSET, 16);
    epoch_size_map.setCounter(BYTE_OFFSET, 4);
    const EpochRecoveryMetadata expected_metadata_epoch_3(
        epoch_t(4),  // sequencer epoch
        ESN_INVALID, // last known good
        esn_t(1),    // last digest record
        0,           // flags
        tail_record,
        epoch_size_map,
        epoch_end_offsets);

    // examine Node 1, it should advance its local LCE to at least epoch 3, and
    // it does not have EpochRecoveryMetadata for epoch 2 because the epoch
    // is empty. And it should have EpochRecoveryMetadata for epoch 3
    auto store = cluster_->getNode(1).createLocalLogStore();
    LastCleanMetadata lce_metadata;
    int rv =
        store->getByIndex(SHARD_IDX)->readLogMetadata(LOG_ID, &lce_metadata);
    ASSERT_LE(epoch_t(3), lce_metadata.epoch_);
    EpochRecoveryMetadata recovery_metadata;
    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(1), &recovery_metadata);
    ASSERT_EQ(0, rv);
    ASSERT_TRUE(recovery_metadata.valid());
    ASSERT_EQ(expected_metadata_epoch_1, recovery_metadata);
    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(2), &recovery_metadata);
    ASSERT_EQ(-1, rv);
    ASSERT_EQ(E::NOTFOUND, err);
    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(3), &recovery_metadata);
    ASSERT_EQ(0, rv);
    ASSERT_TRUE(recovery_metadata.valid());
    ASSERT_EQ(expected_metadata_epoch_3, recovery_metadata);
  }
}

// test the case that Digest is incomplete
TEST_P(RecoveryTest, IncompleteDigest) {
  nodes_ = 5; // 5 nodes, N0 is sequencer only, N4 is absent from recovery
  replication_ = 3;
  extra_ = 0;
  // prepopulate metadata logs to be able to keep track of the exact number of
  // mutations
  pre_provision_epoch_metadata_ = true;
  init();

  const copyset_t copyset = {N1, N2, N3, N4};
  // EpochRecovery instance is running with seal epoch of 7
  // Storage nodes contain the following records (x_w or x(e)) and
  // plugs (o(e)) (x(e) and o(e) mean that the record is written
  // by recovery with seal epoch of e, x_w means that the record is
  // written by the sequencer with wave w):
  //
  //      (1,1) (1,2) (1,3)       LNG=0
  // ------------------------
  // #1  | x_1   o(6)  x(7)
  // #2  | x_1   o(6)  x(7)
  // #3  | x_1   o(6)  x(7)
  // #4  |                                         <-- down and absent
  //
  // expected outcome:
  // (1,1)  x(7)  amend N1, N2 and N3
  // (1,2)  o(7): amend N1, N2 and N3
  // (1,3)  x(7): no mutation needed
  prepopulateData(node_index_t(1),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1))
                       .wave(1),
                   TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .writtenByRecovery(6)
                       .hole(),
                   TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(7)});
  prepopulateData(node_index_t(2),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1))
                       .wave(1),
                   TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .writtenByRecovery(6)
                       .hole(),
                   TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(7)});
  prepopulateData(node_index_t(3),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1))
                       .wave(1),
                   TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .writtenByRecovery(6)
                       .hole(),
                   TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(7)});

  cluster_->setStartingEpoch(LOG_ID, epoch_t(8));
  const epoch_t seal_epoch = epoch_t(7);       // recovery seals epoch up to 2
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3})); // N4 did not participate
  read(lsn(1, 1), lsn(1, 233));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(1, 3),
            }),
            records_);
  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, 233)),
            }),
            gaps_);

  EXPECT_EQ(0, summarize_num_hole_plugs());

  expectAuthoritativeOnly();
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({esn_t(1), esn_t(3)}), // records
               std::vector<esn_t>({esn_t(2)}),           // plugs
               std::vector<esn_t>({esn_t(4)})            // bridges
  );
}

// Test the bug fixed by D4187744. Without D4187744, upon receiving
// releases, we are dropping epochs from unclean epochs too early in
// record cache. This would cause purging to consider the epoch empty
// since it was already evicted from the cache.
TEST_P(RecoveryTest, D4187744) {
  nodes_ = 5; // four storage nodes
  replication_ = 2;
  extra_ = 0;
  // prepopulate metadata logs to avoid spontaneous sequencer reactivations
  pre_provision_epoch_metadata_ = true;
  init();

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4)
  // ----------------------------
  // #1  |  x     x           x
  // #2  |  x           x
  // #3  |              x
  // #4  |
  const copyset_t copyset = {N1, N2};
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 2), esn_t(1))
                          .copyset({N1, N4})
                          .timestamp(std::chrono::milliseconds(2)),
                      TestRecord(LOG_ID, lsn(1, 4), esn_t(1))
                          .copyset({N1, N3})
                          .timestamp(std::chrono::milliseconds(4)),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 4}})),
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(3)),
                  });
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), esn_t(1))
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(3)),
                  });

  epoch_size_map.setCounter(BYTE_OFFSET, 8);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 8);
  tail_record.offsets_map_.setCounter(BYTE_OFFSET, 8);
  const EpochRecoveryMetadata expected_metadata_epoch_1(
      epoch_t(2), // sequencer epoch
      esn_t(1),   // last known good
      esn_t(3),   // last digest record
      0,          // flags
      tail_record,
      epoch_size_map,
      epoch_end_offsets);

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  ASSERT_EQ(0, cluster_->start({0, 2, 3, 4})); // node 1 is inaccessible
  cluster_->waitForRecovery();

  // start node 1
  cluster_->getNode(1).start();

  // write one record in epoch 2, triggering release to be sent to N1
  auto client = cluster_->createClient();
  lsn_t last_lsn = client->appendSync(LOG_ID, Payload("test", 4));
  ASSERT_NE(LSN_INVALID, last_lsn);

  // after sequencer sends a RELEASE for e2 node 1 should delete record (1, 2)
  // and (1, 4)
  ASSERT_EQ(0, cluster_->start());

  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 1
  ld_info("Waiting for N1 to purge");
  {
    // start reader to trigger RELEASEs to be sent because of the initial
    // log storage state recover, which triggers purging
    auto read_client = cluster_->createClient();
    auto reader = read_client->createReader(1);
    cluster_->getNode(1).waitForPurge(LOG_ID, epoch_t(2));
  }

  ld_info("Purging done");
  read(lsn(1, 1), last_lsn);
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1), lsn(1, 3), last_lsn}), records_);
  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
            }),
            gaps_);
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               {esn_t(3)},  // records
               {esn_t(2)},  // hole plugs
               {esn_t(4)}); // bridge

  {
    // examine Node 1 it will get epoch recovery metadata from
    // other nodes and store it in its local log store.
    auto store = cluster_->getNode(1).createLocalLogStore();
    LastCleanMetadata lce_metadata;
    int rv =
        store->getByIndex(SHARD_IDX)->readLogMetadata(LOG_ID, &lce_metadata);
    ASSERT_EQ(epoch_t(1), lce_metadata.epoch_);
    EpochRecoveryMetadata recovery_metadata;
    rv = store->getByIndex(SHARD_IDX)->readPerEpochLogMetadata(
        LOG_ID, epoch_t(1), &recovery_metadata);

    ASSERT_EQ(0, rv);
    ASSERT_TRUE(recovery_metadata.valid());
    ASSERT_EQ(expected_metadata_epoch_1, recovery_metadata);
  }
}

// test the case that there are different bridge records existed because of
// previous unfinished epoch recovery effort
TEST_P(RecoveryTest, BridgeRecords) {
  nodes_ = 4;
  replication_ = 3;
  extra_ = 0;

  init();

  const copyset_t copyset = {N1, N2, N3};

  // EpochRecovery instance is running with seal epoch of 7.
  // Storage nodes contain the following records (x, x_w or x(e)),
  // plugs o(e) and bridge b(e) (x(e), o(e), and b(e) mean that the record
  // is written by recovery with seal epoch of e, x means that the record was
  // written by the sequencer with wave 1, while x_w means that the record was
  // written by the sequencer with wave w):
  //
  //      (1,1) (2,1)  (2,2) (4,1) (4,2) (4,3)
  // -----------------------------------------
  // #1  |  x    b(6)         b(4)
  // #2  |  x           x(2)        b(2)
  // #3  |  x    x(5)   x(2)  x(5)  o(3)  x(5)
  //
  // expected outcome:
  // (1,1)  no mutation: since its esn == lng
  // (1,2)  b(7): store N1, N2, and N3, bridge record for epoch 1
  // (2,1)  b(7): amend N1, store N2, overwrite N3, bridge for epoch 2
  // (2,2)  o(7): store N1, amend N2 and N3
  //    epoch 3 is empty and no bridge record will be stored
  // (4,1)  x(7): overwrite N1, store N2, amend N3
  // (4,2)  o(7): store N1, amend N2 and N3
  // (4,3)  x(7): store N1 and N2, amend N3
  // (4,4)  b(7): store N1, N2, N3, bridge record for epoch 4

  prepopulateData(node_index_t(1),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1)),
                   TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .writtenByRecovery(6)
                       .bridge(),
                   TestRecord(LOG_ID, lsn(4, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(4))
                       .writtenByRecovery(4)
                       .bridge()});

  prepopulateData(node_index_t(2),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1)),
                   TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(2)
                       .hole(),
                   TestRecord(LOG_ID, lsn(4, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(5))
                       .writtenByRecovery(2)
                       .bridge()});

  prepopulateData(node_index_t(3),
                  {TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(1)),
                   TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(2))
                       .writtenByRecovery(5),
                   TestRecord(LOG_ID, lsn(2, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(2),
                   TestRecord(LOG_ID, lsn(4, 1), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(4))
                       .writtenByRecovery(5),
                   TestRecord(LOG_ID, lsn(4, 2), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(5))
                       .writtenByRecovery(3)
                       .hole(),
                   TestRecord(LOG_ID, lsn(4, 3), ESN_INVALID)
                       .copyset(copyset)
                       .timestamp(std::chrono::milliseconds(6))
                       .writtenByRecovery(5)});

  // latest epoch of store is 4, soft seal 3
  populateSoftSeals(epoch_t(3));

  cluster_->setStartingEpoch(LOG_ID, epoch_t(8));
  const epoch_t seal_epoch = epoch_t(7); // recovery seals epoch up to 2

  ASSERT_EQ(0, cluster_->start());

  read(lsn(1, 1), lsn(4, 4));
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, 1), lsn(4, 1), lsn(4, 3)}), records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::BRIDGE, lsn(1, 2), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
                gap_record_t(GapType::BRIDGE, lsn(2, 1), lsn(2, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(3, 0), lsn(4, 0)),
                gap_record_t(GapType::HOLE, lsn(4, 2), lsn(4, 2)),
                gap_record_t(GapType::BRIDGE, lsn(4, 4), lsn(4, 4)),
            }),
            gaps_);

  expectAuthoritativeOnly();
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(1), seal_epoch),
               std::vector<esn_t>({}),        // records
               std::vector<esn_t>({}),        // plugs
               std::vector<esn_t>({esn_t(2)}) // bridge
  );
  verifyDigest(*buildDigest(epoch_t(2), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({}),         // records
               std::vector<esn_t>({esn_t(2)}), // plugs
               std::vector<esn_t>({esn_t(1)})  // bridge
  );
  // epoch 3 is empty
  verifyDigest(*buildDigest(epoch_t(3), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({}), // records
               std::vector<esn_t>({}), // plugs
               std::vector<esn_t>({})  // bridge
  );
  verifyDigest(*buildDigest(epoch_t(4), ESN_INVALID, seal_epoch),
               std::vector<esn_t>({esn_t(1), esn_t(3)}), // records
               std::vector<esn_t>({esn_t(2)}),           // plugs
               std::vector<esn_t>({esn_t(4)})            // bridge
  );
}

// Test the scenario that epoch recovery is essentially skipped because all
// nodes in the recovery set are in the rebuilding set. Purging later should
// still be able to finish with all nodes up, depite leaving the epoch in
// an inconsistent state.
TEST_P(RecoveryTest, PurgingAfterSkippedNonAuthoritativeRecovery) {
  nodes_ = 7;
  replication_ = 2;
  extra_ = 0;
  enable_rebuilding_ = true;
  // only one data log
  num_logs_ = 1;
  pre_provision_epoch_metadata_ = false;
  single_empty_erm_ = false;

  // metadata log is only stored in N6
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig({6}, 1, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;
  metadata_config_ = meta_config;

  init();

  // Set the nodeset of LOGID to {1, 2, 3, 4, 5}
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N1, N2, N3, N4, N5},
                          ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
  }
  // Set the nodeset of the event log to {6}. These nodes will all
  // remain available during the test.
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N6},
                          ReplicationProperty({{NodeLocationScope::NODE, 1}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  const copyset_t copyset12 = {N1, N2};
  const copyset_t copyset34 = {N3, N4};
  const copyset_t copyset35 = {N3, N5};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3)
  // ----------------------
  // #1  |  x
  // #2  |  x
  // #3  |              o
  // #4  |        x
  // #5  |              x

  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1)),
                  });
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1)),
                  });
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                          .copyset(copyset35)
                          .wave(0)
                          .hole()
                          .timestamp(std::chrono::milliseconds(3)),
                  });
  prepopulateData(node_index_t(4),
                  {
                      TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                          .copyset(copyset34)
                          .timestamp(std::chrono::milliseconds(3)),
                  });
  prepopulateData(node_index_t(5),
                  {
                      TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                          .copyset(copyset35)
                          .timestamp(std::chrono::milliseconds(3)),
                  });

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));

  ASSERT_EQ(0, cluster_->start({0, 6}));
  ld_info("Requesting shard rebuildings.");
  auto client = cluster_->createClient();
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 1, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 2, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 3, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 4, SHARD_IDX));
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 5, SHARD_IDX));

  ld_info("Waiting for the first non-authoritative recovery.");
  cluster_->waitForRecovery();

  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {N1, N2, N3, N4, N5},
      AuthoritativeStatus::AUTHORITATIVE_EMPTY,
      true);

  cluster_->stop();
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3, 4, 5, 6}));

  IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client,
      {N1, N2, N3, N4, N5},
      AuthoritativeStatus::FULLY_AUTHORITATIVE,
      true);

  ld_info("Waiting for epoch recovery to epoch 3.");
  cluster_->waitForRecovery();

  // for every single node in the nodeset, expect its local LCE in epoch 2 and
  // last released in epoch 3.
  for (int i = 1; i < 6; ++i) {
    cluster_->getNode(i).waitForPurge(LOG_ID, epoch_t(2));
    auto stats = cluster_->getNode(1).stats();
  }

  ld_info("Done. Stopping nodes.");
  cluster_->stop();
}

// test the condition where LNG is near ESN_MAX, this used to trigger an integer
// overflow bug which is fixed in D4454692
TEST_P(RecoveryTest, LNGNearESNMAX) {
  nodes_ = 4; // four storage nodes
  replication_ = 2;
  extra_ = 0;
  init();

  const copyset_t copyset = {N1, N2};

  // only one record in ESN_MAX needs to be recovered,
  prepopulateData(
      node_index_t(1),
      {
          TestRecord(LOG_ID, lsn(1, ESN_MAX.val_), esn_t(ESN_MAX.val_ - 1))
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(1)),
      });
  prepopulateData(
      node_index_t(2),
      {
          TestRecord(LOG_ID, lsn(1, ESN_MAX.val_), esn_t(ESN_MAX.val_ - 1))
              .copyset(copyset)
              .timestamp(std::chrono::milliseconds(1)),
      });

  // sequencer crashed and a replacement is starting with epoch 2
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));
  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 1

  ASSERT_EQ(0, cluster_->start());
  read(lsn(1, ESN_MAX.val_), lsn(2, 0));
  EXPECT_EQ(std::vector<lsn_t>({lsn(1, ESN_MAX.val_)}), records_);
  EXPECT_EQ(std::vector<gap_record_t>(
                {gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0))}),
            gaps_);
  cluster_->stop();

  verifyDigest(*buildDigest(epoch_t(1), esn_t(ESN_MAX.val_ - 1), seal_epoch),
               std::vector<esn_t>({ESN_MAX}), // records
               {},                            // plugs
               {});                           // bridge
}

TEST_P(RecoveryTest, TailRecordAtLNG) {
  nodes_ = 4;
  replication_ = 3;
  extra_ = 0;

  init();
  const copyset_t copyset = {N1, N2, N3};
  // tail is (1,1) which already got per-epoch released
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(1), OffsetMap({{BYTE_OFFSET, 0}})}}});
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(1), OffsetMap({{BYTE_OFFSET, 0}})}}});
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(1), OffsetMap({{BYTE_OFFSET, 17}})}}});

  // latest epoch of store is 2, soft seal 1
  populateSoftSeals(epoch_t(1));
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));
  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 2
  ASSERT_EQ(0, cluster_->start());

  read(lsn(1, 1), lsn(1, 1));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
            }),
            records_);
  expectAuthoritativeOnly();
  printRecordCacheStats();

  // get the tail attribute for log 1
  auto client = cluster_->createClient();
  auto tail = client->getTailAttributesSync(LOG_ID);
  ASSERT_NE(nullptr, tail);

  ASSERT_EQ(lsn(1, 1), tail->last_released_real_lsn);
  ASSERT_EQ(std::chrono::milliseconds(1), tail->last_timestamp);
  ASSERT_EQ(17, tail->offsets.getCounter(BYTE_OFFSET));
  cluster_->stop();
}

// test the new Digest and Mutation mechanism that supports
// immutatble consensus
TEST_P(RecoveryTest, TailRecordAtLNGDataLoss) {
  nodes_ = 4;
  replication_ = 3;
  extra_ = 0;

  init();
  const copyset_t copyset = {N1, N2, N3};
  // per-epoch released to (1, 2), however all records of (1, 2) were lost
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(2), OffsetMap({{BYTE_OFFSET, 67}})}}});
  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(2), OffsetMap({{BYTE_OFFSET, 67}})}}});
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(1))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                  },
                  {{epoch_t(1), {esn_t(2), OffsetMap({{BYTE_OFFSET, 67}})}}});
  // latest epoch of store is 2, soft seal 1
  populateSoftSeals(epoch_t(1));
  cluster_->setStartingEpoch(LOG_ID, epoch_t(2));
  const epoch_t seal_epoch = epoch_t(1); // recovery seals epoch up to 2
  ASSERT_EQ(0, cluster_->start());
  expectAuthoritativeOnly();
  printRecordCacheStats();
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  // get the tail attribute for log 1
  auto client = cluster_->createClient();
  auto tail = client->getTailAttributesSync(LOG_ID);
  ASSERT_NE(nullptr, tail);

  ASSERT_EQ(lsn(1, 2), tail->last_released_real_lsn);
  // TODO T20425135: report dataloss for tail record
  ASSERT_EQ(std::chrono::milliseconds(0), tail->last_timestamp);
  ASSERT_EQ(67, tail->offsets.getCounter(BYTE_OFFSET));
  cluster_->stop();
}

// a more comprehensive test for different computing tail record scenarios
TEST_P(RecoveryTest, ComputeTailRecord) {
  nodes_ = 4;
  replication_ = 2;
  extra_ = 0;

  // allow quick reactivations in this test
  seq_reactivation_limit_ = "100/1s";

  init();

  // epoch 1: empty
  // epoch 2: empty
  // epoch 3: single record (3, 1) local LNG 1. epoch offset 9.
  // epoch 4: empty
  // epoch 5: local lng (5, 1) stored on N1, N2, (5, 2)
  //          only per-epoch released on N1, epoch offset 17.
  // epoch 6: LNG 0, (6, 1) hole plug, (6, 2) record, (6, 3) plug,
  //          epoch offset 7.
  // epoch 7: (7,1) lng 1, (7,2) hole plug, (7,3) record. epoch offset 23.
  // epoch 8: empty
  //
  //      (3,1) (5,1) (5,2) (6,1) (6,2) (6,3) (7,1)  (7,2)  (7,3)
  // ------------------------------------------------------------
  // #1  |  x     x                 x    o(9)                 x
  // #2  |  x     x     x           x    x(7)   x
  // #3  |              x                       x             x
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(3, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(31))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 9}})),
                      TestRecord(LOG_ID, lsn(5, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(51))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 12}})),
                      TestRecord(LOG_ID, lsn(6, 2), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(62))
                          .payload(Payload(std::string(7, 'a').c_str(), 7))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 7}})),
                      TestRecord(LOG_ID, lsn(6, 3), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(63))
                          .writtenByRecovery(9)
                          .hole(),
                      TestRecord(LOG_ID, lsn(7, 3), ESN_INVALID)
                          .copyset({N1, N3})
                          .timestamp(std::chrono::milliseconds(73))
                          .payload(Payload(std::string(14, 'a').c_str(), 14))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 23}})),
                  },
                  {
                      {epoch_t(3), {esn_t(1), OffsetMap({{BYTE_OFFSET, 9}})}},
                      {epoch_t(5), {esn_t(2), OffsetMap({{BYTE_OFFSET, 17}})}},
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(3, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(31))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 9}})),
                      TestRecord(LOG_ID, lsn(5, 1), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(51))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 12}})),
                      TestRecord(LOG_ID, lsn(5, 2), ESN_INVALID)
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(52))
                          .payload(Payload{std::string(5, 'a').c_str(), 5})
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                      TestRecord(LOG_ID, lsn(6, 2), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(62))
                          .payload(Payload(std::string(7, 'a').c_str(), 7))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 7}})),
                      TestRecord(LOG_ID, lsn(6, 3), ESN_INVALID)
                          .copyset({N1, N2})
                          .timestamp(std::chrono::milliseconds(63))
                          .writtenByRecovery(7),
                      TestRecord(LOG_ID, lsn(7, 1), ESN_INVALID)
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(71))
                          .payload(Payload(std::string(9, 'a').c_str(), 9))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 9}})),
                  },
                  {
                      {epoch_t(3), {esn_t(1), OffsetMap({{BYTE_OFFSET, 0}})}},
                      {epoch_t(5), {esn_t(1), OffsetMap({{BYTE_OFFSET, 12}})}},
                      {epoch_t(7), {esn_t(1), OffsetMap({{BYTE_OFFSET, 23}})}},
                  });
  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(5, 2), ESN_INVALID)
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(52))
                          .payload(Payload{std::string(5, 'a').c_str(), 5})
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 17}})),
                      TestRecord(LOG_ID, lsn(7, 1), ESN_INVALID)
                          .copyset({N2, N3})
                          .timestamp(std::chrono::milliseconds(71))
                          .payload(Payload(std::string(9, 'a').c_str(), 9))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 9}})),
                      TestRecord(LOG_ID, lsn(7, 3), ESN_INVALID)
                          .copyset({N1, N3})
                          .timestamp(std::chrono::milliseconds(73))
                          .payload(Payload(std::string(14, 'a').c_str(), 14))
                          .offsetsWithinEpoch(OffsetMap({{BYTE_OFFSET, 23}})),
                  },
                  {
                      {epoch_t(5), {esn_t(1), OffsetMap({{BYTE_OFFSET, 12}})}},
                      {epoch_t(7), {esn_t(1), OffsetMap({{BYTE_OFFSET, 23}})}},
                  });

  cluster_->setStartingEpoch(LOG_ID, epoch_t(1));
  ASSERT_EQ(0, cluster_->start());

  auto client = cluster_->createClient();
  for (epoch_t::raw_type current = 1; current <= 8; ++current) {
    cluster_->getNode(0).waitForRecovery(LOG_ID);
    std::unique_ptr<LogTailAttributes> tail;
    wait_until([&]() {
      tail = client->getTailAttributesSync(LOG_ID);
      return tail != nullptr;
    });
    ASSERT_NE(nullptr, tail);

    // TODO: use tail record when api changes
#define CHECK_TAIL(_lsn, _ts, _offset)               \
  do {                                               \
    ASSERT_EQ((_lsn), tail->last_released_real_lsn); \
    ASSERT_EQ((_ts), tail->last_timestamp);          \
    ASSERT_EQ((_offset), tail->offsets);             \
  } while (0)

    switch (current) {
      case 1:
        CHECK_TAIL(LSN_INVALID,
                   std::chrono::milliseconds(0),
                   RecordOffset({{BYTE_OFFSET, 0}}));
        break;
      case 2:
        CHECK_TAIL(LSN_INVALID,
                   std::chrono::milliseconds(0),
                   RecordOffset({{BYTE_OFFSET, 0}}));
        break;
      case 3:
        CHECK_TAIL(lsn(3, 1),
                   std::chrono::milliseconds(31),
                   RecordOffset({{BYTE_OFFSET, 9}}));
        break;
      case 4:
        CHECK_TAIL(lsn(3, 1),
                   std::chrono::milliseconds(31),
                   RecordOffset({{BYTE_OFFSET, 9}}));
        break;
      case 5:
        CHECK_TAIL(lsn(5, 2),
                   std::chrono::milliseconds(52),
                   RecordOffset({{BYTE_OFFSET, 26}}));
        break;
      case 6:
        CHECK_TAIL(lsn(6, 2),
                   std::chrono::milliseconds(62),
                   RecordOffset({{BYTE_OFFSET, 33}}));
        break;
      case 7:
        CHECK_TAIL(lsn(7, 3),
                   std::chrono::milliseconds(73),
                   RecordOffset({{BYTE_OFFSET, 56}}));
        break;
      case 8:
        CHECK_TAIL(lsn(7, 3),
                   std::chrono::milliseconds(73),
                   RecordOffset({{BYTE_OFFSET, 56}}));
        break;
      default:
        ASSERT_FALSE(true);
    }
#undef CHECK_TAIL

    // re-activare sequencer to advance epoch
    ASSERT_EQ(logid_t(1), LOG_ID);
    std::string reply = cluster_->getNode(0).sendCommand("up --logid 1");
    ld_info("reply: %s", reply.c_str());
    ASSERT_NE(std::string::npos,
              reply.find("Started sequencer activation for log 1"));
  }

  cluster_->stop();
}

// test that with --bridge-record-in-empty-epoch set to true, epoch recovery
// should insert bridge record in empty epochs for data logs. check all bridge
// gaps by reading through all the epochs that were recovered. verify that
// bridge records can be found by reading from local log store after recovery
// is done.
TEST_P(RecoveryTest, BridgeRecordForEmptyEpochs) {
  nodes_ = 4;
  replication_ = 3;
  extra_ = 0;
  bridge_empty_epoch_ = true;

  init();

  const copyset_t copyset = {N1, N2, N3};

  // EpochRecovery instance is running with seal epoch of 7.
  // with the setting of bridge record for empty epoch turned on, bridge records
  // should be inserted for every empty epoch from epoch 1 to epoch 7.
  //
  //      (2,1) (4,1)
  // -----------------
  // #1  | o(2)
  // #2  |       x(1)
  // #3  |       o(4)
  //
  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(2, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(21))
                          .hole()
                          .writtenByRecovery(2),
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(4, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(41))
                          .writtenByRecovery(1),
                  });

  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(4, 1), ESN_INVALID)
                          .copyset(copyset)
                          .timestamp(std::chrono::milliseconds(41))
                          .writtenByRecovery(4)
                          .hole(),
                  });

  populateSoftSeals(epoch_t(1));
  cluster_->setStartingEpoch(LOG_ID, epoch_t(8));
  const epoch_t seal_epoch = epoch_t(7); // recovery seals epoch up to 2
  ASSERT_EQ(0, cluster_->start());

  read(lsn(1, 1), lsn(8, 0));
  EXPECT_EQ(std::vector<lsn_t>({}), records_);

  std::vector<gap_record_t> expected_gaps;
  expected_gaps.push_back(
      gap_record_t(GapType::BRIDGE, lsn(1, 1), lsn(1, ESN_MAX.val_)));
  for (int e = 2; e < 8; ++e) {
    expected_gaps.push_back(
        gap_record_t(GapType::BRIDGE, lsn(e, 0), lsn(e, 0)));
    expected_gaps.push_back(
        gap_record_t(GapType::BRIDGE, lsn(e, 1), lsn(e, ESN_MAX.val_)));
  }
  expected_gaps.push_back(gap_record_t(GapType::BRIDGE, lsn(8, 0), lsn(8, 0)));

  EXPECT_EQ(expected_gaps, gaps_);
  expectAuthoritativeOnly();
  printRecordCacheStats();
  cluster_->stop();

  for (int e = 1; e < 8; ++e) {
    verifyDigest(*buildDigest(epoch_t(e), ESN_INVALID, seal_epoch),
                 std::vector<esn_t>({}),        // records
                 std::vector<esn_t>({}),        // plugs
                 std::vector<esn_t>({esn_t(1)}) // bridge
    );
  }
}

// 1) digest should respect nodes in draining and include them into f-majority;
// 2) mutation should not happen on draining nodes
TEST_P(RecoveryTest, AuthoritativeRecoveryWithDrainingNodes) {
  nodes_ = 6;
  replication_ = 2;
  extra_ = 0;
  enable_rebuilding_ = true;
  // shorten the mutation timeout for faster retries
  recovery_timeout_ = std::chrono::milliseconds(5000);
  // requires everyone to participate
  recovery_grace_period_ = std::chrono::milliseconds(100000);

  // pre-provisioning metadata not to break the "num_holes_plugged" math with
  // spontaneous sequencer activations
  pre_provision_epoch_metadata_ = false;
  let_sequencers_provision_metadata_ = false;

  // metadata log is only stored in N5
  Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
      /*nodeset=*/{5}, /*max_replication=*/1, NodeLocationScope::NODE);
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;
  metadata_config_ = meta_config;

  init();

  // Set the nodeset of LOGID to {1, 2, 3, 4}
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N1, N2, N3, N4},
                          ReplicationProperty({{NodeLocationScope::NODE, 2}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(LOG_ID, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        LOG_ID,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
  }
  // Set the nodeset of the event log to {5}. These nodes will all
  // remain available during the test.
  {
    std::vector<std::unique_ptr<EpochMetaData>> epoch_metadata;
    epoch_metadata.emplace_back(
        new EpochMetaData(StorageSet{N5},
                          ReplicationProperty({{NodeLocationScope::NODE, 1}}),
                          epoch_t(1),
                          epoch_t(1)));
    auto meta_provisioner = cluster_->createMetaDataProvisioner();
    int rv = meta_provisioner->prepopulateMetaDataLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS, epoch_metadata);
    ASSERT_EQ(0, rv);
    auto provisioner = [&](logid_t, std::unique_ptr<EpochMetaData>& info) {
      if (!info) {
        info = std::make_unique<EpochMetaData>();
      }
      *info = *epoch_metadata[0];
      info->h.epoch = epoch_t(1);
      return EpochMetaData::UpdateResult::SUBSTANTIAL_RECONFIGURATION;
    };
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::EVENT_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
    rv = meta_provisioner->provisionEpochMetaDataForLog(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
        std::make_shared<SimpleEpochMetaDataUpdater>(provisioner),
        true /* write_metadata_logs */);
    ASSERT_EQ(0, rv);
  }

  const copyset_t copyset12 = {N1, N2};
  const copyset_t copyset13 = {N1, N3};
  const copyset_t copyset14 = {N1, N4};

  // Storage nodes contain the following records (x) and plugs (o):
  //      (1,1) (1,2) (1,3) (1,4)
  // ----------------------------
  // #1  | x(5)        x(2)              <- draining
  // #2  | x(5)  o(4)                    <- draining
  // #3  |       x(3)                    <- fully auth and writable
  // #4  |             o(3)  x           <- fully auth and writable
  //
  // despite that we have only two writable nodes, this recovery should be
  // authoritative.
  // expected outcome:
  // (1,1)  x(7)  stored on N3 and N4
  // (1,2)  o(7): stored on N3 and N4
  // (1,3)  o(7): stored on N3 and N4
  // (1,4)  x(7): stored on N3 and N4
  // (1,5)  b(7): stored on N3 and N4
  //
  // N1 and N2 should not participate in mutation and will have all their
  // copies removed by purging later

  prepopulateData(node_index_t(1),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1))
                          .writtenByRecovery(5),
                      TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                          .copyset(copyset13)
                          .timestamp(std::chrono::milliseconds(3))
                          .writtenByRecovery(2),
                  });

  prepopulateData(node_index_t(2),
                  {
                      TestRecord(LOG_ID, lsn(1, 1), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(1))
                          .writtenByRecovery(5),
                      TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                          .copyset(copyset12)
                          .timestamp(std::chrono::milliseconds(2))
                          .writtenByRecovery(4)
                          .hole(),
                  });

  prepopulateData(node_index_t(3),
                  {
                      TestRecord(LOG_ID, lsn(1, 2), ESN_INVALID)
                          .copyset(copyset13)
                          .timestamp(std::chrono::milliseconds(3))
                          .writtenByRecovery(3),
                  });

  prepopulateData(node_index_t(4),
                  {TestRecord(LOG_ID, lsn(1, 3), ESN_INVALID)
                       .copyset(copyset12)
                       .timestamp(std::chrono::milliseconds(3))
                       .writtenByRecovery(3)
                       .hole(),
                   TestRecord(LOG_ID, lsn(1, 4), ESN_INVALID)
                       .copyset(copyset14)
                       .timestamp(std::chrono::milliseconds(4))});

  cluster_->setStartingEpoch(LOG_ID, epoch_t(8));
  const epoch_t seal_epoch = epoch_t(7);

  // start N0, N1, and N5. These nodes are enough to operate
  // event log but not enough to let LOG_ID have f-majority
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 5}));
  // request draining for N1 and N2
  ld_info("Requesting draining for shard N1 and N2.");
  auto client = cluster_->createClient();
  auto flags =
      SHARD_NEEDS_REBUILD_Header::RELOCATE | SHARD_NEEDS_REBUILD_Header::DRAIN;
  ASSERT_NE(LSN_INVALID, requestShardRebuilding(*client, 1, SHARD_IDX, flags));
  const lsn_t to_sync = requestShardRebuilding(*client, 2, SHARD_IDX, flags);
  ASSERT_NE(LSN_INVALID, to_sync);

  // wait for N0 and N1 to pick up the up-to-date event log version.
  cluster_->waitUntilEventLogSynced(to_sync, {0, 1, 2});

  // half of the time, kill sequencer so that it does not have up-to-date
  // event log and we may fallback to intersection check
  bool kill_seq = (folly::Random::rand64(2) == 0);
  if (kill_seq) {
    cluster_->getNode(0).kill();
  }

  // For this test we kept N0 and N1 running so both epoch recovery and N1
  // always know about the current rebuilding set
  ASSERT_EQ(0, cluster_->start({3, 4}));
  if (kill_seq) {
    ASSERT_EQ(0, cluster_->start({0}));
  }

  ld_info("Waiting for recovery.");
  cluster_->getNode(0).waitForRecovery(LOG_ID);

  ld_info("Waiting for purging on N1 and N2.");
  cluster_->getNode(1).waitForPurge(LOG_ID, epoch_t(2));
  cluster_->getNode(2).waitForPurge(LOG_ID, epoch_t(2));

  auto stats = cluster_->getNode(0).stats();
  // all epoch recoveries must be fully authoritative
  EXPECT_EQ(0, stats["non_auth_recovery_epochs"]);

  read(lsn(1, 1), lsn(2, 0));
  EXPECT_EQ(std::vector<lsn_t>({
                lsn(1, 1),
                lsn(1, 4),
            }),
            records_);

  EXPECT_EQ(std::vector<gap_record_t>({
                gap_record_t(GapType::HOLE, lsn(1, 2), lsn(1, 2)),
                gap_record_t(GapType::HOLE, lsn(1, 3), lsn(1, 3)),
                gap_record_t(GapType::BRIDGE, lsn(1, 5), lsn(1, ESN_MAX.val_)),
                gap_record_t(GapType::BRIDGE, lsn(2, 0), lsn(2, 0)),
            }),
            gaps_);

  printRecordCacheStats();
  cluster_->stop();

  // first check digest from {N1, N2}, and it should be empty because
  // purging deletes them
  verifyDigest(*buildDigest(epoch_t(1),
                            ESN_INVALID,
                            {N1, N2},
                            replication_,
                            NodeLocationScope::NODE,
                            seal_epoch),
               // records
               std::vector<esn_t>({}),
               // plugs
               std::vector<esn_t>({}),
               // bridge
               std::vector<esn_t>({})

  );

  // all epoch recovery results should be stored on {N3, N4} which
  // participated in mutation
  verifyDigest(*buildDigest(epoch_t(1),
                            ESN_INVALID,
                            {N3, N4},
                            replication_,
                            NodeLocationScope::NODE,
                            seal_epoch),
               // records
               std::vector<esn_t>({esn_t(1), esn_t(4)}),
               // plugs
               std::vector<esn_t>({esn_t(2), esn_t(3)}),
               // bridge
               std::vector<esn_t>({esn_t(5)}));
}

} // namespace
