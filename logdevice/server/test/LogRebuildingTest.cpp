/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogRebuilding.h"

#include <vector>

#include <folly/hash/Hash.h> // implements hash functor for tuples.
#include <gtest/gtest.h>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/RecordRebuildingStore.h"

using namespace facebook::logdevice;

// Convenient shortcuts for writting ShardIDs.
#define N0S0 ShardID(0, 0)
#define N1S0 ShardID(1, 0)
#define N2S0 ShardID(2, 0)
#define N3S0 ShardID(3, 0)
#define N4S0 ShardID(4, 0)
#define N5S0 ShardID(5, 0)
#define N6S0 ShardID(6, 0)
#define N7S0 ShardID(7, 0)
#define N8S0 ShardID(8, 0)
#define N9S0 ShardID(9, 0)
#define N10S0 ShardID(10, 0)

namespace facebook { namespace logdevice {

using RecordSource = MetaDataLogReader::RecordSource;
using LocalLogStoreReader::ReadContext;
using LocalLogStoreReader::ReadPointer;

static constexpr logid_t kLogID{1};
static const NodeID kMyNodeID{0, 1};
static constexpr size_t kNumNodes{20};
static constexpr size_t kRecordSize{102400};
const ServerInstanceId
    SERVER_INSTANCE_ID(std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count());

namespace {

struct PostedReadStorageTask {
  lsn_t readPtr;
  lsn_t windowHigh;
  RecordTimestamp tsWindowHigh;
  lsn_t untilLsn;
  bool operator==(const PostedReadStorageTask& other) const {
    auto as_tuple = [](const PostedReadStorageTask& m) {
      return std::tie(m.readPtr, m.windowHigh, m.tsWindowHigh, m.untilLsn);
    };
    return as_tuple(*this) == as_tuple(other);
  }
};

std::ostream& operator<<(std::ostream& os, const PostedReadStorageTask& m) {
  os << "PostedReadStorageTask{";
  os << "readPtr: " << lsn_to_string(m.readPtr) << ", ";
  os << "windowHigh: " << lsn_to_string(m.windowHigh) << ", ";
  os << "tsWindowHigh: " << format_time(m.tsWindowHigh) << ", ";
  os << "untilLsn: " << lsn_to_string(m.untilLsn);
  os << "}";
  return os;
};

/**
 * Creates a fake record as if it was read from the local log store
 */
RawRecord createFakeRecord(lsn_t lsn,
                           size_t payload_size,
                           std::initializer_list<ShardID> copyset = {N1S0},
                           uint32_t wave = 1,
                           STORE_flags_t flags = 0) {
  std::chrono::milliseconds timestamp(0);

  STORE_Header header;
  header.rid = RecordID{lsn_to_esn(lsn), lsn_to_epoch(lsn), kLogID};
  header.timestamp = timestamp.count();
  header.last_known_good = esn_t(0);
  header.wave = wave;
  header.flags = flags;
  header.copyset_size = copyset.size();

  // Create a StoreChainLink array from the supplied list of node indices
  std::vector<StoreChainLink> chain(copyset.size());
  std::transform(copyset.begin(), copyset.end(), chain.data(), [](ShardID n) {
    return StoreChainLink{n, ClientID()};
  });

  std::string buf;
  Slice header_blob = LocalLogStoreRecordFormat::formRecordHeader(
      header, chain.data(), &buf, false, std::map<KeyType, std::string>());

  size_t log_store_size = header_blob.size + payload_size;
  void* log_store_data = malloc(log_store_size);
  ld_check(log_store_data);
  memcpy(log_store_data, header_blob.data, header_blob.size);
  return RawRecord(lsn,
                   Slice(log_store_data, log_store_size),
                   /* owned payload */ true);
}

class MockRecordRebuildingAmend;
class MockLogRebuilding;

class MockRecordRebuildingStore : public RecordRebuildingStore {
 public:
  MockRecordRebuildingStore(size_t block_id,
                            RawRecord record,
                            RecordRebuildingOwner* owner,
                            std::shared_ptr<ReplicationScheme> replication,
                            lsn_t lsn,
                            Settings& settings)
      : RecordRebuildingStore(block_id,
                              /*shard=*/0,
                              std::move(record),
                              owner,
                              replication),
        settings_(settings) {
    lsn_ = lsn;
  }
  void start(bool) override {}
  void onComplete() override {}

  void updateFlushTokenMap(FlushTokenMap store_memtable_id_map) {
    flushTokenMap_ = store_memtable_id_map;
  }

  const Settings& getSettings() const override {
    return settings_;
  }

  Settings& settings_;
  FlushTokenMap flushTokenMap_;
};

class MockRecordRebuildingAmend : public RecordRebuildingAmend {
 public:
  explicit MockRecordRebuildingAmend(
      lsn_t lsn,
      RecordRebuildingOwner* owner,
      std::shared_ptr<ReplicationScheme> replication,
      STORE_Header storeHeader,
      LocalLogStoreRecordFormat::flags_t flags,
      copyset_t newCopyset,
      copyset_t amendRecipients,
      uint32_t rebuildingWave,
      Settings& settings)
      : RecordRebuildingAmend(lsn,
                              /*shard=*/0,
                              owner,
                              replication,
                              storeHeader,
                              flags,
                              newCopyset,
                              amendRecipients,
                              rebuildingWave),
        settings_(settings) {}

  void start(bool) override;

  const Settings& getSettings() const override {
    return settings_;
  }

  Settings& settings_;
  ~MockRecordRebuildingAmend() override;
};

struct ReceivedMessages {
  std::vector<PostedReadStorageTask> postedReadStorageTasks;
  std::vector<lsn_t> writtenCheckpoints;
  bool notifyCompleteWasCalled{false};
  bool readCheckpointWasCalled{false};
  bool notifyDurabilityStatusWasCalled{false};
  bool notifyWindowEndWasCalled{false};
  bool notifyLogRebuildingSizeWasCalled{false};
  bool notifyReachedUntilLsnWasCalled{false};
  std::vector<lsn_t> createdRecordRebuildingStores;
  std::vector<lsn_t> createRecordRebuildingAmends;
  std::vector<lsn_t> pendingAmendsStarted;
  bool readNewBatchTimerWasActivated{false};
};

class MockLogRebuilding : public LogRebuilding {
 public:
  explicit MockLogRebuilding(
      std::shared_ptr<UpdateableConfig> config,
      ReceivedMessages& received_messages,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      Settings& settings)
      : LogRebuilding(ShardRebuildingV1Ref(nullptr),
                      kLogID,
                      /*shard=*/0,
                      rebuilding_settings,
                      nullptr),
        config(config),
        received(received_messages),
        rebuilding_settings_(rebuilding_settings),
        settings_(settings) {}

  ~MockLogRebuilding() override {}

  void readCheckpoint() override {
    received.readCheckpointWasCalled = true;
  }

  void writeCheckpoint(lsn_t rebuilt_upto) override {
    received.writtenCheckpoints.push_back(rebuilt_upto);
  }

  void putReadStorageTask(const ReadContext& ctx) override {
    ld_info("Putting storage task with lsn=%s window_high=%ld",
            lsn_to_string(ctx.read_ptr_.lsn).c_str(),
            ctx.ts_window_high_.count());
    received.postedReadStorageTasks.push_back(
        PostedReadStorageTask{ctx.read_ptr_.lsn,
                              ctx.window_high_,
                              RecordTimestamp(ctx.ts_window_high_),
                              ctx.until_lsn_});
  }

  std::shared_ptr<UpdateableConfig> getConfig() const override {
    return config;
  }

  NodeID getMyNodeID() const override {
    return kMyNodeID;
  }

  const Settings& getSettings() const override {
    return settings_;
  }

  ServerInstanceId getServerInstanceId() const override {
    return SERVER_INSTANCE_ID;
  }

  void notifyComplete() override {
    received.notifyCompleteWasCalled = true;
  }

  void notifyWindowEnd(RecordTimestamp next) override {
    ld_info(
        "Reached end of window, next_timestamp=%s", format_time(next).c_str());
    received.notifyWindowEndWasCalled = true;
  }

  void notifyLogRebuildingSize() override {
    received.notifyLogRebuildingSizeWasCalled = true;
  }

  void notifyReachedUntilLsn() override {
    received.notifyReachedUntilLsnWasCalled = true;
  }

  void deleteThis() override {}

  std::shared_ptr<ReplicationScheme>
  createReplicationScheme(EpochMetaData metadata,
                          NodeID sequencer_node_id) override {
    UpdateableSettings<Settings> s;
    return std::make_shared<ReplicationScheme>(
        getLogID(),
        std::move(metadata),
        getConfig()->getNodesConfiguration(),
        getMyNodeID(),
        nullptr,
        *s.get(),
        /*relocate_local_records*/ false,
        sequencer_node_id);
  }

  std::unique_ptr<RecordRebuildingStore> createRecordRebuildingStore(
      size_t block_id,
      RawRecord record,
      std::shared_ptr<ReplicationScheme> replication) override {
    lsn_t lsn = record.lsn;
    received.createdRecordRebuildingStores.push_back(lsn);
    return std::make_unique<MockRecordRebuildingStore>(
        block_id, std::move(record), this, replication, lsn, settings_);
  }

  std::unique_ptr<RecordRebuildingAmend>
  createRecordRebuildingAmend(RecordRebuildingAmendState rras) override {
    received.createRecordRebuildingAmends.push_back(rras.lsn_);
    return std::make_unique<MockRecordRebuildingAmend>(rras.lsn_,
                                                       this,
                                                       rras.replication_,
                                                       rras.storeHeader_,
                                                       rras.flags_,
                                                       rras.newCopyset_,
                                                       rras.amendRecipients_,
                                                       rras.rebuildingWave_,
                                                       settings_);
  }

  virtual void pendingAmendsStarted(lsn_t lsn) {
    received.pendingAmendsStarted.push_back(lsn);
  }

  std::unique_ptr<BackoffTimer>
  createTimer(std::function<void()> /*callback*/) override {
    return nullptr;
  }

  std::unique_ptr<Timer> createIteratorTimer() override {
    return nullptr;
  }

  std::unique_ptr<Timer> createStallTimer() override {
    stallTimerCreated = true;
    return nullptr;
  }

  std::unique_ptr<Timer> createNotifyRebuildingCoordinatorTimer() override {
    notifyRebuildingCoordinatorTimerCreated = true;
    return nullptr;
  }

  std::unique_ptr<Timer> createReadNewBatchTimer() override {
    readNewBatchTimerCreated = true;
    return nullptr;
  }

  void activateReadNewBatchTimer() override {
    ASSERT_TRUE(readNewBatchTimerCreated);
    received.readNewBatchTimerWasActivated = true;
    fireReadNewBatchTimer();
    return;
  }

  void activateNotifyRebuildingCoordinatorTimer() override {
    ASSERT_TRUE(notifyRebuildingCoordinatorTimerCreated);
    ASSERT_FALSE(notifyRebuildingCoordinatorTimerActive);
    notifyRebuildingCoordinatorTimerActive = true;
    return;
  }

  void activateStallTimer() override {
    ASSERT_TRUE(stallTimerCreated);
    ASSERT_FALSE(stallTimerActive);
    stallTimerActive = true;
    return;
  }

  void cancelStallTimer() override {
    ASSERT_TRUE(stallTimerActive);
    stallTimerActive = false;
    return;
  }

  bool isStallTimerActive() override {
    return stallTimerActive;
  }

  void fireStallTimer() {
    ASSERT_TRUE(stallTimerActive);
    stallTimerActive = false;
    restart();
  }

  void fireReadNewBatchTimer() {
    readNewBatch();
  }

  void fireNotifyRebuildingCoordinatorTimer() {
    ASSERT_TRUE(notifyRebuildingCoordinatorTimerActive);
    notifyRebuildingCoordinatorTimerActive = false;
    notifyRebuildingCoordinator();
  }

  size_t getMaxBlockSize() override {
    return 64 * 1024 * 1024;
  }

  void invalidateIterators() override {}

  const std::shared_ptr<UpdateableConfig> config;
  ReceivedMessages& received;
  UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  Settings& settings_;
  bool stallTimerActive = false;
  bool notifyRebuildingCoordinatorTimerActive = false;
  bool stallTimerCreated = false;
  bool readNewBatchTimerCreated = false;
  bool notifyRebuildingCoordinatorTimerCreated = false;
  std::function<void()> stalltimerCallback;
  friend class MockRecordRebuildingAmend;
};

MockRecordRebuildingAmend::~MockRecordRebuildingAmend() {}

} // namespace

/**
 * Test fixture.
 */
class LogRebuildingTest : public ::testing::Test {
 public:
  LogRebuildingTest()
      : config(std::make_shared<UpdateableConfig>()),
        rebuilding_settings_(create_default_settings<RebuildingSettings>()),
        settings_(create_default_settings<Settings>()) {
    rebuilding_settings_.use_rocksdb_cache = false; // #14697349
    dbg::assertOnData = true;
  }

  void init() {
    updateConfig();
    rebuilding = std::make_unique<MockLogRebuilding>(
        config,
        received,
        UpdateableSettings<RebuildingSettings>(rebuilding_settings_),
        settings_);
  }

  void updateConfig() {
    Configuration::Nodes nodes;
    for (int i = 0; i < kNumNodes; ++i) {
      Configuration::Node& node = nodes[i];
      node.address = Sockaddr(
          get_localhost_address_str(), folly::to<std::string>(4440 + i));
      node.generation = 1;
      node.addSequencerRole();
      node.addStorageRole();
    }

    auto logs_config = std::make_unique<configuration::LocalLogsConfig>();

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(3);
    log_attrs.set_scdEnabled(true);
    logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(
                            kLogID.val_, kLogID.val_ + 1),
                        "mylogs",
                        log_attrs);

    Configuration::NodesConfig nodes_config(std::move(nodes));
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);

    config->updateableServerConfig()->update(
        ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
    config->updateableNodesConfiguration()->update(
        config->getServerConfig()
            ->getNodesConfigurationFromServerConfigSource());
    config->updateableLogsConfig()->update(std::move(logs_config));
  }

  void fireStallTimer() {
    rebuilding->fireStallTimer();
  }

  void fireNotifyRebuildingCoordinatorTimer() {
    rebuilding->fireNotifyRebuildingCoordinatorTimer();
  }

  void restart() {
    rebuilding->restart();
  }

  void onAllStoresReceived(lsn_t lsn, FlushTokenMap flushTokenMap) {
    rebuilding->onAllStoresReceived(
        lsn, std::make_unique<FlushTokenMap>(std::move(flushTokenMap)));
  }

  void onAllAmendsReceived(lsn_t lsn, FlushTokenMap flushTokenMap) {
    rebuilding->onAllAmendsReceived(
        lsn, std::make_unique<FlushTokenMap>(std::move(flushTokenMap)));
  }

  void onCheckpointRead(Status status, lsn_t version, lsn_t rebuilt_upto) {
    ASSERT_TRUE(received.readCheckpointWasCalled);
    rebuilding->onCheckpointRead(status,
                                 rebuilding->getRebuildingVersion(),
                                 version,
                                 rebuilt_upto,
                                 LSN_INVALID);
  }

  void onMemtableFlushed(node_index_t node_index,
                         ServerInstanceId server_instance_id,
                         FlushToken memtable_id) {
    rebuilding->onMemtableFlushed(node_index, server_instance_id, memtable_id);
  }

  void onGracefulShutdown(node_index_t node_index,
                          ServerInstanceId server_instance_id) {
    rebuilding->onGracefulShutdown(node_index, server_instance_id);
  }

  void addEpochMetaDataToPlan(RebuildingPlan& plan,
                              epoch_t since,
                              epoch_t until,
                              StorageSet storage_set) {
    auto metadata = std::make_shared<EpochMetaData>(
        std::move(storage_set),
        ReplicationProperty({{NodeLocationScope::NODE, 3}}),
        since,
        since);
    plan.addEpochRange(since, until, metadata);
  }

  void onReadTaskDone(RebuildingReadStorageTask::RecordContainer& records,
                      Status status,
                      ReadPointer& read_ptr,
                      RecordTimestamp ts_w_high = RecordTimestamp::max(),
                      int version = 1) {
    rebuilding->onReadTaskDone(records, status, read_ptr, ts_w_high, version);
  }

  void window(RecordTimestamp ts) {
    rebuilding->window(ts);
  }

  std::shared_ptr<UpdateableConfig> config;
  std::unique_ptr<MockLogRebuilding> rebuilding;
  ReceivedMessages received;
  RebuildingSettings rebuilding_settings_;
  Settings settings_;
};

void MockRecordRebuildingAmend::start(bool /*unused*/) {
  auto lr = dynamic_cast<MockLogRebuilding*>(owner_);
  ld_check(lr);
  lr->pendingAmendsStarted(lsn_);
}

#define ASSERT_NO_READ_STORAGE_TASK() \
  { ASSERT_TRUE(received.postedReadStorageTasks.empty()); }

#define ASSERT_READ_STORAGE_TASK(read_ptr, w_high, ts_w_high, until_lsn)    \
  {                                                                         \
    auto s = PostedReadStorageTask{read_ptr, w_high, ts_w_high, until_lsn}; \
    std::vector<PostedReadStorageTask> tmp;                                 \
    tmp.push_back(s);                                                       \
    ASSERT_EQ(tmp, received.postedReadStorageTasks);                        \
    received.postedReadStorageTasks.clear();                                \
  }

#define ASSERT_NO_CHECKPOINT() \
  { ASSERT_TRUE(received.writtenCheckpoints.empty()); }

#define ASSERT_NOTIFY_LOG_REBUILDING_SIZE_CALLED()          \
  {                                                         \
    ASSERT_TRUE(received.notifyLogRebuildingSizeWasCalled); \
    received.notifyLogRebuildingSizeWasCalled = false;      \
  }

#define ASSERT_NOTIFY_REACHED_UNTILLSN_CALLED()           \
  {                                                       \
    ASSERT_TRUE(received.notifyReachedUntilLsnWasCalled); \
    received.notifyReachedUntilLsnWasCalled = false;      \
  }

#define ASSERT_NOTIFY_COMPLETE_CALLED()            \
  {                                                \
    ASSERT_TRUE(received.notifyCompleteWasCalled); \
    received.notifyCompleteWasCalled = false;      \
  }

#define ASSERT_NOTIFY_WINDOW_END_CALLED()           \
  {                                                 \
    ASSERT_TRUE(received.notifyWindowEndWasCalled); \
    received.notifyWindowEndWasCalled = false;      \
  }

#define ASSERT_CHECKPOINT(...)                   \
  {                                              \
    std::vector<lsn_t> tmp{__VA_ARGS__};         \
    ASSERT_EQ(tmp, received.writtenCheckpoints); \
    received.writtenCheckpoints.clear();         \
  }

#define ASSERT_NO_RECORD_REBUILDING_CREATED() \
  { ASSERT_TRUE(received.createdRecordRebuildingStores.empty()); }

#define ASSERT_RECORD_REBUILDING_CREATED_RANGE(lo, hi)      \
  {                                                         \
    std::vector<lsn_t> tmp;                                 \
    for (lsn_t i = lo; i <= hi; ++i) {                      \
      tmp.push_back(i);                                     \
    }                                                       \
    ASSERT_EQ(tmp, received.createdRecordRebuildingStores); \
    received.createdRecordRebuildingStores.clear();         \
  }

#define ASSERT_RECORD_REBUILDING_CREATED(...)               \
  {                                                         \
    std::vector<lsn_t> tmp{__VA_ARGS__};                    \
    ASSERT_EQ(tmp, received.createdRecordRebuildingStores); \
    received.createdRecordRebuildingStores.clear();         \
  }

#define ASSERT_NO_RECORD_REBUILDING_AMEND_CREATED() \
  { ASSERT_TRUE(received.createdRecordRebuildingAmends.empty()); }

#define ASSERT_RECORD_REBUILDING_AMEND_CREATED_RANGE(lo, hi) \
  {                                                          \
    std::vector<lsn_t> tmp;                                  \
    for (lsn_t i = lo; i <= hi; ++i) {                       \
      tmp.push_back(i);                                      \
    }                                                        \
    ASSERT_EQ(tmp, received.createdRecordRebuildingAmends);  \
    received.createdRecordRebuildingAmends.clear();          \
  }

#define ASSERT_RECORD_REBUILDING_AMEND_CREATED(...)         \
  {                                                         \
    std::vector<lsn_t> tmp{__VA_ARGS__};                    \
    ASSERT_EQ(tmp, received.createdRecordRebuildingAmends); \
    received.createdRecordRebuildingAmends.clear();         \
  }

#define ASSERT_PENDING_AMENDS_STARTED_RANGE(lo, hi)  \
  {                                                  \
    std::vector<lsn_t> tmp;                          \
    for (lsn_t i = lo; i <= hi; ++i) {               \
      tmp.push_back(i);                              \
    }                                                \
    std::sort(received.pendingAmendsStarted.begin(), \
              received.pendingAmendsStarted.end());  \
    ASSERT_EQ(tmp, received.pendingAmendsStarted);   \
    received.pendingAmendsStarted.clear();           \
  }

#define ASSERT_PENDING_AMENDS_STARTED(...)         \
  {                                                \
    std::vector<lsn_t> tmp{__VA_ARGS__};           \
    ASSERT_EQ(tmp, received.pendingAmendsStarted); \
    received.pendingAmendsStarted.clear();         \
  }

#define ASSERT_NO_PENDING_AMENDS_STARTED() \
  { ASSERT_TRUE(received.pendingAmendsStarted.empty()); }

#define ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED()          \
  {                                                      \
    ASSERT_TRUE(received.readNewBatchTimerWasActivated); \
    received.readNewBatchTimerWasActivated = false;      \
  }

// We read only one epoch. Verify the scheduling of RecordRebuildingStore state
// machines.
TEST_F(LogRebuildingTest, Simple) {
  rebuilding_settings_.max_records_in_flight = 50;

  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{42}.
  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           RecordTimestamp::max(),            // ts_window_high
                           until_lsn);

  // The storage task comes back with 63 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 63; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
  onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

  // max_records_in_flight in flight is 50, so we should only start a
  // RecordRebuildingStore state machine for the first 50 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{50}));

  // With WAL, Stores are considered immediately durable
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{1}), FlushTokenMap());
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

  // The first record in the buffer completes.
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{1}), FlushTokenMap());

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 13; ++i) {
    onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{52}), compose_lsn(epoch_t{43}, esn_t{62}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{2}), FlushTokenMap());
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{2}), FlushTokenMap());

  // The remaining records complete.
  for (uint32_t i = 63; i >= 14; --i) {
    onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{64}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 1 record.
  records.clear();
  records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), 10));
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // The record is rebuilt.
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{64}), FlushTokenMap());
  ASSERT_NO_RECORD_REBUILDING_CREATED();
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{64}), FlushTokenMap());

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{900}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),    // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 0 records and we are done.
  records.clear();
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

TEST_F(LogRebuildingTest, SimpleWithoutWal) {
  rebuilding_settings_.max_records_in_flight = 50;
  rebuilding_settings_.max_amends_in_flight = 40;

  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{42}.
  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           RecordTimestamp::max(),            // ts_window_high
                           until_lsn);

  // The storage task comes back with 63 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 63; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
  onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

  // max_records_in_flight in flight is 50, so we should only start a
  // RecordRebuildingStore state machine for the first 50 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{50}));

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));

  // The first record in the buffer completes.
  // This causes a new RecordRebuildingStore to start
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 112}});
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 13; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable
    // with token 111. Amends shouls be started immediately.
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{52}), compose_lsn(epoch_t{43}, esn_t{62}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 112}});

  // The remaining records complete.
  // Since max amends in flight is 40, only the first 40 lsns have RRA started.
  for (uint32_t i = 14; i <= 63; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  // Now we get a flush notification from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);

  ASSERT_PENDING_AMENDS_STARTED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{14}), compose_lsn(epoch_t{43}, esn_t{53}));

  // As more RRAs complete, remaining RRAs are started
  for (uint32_t i = 14; i <= 53; i++) {
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }
  ASSERT_PENDING_AMENDS_STARTED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{54}), compose_lsn(epoch_t{43}, esn_t{63}));

  for (uint32_t i = 54; i <= 63; i++) {
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }

  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{64}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 1 record.
  records.clear();
  records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), 10));
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // The record is rebuilt.
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));

  // Node 1 notifies about flushing token 115
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 115);
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 115}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 116}});
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 116);

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{900}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),    // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 0 records and we are done.
  records.clear();
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

TEST_F(LogRebuildingTest, SimpleWithoutWal2) {
  rebuilding_settings_.max_records_in_flight = 50;
  rebuilding_settings_.max_amends_in_flight = 40;

  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{42}.
  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           RecordTimestamp::max(),            // ts_window_high
                           until_lsn);

  // The storage task comes back with 63 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 63; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
  onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

  // max_records_in_flight in flight is 50, so we should only start a
  // RecordRebuilding state machine for the first 50 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{50}));

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));

  // The first record in the buffer completes.
  // This causes a new RecordRebuildingStore to start
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 112}});
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 13; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable with
    // token 111. Amends shouls be started immediately.
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{52}), compose_lsn(epoch_t{43}, esn_t{62}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 112}});

  // Now we receive a memtable flush notification. Note the subtle difference in
  // code executed depending on when the notification is received. In this case,
  // we receive a notification but there are no RRs that LogRebuilding knows of
  // which is waiting got this notification. When RRs complete and inform
  // LogRebuilding about the flushtoken to wait for, it should recognize that it
  // already received a flush notification and mark the stores as durable and
  // move on to next stage.
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);

  // The remaining records complete.
  // Since max amends in flight is 40, only the first 40 lsns have RRA started.
  for (uint32_t i = 14; i <= 63; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_PENDING_AMENDS_STARTED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{14}), compose_lsn(epoch_t{43}, esn_t{53}));

  // As more RRAs complete, remaining RRAs are started
  for (uint32_t i = 14; i <= 53; i++) {
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }
  ASSERT_PENDING_AMENDS_STARTED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{54}), compose_lsn(epoch_t{43}, esn_t{63}));

  for (uint32_t i = 54; i <= 63; i++) {
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }

  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{64}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 1 record.
  records.clear();
  records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), 10));
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // The record is rebuilt.
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));

  // Node 1 notifies about flushing token 115
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 115);
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 115}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 116}});
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 116);

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{900}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),    // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 0 records and we are done.
  records.clear();
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

// Verify that LogRebuilding skips epochs whose nodeset does not contain any
// node in the rebuilding set.
TEST_F(LogRebuildingTest, VerifyEpochSkipping) {
  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));

  // The rebuilding node is in the nodeset of epochs
  // [1, 3] U [8, 8] U [11, ...]
  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{3},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  addEpochMetaDataToPlan(
      plan,
      epoch_t{8},
      epoch_t{8},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  addEpochMetaDataToPlan(
      plan,
      epoch_t{11},
      epoch_t{EPOCH_MAX},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = LSN_MAX;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No read storage task should be issued until we read the checkpoint.
  ASSERT_NO_READ_STORAGE_TASK();

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{3}.
  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),  // read_ptr
                           compose_lsn(epoch_t{3}, ESN_MAX), // window_high
                           RecordTimestamp::max(),           // ts_window_high
                           compose_lsn(EPOCH_MAX, ESN_MAX)); // until_lsn

  // The read storage task comes back with 0 records and
  // E::WINDOW_END_REACHED, read_pointer_.lsn points to a record in epoch 5.
  RebuildingReadStorageTask::RecordContainer records;
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{5}, esn_t{0})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // Event though the task came back with the read pointer at epoch 5, the next
  // task should be issued for reading epoch 8 as epochs 5, 6, 7 can be skipped.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{8}, ESN_MIN), // read_ptr
                           compose_lsn(epoch_t{8}, ESN_MAX), // window_high
                           RecordTimestamp::max(),           // ts_window_high
                           compose_lsn(EPOCH_MAX, ESN_MAX)); // until_lsn

  // The task comes back with the read pointer in epoch 9.
  read_ptr = ReadPointer{compose_lsn(epoch_t{9}, esn_t{0})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // Similarly, we jump to epoch 11.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{11}, ESN_MIN), // read_ptr
                           compose_lsn(EPOCH_MAX, ESN_MAX),   // window_high
                           RecordTimestamp::max(),            // ts_window_high
                           compose_lsn(EPOCH_MAX, ESN_MAX));  // until_lsn

  ASSERT_FALSE(received.notifyCompleteWasCalled);

  // The task comes back with E::UNTIL_LSN_REACHED.
  read_ptr = ReadPointer{compose_lsn(epoch_t{11}, esn_t{42})};
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);

  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with e11n41.
  ASSERT_CHECKPOINT(LSN_MAX);
}

TEST_F(LogRebuildingTest, StoreAmendTimeoutWithoutWal) {
  rebuilding_settings_.max_records_in_flight = 50;

  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);
  bool flushShouldSucceed = false;

  do {
    // LogRebuilding should issue a storage task to start reading from the
    // oldest lsn with window_high=epoch_t{42}.
    ASSERT_READ_STORAGE_TASK(flushShouldSucceed
                                 ? compose_lsn(epoch_t{43}, ESN_MIN)
                                 : compose_lsn(EPOCH_MIN, ESN_MIN), // read_ptr
                             compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                             RecordTimestamp::max(), // ts_window_high
                             until_lsn);

    // The storage task comes back with 63 records.
    RebuildingReadStorageTask::RecordContainer records;
    for (unsigned int i = 1; i <= 63; ++i) {
      records.push_back(
          createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
    }
    auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
    onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

    // max_records_in_flight in flight is 50, so we should only start a
    // RecordRebuildingStore state machine for the first 50 recrods.
    ASSERT_RECORD_REBUILDING_CREATED_RANGE(compose_lsn(epoch_t{43}, esn_t{1}),
                                           compose_lsn(epoch_t{43}, esn_t{50}));

    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
    ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

    // Flush message is received from Node 1
    onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);

    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 115}});
    ASSERT_NO_RECORD_REBUILDING_CREATED();

    // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet
    // that should not prevent new RecordRebuildingStores from being started
    for (uint32_t i = 3; i <= 13; ++i) {
      onAllStoresReceived(
          compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
      // We already received notification from Node 1 about flushing memtable
      // with token 111. Amends shouls be started immediately.
      ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
      onAllAmendsReceived(
          compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 115}});
    }

    ASSERT_RECORD_REBUILDING_CREATED_RANGE(compose_lsn(epoch_t{43}, esn_t{52}),
                                           compose_lsn(epoch_t{43}, esn_t{62}));

    // Now e43n2 (at position 1 in the buffer) completes
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
    ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 115}});

    onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);
    // The remaining records complete.
    for (uint32_t i = 63; i >= 14; --i) {
      onAllStoresReceived(
          compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
      ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
      onAllAmendsReceived(
          compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 115}});
    }
    ASSERT_NO_RECORD_REBUILDING_CREATED();

    // The buffer is empty. An new storage task is issued.
    ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{64}), // read_ptr
                             compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                             RecordTimestamp::max(), // ts_window_high
                             until_lsn);

    // The task comes back with 1 record.
    records.clear();
    records.push_back(
        createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), 10));
    read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
    onReadTaskDone(records, E::PARTIAL, read_ptr);

    ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));

    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 113}});
    onMemtableFlushed(1, SERVER_INSTANCE_ID, 113);
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 115}});

    // The buffer is empty. An new storage task is issued.
    ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{900}), // read_ptr
                             compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                             RecordTimestamp::max(), // ts_window_high
                             until_lsn);

    // The task comes back with 0 records and we are done.
    records.clear();
    onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
    // e43n1-64 - waiting for amends to be durable
    ASSERT_NOTIFY_REACHED_UNTILLSN_CALLED();

    ASSERT_FALSE(rebuilding->stallTimerActive);

    if (flushShouldSucceed) {
      onMemtableFlushed(1, SERVER_INSTANCE_ID, 115);
      fireNotifyRebuildingCoordinatorTimer();
      ASSERT_NOTIFY_COMPLETE_CALLED();
      break;
    } else {
      restart();
      flushShouldSucceed = true;
    }
  } while (true);

  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

TEST_F(LogRebuildingTest, WaitForPendingAmendsBeforeReadingFurther) {
  rebuilding_settings_.max_records_in_flight = 50;
  rebuilding_settings_.checkpoint_interval_mb = 1;
  rebuilding_settings_.max_log_rebuilding_size_mb = 0.001;
  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart version*/);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           RecordTimestamp::max(),            // ts_window_high
                           until_lsn);

  // The storage task comes back with 63 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 63; ++i) {
    records.push_back(
        createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), kRecordSize));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
  onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

  // max_records_in_flight in flight is 50, so we should only start a
  // RecordRebuildingStore state machine for the first 50 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{50}));

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));

  // The first record in the buffer completes.
  // This causes a new RecordRebuildingStore to start
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 112}});
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 13; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable
    // with token 111. Amends shouls be started immediately.
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{52}), compose_lsn(epoch_t{43}, esn_t{62}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 112}});

  onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);
  for (uint32_t i = 63; i >= 14; --i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // The buffer is empty. An new storage task should issued.
  // but we have reached max log rebuilding size threshold.
  // So, wait for pending amends to come below threshold before
  // proceeding.

  ASSERT_NO_READ_STORAGE_TASK();
  ASSERT_CHECKPOINT(compose_lsn(epoch_t{43}, esn_t{13}));
  // timer should be activated
  ASSERT_TRUE(rebuilding->stallTimerActive);

  // Stores are flushed.
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 113);

  // All lsn in pendingAmends should have started now.
  ASSERT_PENDING_AMENDS_STARTED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{14}), compose_lsn(epoch_t{43}, esn_t{63}));

  for (unsigned int i = 14; i <= 63; i++) {
    lsn_t lsn = compose_lsn(epoch_t{43}, esn_t{i});
    onAllAmendsReceived(lsn, {{{1, SERVER_INSTANCE_ID}, 114}});
  }

  // Amends are flushed.
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 114);

  ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED();

  // We have made progress and hence read task is posted.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{64}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);

  // The task comes back with 1 record.
  records.clear();
  records.push_back(
      createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), kRecordSize));
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // The record is rebuilt.
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));

  // Node 1 notifies about flushing token 115
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 115);
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 115}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{64}), {{{1, SERVER_INSTANCE_ID}, 116}});
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 116);

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED();
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{900}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),    // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);
  ASSERT_CHECKPOINT(compose_lsn(epoch_t{43}, esn_t{63}));

  // The task comes back with 0 records and we are done.
  records.clear();
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

TEST_F(LogRebuildingTest, RestartAcrossWindow) {
  rebuilding_settings_.max_records_in_flight = 40;

  init();

  // We start reading with the local window ending at RecordTimestamp::now().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));
  RecordTimestamp max_ts = RecordTimestamp::now();

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    max_ts,
                    1 /* version */,
                    1 /*restart version*/);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           max_ts,                            // ts_window_high
                           until_lsn);

  // The storage task comes back with 50 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 50; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{51})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // max_records_in_flight in flight is 40, so we should only start a
  // RecordRebuildingStore state machine for the first 40 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{40}));

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{41}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 112}});

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 11; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable
    // with token 111. Amends shouls be started immediately.
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{42}), compose_lsn(epoch_t{43}, esn_t{50}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_NO_RECORD_REBUILDING_CREATED();
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 112}});

  onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);

  for (uint32_t i = 50; i >= 12; --i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // We have processed all record and have reached end of batch
  // No reads should be issued till we get a window message.
  ASSERT_NO_READ_STORAGE_TASK();
  ASSERT_NOTIFY_WINDOW_END_CALLED();

  max_ts = RecordTimestamp::now();
  window(max_ts);

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{51}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           max_ts, // ts_window_high
                           until_lsn);

  records.clear();
  // The storage task comes back with 10 records.
  for (unsigned int i = 51; i <= 60; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{51}), compose_lsn(epoch_t{43}, esn_t{60}));

  for (uint32_t i = 51; i <= 60; i++) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 114}});
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // Since we have reached end of the batch and hit UNTIL_LSN, we should now
  // wait for all amends to complete.
  ASSERT_FALSE(rebuilding->stallTimerActive);
  ASSERT_NOTIFY_REACHED_UNTILLSN_CALLED();
  restart();

  // Read task with read ptr at e43n12 should be issued.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{12}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           RecordTimestamp::max(), // ts_window_high
                           until_lsn);
}

TEST_F(LogRebuildingTest, RestartAfterStoresDurable) {
  rebuilding_settings_.max_records_in_flight = 40;
  rebuilding_settings_.max_amends_in_flight = 1;

  init();

  // We start reading with the local window ending at RecordTimestamp::now().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));
  RecordTimestamp max_ts = RecordTimestamp::now();

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    max_ts,
                    1 /* version */,
                    1 /*restart version*/);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           max_ts,                            // ts_window_high
                           until_lsn);

  // The storage task comes back with 50 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 50; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{51})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // max_records_in_flight in flight is 40, so we should only start a
  // RecordRebuildingStore state machine for the first 40 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{40}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{41}));
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));

  // More stores complete, but no amends should be started as max_in_flight is 1
  for (uint32_t i = 2; i <= 11; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable
    // with token 111. Amends shouls be started immediately.
    ASSERT_NO_PENDING_AMENDS_STARTED();
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{42}), compose_lsn(epoch_t{43}, esn_t{50}));

  // Now restart
  restart();
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{1}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),  // window_high
                           max_ts,                             // ts_window_high
                           until_lsn);

  // The storage task comes back with 50 records.
  records.clear();
  for (unsigned int i = 1; i <= 50; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{51})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // max_records_in_flight in flight is 40, so we should only start a
  // RecordRebuildingStore state machine for the first 40 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{40}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 113);

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 113}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{41}));
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 113}});

  // This record was stored durably in previous iteration. That should have
  // no imapct on the current iteration.
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 114}});
  ASSERT_NO_PENDING_AMENDS_STARTED();
}

TEST_F(LogRebuildingTest, RestartAfterWindowDurable) {
  rebuilding_settings_.max_records_in_flight = 40;

  init();

  // We start reading with the local window ending at RecordTimestamp::now().
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));
  RecordTimestamp max_ts = RecordTimestamp::now();

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    max_ts,
                    1 /* version */,
                    1 /*restart version*/);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);
  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{42}.
  ASSERT_READ_STORAGE_TASK(compose_lsn(EPOCH_MIN, ESN_MIN),   // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX), // window_high
                           max_ts,                            // ts_window_high
                           until_lsn);

  // The storage task comes back with 50 records.
  RebuildingReadStorageTask::RecordContainer records;
  for (unsigned int i = 1; i <= 50; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{51})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);

  // max_records_in_flight in flight is 40, so we should only start a
  // RecordRebuildingStore state machine for the first 40 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{40}));

  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{41}));

  // Flush message is received from Node 1
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 111);
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{1}), {{{1, SERVER_INSTANCE_ID}, 112}});

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 11; ++i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 111}});
    // We already received notification from Node 1 about flushing memtable with
    // token 111. Amends shouls be started immediately.
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{42}), compose_lsn(epoch_t{43}, esn_t{50}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 111}});
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_NO_RECORD_REBUILDING_CREATED();
  onAllAmendsReceived(
      compose_lsn(epoch_t{43}, esn_t{2}), {{{1, SERVER_INSTANCE_ID}, 112}});

  onMemtableFlushed(1, SERVER_INSTANCE_ID, 112);

  for (uint32_t i = 50; i >= 12; --i) {
    onAllStoresReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 112}});
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(
        compose_lsn(epoch_t{43}, esn_t{i}), {{{1, SERVER_INSTANCE_ID}, 113}});
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // We have processed all record and have reached end of batch
  // No reads should be issued till we get a window message.
  ASSERT_NO_READ_STORAGE_TASK();
  ASSERT_NOTIFY_WINDOW_END_CALLED();

  // All amends are flushed as well
  onMemtableFlushed(1, SERVER_INSTANCE_ID, 115);
  fireNotifyRebuildingCoordinatorTimer();
  ASSERT_NOTIFY_LOG_REBUILDING_SIZE_CALLED();

  // Timer expires on Rebuilding Coordinator and it calls restart
  // before it received the updated status from this LogRebuilding.
  restart();
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{43}, esn_t{51}), // read_ptr
                           compose_lsn(epoch_t{43}, ESN_MAX),   // window_high
                           max_ts, // ts_window_high
                           until_lsn);

  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{51})};
  onReadTaskDone(records, E::WINDOW_END_REACHED, read_ptr);
  ASSERT_NOTIFY_WINDOW_END_CALLED();
}

// We read a checkpoint that has a LSN greater than the first LSN we are
// supposed to read from. Verify that the first storage task issued reads from
// that LSN.
TEST_F(LogRebuildingTest, ReadCheckpoint) {
  init();

  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));

  // The rebuilding node is in the nodeset of epochs
  // [1, 3] U [8, 8] U [11, ...]
  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{3},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  addEpochMetaDataToPlan(
      plan,
      epoch_t{8},
      epoch_t{8},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  addEpochMetaDataToPlan(
      plan,
      epoch_t{11},
      epoch_t{EPOCH_MAX},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = LSN_MAX;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No read storage task should be issued until we read the checkpoint.
  ASSERT_NO_READ_STORAGE_TASK();

  // We have a checkpoint at e8n42
  onCheckpointRead(E::OK, lsn_t{1}, compose_lsn(epoch_t{8}, esn_t{42}));

  // LogRebuilding should issue a storage task to start reading from
  // e8n43.
  ASSERT_READ_STORAGE_TASK(compose_lsn(epoch_t{8}, esn_t{43}), // read_ptr
                           compose_lsn(epoch_t{8}, ESN_MAX),   // window_high
                           RecordTimestamp::max(),             // ts_window_high
                           compose_lsn(EPOCH_MAX, ESN_MAX));   // until_lsn

  // The task reaches the end of the log with no records.
  RebuildingReadStorageTask::RecordContainer records;
  auto read_ptr = ReadPointer{LSN_MAX};
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX.
  ASSERT_CHECKPOINT(LSN_MAX);
}

// We read a checkpoint that contains LSN_MAX.
TEST_F(LogRebuildingTest, ReadCheckpointLSN_MAX) {
  init();

  auto rebuilding_set = std::make_shared<RebuildingSet>();
  rebuilding_set->all_dirty_time_intervals.insert(allRecordTimeInterval());
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = LSN_MAX;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // We read a checkpoint to LSN_MAX, this should cause the state machine to
  // complete.
  onCheckpointRead(E::OK, lsn_t{1}, LSN_MAX);
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // No read storage task should have been issued.
  ASSERT_NO_READ_STORAGE_TASK();
  // No checkpoint should have been written.
  ASSERT_NO_CHECKPOINT();
}

TEST_F(LogRebuildingTest, Simple2) {
  rebuilding_settings_.max_records_in_flight = 50;

  init();

  // We start reading with the local window ending at RecordTimestamp::max().
  auto rebuilding_set = std::make_shared<RebuildingSet>();

  RecordTimeIntervals n1_dirty_ranges;
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(10)),
                         RecordTimestamp(std::chrono::seconds(20))));

  rebuilding_set->all_dirty_time_intervals.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(10)),
                         RecordTimestamp(std::chrono::seconds(20))));
  rebuilding_set->shards.emplace(
      N1S0, RebuildingNodeInfo(RebuildingMode::RESTORE));
  lsn_t until_lsn = compose_lsn(epoch_t(43), esn_t(1000));

  RebuildingPlan plan;
  addEpochMetaDataToPlan(
      plan,
      EPOCH_MIN,
      epoch_t{43},
      StorageSet{N0S0, N1S0, N2S0, N3S0, N4S0, N5S0, N6S0, N7S0, N8S0, N9S0});
  plan.untilLSN = until_lsn;

  rebuilding->start(rebuilding_set,
                    std::move(plan),
                    RecordTimestamp::max(),
                    1 /* version */,
                    1 /* restart_version */);

  // No checkpoint is found.
  onCheckpointRead(E::NOTFOUND, LSN_INVALID, LSN_INVALID);

  // LogRebuilding should issue a storage task to start reading from the oldest
  // lsn with window_high=epoch_t{42}.
  ASSERT_READ_STORAGE_TASK(
      compose_lsn(EPOCH_MIN, ESN_MIN),           // read_ptr
      compose_lsn(epoch_t{43}, ESN_MAX),         // window_high
      RecordTimestamp(std::chrono::seconds(20)), // ts_window_high
      until_lsn);

  RebuildingReadStorageTask::RecordContainer records;
  auto read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{20})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // Since no records were returned, we should have scheduled a new batch
  // by activating readNewBatchTimer.
  ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED();
  ASSERT_READ_STORAGE_TASK(
      compose_lsn(epoch_t(43), esn_t{20}),       // read_ptr
      compose_lsn(epoch_t{43}, ESN_MAX),         // window_high
      RecordTimestamp(std::chrono::seconds(20)), // ts_window_high
      until_lsn);

  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{30})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // Since no records were returned, we should have scheduled a new batch
  // by activating readNewBatchTimer.
  ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED();
  ASSERT_READ_STORAGE_TASK(
      compose_lsn(epoch_t(43), esn_t{30}),       // read_ptr
      compose_lsn(epoch_t{43}, ESN_MAX),         // window_high
      RecordTimestamp(std::chrono::seconds(20)), // ts_window_high
      until_lsn);

  // The storage task comes back with 63 records.
  for (unsigned int i = 1; i <= 63; ++i) {
    records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{i}), 10));
  }

  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{64})};
  onReadTaskDone(records, E::BYTE_LIMIT_REACHED, read_ptr);

  // max_records_in_flight in flight is 50, so we should only start a
  // RecordRebuildingStore state machine for the first 50 recrods.
  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{1}), compose_lsn(epoch_t{43}, esn_t{50}));

  // With WAL, Stores are considered immediately durable
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{1}), FlushTokenMap());
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{1}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{51}));

  // The first record in the buffer completes.
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{1}), FlushTokenMap());

  // Records e43n3 to e43n13 complete. Even though e43n2 has not completed yet,
  // that should not prevent new RecordRebuildingStores from being started
  for (uint32_t i = 3; i <= 13; ++i) {
    onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
  }

  ASSERT_RECORD_REBUILDING_CREATED_RANGE(
      compose_lsn(epoch_t{43}, esn_t{52}), compose_lsn(epoch_t{43}, esn_t{62}));

  // Now e43n2 (at position 1 in the buffer) completes
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{2}), FlushTokenMap());
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{2}));
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{63}));
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{2}), FlushTokenMap());

  // The remaining records complete.
  for (uint32_t i = 63; i >= 14; --i) {
    onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
    ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{i}));
    onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{i}), FlushTokenMap());
  }
  ASSERT_NO_RECORD_REBUILDING_CREATED();

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_NEW_BATCH_TIMER_ACTIVATED();
  ASSERT_READ_STORAGE_TASK(
      compose_lsn(epoch_t{43}, esn_t{64}),       // read_ptr
      compose_lsn(epoch_t{43}, ESN_MAX),         // window_high
      RecordTimestamp(std::chrono::seconds(20)), // ts_window_high
      until_lsn);

  // The task comes back with 1 record.
  records.clear();
  records.push_back(createFakeRecord(compose_lsn(epoch_t{43}, esn_t{64}), 10));
  read_ptr = ReadPointer{compose_lsn(epoch_t{43}, esn_t{900})};
  onReadTaskDone(records, E::PARTIAL, read_ptr);

  // The record is rebuilt.
  ASSERT_RECORD_REBUILDING_CREATED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllStoresReceived(compose_lsn(epoch_t{43}, esn_t{64}), FlushTokenMap());
  ASSERT_NO_RECORD_REBUILDING_CREATED();
  ASSERT_PENDING_AMENDS_STARTED(compose_lsn(epoch_t{43}, esn_t{64}));
  onAllAmendsReceived(compose_lsn(epoch_t{43}, esn_t{64}), FlushTokenMap());

  // The buffer is empty. An new storage task is issued.
  ASSERT_READ_STORAGE_TASK(
      compose_lsn(epoch_t{43}, esn_t{900}),      // read_ptr
      compose_lsn(epoch_t{43}, ESN_MAX),         // window_high
      RecordTimestamp(std::chrono::seconds(20)), // ts_window_high
      until_lsn);

  // The task comes back with 0 records and we are done.
  records.clear();
  onReadTaskDone(records, E::UNTIL_LSN_REACHED, read_ptr);
  // We are done.
  ASSERT_TRUE(received.notifyCompleteWasCalled);
  // We should write a checkpoint with LSN_MAX because we rebuilt everything.
  ASSERT_CHECKPOINT(LSN_MAX);
}

// TODO(#7781951): write a test to verify the timestamp window behavior, in
//                 particular, that LogRebuilding stops issuing storage tasks if
//                 the last one came back with E::WINDOW_END_REACHED because we
//                 reached the end of the timestamp window, and waits for
//                 RebuildingCoordinator to notify that the window was slid.

// TODO(#7781951): write a test to verify that the sliding window of
//                 RecordRebuildingStore state machines is working properly.

}} // namespace facebook::logdevice
