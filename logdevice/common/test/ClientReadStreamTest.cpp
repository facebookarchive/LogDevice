/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStream.h"

#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

#include <folly/MapUtil.h>
#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/client_read_stream/ClientReadStreamConnectionHealth.h"
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)

namespace facebook { namespace logdevice {

using ConnectionState = ClientReadStreamSenderState::ConnectionState;
using PerShardStatusMap = std::unordered_map<ShardID, Status, ShardID::Hash>;
using RecordSource = MetaDataLogReader::RecordSource;

struct StartMessage {
  ShardID dest;
  lsn_t start_lsn;
  lsn_t until_lsn;
  lsn_t window_high;
  filter_version_t filter_version;
  bool scd_enabled;
  SCDCopysetReordering scd_copyset_reordering;
  small_shardset_t filtered_out;
  ReadStreamAttributes attrs;

  bool operator==(const StartMessage& other) const {
    auto as_tuple = [](const StartMessage& m) {
      return std::make_tuple(m.dest,
                             m.start_lsn,
                             m.until_lsn,
                             m.window_high,
                             m.filter_version,
                             m.scd_enabled,
                             std::unordered_set<ShardID, ShardID::Hash>{
                                 m.filtered_out.begin(), m.filtered_out.end()},
                             m.attrs);
      // `scd_copyset_reordering' not considered as most tests don't care
      // about it
    };
    return as_tuple(*this) == as_tuple(other);
  }
};

struct WindowMessage {
  ShardID dest;
  lsn_t low;
  lsn_t high;

  bool operator==(const WindowMessage& other) const {
    auto as_tuple = [](const WindowMessage& m) {
      return std::tie(m.dest, m.low, m.high);
    };
    return as_tuple(*this) == as_tuple(other);
  }
};

struct GapMessage {
  GapType type;
  lsn_t low;
  lsn_t high;

  bool operator==(const GapMessage& other) const {
    auto as_tuple = [](const GapMessage& m) {
      return std::tie(m.type, m.low, m.high);
    };
    return as_tuple(*this) == as_tuple(other);
  }
};

::std::ostream& operator<<(::std::ostream& os, const StartMessage& m) {
  os << "{";
  os << "dest: " << m.dest.toString() << ", ";
  os << "start_lsn: " << lsn_to_string(m.start_lsn) << ", ";
  os << "until_lsn: " << lsn_to_string(m.until_lsn) << ", ";
  os << "window_high: " << lsn_to_string(m.window_high) << ", ";
  os << "filter_version: " << m.filter_version.val_ << ", ";
  os << "scd_enabled: " << m.scd_enabled << ", ";
  os << "scd_copyset_reordering: " << static_cast<int>(m.scd_copyset_reordering)
     << ", ";
  os << "filtered_out: {";
  for (ShardID shard : m.filtered_out) {
    os << shard.toString() << ", ";
  }
  os << "}}";
  return os;
};

::std::ostream& operator<<(::std::ostream& os, const WindowMessage& m) {
  os << "{";
  os << "dest: " << m.dest.toString() << ", ";
  os << "low: " << lsn_to_string(m.low) << ", ";
  os << "high: " << lsn_to_string(m.high) << "}";
  return os;
};

::std::ostream& operator<<(::std::ostream& os, const GapMessage& m) {
  os << "{";
  os << "type: " << (int)m.type << ", ";
  os << "low: " << lsn_to_string(m.low) << ", ";
  os << "high: " << lsn_to_string(m.high) << "}";
  return os;
};

const logid_t LOG_ID(1);
const lsn_t LSN_MIN(compose_lsn(EPOCH_MIN, ESN_MIN));

namespace {

struct CacheEntry {
  epoch_t epoch;
  epoch_t until;
  RecordSource source;
  EpochMetaData metadata;
};

} // namespace

struct TestState {
  TestState()
      : config(std::make_shared<UpdateableConfig>()),
        settings(create_default_settings<Settings>()) {}
  std::vector<ShardID> shards{N0, N1, N2, N3, N4, N5};

  // Outgoing messages from ClientReadStream go into these vectors
  std::vector<lsn_t> recv;
  std::vector<StartMessage> start;
  std::vector<ShardID> stop;
  std::vector<WindowMessage> window;
  std::vector<GapMessage> gap;

  std::unordered_map<ShardID, SocketCallback*, ShardID::Hash> on_close;
  std::unordered_map<node_index_t, bool> cluster_state;
  std::vector<epoch_t> metadata_req;
  bool callbacks_accepting = true;
  bool disposed = false;

  // default metadata to be delivered when epoch metadata is requested
  EpochMetaData default_metadata;
  // If set, deps_->getMetaDataForEpoch() will be a no-op and tests are
  // required to directly call onEpochMetaData() to simulate getting metadata
  // in an asynchronous manner. This is used by tests that test nodeset related
  // features.
  // By default this is not set, and the function will deliver the default
  // metadata (set by the test read stream) with until epoch set to
  // lsn_to_epoch(LSN_MAX). The read stream should no longer request epoch
  // metadata again. This is to allow all existing test items that do not test
  // nodeset feature to pass.
  bool disable_default_metadata = false;

  std::shared_ptr<UpdateableConfig> config;
  Settings settings;

  // Map of error codes to be returned by sendStartMessage() for a given
  // ShardID.
  PerShardStatusMap send_start_errors;

  // Protocol version overrides for sockets to servers
  std::unordered_map<node_index_t, uint16_t> protos;

  EventLogRebuildingSet rebuilding_set;

  bool nodes_may_send_shard_status_update_message = true;

  bool connection_healthy = true;

  // last metadata request requires consistent entries from the cache
  folly::Optional<bool> require_consistent_from_cache;

  std::vector<CacheEntry> cache_entries_;

  bool has_memory_pressure = false;
  std::unordered_map<ShardID, ClientReadStreamSenderState, ShardID::Hash>*
      storage_set_states;
};

/**
 * Mock implementation of ClientReadStreamDependencies that captures all
 * outgoing communication from ClientReadStream to allow inspection by tests.
 */
class MockClientReadStreamDependencies : public ClientReadStreamDependencies {
 public:
  explicit MockClientReadStreamDependencies(TestState& state) : state_(state) {}

  bool getMetaDataForEpoch(read_stream_id_t /*rsid*/,
                           epoch_t epoch,
                           MetaDataLogReader::Callback cb,
                           bool /*allow_from_cache*/,
                           bool require_consistent_from_cache) override {
    state_.metadata_req.push_back(epoch);
    state_.require_consistent_from_cache = require_consistent_from_cache;

    if (state_.disable_default_metadata) {
      // The test explicitly calls onEpochMetaData().
      return false;
    }

    // otherwise, provide the default metadata and make it effective
    // for all future epochs
    ld_check(state_.default_metadata.isValid());
    std::unique_ptr<EpochMetaData> metadata =
        std::make_unique<EpochMetaData>(state_.default_metadata);

    cb(E::OK,
       MetaDataLogReader::Result{LOG_ID,
                                 epoch,
                                 lsn_to_epoch(LSN_MAX),
                                 RecordSource::LAST,
                                 compose_lsn(epoch_t(epoch), esn_t(1)),
                                 std::chrono::milliseconds(0),
                                 std::move(metadata)});
    return true;
  }

  void
  updateEpochMetaDataCache(epoch_t epoch,
                           epoch_t until,
                           const EpochMetaData& metadata,
                           MetaDataLogReader::RecordSource source) override {
    state_.cache_entries_.push_back(CacheEntry{epoch, until, source, metadata});
  }

  int sendStartMessage(ShardID shard,
                       SocketCallback* onclose,
                       START_Header header,
                       const small_shardset_t& filtered_out,
                       const ReadStreamAttributes* attrs) override {
    // Check if the test wants to simulate failure to send START to that shard.
    auto it = state_.send_start_errors.find(shard);
    if (it != state_.send_start_errors.end()) {
      err = it->second;
      return -1;
    }
    ReadStreamAttributes reader_attrs =
        attrs == nullptr ? ReadStreamAttributes() : (*attrs);
    state_.start.push_back(
        StartMessage{shard,
                     lsn_t(header.start_lsn),
                     lsn_t(header.until_lsn),
                     lsn_t(header.window_high),
                     header.filter_version,
                     (bool)(header.flags & START_Header::SINGLE_COPY_DELIVERY),
                     header.scd_copyset_reordering,
                     filtered_out,
                     reader_attrs});
    state_.on_close[shard] = onclose;

    return 0;
  }

  int sendStopMessage(ShardID shard) override {
    state_.stop.push_back(shard);
    return 0;
  }

  int sendWindowMessage(ShardID shard,
                        lsn_t window_low,
                        lsn_t window_high) override {
    EXPECT_LE(window_low, window_high);
    state_.window.push_back(WindowMessage{shard, window_low, window_high});
    return 0;
  }

  bool recordCallback(std::unique_ptr<DataRecord>& record) override {
    state_.recv.push_back(record->attrs.lsn);
    return state_.callbacks_accepting;
  }

  bool gapCallback(const GapRecord& gap) override {
    state_.gap.push_back(GapMessage{gap.type, gap.lo, gap.hi});
    return state_.callbacks_accepting;
  }

  void healthCallback(bool is_healthy) override {
    state_.connection_healthy = is_healthy;
  }

  void dispose() override {
    state_.disposed = true;
  }

  // Stub out timer functionality for now

  std::unique_ptr<BackoffTimer>
  createBackoffTimer(std::chrono::milliseconds,
                     std::chrono::milliseconds) override {
    return std::make_unique<MockBackoffTimer>();
  }

  std::unique_ptr<BackoffTimer> createBackoffTimer(
      const chrono_expbackoff_t<std::chrono::milliseconds>&) override {
    return std::make_unique<MockBackoffTimer>();
  }

  std::unique_ptr<Timer>
  createTimer(std::function<void()> /*cb*/ = nullptr) override {
    return std::make_unique<MockTimer>();
  }

  void setClientReadStream(ClientReadStream* client_read_stream) {
    client_read_stream_ = client_read_stream;
  }

  std::function<ClientReadStream*(read_stream_id_t)>
  getStreamByIDCallback() override {
    ld_check(client_read_stream_);
    return [=](read_stream_id_t /*rsid*/) { return client_read_stream_; };
  }

  const Settings& getSettings() const override {
    return state_.settings;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    auto cfg = state_.config->get();
    return cfg->serverConfig()->getNodesConfigurationFromServerConfigSource();
  }

  ShardAuthoritativeStatusMap getShardStatus() const override {
    auto cfg = state_.config->get();
    return state_.rebuilding_set.toShardStatusMap(*getNodesConfiguration());
  }

  void refreshClusterState() override {}

  folly::Optional<uint16_t>
  getSocketProtocolVersion(node_index_t nid) const override {
    return folly::get_default(
        state_.protos, nid, Compatibility::MAX_PROTOCOL_SUPPORTED);
  }

  bool hasMemoryPressure() const override {
    return state_.has_memory_pressure;
  }

 private:
  ClientReadStream* client_read_stream_ = nullptr;
  TestState& state_;
};

static std::unique_ptr<DataRecordOwnsPayload>
mockRecord(lsn_t lsn, RECORD_flags_t flags = 0) {
  std::chrono::milliseconds timestamp(0);

  static const char* data = "data";
  int payload_size = strlen(data);
  void* payload = malloc(payload_size);
  ld_check(payload);
  memcpy(payload, data, payload_size);

  return std::make_unique<DataRecordOwnsPayload>(
      LOG_ID, Payload(payload, payload_size), lsn, timestamp, flags);
}

static GAP_Message mockGap(ShardID shard,
                           lsn_t lo,
                           lsn_t hi,
                           GapReason reason = GapReason::NO_RECORDS) {
  GAP_Header gap{
      LOG_ID,
      read_stream_id_t(shard.node() + 1),
      lo,
      hi,
      reason,
      GAP_flags_t{0},
      0 /* shard */
  };
  return GAP_Message(gap, TrafficClass::READ_BACKLOG);
}

static lsn_t lsn(int epoch, int esn) {
  return compose_lsn(epoch_t(epoch), esn_t(esn));
}

// Helper function to calculate the max LSN that can fit into a window
lsn_t calc_buffer_max(lsn_t start, size_t buffer_size) {
  return start + buffer_size - 1;
}

/**
 * Fixture that performs setup common to all tests.  Tests can adjust
 * parameters like start_lsn_ before calling start().
 */
class ClientReadStreamTest
    : public ::testing::TestWithParam<ClientReadStreamBufferType> {
 public:
  void SetUp() override {
    dbg::currentLevel = dbg::Level::SPEW;
    dbg::assertOnData = true;
    state_ = TestState();
  }

  void start(logid_t logid = LOG_ID,
             const ReadStreamAttributes* attrs = nullptr) {
    std::unique_ptr<ClientReadStreamDependencies> deps(
        new MockClientReadStreamDependencies(state_));

    updateConfig();
    read_stream_ = std::make_unique<ClientReadStream>(
        read_stream_id_t(1),
        logid,
        start_lsn_,
        until_lsn_,
        flow_control_threshold_,
        buffer_type_.hasValue() ? buffer_type_.value() : GetParam(),
        buffer_size_,
        std::move(deps),
        state_.config,
        nullptr,
        attrs);
    static_cast<MockClientReadStreamDependencies&>(read_stream_->getDeps())
        .setClientReadStream(read_stream_.get());
    state_.storage_set_states = &read_stream_->storage_set_states_;
    updateMetaData();

    if (require_full_read_set_) {
      read_stream_->requireFullReadSet();
    }
    if (ignore_released_status_) {
      read_stream_->ignoreReleasedStatus();
    }
    if (do_not_skip_partially_trimmed_sections_) {
      read_stream_->doNotSkipPartiallyTrimmedSections();
    }

    read_stream_->start();
    // Simulate storage nodes having replied STARTED already
    overrideConnectionStates(ConnectionState::READING);
  }

  void onDataRecord(ShardID shard,
                    std::unique_ptr<DataRecordOwnsPayload> record) {
    read_stream_->onDataRecord(shard, std::move(record));
  }

  void onGap(ShardID shard, const GAP_Message& msg) {
    read_stream_->onGap(shard, msg);
  }

  void overrideConnectionStates(ConnectionState state,
                                std::vector<ShardID> shards) {
    std::shared_ptr<Configuration> config = state_.config->get();
    for (ShardID shard : shards) {
      auto it = read_stream_->storage_set_states_.find(shard);
      if (it == read_stream_->storage_set_states_.end()) {
        continue;
      }
      read_stream_->overrideConnectionState(shard, state);
    }
  }

  void overrideConnectionStates(ConnectionState state) {
    overrideConnectionStates(state, state_.shards);
  }

  void reconnectTimerCallback(ShardID shard) {
    ASSERT_TRUE(read_stream_->reconnectTimerIsActive(shard));
    read_stream_->reconnectTimerCallback(shard);
  }

  void startedTimerCallback(ShardID shard) {
    read_stream_->startedTimerCallback(shard);
  }

  bool reconnectTimerIsActive(ShardID shard) const {
    return read_stream_->reconnectTimerIsActive(shard);
  }

  void fireReadMetadataRetryTimer() {
    ASSERT_NE(nullptr, read_stream_->retry_read_metadata_);
    ASSERT_TRUE(read_stream_->retry_read_metadata_->isActive());
    dynamic_cast<MockBackoffTimer*>(read_stream_->retry_read_metadata_.get())
        ->trigger();
  }

  void onStartSent(ShardID shard, Status status) {
    read_stream_->onStartSent(shard, status);
  }

  /**
   * Updates state_.config with shards from state_.shards.
   */
  void updateConfig() {
    configuration::Nodes nodes;
    for (ShardID shard : state_.shards) {
      Configuration::Node& node = nodes[shard.node()];
      node.address =
          Sockaddr("::1", folly::to<std::string>(4440 + shard.node()));
      node.generation = 1;
      node.addSequencerRole();
      node.addStorageRole();

      auto it = node_locations_.find(shard.node());
      if (it != node_locations_.end()) {
        NodeLocation loc;
        int rv = loc.fromDomainString(it->second);
        ASSERT_EQ(0, rv);
        node.location = std::move(loc);
      }
    }

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(replication_factor_);
    log_attrs.set_syncReplicationScope(sync_replication_scope_);
    log_attrs.set_scdEnabled(scd_enabled_);
    log_attrs.set_maxWritesInFlight(sequencer_window_size_);

    Configuration::NodesConfig nodes_config(std::move(nodes));
    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    logs_config->insert(boost::icl::right_open_interval<logid_t::raw_type>(
                            LOG_ID.val_, LOG_ID.val_ + 1),
                        "log",
                        log_attrs);

    // metadata stored on all nodes with max replication factor 3
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);

    state_.config->updateableServerConfig()->update(
        ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
    state_.config->updateableNodesConfiguration()->update(
        getNodesConfiguration());
    state_.config->updateableLogsConfig()->update(std::move(logs_config));
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    auto cfg = state_.config->get();
    return cfg->serverConfig()->getNodesConfigurationFromServerConfigSource();
  }

  BackoffTimer* getGracePeriodTimer() const {
    return read_stream_->grace_period_.get();
  }
  BackoffTimer* getRetryReadMetaDataTimer() const {
    return read_stream_->retry_read_metadata_.get();
  }
  BackoffTimer* getRedeliveryTimer() const {
    return read_stream_->redelivery_timer_.get();
  }
  BackoffTimer* getReattemptStartTimer() const {
    return read_stream_->reattempt_start_timer_.get();
  }
  BackoffTimer* getStallGracePeriodTimer() const {
    return read_stream_->connection_health_tracker_->stall_grace_period_.get();
  }

  bool isCurrentlyInSingleCopyDeliveryMode() const {
    return read_stream_->scd_ && read_stream_->scd_->isActive();
  }

  void scdShardsDownFailoverTimerCallback() {
    ld_check(read_stream_->scd_);
    read_stream_->scd_->shards_down_failover_timer_.callback();
  }

  void scdAllSendAllFailoverTimerCallback() {
    ld_check(read_stream_->scd_);
    read_stream_->scd_->all_send_all_failover_timer_.callback();
  }

  bool rewindScheduled() {
    return read_stream_->rewindScheduled();
  }

  void triggerScheduledRewind() {
    EXPECT_TRUE(rewindScheduled());
    read_stream_->rewind("test");
  }

  // generate a metadata entry with given _epoch_ using all shards in
  // state_.shards and current replication_factor_
  EpochMetaData genMetaDataForEpoch(epoch_t epoch) {
    return EpochMetaData(
        state_.shards,
        ReplicationProperty(replication_factor_, sync_replication_scope_),
        epoch,
        epoch);
  }

  // change metadata to be returned when reading starts and
  // disable_default_metadata is not False.
  void updateMetaData() {
    state_.default_metadata = genMetaDataForEpoch(EPOCH_MIN);
  }

  // helper method to deliver an epoch metadata for _epoch_ to the
  // readstream
  void onEpochMetaData(epoch_t epoch,
                       epoch_t since,
                       epoch_t until,
                       int replication,
                       NodeLocationScope sync_replication_scope,
                       StorageSet shards,
                       RecordSource source = RecordSource::NOT_LAST) {
    ld_check(read_stream_);
    read_stream_->onEpochMetaData(
        E::OK,
        MetaDataLogReader::Result{
            LOG_ID,
            epoch,
            until,
            source,
            compose_lsn(epoch_t(epoch), esn_t(1)),
            std::chrono::milliseconds(0),
            std::make_unique<EpochMetaData>(
                shards,
                ReplicationProperty(replication, sync_replication_scope),
                since,
                since)});
  }

  epoch_t getLastEpochWithMetaData() const {
    return read_stream_->last_epoch_with_metadata_;
  }

  // Make it so that each time ClientReadStream calls sendStart() for a given
  // shard, the call fails with status `err`.
  void setSendStartErrorForShard(ShardID shard, Status err) {
    if (err == E::OK) {
      state_.send_start_errors.erase(shard);
    } else {
      state_.send_start_errors[shard] = err;
    }
  }

  lsn_t addToRebuildingSet(ShardID shard) {
    auto cfg = state_.config->get();
    lsn_t lsn = state_.rebuilding_set.getLastUpdate() + 1;
    SHARD_NEEDS_REBUILD_Header h(
        shard.node(), shard.shard(), "unittest", "ClientReadStreamTest", 0);
    state_.rebuilding_set.update(lsn,
                                 std::chrono::milliseconds(),
                                 SHARD_NEEDS_REBUILD_Event(h),
                                 *getNodesConfiguration());
    read_stream_->applyShardStatus("addToRebuildingSet");
    return lsn;
  }

  void markUnrecoverable(ShardID shard) {
    auto cfg = state_.config->get();
    lsn_t lsn = state_.rebuilding_set.getLastUpdate() + 1;
    SHARD_UNRECOVERABLE_Header h{
        shard.node(), static_cast<uint32_t>(shard.shard())};
    state_.rebuilding_set.update(lsn,
                                 std::chrono::milliseconds(),
                                 SHARD_UNRECOVERABLE_Event(h),
                                 *getNodesConfiguration());
    read_stream_->applyShardStatus("markUnrecoverable");
  }

  void onAllDonorsFinishRebuilding(lsn_t version) {
    auto cfg = state_.config->get();
    for (ShardID shard : state_.shards) {
      lsn_t lsn = state_.rebuilding_set.getLastUpdate() + 1;
      SHARD_IS_REBUILT_Header h{
          shard.node(), static_cast<uint32_t>(shard.shard()), version};
      state_.rebuilding_set.update(lsn,
                                   std::chrono::milliseconds(),
                                   SHARD_IS_REBUILT_Event(h),
                                   *getNodesConfiguration());
      read_stream_->applyShardStatus("onAllDonorsFinishRebuilding");
    }
  }

  struct TestStep {
    enum {
      RECORDS,
      WINDOW,
    } type;
    lsn_t start;
    lsn_t end;
  };

  void runTestSteps(std::vector<TestStep> steps);

  lsn_t start_lsn_ = LSN_MIN;
  lsn_t until_lsn_ = LSN_MAX;
  double flow_control_threshold_ = 0.5;
  size_t replication_factor_ = 1;
  NodeLocationScope sync_replication_scope_ = NodeLocationScope::NODE;
  int sequencer_window_size_ = 2;

  size_t buffer_size_ = 1;
  bool require_full_read_set_ = false;
  bool scd_enabled_ = false;
  bool ignore_released_status_ = false;
  bool do_not_skip_partially_trimmed_sections_ = false;
  std::unordered_map<node_index_t, std::string> node_locations_;

  TestState state_;

  std::unique_ptr<ClientReadStream> read_stream_;

  // if specified in a test, the read stream will use the specified buffer type
  // rather than generating a test for each possilbe buffer types
  folly::Optional<ClientReadStreamBufferType> buffer_type_;
};

//
// Helper macros for assertions on recv, start_ and stop_.  The macros
// consume the vector elements, making it simpler to check for changes in
// them.
//

bool window_cmp(WindowMessage const& a, WindowMessage const& b) {
  return a.dest < b.dest;
}

bool start_cmp(StartMessage const& a, StartMessage const& b) {
  return a.dest < b.dest;
}

#define ASSERT_RECV(...)                                         \
  {                                                              \
    ASSERT_EQ((std::vector<lsn_t>({__VA_ARGS__})), state_.recv); \
    state_.recv.clear();                                         \
  }

#define ASSERT_START_MESSAGES(start_lsn,                                   \
                              until_lsn,                                   \
                              window_high,                                 \
                              filter_version,                              \
                              scd_enabled,                                 \
                              filtered_out,                                \
                              ...)                                         \
  {                                                                        \
    std::stable_sort(state_.start.begin(), state_.start.end(), start_cmp); \
    std::vector<StartMessage> tmp;                                         \
    for (auto shard : {__VA_ARGS__}) {                                     \
      tmp.push_back(StartMessage{shard,                                    \
                                 start_lsn,                                \
                                 until_lsn,                                \
                                 window_high,                              \
                                 filter_version,                           \
                                 scd_enabled,                              \
                                 SCDCopysetReordering::NONE,               \
                                 filtered_out,                             \
                                 ReadStreamAttributes()});                 \
    }                                                                      \
    ASSERT_EQ(tmp, state_.start);                                          \
    state_.start.clear();                                                  \
  }

#define ASSERT_NO_START_MESSAGES() \
  { ASSERT_TRUE(state_.start.empty()); }

// TODO: add shard
#define ON_STARTED_FULL(filter_version, status, last_released, ...)    \
  {                                                                    \
    STARTED_Header header = {LOG_ID,                                   \
                             read_stream_id_t(1),                      \
                             status,                                   \
                             filter_version,                           \
                             last_released,                            \
                             /*shard_idx*/ 0};                         \
    for (auto shard : {__VA_ARGS__}) {                                 \
      read_stream_->onStarted(                                         \
          shard, STARTED_Message(header, TrafficClass::READ_BACKLOG)); \
    }                                                                  \
  }

#define ON_STARTED(filter_version, ...) \
  ON_STARTED_FULL(filter_version, E::OK, LSN_INVALID, __VA_ARGS__)

#define ON_STARTED_REBUILDING(filter_version, ...) \
  ON_STARTED_FULL(filter_version, E::REBUILDING, LSN_INVALID, __VA_ARGS__)

#define ON_STARTED_FAILED(filter_version, ...) \
  ON_STARTED_FULL(filter_version, E::FAILED, LSN_INVALID, __VA_ARGS__)

#define ON_STARTED_ACCESS(filter_version, ...) \
  ON_STARTED_FULL(filter_version, E::ACCESS, LSN_INVALID, __VA_ARGS__)

#define ON_STARTED_SYSLIMIT(filter_version, ...) \
  ON_STARTED_FULL(filter_version, E::SYSLIMIT, LSN_INVALID, __VA_ARGS__)

#define ASSERT_STOP_MESSAGES(...)                                  \
  {                                                                \
    std::stable_sort(state_.stop.begin(), state_.stop.end());      \
    ASSERT_EQ((std::vector<ShardID>({__VA_ARGS__})), state_.stop); \
    state_.stop.clear();                                           \
  }

#define ASSERT_WINDOW_MESSAGES(lo, hi, ...)                                   \
  {                                                                           \
    std::stable_sort(state_.window.begin(), state_.window.end(), window_cmp); \
    std::vector<WindowMessage> tmp;                                           \
    for (auto shard : {__VA_ARGS__}) {                                        \
      tmp.push_back(WindowMessage{shard, lo, hi});                            \
    }                                                                         \
    ASSERT_EQ(tmp, state_.window);                                            \
    state_.window.clear();                                                    \
  }

#define ASSERT_NO_WINDOW_MESSAGES() \
  { ASSERT_EQ(std::vector<WindowMessage>(), state_.window); }

#define ASSERT_GAP_MESSAGES(...)                                     \
  do {                                                               \
    ASSERT_EQ((std::vector<GapMessage>({__VA_ARGS__})), state_.gap); \
    state_.gap.clear();                                              \
  } while (0)

#define ASSERT_METADATA_REQ(...)                                           \
  do {                                                                     \
    ASSERT_EQ((std::vector<epoch_t>({__VA_ARGS__})), state_.metadata_req); \
    state_.metadata_req.clear();                                           \
    state_.require_consistent_from_cache.clear();                          \
  } while (0)

#define SET_NODE_ALIVE(nid, alive) state_.cluster_state[nid] = alive

void ClientReadStreamTest::runTestSteps(std::vector<TestStep> steps) {
  for (auto step : steps) {
    switch (step.type) {
      case TestStep::RECORDS:
        // simulate sending records
        for (lsn_t i = step.start; i < step.end; i++) {
          read_stream_->onDataRecord(N0, mockRecord(lsn(1, i)));
          ASSERT_RECV(lsn(1, i));
        }
        break;
      case TestStep::WINDOW:
        ASSERT_WINDOW_MESSAGES(step.start, step.end, N0, N1, N2, N3);
        break;
    }
  }
}

// create unit test for each of the ClientReadStreamBuffer implementations
INSTANTIATE_TEST_CASE_P(
    ClientReadStreamTest,
    ClientReadStreamTest,
    ::testing::Values(ClientReadStreamBufferType::CIRCULAR,
                      ClientReadStreamBufferType::ORDERED_MAP));

/**
 * Simple test where records come in order from different nodes.
 */
TEST_P(ClientReadStreamTest, Simple) {
  start();
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  ASSERT_RECV(lsn(1, 3));
  onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 4));
}

/**
 * Buffering and delivering records that are received out of order.
 */
TEST_P(ClientReadStreamTest, Buffering) {
  buffer_size_ = 3;
  start();
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 4), lsn(1, 5), lsn(1, 6));
}

/**
 * If multiple storage nodes send the same record, we must not deliver it only
 * once.
 */
TEST_P(ClientReadStreamTest, Deduplication) {
  buffer_size_ = 2;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));
  onDataRecord(N1, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV();

  // What if we buffer?
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  ASSERT_RECV();
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2), lsn(1, 3));
}

/**
 * With LSNs at the edge of the range there is the danger of overflow when
 * deciding if the record can be buffered.
 */
TEST_P(ClientReadStreamTest, HighLSNOverflow) {
  buffer_size_ = 100;
  start_lsn_ = LSN_MAX - 10;
  start();
  onDataRecord(N0, mockRecord(start_lsn_ + 1));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(start_lsn_));
  ASSERT_RECV(start_lsn_, start_lsn_ + 1);
}

/**
 * With a buffer large enough to fit the entire range of records, need to take
 * care to cap the flow control window to until_lsn.
 */
TEST_P(ClientReadStreamTest, LowUntilLSNLargeBuffer) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  buffer_size_ = 4096;
  state_.shards.resize(1);
  start();
  // Before a bug was fixed, window_high in the START message would be larger
  // than until_lsn.
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        until_lsn_,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);
}

/**
 * The ClientReadStream destructor should send STOP messages to storage nodes
 * so that they don't keep sending records.
 */
TEST_P(ClientReadStreamTest, Stop) {
  start();
  ASSERT_STOP_MESSAGES();
  read_stream_.reset();
  sort(state_.stop.begin(), state_.stop.end()); // order does not matter
  ASSERT_STOP_MESSAGES(state_.shards);
}

/**
 * The client should broadcast WINDOW messages exactly when the
 * threshold is reached.
 */
TEST_P(ClientReadStreamTest, LargeBuffer) {
  state_.shards.resize(4);
  buffer_size_ = 1000;
  std::vector<std::pair<int, lsn_t>> lsns;

  // it should not trigger flow control as long as next_lsn_to_deliver_
  // does not reach boundary_lsn
  lsn_t boundary_lsn =
      (buffer_size_ - 1) * flow_control_threshold_ + start_lsn_;

  // the maximum possible lsn the client can accept
  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  std::hash<int> hash_fn;
  for (lsn_t i = lsn(1, 1); i < boundary_lsn; ++i) {
    int nodeID = hash_fn(i) % 4;
    lsns.push_back(std::make_pair(nodeID, i));
  }

  // these messages should be buffered and should not trigger flow control.
  lsns.push_back(std::make_pair(0, lsn(1, 800)));
  lsns.push_back(std::make_pair(1, lsn(1, 900)));
  lsns.push_back(std::make_pair(3, lsn(1, 1000)));

  // the client should tell storage nodes of its buffer limit initially
  start();
  ASSERT_START_MESSAGES(start_lsn_,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // no start/stop messages should be sent
  for (int i = 0; i < lsns.size(); ++i) {
    lsn_t lsn = lsns[i].second;
    onDataRecord(state_.shards[lsns[i].first], mockRecord(lsn));
    ASSERT_NO_START_MESSAGES();
    ASSERT_STOP_MESSAGES();
    ASSERT_NO_WINDOW_MESSAGES()
  }

  state_.recv.clear();
  // trigger flow control

  onDataRecord(N2, mockRecord(boundary_lsn));
  ASSERT_RECV(boundary_lsn);
  lsn_t next_lsn = boundary_lsn + 1;
  buffer_max = calc_buffer_max(next_lsn, buffer_size_);
  ASSERT_WINDOW_MESSAGES(next_lsn, buffer_max, N0, N1, N2, N3);
}

/**
 * If reading a small range of LSNs that all fit into the window, we should
 * never slide the window.
 */
TEST_P(ClientReadStreamTest, SmallRange) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 10);
  buffer_size_ = until_lsn_ - start_lsn_ + 1;
  state_.shards = {N0};

  start();
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        until_lsn_,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);

  for (lsn_t i = start_lsn_; i <= until_lsn_; ++i) {
    onDataRecord(N0, mockRecord(lsn(1, i)));
    ASSERT_RECV(lsn(1, i));
  }

  // Should not have sent any WINDOW messages
  ASSERT_NO_WINDOW_MESSAGES()
}

TEST_P(ClientReadStreamTest, MultipleWindowRound) {
  state_.shards.resize(4);
  buffer_size_ = 7;
  until_lsn_ = lsn(1, 15);
  flow_control_threshold_ = 0.5;

  start();
  ASSERT_START_MESSAGES(lsn(1, 1),
                        lsn(1, 15),
                        lsn(1, 7),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // buffer_size_(7) * flow_control_threshold_(0.5) + start_lsn_(1) = 4.5
  // int(4.5) = 4
  // flow control should be triggered after receiving the first 4 records.
  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N3, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 6)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));

  // no WINDOW message should be issued, as the client is still
  // waiting for lsn 2
  ASSERT_NO_WINDOW_MESSAGES()
  ASSERT_RECV(lsn(1, 1));

  onDataRecord(N3, mockRecord(lsn(1, 2)));
  // got the first 4 recods, WINDOW message should be broadcasted
  // buffer_size_(7) * flow_control_threshold(0.5)
  // + next_lsn_to_deliver(7) = 10.5; int(10.5) = 10;
  // window_high = 7 + 6 = 14
  ASSERT_RECV(lsn(1, 2), lsn(1, 3), lsn(1, 4), lsn(1, 5), lsn(1, 6));
  ASSERT_WINDOW_MESSAGES(lsn(1, 7), lsn(1, 13), N0, N1, N2, N3);

  onDataRecord(N2, mockRecord(lsn(1, 7)));
  onDataRecord(N0, mockRecord(lsn(1, 8)));
  onDataRecord(N0, mockRecord(lsn(1, 11)));
  onDataRecord(N1, mockRecord(lsn(1, 9)));
  onDataRecord(N3, mockRecord(lsn(1, 12)));
  onDataRecord(N3, mockRecord(lsn(1, 13)));

  ASSERT_RECV(lsn(1, 7), lsn(1, 8), lsn(1, 9));
  // no WINDOW message should be issued, as the client is still
  // waiting for lsn 10
  ASSERT_NO_WINDOW_MESSAGES()

  onDataRecord(N2, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 10), lsn(1, 11), lsn(1, 12), lsn(1, 13));
  ASSERT_WINDOW_MESSAGES(lsn(1, 14), lsn(1, 15), N0, N1, N2, N3);

  onDataRecord(N3, mockRecord(lsn(1, 15)));
  onDataRecord(N0, mockRecord(lsn(1, 14)));
  ASSERT_RECV(lsn(1, 14), lsn(1, 15));
  // no more WINDOW_MESSAGES, as until_lsn is reached.
  ASSERT_NO_WINDOW_MESSAGES()
}

TEST_P(ClientReadStreamTest, DynamicWindowScaling) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  until_lsn_ = lsn(1, 100);
  // no flow control for this test to make it easier to compute when the window
  // messages are to be sent.
  flow_control_threshold_ = 1;

  start();
  ASSERT_START_MESSAGES(lsn(1, 1),
                        lsn(1, 100),
                        lsn(1, 10),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  std::vector<TestStep> steps = {
      // expect receiving 10 records
      {TestStep::RECORDS, lsn(1, 1), lsn(1, 11)},
      // expect window messages to be sent (original size 10)
      {TestStep::WINDOW, lsn(1, 11), lsn(1, 20)},
      // expect the next 9 records
      {TestStep::RECORDS, lsn(1, 11), lsn(1, 20)},
  };
  runTestSteps(steps);

  // at this point next window messages haven't been sent yet
  ASSERT_NO_WINDOW_MESSAGES();

  // now simulate memory pressure to trigger downsizing window
  state_.has_memory_pressure = true;

  steps = {// expect next record
           {TestStep::RECORDS, lsn(1, 20), lsn(1, 21)},
           // next window is sent with size half of previous one (5)
           {TestStep::WINDOW, lsn(1, 21), lsn(1, 25)},
           {TestStep::RECORDS, lsn(1, 21), lsn(1, 26)},
           // window size 2
           {TestStep::WINDOW, lsn(1, 26), lsn(1, 27)},
           {TestStep::RECORDS, lsn(1, 26), lsn(1, 28)},
           // window size 1
           {TestStep::WINDOW, lsn(1, 28), lsn(1, 28)},
           {TestStep::RECORDS, lsn(1, 28), lsn(1, 29)},
           // window size 1
           {TestStep::WINDOW, lsn(1, 29), lsn(1, 29)},
           {TestStep::RECORDS, lsn(1, 29), lsn(1, 30)},
           // window size 1
           {TestStep::WINDOW, lsn(1, 30), lsn(1, 30)}};
  runTestSteps(steps);

  // come out of memory pressure
  state_.has_memory_pressure = false;

  steps = {{TestStep::RECORDS, lsn(1, 30), lsn(1, 31)},
           // memory pressure relieved. window should increase by one (2)
           {TestStep::WINDOW, lsn(1, 31), lsn(1, 32)},
           {TestStep::RECORDS, lsn(1, 31), lsn(1, 33)},
           // window size 3
           {TestStep::WINDOW, lsn(1, 33), lsn(1, 35)},
           {TestStep::RECORDS, lsn(1, 33), lsn(1, 36)},
           // window size 4
           {TestStep::WINDOW, lsn(1, 36), lsn(1, 39)},
           {TestStep::RECORDS, lsn(1, 36), lsn(1, 40)},
           // window size 5
           {TestStep::WINDOW, lsn(1, 40), lsn(1, 44)},
           {TestStep::RECORDS, lsn(1, 40), lsn(1, 45)},
           // window size 6
           {TestStep::WINDOW, lsn(1, 45), lsn(1, 50)},
           {TestStep::RECORDS, lsn(1, 45), lsn(1, 51)},
           // window size 7
           {TestStep::WINDOW, lsn(1, 51), lsn(1, 57)},
           {TestStep::RECORDS, lsn(1, 51), lsn(1, 58)},
           // window size 8
           {TestStep::WINDOW, lsn(1, 58), lsn(1, 65)},
           {TestStep::RECORDS, lsn(1, 58), lsn(1, 66)},
           // window size 9
           {TestStep::WINDOW, lsn(1, 66), lsn(1, 74)},
           {TestStep::RECORDS, lsn(1, 66), lsn(1, 75)},
           // window size 10
           {TestStep::WINDOW, lsn(1, 75), lsn(1, 84)},
           {TestStep::RECORDS, lsn(1, 75), lsn(1, 85)},
           // window size 10 again since it's the max buffer size
           {TestStep::WINDOW, lsn(1, 85), lsn(1, 94)},
           {TestStep::RECORDS, lsn(1, 85), lsn(1, 95)},
           // window size 6 because we are reaching tail.
           {TestStep::WINDOW, lsn(1, 95), lsn(1, 100)},
           {TestStep::RECORDS, lsn(1, 95), lsn(1, 101)}};
  runTestSteps(steps);
  ASSERT_NO_WINDOW_MESSAGES();
}

/**
 * Receiving the same LSN from the same node more than once should not be an
 * issue.  This can happen when:
 * - the appender had to retry writing (sent out multiple waves of STORE
 *   messages), and
 * - the same storage node was selected for multiple waves, and
 * - redundant copies were not cleaned up properly
 */
TEST_P(ClientReadStreamTest, DuplicateRecord) {
  start();
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  ASSERT_RECV();
}

/**
 * Two storage nodes send records with LSNs 3 and 4, respectively. Gap
 * [1, 2] should be reported and those records delivered afterwards.
 */
TEST_P(ClientReadStreamTest, SimpleGap) {
  state_.shards.resize(2);
  buffer_size_ = 4096;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 2)});
  ASSERT_RECV(lsn(1, 3), lsn(1, 4));
}

/**
 * This test covers the case in which there are two gaps inside the buffer.
 * First, storage node 1 sends records 5 and 9, and then storage node 2
 * sends record 7.
 *
 * lsn     1 2 3 4 5 6 7 8 9
 * record          x   x   x
 *
 * At the moment SN 2 delivers record 7, ClientReadStream needs to report
 * two gaps: [1, 4] and [6, 6]. Record 9 shouldn't be delivered yet as we
 * don't know if [8, 8] is a gap or some SN will deliver it.
 */
TEST_P(ClientReadStreamTest, TwoGaps) {
  state_.shards.resize(2);
  buffer_size_ = 4096;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 9)));

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  onDataRecord(N1, mockRecord(lsn(1, 7)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 4)},
                      GapMessage{GapType::DATALOSS, lsn(1, 6), lsn(1, 6)});
  ASSERT_RECV(lsn(1, 5), lsn(1, 7));
}

TEST_P(ClientReadStreamTest, GapAfterDelivered) {
  state_.shards.resize(2);
  buffer_size_ = 4096;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  onDataRecord(N1, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));
  onDataRecord(N1, mockRecord(lsn(1, 9)));

  ASSERT_RECV(lsn(1, 1),
              lsn(1, 2),
              lsn(1, 3),
              lsn(1, 4),
              lsn(1, 5),
              lsn(1, 9),
              lsn(1, 10));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 6), lsn(1, 8)});
}

/**
 * In this test, an explicit GAP message [(0, 2), (1, 1)] is sent. It should
 * be treated as an epoch bump.
 */
TEST_P(ClientReadStreamTest, EpochBump) {
  state_.shards.resize(1);
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));

  onGap(N0, mockGap(N0, lsn(1, 2), lsn(2, 1)));
  onDataRecord(N0, mockRecord(lsn(2, 2)));

  ASSERT_RECV(lsn(2, 2));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1, 2), lsn(2, 1)});
}

/**
 * In this test there are two storage nodes with the following records
 * available:
 *
 * SN 1: (1, 1), (2, 1)
 * SN 2: (2, 2)
 */
TEST_P(ClientReadStreamTest, TwoEpochs) {
  state_.shards.resize(2);
  buffer_size_ = 1024;
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(2, 0)));
  onGap(N1, mockGap(N0, lsn(1, 1), lsn(3, 0)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1, 1), lsn(2, 0)});
  ASSERT_WINDOW_MESSAGES(lsn(2, 1), lsn(2, 1) + buffer_size_ - 1, N0, N1);

  onDataRecord(N0, mockRecord(lsn(2, 1)));
  ASSERT_RECV(lsn(2, 1));

  onGap(N0, mockGap(N0, lsn(2, 2), lsn(3, 0)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(2, 2), lsn(3, 0)});

  onDataRecord(N0, mockRecord(lsn(3, 1)));
  onDataRecord(N1, mockRecord(lsn(3, 2)));

  ASSERT_RECV(lsn(3, 1), lsn(3, 2));
}

/**
 * In this test, a gap message is sent for LSNs that fit inside the client's
 * window.
 */
TEST_P(ClientReadStreamTest, GapInsideWindow) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));

  ASSERT_RECV(lsn(1, 1), lsn(1, 2));

  onGap(N0, mockGap(N0, lsn(1, 2), lsn(1, 10)));
  onDataRecord(N0, mockRecord(lsn(1, 11)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));
  onDataRecord(N1, mockRecord(lsn(1, 12)));

  ASSERT_RECV(lsn(1, 3), lsn(1, 5), lsn(1, 11), lsn(1, 12));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 4), lsn(1, 4)},
                      GapMessage{GapType::DATALOSS, lsn(1, 6), lsn(1, 10)});
}

/**
 * In this test, storage nodes send the following messages:
 *   SN 1: GAP [1, 2], GAP [3, 4], RECORD 6
 *   SN 2: GAP [1, 3], GAP [4, 5], RECORD 7
 *
 * Client's buffer should look like this ('.' is empty, 'g' is a gap marker,
 * 'r' denotes a record):
 * record):
 *    LSN: 1 2 3 4 5 6 7 8
 *    --------------------
 *    SN1: . . g . g r . .
 *    SN2: . . . g . g r .
 *
 * Gaps should be reported in the following order: [1, 2], [3, 3], [4, 4],
 * [5, 5].
 */
TEST_P(ClientReadStreamTest, MultipleGapsInsideWindow) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 2)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 3)));
  onGap(N0, mockGap(N0, lsn(1, 3), lsn(1, 4)));
  onGap(N1, mockGap(N1, lsn(1, 4), lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  onDataRecord(N1, mockRecord(lsn(1, 7)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 2)},
                      GapMessage{GapType::DATALOSS, lsn(1, 3), lsn(1, 3)},
                      GapMessage{GapType::DATALOSS, lsn(1, 4), lsn(1, 4)},
                      GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6), lsn(1, 7));
}

/**
 * A storage node first sends a gap message with LSN outside of the window,
 * but then sends another gap message with a smaller LSN. This could happen
 * if a storage node receives a trim message with LSN x right after sending
 * a gap message with LSN y > x (before client got to update its window).
 */
TEST_P(ClientReadStreamTest, NonMonotonicGaps) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));

  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 200)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 10), GapReason::TRIM));
  onDataRecord(N0, mockRecord(lsn(1, 21)));

  ASSERT_RECV(lsn(1, 1), lsn(1, 21));
  ASSERT_NO_WINDOW_MESSAGES()
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 2), lsn(1, 10)},
                      GapMessage{GapType::DATALOSS, lsn(1, 11), lsn(1, 20)});

  onGap(N0, mockGap(N0, lsn(1, 22), lsn(1, 200)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 22), lsn(1, 200)});
}

// Same as previous but the large gap fits inside window.
TEST_P(ClientReadStreamTest, NonMonotonicGapsInsideWindow) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));

  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 100)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 10), GapReason::TRIM));
  onDataRecord(N0, mockRecord(lsn(1, 21)));

  ASSERT_RECV(lsn(1, 1), lsn(1, 21));
  ASSERT_NO_WINDOW_MESSAGES()
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 2), lsn(1, 10)},
                      GapMessage{GapType::DATALOSS, lsn(1, 11), lsn(1, 20)});

  onGap(N0, mockGap(N0, lsn(1, 22), lsn(1, 100)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 22), lsn(1, 100)});
}

/**
 * Verify that when skipping partially trimmed sections is disabled, partially
 * trimmed gaps are NOT eagerly reported and any records available on the
 * partially trimmed sections are still delivered.
 */
TEST_P(ClientReadStreamTest, DisableSkippingPartiallyTrimmedSections) {
  do_not_skip_partially_trimmed_sections_ = true;
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onGap(N0, mockGap(N0, lsn(1, 2), lsn(1, 1000), GapReason::TRIM));
  // lsn 1 is delivered but the partially trimmed gap is not.
  ASSERT_RECV(lsn(1, 1));

  onDataRecord(N1, mockRecord(lsn(1, 4)));
  // We should have deliverred a trim gap 2-3 followed by lsn 4.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 2), lsn(1, 3)});
  ASSERT_RECV(lsn(1, 4));

  onGap(N1, mockGap(N1, lsn(1, 5), lsn(1, 1000), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 5), lsn(1, 1000)});

  onDataRecord(N0, mockRecord(lsn(1, 1001)));
  onDataRecord(N1, mockRecord(lsn(1, 1002)));

  ASSERT_RECV(lsn(1, 1001), lsn(1, 1002));
}

/**
 * Verify that any trim gap reported by a storage node immediately causes a
 * TRIM gap to be sent to the user and the buffer to be rotated.
 */
TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSections) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start_lsn_ =
      LSN_OLDEST; // We want to make sure that the first bridge gap plays well
                  // with the whole skip partially trimmed sections mechanism.
  start();

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, LSN_OLDEST, lsn(1, 0)});

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 10), GapReason::TRIM));

  // We should have deliverred bridge gap from epoch 0, then lsn e1n1 followed
  // by a trim gap up to lsn e1n10.
  ASSERT_RECV(lsn(1, 1));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 2), lsn(1, 10)});

  // N0 sent lsn 4 but we already rotated the buffer, it is discared.
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  onDataRecord(N0, mockRecord(lsn(1, 6)));
  onDataRecord(N0, mockRecord(lsn(1, 12)));
  onDataRecord(N1, mockRecord(lsn(1, 11)));
  ASSERT_RECV(lsn(1, 11), lsn(1, 12));
}

/**
 * Similar to SkipPartiallyTrimmedSections but while in SCD mode. TRIM is
 * currently the only gap that can be issued while in SCD mode.
 */
TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSectionsScd) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 11)));
  onDataRecord(N0, mockRecord(lsn(1, 12)));
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();

  // N2 sends a TRIM gap. the gap is sent to the user eagerly and then we issue
  // records 11, 12.
  onGap(N2, mockGap(N2, lsn(1, 1), lsn(1, 10), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 10)});
  ASSERT_RECV(lsn(1, 11), lsn(1, 12));
}

/**
 * Same as SkipPartiallyTrimmedSectionsScd, but this time the trim point is
 * moved past the window.
 */
TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSectionsScdPastWindow) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();

  // N2 sends a TRIM gap. the gap is sent to the user eagerly.
  onGap(N2, mockGap(N2, lsn(1, 1), lsn(1, 142), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 142)});
  ASSERT_RECV();

  lsn_t next_lsn = lsn(1, 143);
  ASSERT_WINDOW_MESSAGES(
      next_lsn, calc_buffer_max(next_lsn, buffer_size_), N0, N1, N2, N3);

  onDataRecord(N1, mockRecord(lsn(1, 143)));
  ASSERT_RECV(lsn(1, 143));
}

/**
 * Same as SkipPartiallyTrimmedSectionsScd, but this time the trim point is
 * moved past the current epoch which means we first retrieve that new epoch's
 * metadata before issuing it.
 */
TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSectionsScdPastEpoch) {
  start_lsn_ = lsn(1, 1);
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;
  state_.disable_default_metadata = true;
  start();

  ASSERT_METADATA_REQ(epoch_t(1));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(1),
                  epoch_t(1),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2, N3});

  overrideConnectionStates(ConnectionState::READING);

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N1,
                        N2,
                        N3);

  onDataRecord(N1, mockRecord(lsn(1, 3)));
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();

  // N2 sends a TRIM gap.
  onGap(N2, mockGap(N2, lsn(1, 1), lsn(2, 142), GapReason::TRIM));
  // We will read the metadata log first.
  ASSERT_GAP_MESSAGES();

  // After retrieving the metadata, the gap is issued.
  ASSERT_METADATA_REQ(epoch_t(2));
  onEpochMetaData(epoch_t(2),
                  epoch_t(2),
                  epoch_t(2),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N2, N3});
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0);
  ON_STARTED(filter_version_t{1}, N0);
  ASSERT_STOP_MESSAGES(N1);
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(2, 142)});
  ASSERT_RECV();

  lsn_t next_lsn = lsn(2, 143);
  ASSERT_WINDOW_MESSAGES(
      next_lsn, calc_buffer_max(next_lsn, buffer_size_), N0, N2, N3);

  onDataRecord(N2, mockRecord(lsn(2, 144)));
  ASSERT_RECV();
  onDataRecord(N0, mockRecord(lsn(2, 143)));
  ASSERT_RECV(lsn(2, 143), lsn(2, 144));
}

// fix infinite recursions in T7517983
TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSectionsT7517983) {
  state_.shards.resize(2);
  // The first window will be [1, 3], the second [4, 6]
  buffer_size_ = 3;
  flow_control_threshold_ = 1.0;
  start();

  // One node sends a trim gap outside the window...
  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 6), GapReason::TRIM));

  // the other node also sends a trim gap outside the window, but
  // its right end is smaller than trim_point_
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 4), GapReason::TRIM));

  // the readstream should report a gap to 6
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 6)});
}

/**
 * Test the case when skipping partially trimmed sections is disabled and one
 * storage node sends two gap messages, one that falls inside client's current
 * buffer and the second one that doesn't (but will after sliding the window,
 * i.e. second_gap <= first_gap + window_size).
 */
TEST_P(ClientReadStreamTest,
       DisableSkippingPartiallyTrimmedSectionsInsideWindowAfterSliding) {
  do_not_skip_partially_trimmed_sections_ = true;
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 100), GapReason::TRIM));
  // second gap is outside of the current window
  onGap(N0, mockGap(N0, lsn(1, 101), lsn(1, 160), GapReason::TRIM));

  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 99), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 99)});
  ASSERT_WINDOW_MESSAGES(lsn(1, 100), lsn(1, 100) + buffer_size_ - 1, N0, N1);

  onGap(N1, mockGap(N1, lsn(1, 100), lsn(1, 130), GapReason::TRIM));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 100), lsn(1, 100)},
                      GapMessage{GapType::TRIM, lsn(1, 101), lsn(1, 130)});

  onGap(N1, mockGap(N1, lsn(1, 131), lsn(1, 170), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 131), lsn(1, 160)});

  ASSERT_NO_WINDOW_MESSAGES()
}

TEST_P(ClientReadStreamTest,
       DisableSkippingPartiallyTrimmedSectionsOutsideWindow) {
  do_not_skip_partially_trimmed_sections_ = true;
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 10), GapReason::TRIM));
  onGap(N0, mockGap(N0, lsn(1, 11), lsn(1, 20), GapReason::TRIM));
  // the following gap is outside of the window
  onGap(N0, mockGap(N0, lsn(1, 21), lsn(1, 150), GapReason::TRIM));

  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 200), GapReason::TRIM));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 10)},
                      GapMessage{GapType::TRIM, lsn(1, 11), lsn(1, 20)},
                      GapMessage{GapType::TRIM, lsn(1, 21), lsn(1, 150)});
}

/**
 * Tests that when skipping partially trimmed sections is disabled and a gap is
 * reported, its right endpoint is at most until_lsn.
 */
TEST_P(ClientReadStreamTest,
       DisableSkippingPartiallyTrimmedSectionsGapUntilLsn) {
  do_not_skip_partially_trimmed_sections_ = true;
  state_.shards.resize(2);
  buffer_size_ = 128;
  until_lsn_ = lsn(1, 1000);
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 2000), GapReason::TRIM));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 2000), GapReason::TRIM));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 1000)});
}

TEST_P(ClientReadStreamTest, DataLossAtLogEnd) {
  // Suppose we read the range [1, 100] and there is a single node in the
  // cluster
  state_.shards.resize(1);
  until_lsn_ = lsn(1, 100);
  buffer_size_ = 4096;
  start();

  // Suppose the node only has record 50.
  onDataRecord(N0, mockRecord(lsn(1, 50)));

  // We should report data loss for [1, 49] and deliver record 50.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 49)});
  ASSERT_RECV(lsn(1, 50));

  // The node will send us a courtesy gap so that we know not to expect
  // anything else from it.
  onGap(N0, mockGap(N0, lsn(1, 51), lsn(1, 100), GapReason::NO_RECORDS));
  // We need to conclude that no other nodes can send us anything, and report
  // data loss.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 51), lsn(1, 100)});
}

TEST_P(ClientReadStreamTest, GapAtWindowBegin) {
  state_.shards.resize(1);
  start_lsn_ = lsn(1, 1);
  start();
  lsn_t gap_hi = lsn(1, sequencer_window_size_ + 1);

  onGap(N0, mockGap(N0, lsn(1, 1), gap_hi, GapReason::NO_RECORDS));

  // We should report data loss for [1, gap_hi]
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), gap_hi});
}

TEST_P(ClientReadStreamTest, GapAfterShrinking) {
  state_.shards.resize(2); // there are two storage nodes initially
  buffer_size_ = 128;
  start_lsn_ = lsn(1, 1);
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 10)));
  onDataRecord(N1, mockRecord(lsn(1, 1)));

  // second storage node has been removed from the cluster
  state_.shards.resize(1);
  updateConfig();
  read_stream_->noteConfigurationChanged();

  ASSERT_RECV(lsn(1, 1));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 2), lsn(1, 10)});
}

/**
 * If, while waiting on a STARTED reply from a node, the window slides thanks
 * to progress from other nodes, we need to let that node know about the new
 * window.
 */
TEST_P(ClientReadStreamTest, WindowSlideWhileStarting) {
  start_lsn_ = lsn(1, 1);
  buffer_size_ = 3; // holds [1, 3] initially
  flow_control_threshold_ = 1.0;
  state_.shards.resize(2);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  // N0 immediately replies with STARTED
  ON_STARTED(filter_version_t{1}, N0);
  // We know its window is still uptodate, no need to send out a WINDOW
  ASSERT_NO_WINDOW_MESSAGES()

  // N0 sends enough data records to fill the initial window
  read_stream_->onDataRecord(state_.shards[0], mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(state_.shards[0], mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(state_.shards[0], mockRecord(lsn(1, 3)));
  // Window slides
  ASSERT_WINDOW_MESSAGES(lsn(1, 4), lsn(1, 6), N0);

  // Now N1 sends STARTED
  ON_STARTED(filter_version_t{1}, N1)
  // We know its window is outdated since the START so we need to send an
  // update
  ASSERT_WINDOW_MESSAGES(lsn(1, 4), lsn(1, 6), N1);
}

// Ensures that HOLE gaps are correctly reported
TEST_P(ClientReadStreamTest, HoleGap) {
  state_.shards.resize(2);
  start_lsn_ = lsn(1, 1);
  buffer_size_ = 128;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N0, mockRecord(lsn(1, 3), RECORD_Header::HOLE));
  onDataRecord(N1, mockRecord(lsn(1, 4)));

  ASSERT_RECV(lsn(1, 1), lsn(1, 4));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 2), lsn(1, 2)},
                      GapMessage{GapType::HOLE, lsn(1, 3), lsn(1, 3)});
}

// Ensures that Access Gaps are being sent correctly
TEST_P(ClientReadStreamTest, AccessGap) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  start();

  // Override connection state to start sent. We want to test the the response
  // to a STARTED_Message being received
  overrideConnectionStates(ConnectionState::START_SENT);

  ON_STARTED_ACCESS(filter_version_t{1}, N0);

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));

  // Check that no data records are sent
  ASSERT_RECV();

  // Check that all GAP Record was received
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// Ensures that STARTED with status E::SYSLIMIT reconnects
TEST_P(ClientReadStreamTest, STARTED_Syslimit) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  start();

  overrideConnectionStates(ConnectionState::START_SENT);

  ASSERT_FALSE(reconnectTimerIsActive(N0));

  ON_STARTED_SYSLIMIT(filter_version_t{1}, N0);

  ASSERT_TRUE(reconnectTimerIsActive(N0));
}

// Ensures that Access Gaps are being sent even if client can not accept
// gap records for a period of time
TEST_P(ClientReadStreamTest, AccessGapRedelivery) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  start();

  // Override connection state to start sent. We want to test the the response
  // to a STARTED_Message being received
  overrideConnectionStates(ConnectionState::START_SENT);

  state_.callbacks_accepting = false;
  ON_STARTED_ACCESS(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N1)
  ON_STARTED(filter_version_t{1}, N2)

  onDataRecord(N1, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));

  // Check that no data records are sent
  ASSERT_RECV();

  // Gap message was still delivered, however state_.callbacks_accepting is
  // false so the clientReadStream believes that we did not get the gap record
  // yet
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  state_.callbacks_accepting = true;
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();
  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());

  onDataRecord(N2, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));

  // Check to see that clientReadStream redelivered the GAP record
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  // Check no data has been received
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// Similar to the test above, with gaps instead of data records
TEST_P(ClientReadStreamTest, AccessGapRedeliveryWithGaps) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  state_.shards.resize(3);
  start();

  // Override connection state to start sent. We want to test the the response
  // to a STARTED_Message being received
  overrideConnectionStates(ConnectionState::START_SENT);

  state_.callbacks_accepting = false;

  ON_STARTED(filter_version_t{1}, N1)
  ON_STARTED(filter_version_t{1}, N2)

  ON_STARTED_ACCESS(filter_version_t{1}, N0);
  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 3)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 4)));

  // Gap message was still delivered, however state_.callbacks_accepting is
  // false so the clientReadStream believes that we did not get the gap record
  // yet.  We check to see that only the access gap was delivered and not any
  // data loss gaps
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  state_.callbacks_accepting = true;
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();
  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());

  onDataRecord(N2, mockRecord(lsn(1, 5)));
  onDataRecord(N2, mockRecord(lsn(1, 6)));
  onDataRecord(N2, mockRecord(lsn(1, 7)));

  // Check to see that clientReadStream redelivered the GAP record
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  // Check no data has been received
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// check that if a minority of SN (with out of date ACL) send data with
// LSN > next_lsn, then a SN with the correct ACL sends an E::ACCESS STARTED
// message, then nothing will be delivered
TEST_P(ClientReadStreamTest, AccessGapStaleACLWithDataSent) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  buffer_size_ = 10;
  replication_factor_ = 2; // f-majority is 3
  // flow_control_threshold_ = 1.0;
  state_.shards.resize(3);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying ACCESS after N0 replies with OK.
  overrideConnectionStates(ConnectionState::START_SENT);

  ON_STARTED(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N2);

  ASSERT_NO_WINDOW_MESSAGES();

  // N0 sends data, not enough to fill buffer
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 7)));

  // Check that we haven't received anything yet
  ASSERT_RECV();

  // N1 now sends an a started message with E::ACCESS status
  ON_STARTED_ACCESS(filter_version_t{1}, N1);

  // Check to see that clientReadStream deliver the GAP record
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), lsn(1, 100)}, );

  // read stream should have records 1-3 now
  onDataRecord(N2, mockRecord(lsn(1, 1)));

  // Check that we haven't received anything yet even through we could have
  // delivered lsn 1..3
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// Test to see that when a majority of SN responds say they are missing
// lsn 1, but send data for lsn 2-4, the data is not delivered if
// a SN sends a STARTED_Message with E::ACCESS status before the timer
// triggered.
TEST_P(ClientReadStreamTest, AccessGapStaleACLWithFmajorityDataLoss) {
  state_.shards.resize(4);
  buffer_size_ = 4096;
  replication_factor_ = 2; // f-majority is 3
  start();

  overrideConnectionStates(ConnectionState::START_SENT);
  ON_STARTED(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N1);
  ON_STARTED(filter_version_t{1}, N2);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  ASSERT_GAP_MESSAGES();

  // a majority do not have lsn 1
  onDataRecord(N2, mockRecord(lsn(1, 4)));
  // no gap yet, but timer should be activated
  ASSERT_GAP_MESSAGES();

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);

  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());

  // Got STARTED_Message with E::ACCESS status from SN 3. With this,
  // No data records nor DATALOSS gap records should be delivered.
  ON_STARTED_ACCESS(filter_version_t{1}, N3);

  // simulate grace period expiring
  timer->getCallback()();

  // we should only get a ACCESS gap
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), until_lsn_});
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// Similar to AccessGapStaleACLWithFmajorityDataLoss but with gaps
TEST_P(ClientReadStreamTest, AccessGapStaleACLWithFmajorityGAP) {
  state_.shards.resize(4);
  buffer_size_ = 4096;
  replication_factor_ = 2; // f-majority is 3
  start();

  overrideConnectionStates(ConnectionState::START_SENT);
  ON_STARTED(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N1);
  ON_STARTED(filter_version_t{1}, N2);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 1)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 1)));

  // a majority do not have lsn 1
  onDataRecord(N2, mockRecord(lsn(1, 4)));
  onGap(N2, mockGap(N2, lsn(1, 1), lsn(1, 1)));

  ASSERT_GAP_MESSAGES();

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);

  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());

  // Got STARTED_Message with E::ACCESS status from SN 3. With this,
  // No data records nor DATALOSS gap records should be delivered.
  ON_STARTED_ACCESS(filter_version_t{1}, N3);

  // simulate grace period expiring
  timer->getCallback()();

  // we should only get a ACCESS gap
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 1), until_lsn_});
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// This tests that client reads stream will stop sending information
// as soon as it gets a STARTED_Message with E::ACCESS status, even after it
// has leaked some information
TEST_P(ClientReadStreamTest, AccessGapStaleACLStopSendingDataQuickly) {
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  buffer_size_ = 10;
  replication_factor_ = 2; // f-majority is 3
  // flow_control_threshold_ = 1.0;
  state_.shards.resize(3);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying ACCESS after N0 replies with OK.
  overrideConnectionStates(ConnectionState::START_SENT);

  ON_STARTED(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N1);

  // N0 and N1 send records 1-3. These will be leaked, no way around that
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));

  // N2 sends STARTED_Message with E::ACCESS status
  ON_STARTED_ACCESS(filter_version_t{1}, N2);

  // N0 and N1 send more data (in order). These should not be leaked
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));

  // Check to see that clientReadStream deliver the ACCESS GAP record
  ASSERT_GAP_MESSAGES(GapMessage{GapType::ACCESS, lsn(1, 4), lsn(1, 100)}, );

  // Check that no more data was leaked
  ASSERT_RECV();

  // Check that the Read Stream destroyed it self correctly
  ASSERT_TRUE(state_.disposed);
}

// One node sends a gap.  Another fills that gap with records.  But then there
// is a legit gap right after.  This can trip up ClientReadStream if gap
// detection state is not updated carefully.
TEST_P(ClientReadStreamTest, MootGapThenProperGap) {
  state_.shards.resize(2);
  buffer_size_ = 128;
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onGap(N1, mockGap(N1, lsn(1, 3), lsn(1, 5)));
  onGap(N0, mockGap(N0, lsn(1, 3), lsn(1, 5)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 3), lsn(1, 5)}, );
}

// Tests gap reporting when not all nodes respond with a record or a gap
// message.
TEST_P(ClientReadStreamTest, PartialNodeset) {
  state_.shards.resize(4);
  buffer_size_ = 4096;
  replication_factor_ = 2; // f-majority is 3
  start();

  lsn_t first_lsn = lsn(1, sequencer_window_size_ + 1);

  onDataRecord(N0, mockRecord(first_lsn + 2));
  onDataRecord(N1, mockRecord(first_lsn + 3));
  ASSERT_GAP_MESSAGES();

  onDataRecord(N2, mockRecord(first_lsn + 4));
  // no gap yet, but timer should be activated
  ASSERT_GAP_MESSAGES();

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);

  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());

  // simulate grace period expiring
  timer->getCallback()();
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::DATALOSS, lsn(1, 1), lsn_t(first_lsn + 1)});

  ASSERT_RECV(first_lsn + 2, first_lsn + 3, first_lsn + 4);
  ASSERT_FALSE(timer->isActive());
}

// In this test, only a single copy of each record is stored. Gaps shouldn't
// be reported until all nodes respond.
TEST_P(ClientReadStreamTest, PartialNodesetOneCopy) {
  state_.shards.resize(3);
  buffer_size_ = 4096;
  replication_factor_ = 1; // f-majority is 3
  start();

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  ASSERT_GAP_MESSAGES();
  ASSERT_FALSE(getGracePeriodTimer()->isActive());

  onDataRecord(N2, mockRecord(lsn(1, 5)));
  // gap is immediately reported (without starting a timer)
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 2)});
  ASSERT_FALSE(getGracePeriodTimer()->isActive());
}

// Tests that ClientReadStream is disposed from a grace period timer after
// until_lsn is reached
TEST_P(ClientReadStreamTest, DisposedAfterUntilLSN) {
  state_.shards.resize(3);
  buffer_size_ = 4096;
  replication_factor_ = 2;
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 100);
  start();

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 100)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 100)));
  ASSERT_FALSE(state_.disposed);

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE(timer->isActive());

  timer->getCallback()();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 100)});

  ASSERT_TRUE(state_.disposed);
}

// This test simulates a scenario in which a storage node has no records, so it
// sends a big gap [start_lsn, until_lsn] and destroys the stream. Clients must
// not rely on storage nodes resending GAP messages after sliding their
// windows.
TEST_P(ClientReadStreamTest, GapUntilLSN) {
  state_.shards.resize(2);
  buffer_size_ = 100;
  start_lsn_ = lsn(1, 1);
  until_lsn_ = lsn(1, 1000);
  start();

  onGap(N0, mockGap(N0, start_lsn_, until_lsn_));

  onDataRecord(N1, mockRecord(lsn(1, 1)));
  // next record is outside of the window
  onGap(N1, mockGap(N1, lsn(1, 2), lsn(1, 150)));

  ASSERT_RECV(lsn(1, 1));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 2), lsn(1, 150)});

  // client slides the window, node #2 sends the remaining records it has
  onDataRecord(N1, mockRecord(lsn(1, 151)));
  onDataRecord(N1, mockRecord(lsn(1, 161)));

  // we should be able to detect a gap even if node #0 doesn't resend the GAP
  // message

  ASSERT_RECV(lsn(1, 151), lsn(1, 161));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 152), lsn(1, 160)});
}

// A scenario where gap detection was failing (#4724417)
TEST_P(ClientReadStreamTest, GapScenarioT4724417) {
  replication_factor_ = 1; // require all nodes to report gaps
  state_.shards.resize(2);
  buffer_size_ = 128;
  until_lsn_ = lsn(4, 0);
  start();

  // Epoch by epoch, each node should deliver what it knows

  onGap(N0, mockGap(N0, lsn(1, 1), lsn(2, 0)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(2, 1)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1, 1), lsn(2, 0)});

  onDataRecord(N0, mockRecord(lsn(2, 1)));
  onDataRecord(N0, mockRecord(lsn(2, 2)));
  onGap(N0, mockGap(N0, lsn(2, 3), lsn(3, 0)));
  onDataRecord(N1, mockRecord(lsn(2, 2)));
  onGap(N1, mockGap(N1, lsn(2, 3), lsn(4, 0))); // note e3n0
  ASSERT_RECV(lsn(2, 1), lsn(2, 2));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(2, 3), lsn(3, 0)});

  onDataRecord(N0, mockRecord(lsn(3, 1)));
  onGap(N0, mockGap(N0, lsn(3, 2), lsn(4, 0)));
  ASSERT_RECV(lsn(3, 1));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 2), lsn(4, 0)});
}

// Tests that gap detection works when some records/gaps are within the
// intersection of old and new windows.
TEST_P(ClientReadStreamTest, GapOverlappingWindows) {
  state_.shards.resize(2);
  buffer_size_ = 20;
  flow_control_threshold_ = 0.7;
  start_lsn_ = lsn(1, 1);

  start();

  // in both old and new window
  onDataRecord(N0, mockRecord(lsn(1, 16)));
  onDataRecord(N0, mockRecord(lsn(1, 18)));
  // outside the old window
  onGap(N0, mockGap(N0, lsn(1, 19), lsn(1, 24)));

  // N1 has no records; send two consecutive gaps to make the window
  // slide to lsn 17, not all the way to 25
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(1, 16)));
  onGap(N1, mockGap(N1, lsn(1, 17), lsn(1, 24)));

  // once record 16 is delivered, ClientReadStream should slide the window and
  // detect other gaps, including the one outside of the previous window

  ASSERT_RECV(lsn(1, 16), lsn(1, 18));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 15)},
                      GapMessage{GapType::DATALOSS, lsn(1, 17), lsn(1, 17)},
                      GapMessage{GapType::DATALOSS, lsn(1, 19), lsn(1, 24)}, );
  ASSERT_WINDOW_MESSAGES(lsn(1, 17), lsn(1, 17 + 20 - 1), N0, N1);
}

// Very rare and unlikely situation:
// A storage node (N1) is stuck and does not send anything while the socket is
// still open. There is a timer that ticks periodically for the sole purpose of
// doing SCD failover when this happens, ie between two ticks of the timer we
// did not make any progress and a storage node did not send anything.
TEST_P(ClientReadStreamTest, ScdFailoverTimerOneNodeStuck) {
  state_.shards.resize(4);
  buffer_size_ = 30;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  onDataRecord(N3, mockRecord(lsn(1, 6)));
  onDataRecord(N2, mockRecord(lsn(1, 7)));
  onDataRecord(N0, mockRecord(lsn(1, 8)));
  onDataRecord(N2, mockRecord(lsn(1, 9)));
  ASSERT_RECV();

  // At this point, record 5 is missing. There is still no gap.
  ASSERT_GAP_MESSAGES();

  onDataRecord(N1, mockRecord(lsn(1, 5)));
  ASSERT_RECV(lsn(1, 5), lsn(1, 6), lsn(1, 7), lsn(1, 8), lsn(1, 9));

  onDataRecord(N3, mockRecord(lsn(1, 11)));
  onDataRecord(N2, mockRecord(lsn(1, 12)));
  onDataRecord(N0, mockRecord(lsn(1, 13)));
  onDataRecord(N2, mockRecord(lsn(1, 14)));
  ASSERT_RECV();

  // At this point, record 10 is missing.
  ASSERT_GAP_MESSAGES();

  SET_NODE_ALIVE(node_index_t{1}, false);

  // Simulate failover timer kicking off twice in a row without making progress.
  scdShardsDownFailoverTimerCallback();
  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();

  // All nodes should receive another START message with N1 in the known down
  // list. filter_version should have been bumbed in the header.
  ASSERT_START_MESSAGES(lsn(1, 10),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N1},
                        N0,
                        N1,
                        N2,
                        N3);

  // A record is received before the storage node responded, it should be
  // discarded.
  onDataRecord(N1, mockRecord(lsn(1, 11)));
  ASSERT_RECV();

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  onDataRecord(N1, mockRecord(lsn(1, 10)));
  onDataRecord(N3, mockRecord(lsn(1, 10)));
  onDataRecord(N3, mockRecord(lsn(1, 11)));
  onDataRecord(N3, mockRecord(lsn(1, 14)));
  onDataRecord(N0, mockRecord(lsn(1, 13)));
  onDataRecord(N2, mockRecord(lsn(1, 12)));
  onDataRecord(N1, mockRecord(lsn(1, 15)));
  onDataRecord(N2, mockRecord(lsn(1, 15)));
  onDataRecord(N0, mockRecord(lsn(1, 17)));
  ASSERT_RECV(
      lsn(1, 10), lsn(1, 11), lsn(1, 12), lsn(1, 13), lsn(1, 14), lsn(1, 15))

  triggerScheduledRewind();
  buffer_max = calc_buffer_max(lsn(1, 15) + 1, buffer_size_);

  // ClientReadStream should slide the window. All nodes should receive a START
  // message with an empty known down list.
  ASSERT_START_MESSAGES(lsn(1, 16),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  ASSERT_GAP_MESSAGES();
  onDataRecord(N0, mockRecord(lsn(1, 17)));
  ASSERT_RECV();
  onDataRecord(N1, mockRecord(lsn(1, 18)));
  ASSERT_RECV();
  onDataRecord(N2, mockRecord(lsn(1, 16)));
  ASSERT_RECV(lsn(1, 16), lsn(1, 17), lsn(1, 18))
}

// Even more unlikely and rare situation:
// Same as ScdFailoverTimerOneNodeStuck but this time failover happens twice ie,
// one node is stuck which causes the streams to be rewound with it in the known
// down list, and then another node is stuck which causes it to be added as
// well.
TEST_P(ClientReadStreamTest, ScdFailoverTimerTwoNodesStuck) {
  state_.shards.resize(4);
  buffer_size_ = 24;
  replication_factor_ = 3;
  scd_enabled_ = true;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  onDataRecord(N3, mockRecord(lsn(1, 6)));
  onDataRecord(N2, mockRecord(lsn(1, 7)));
  onDataRecord(N0, mockRecord(lsn(1, 8)));
  onDataRecord(N2, mockRecord(lsn(1, 9)));
  ASSERT_RECV();

  // At this point, record 5 is missing. There is still no gap.
  ASSERT_GAP_MESSAGES();

  SET_NODE_ALIVE(node_index_t{1}, false);

  // Simulate failover kicking off twice without making progress.
  scdShardsDownFailoverTimerCallback();
  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();

  // All nodes should receive another START message with N1 in the known down
  // list. filter_version should have been bumbed in the header.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N1},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  onDataRecord(N2, mockRecord(lsn(1, 7)));
  onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV(lsn(1, 5), lsn(1, 6), lsn(1, 7), lsn(1, 8));

  // Record 9 is missing. N3 seems down.
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  onDataRecord(N0, mockRecord(lsn(1, 11)));
  onDataRecord(N2, mockRecord(lsn(1, 12)));
  onDataRecord(N0, mockRecord(lsn(1, 13)));
  onDataRecord(N2, mockRecord(lsn(1, 14)));
  ASSERT_RECV();

  SET_NODE_ALIVE(node_index_t{3}, false);

  // At this point, record 9 is missing. There is still no gap.
  ASSERT_GAP_MESSAGES();
  // Simulate failover kicking off twice without making progress.
  scdShardsDownFailoverTimerCallback();
  scdShardsDownFailoverTimerCallback();

  triggerScheduledRewind();
  auto filtered_out = small_shardset_t{N1, N3};
  ASSERT_START_MESSAGES(lsn(1, 9),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        filtered_out,
                        N0,
                        N1,
                        N2,
                        N3);

  SET_NODE_ALIVE(node_index_t{3}, true);
  SET_NODE_ALIVE(node_index_t{1}, true);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // N3 recovers.
  onDataRecord(N3, mockRecord(lsn(1, 9)));
  onDataRecord(N2, mockRecord(lsn(1, 9)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  onDataRecord(N0, mockRecord(lsn(1, 11)));
  onDataRecord(N2, mockRecord(lsn(1, 12)));
  ASSERT_RECV(lsn(1, 9), lsn(1, 10), lsn(1, 11), lsn(1, 12));
  ASSERT_GAP_MESSAGES();

  triggerScheduledRewind();
  buffer_max = calc_buffer_max(lsn(1, 12) + 1, buffer_size_);

  // ClientReadStream should slide the window. All Storage nodes should receive
  // a START message with N3 removed from the known down list.
  ASSERT_START_MESSAGES(lsn(1, 13),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{4},
                        true,
                        small_shardset_t{N1},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{4}, N0, N1, N2, N3);

  // N1 recovers.
  onDataRecord(N3, mockRecord(lsn(1, 13)));
  onDataRecord(N0, mockRecord(lsn(1, 14)));
  onDataRecord(N1, mockRecord(lsn(1, 14)));
  onDataRecord(N2, mockRecord(lsn(1, 15)));
  ASSERT_RECV(lsn(1, 13), lsn(1, 14), lsn(1, 15));
  for (long unsigned int i = 16; i <= 24; ++i) {
    onDataRecord(ShardID(i % 3, 0), mockRecord(lsn(1, i)));
    ASSERT_RECV(lsn(1, i));
  }
  ASSERT_GAP_MESSAGES();

  triggerScheduledRewind();
  buffer_max = calc_buffer_max(lsn(1, 24) + 1, buffer_size_);

  // ClientReadStream should slide the window. All nodes should receive a START
  // message with an empty known down list.
  ASSERT_START_MESSAGES(lsn(1, 25),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{5},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
}

// This test verify the correctness of
// ClientReadStreamScd::checkNeedsFailoverToAllSendAll().
// Verify that ClientReadStream immediately does failover to all send all mode
// if all nodes in the read set reported a lsn greater than
// next_lsn_to_deliver_. Failover should be done immediately without having to
// wait for the next timer tick.
TEST_P(ClientReadStreamTest, ScdFailoverToAllSendAll) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 3;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // record 1 is missing and no node seems to be down. ClientReadStream should
  // failover to all send all mode.
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // All storage nodes send everything they got.
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 4)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4))
  ASSERT_GAP_MESSAGES();
}

// This test verify the correctness of
// ClientReadStreamScd::checkNeedsFailoverToAllSendAll().
// Verify that ClientReadStream fails over to all send mode immediately when it
// is established that the combination of nodes in the filtered_out_ list and
// nodes that sent something greater than next_lsn_to_deliver_ is such that we
// known for sure no one is going to send next_lsn_to_deliver_.
TEST_P(ClientReadStreamTest, ScdFailoverToAllSendAll2) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // N0 and N1 seem down. They were supposed to send records 1, 2 and 4.
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  SET_NODE_ALIVE(node_index_t{0}, false);
  SET_NODE_ALIVE(node_index_t{1}, false);

  // Simulate failover kicking off.
  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();

  auto filtered_out = small_shardset_t{N0, N1};
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        filtered_out,
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Other nodes send missing records.
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  onDataRecord(N3, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  // Record 4 not sent because the new primary node for it has a checksum error
  // for instance.
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  // N0 is in known down but it starts coming back and sends a record anyway.
  onDataRecord(N0, mockRecord(lsn(1, 8)));

  // N0, N1 are in the known down list, N3 sent a gap. However, N2 still has not
  // sent a gap and may still be able to send lsn 4. We should not yet failover
  // to all send all mode.
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());

  // Now N2 sends something.
  onDataRecord(N2, mockRecord(lsn(1, 9)));

  // This time we know for sure that no one is able to send record 4, we should
  // failover to all send all mode.
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // Send record up to buffer_max, at some point ClientReadStream should
  // slide the sender window and recover back to SCD mode.
  for (size_t i = 4; i < buffer_size_; ++i) {
    onDataRecord(N1, mockRecord(lsn(1, i)));
    onDataRecord(N2, mockRecord(lsn(1, i)));
    onDataRecord(N3, mockRecord(lsn(1, i)));
  }

  // We should be back in SCD mode.
  triggerScheduledRewind();
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
}

// Check that we immediately do failover when in single copy delivery mode and a
// node reports a checksum gap for a lsn.
TEST_P(ClientReadStreamTest, ScdChecksumFail) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  onGap(N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::CHECKSUM_FAIL));

  // ClientReadStream should have rewinded as soon as it received the
  // CHECKSUM_FAIL gap.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // N2 sends the GAP again for e1n5
  onGap(N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::CHECKSUM_FAIL));

  // N2 is already in the known down list, we should not rewind again.
  EXPECT_FALSE(rewindScheduled());

  // But N3 sends the record for e1n5
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  ASSERT_RECV(lsn(1, 5), lsn(1, 6));
}

// Check that ClientReadStream will failover to all send all mode when there is
// an epoch bump. We never ship a gap in the numbering sequencer to the client
// unless we are certain there are no records, hence the failover to all send
// all mode.
TEST_P(ClientReadStreamTest, ScdEpochBump) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);
  ASSERT_NO_WINDOW_MESSAGES();

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  ASSERT_NO_WINDOW_MESSAGES();

  // All nodes send a gap.
  onGap(N0, mockGap(N0, lsn(1, 3), lsn(2, 1)));
  onGap(N1, mockGap(N1, lsn(1, 1), lsn(2, 2)));
  onGap(N2, mockGap(N2, lsn(1, 2), lsn(2, 3)));
  EXPECT_FALSE(rewindScheduled());
  onGap(N3, mockGap(N3, lsn(1, 4), lsn(2, 4)));
  ASSERT_NO_WINDOW_MESSAGES();

  // However, no gap should be sent to the client. We should rewind and failover
  // to all send all mode just to be sure we did not miss a record.
  ASSERT_GAP_MESSAGES();
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);
  ASSERT_NO_WINDOW_MESSAGES();

  // All nodes send a gap.

  onGap(N0, mockGap(N0, lsn(1, 4), lsn(2, 1)));
  ASSERT_GAP_MESSAGES();
  ASSERT_NO_WINDOW_MESSAGES();
  EXPECT_FALSE(rewindScheduled());
  onGap(N1, mockGap(N1, lsn(1, 4), lsn(2, 1)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1, 4), lsn(2, 1)});
  ASSERT_WINDOW_MESSAGES(lsn(2, 2), lsn(2, 11), N0, N1, N2, N3);

  // Right now grace period is disabled for bridge gaps, so the rewind back to
  // scd is scheduled as early as here.
  EXPECT_TRUE(rewindScheduled());

  onGap(N2, mockGap(N2, lsn(1, 4), lsn(2, 1)));
  onGap(N3, mockGap(N3, lsn(1, 4), lsn(2, 1)));

  ASSERT_NO_WINDOW_MESSAGES();
  ASSERT_GAP_MESSAGES();

  // We should go back to single copy delivery mode because we slid the sender
  // window.
  triggerScheduledRewind();
  buffer_max = calc_buffer_max(lsn(2, 1) + 1, buffer_size_);
  ASSERT_START_MESSAGES(lsn(2, 2),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);
  EXPECT_FALSE(rewindScheduled());
  ASSERT_NO_WINDOW_MESSAGES();

  // Nodes start shipping some records.
  onDataRecord(N2, mockRecord(lsn(2, 3)));
  onDataRecord(N3, mockRecord(lsn(2, 2)));
  onDataRecord(N1, mockRecord(lsn(2, 2)));
  onDataRecord(N0, mockRecord(lsn(2, 2)));
  onDataRecord(N1, mockRecord(lsn(2, 3)));
  onDataRecord(N3, mockRecord(lsn(2, 3)));
  ASSERT_RECV(lsn(2, 2), lsn(2, 3));
  ASSERT_GAP_MESSAGES();
}

// Check that we immediately do failover when in single copy delivery and the
// socket closed callback is called.
TEST_P(ClientReadStreamTest, ScdOnCloseCallback) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // Socket for N1 closes
  auto& cb = *state_.on_close[N1];
  cb(E::PEER_CLOSED, Address(NodeID(N1.node())));
  triggerScheduledRewind();

  // ClientReadStream should rewind with N1 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N1},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  onDataRecord(N3, mockRecord(lsn(1, 4)));
  onDataRecord(N0, mockRecord(lsn(1, 5)));
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV(lsn(1, 4), lsn(1, 5), lsn(1, 6));
  ASSERT_GAP_MESSAGES();
}

// Check that if the socket for a node closes while all the other nodes are in
// the known down list, we failover to all send all mode.
TEST_P(ClientReadStreamTest, ScdOnCloseCallbackFailoverToAllSendAll) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));

  onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  SET_NODE_ALIVE(node_index_t{0}, false);
  SET_NODE_ALIVE(node_index_t{1}, false);
  SET_NODE_ALIVE(node_index_t{2}, false);

  // Timer triggert twice without making progress.
  scdShardsDownFailoverTimerCallback();
  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();

  // ClientReadStream should rewind with N0, N1, N2 in the filtered out list.
  const auto filtered_out = small_shardset_t{N0, N1, N2};
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        filtered_out,
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Now, socket for N3 closes
  auto& cb = *state_.on_close[N3];
  cb(E::PEER_CLOSED, Address(NodeID(N3.node())));
  triggerScheduledRewind();
  EXPECT_FALSE(rewindScheduled());

  // ClientReadStream should rewind in all send all mode.
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
}

// Check that if a node is removed from the cluster while it was in the
// known down list, we rewind the stream after removing it from the known down
// list.
TEST_P(ClientReadStreamTest, ScdShrinkWhileNodeKnownDown) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 4)));

  SET_NODE_ALIVE(node_index_t{3}, false);

  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();

  // ClientReadStream should rewind with N3 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N3},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // The cluster is shrunk: N3 is removed.
  state_.shards.resize(3);
  updateConfig();
  read_stream_->noteConfigurationChanged();

  triggerScheduledRewind();

  // ClientReadStream should rewind without N3 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2);
}

// Check that we failover to all send all mode if shrinking causes us to be in a
// state where all nodes don't have next_lsn_to_deliver_.
TEST_P(ClientReadStreamTest, ScdShrinkCausesFailoverToAllSendAll) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 4)));

  // The cluster is shrunk: N3 is removed.
  state_.shards.resize(3);
  updateConfig();
  read_stream_->noteConfigurationChanged();

  // The 3 remaining nodes reported that they don't have lsn 1.
  // ClientReadStream should rewind with all send all mode.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2);

  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
}

// Verify that if the config changes and scd gets disabled, we switch to all
// send all mode.
TEST_P(ClientReadStreamTest, ScdConfigChangedDisabled) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 3;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // The config changes, scd is disabled on the log.
  scd_enabled_ = false;
  updateConfig();
  read_stream_->noteConfigurationChanged();

  // Nodes should get another START message with scd disabled.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
}

// Verify that nothing happens if we disable scd on the log but scd was not
// active because we did failover to all send all.
TEST_P(ClientReadStreamTest, ScdConfigChangedDisabledButNotActive) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 3;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  // We are in all send all because nobody has record 1.
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // The config changes, scd is disabled on the log.
  scd_enabled_ = false;
  updateConfig();
  read_stream_->noteConfigurationChanged();
  // But there is no need to rewind because we are already in all send all mode.
  ASSERT_NO_START_MESSAGES();
}

// Verify that if the log is now configured to use scd, we switch to single
// copy delivery mode.
TEST_P(ClientReadStreamTest, ScdConfigChangedEnabled) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 3;
  scd_enabled_ = false;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));

  // The config changes, scd is enabled on the log.
  scd_enabled_ = true;
  updateConfig();
  read_stream_->noteConfigurationChanged();

  // All streams should rewind with scd enabled.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
}

// If SCD is active and a node is rebuilding (responds STARTED with
// E::REBUILDING), the stream should be rewound immediately so that we don't
// waste time waiting for the failover timer to trigger.
TEST_P(ClientReadStreamTest, ScdNodeRebuilding) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  // Undo start() overriding states to READING.  We want to simulate N2
  // replying STARTED with E::REBUILDING.
  overrideConnectionStates(ConnectionState::START_SENT);

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // N0, N1, N3 respond normally
  ON_STARTED(filter_version_t{1}, N0, N1, N3);
  // They also send some records
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N3, mockRecord(lsn(1, 8)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 1));
  ASSERT_GAP_MESSAGES();

  // N2 responds with E::REBUILDING
  ON_STARTED_REBUILDING(filter_version_t{1}, N2);
  triggerScheduledRewind();

  // The stream should be rewound with N2 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 2),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N3);

  // N2 responds with E::REBUILDING again
  ON_STARTED_REBUILDING(filter_version_t{2}, N2);

  // This time other nodes send what N2 should have sent.
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  onDataRecord(N0, mockRecord(lsn(1, 7)));
  onDataRecord(N3, mockRecord(lsn(1, 8)));
  onDataRecord(N1, mockRecord(lsn(1, 9)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 2),
              lsn(1, 3),
              lsn(1, 4),
              lsn(1, 5),
              lsn(1, 6),
              lsn(1, 7),
              lsn(1, 8),
              lsn(1, 9),
              lsn(1, 10));

  // We simulate the socket for N2 to be closed, which means that N2 finished
  // rebuilding and shut down its server and listener.
  auto& cb = *state_.on_close[N2];
  cb(E::PEER_CLOSED, Address(NodeID(N2.node())));
  // Simulate the reconnect timer triggering.
  reconnectTimerCallback(N2);
  // N2 receives a START message and this time responds with E::OK.
  // Note that it sees itself in the known down list since this list has not
  // been updated yet.
  ASSERT_START_MESSAGES(lsn(1, 11),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N2);
  ON_STARTED(filter_version_t{2}, N2);
  // Here, because N2 will start sending records, the stream will be rewound
  // with an empty known down list.
}

// Simulate the case when we fail to send START to a node while in SCD mode.
// If that happens, we should do immediate failover but the reconnect timer
// should be activated as well.
TEST_P(ClientReadStreamTest, ScdNodeStartNotSent) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  // Undo start() overriding states to READING.  We want to simulate not being
  // able to send START to N2.
  overrideConnectionStates(ConnectionState::START_SENT, {N0, N1, N2, N3});
  overrideConnectionStates(ConnectionState::CONNECTING, {N2});

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // N0, N1, N3 respond normally
  ON_STARTED(filter_version_t{1}, N0, N1, N3);
  // They also send some records
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N3, mockRecord(lsn(1, 8)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 1));
  ASSERT_GAP_MESSAGES();

  // We could not send START to N2
  onStartSent(N2, E::FAILED);
  triggerScheduledRewind();

  // The stream should be rewound with N2 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 2),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N3);
  // We could not send START to N2 again
  onStartSent(N2, E::FAILED);

  // This time other nodes send what N2 should have sent.
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N1, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N3, mockRecord(lsn(1, 5)));
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  onDataRecord(N0, mockRecord(lsn(1, 7)));
  onDataRecord(N3, mockRecord(lsn(1, 8)));
  onDataRecord(N1, mockRecord(lsn(1, 9)));
  onDataRecord(N0, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 2),
              lsn(1, 3),
              lsn(1, 4),
              lsn(1, 5),
              lsn(1, 6),
              lsn(1, 7),
              lsn(1, 8),
              lsn(1, 9),
              lsn(1, 10));

  // Simulate the reconnect timer triggering.
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  reconnectTimerCallback(N2);

  // This time START is sent and the node responds.
  ASSERT_START_MESSAGES(lsn(1, 11),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N2);
  ON_STARTED(filter_version_t{2}, N2);
}

// Similar to ScdFailoverToAllSendAll test, but 1 node did not send anything
// because it is rebuilding. We should take it into account and do failover
// immediately.
TEST_P(ClientReadStreamTest, ScdFailoverToAllSendAllWhenRebuildingNodes) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 3;
  scd_enabled_ = true;
  start();

  // Undo start() overriding states to READING.  We want to simulate N2
  // replying STARTED with E::REBUILDING.
  overrideConnectionStates(ConnectionState::START_SENT);

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // N0, N1, N3 respond normally
  ON_STARTED(filter_version_t{1}, N0, N1, N3);
  // N2 responds with E::REBUILDING
  ON_STARTED_REBUILDING(filter_version_t{1}, N2);
  triggerScheduledRewind();

  // The stream should be rewound with N2 in the known down list.
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  ON_STARTED(filter_version_t{2}, N0, N1, N3);
  // N2 responds with E::REBUILDING again.
  ON_STARTED_REBUILDING(filter_version_t{2}, N2);

  // Some records are successfully received and shipped.
  onDataRecord(N1, mockRecord(lsn(1, 1)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  // Now N0, N1, N3 send something greater than lsn 5.
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  onDataRecord(N0, mockRecord(lsn(1, 7)));
  onDataRecord(N1, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // Record 5 is missing. gap_nodes_ + gap_nodes_outside_window_ <
  // readSetSize(), however ClientReadStreamScd should take
  // gap_nodes_rebuilding_ (=1 here) into consideration. So we should do
  // immediate failover.
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
}

// Simulate situation where we cannot connect to half of the storage nodes.
// Verify that SCD failover works correctly. Also verify that the readers
// re-connect as soon as the nodes come back.
// In this test, we exercise the failover mechanism that happens where a call to
// sendStartMessage() fails synchronously instead of being an asynchronous error
// reported by onStartSent().
TEST_P(ClientReadStreamTest, ScdFailoverWhenSendStartFails) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 2;
  scd_enabled_ = true;

  // Make it so that we will fail to send START to N2, N3 synchronously.
  setSendStartErrorForShard(N2, E::UNREACHABLE);
  setSendStartErrorForShard(N3, E::UNREACHABLE);
  start();

  // START should have been sent to N0, N1 only.
  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1);

  // Trigger the timer that takes care of trying rewinding the streams with N0
  // and N1 in the blacklist.
  triggerScheduledRewind();

  // Verify that we did send a new START to N0, N1 with N2, N3 in the known down
  // list.
  auto down = small_shardset_t{N2, N3};
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        down,
                        N0,
                        N1);

  // However sendStart() should still fail with E::UNREACHABLE for N2, N3.
  // Since they are already in the filtered_out_ list, once the immediate
  // failover timer ticks, this time their reconnect timer should be activated.
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  // N0, N1 send STARTED for the first START, but it is discarded because filter
  // version is now 2.
  ON_STARTED(filter_version_t{1}, N0, N1);
  // N0, N1 send STARTED for the second START.
  ON_STARTED(filter_version_t{2}, N0, N1);

  // Trigger the reconnect timer on N2, N3.
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  // sendStart() should still return E::UNREACHABLE. The reconnect timer should
  // still be active to try again later.
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  // Let's make it so that now we can connect to N2, N3, and let's trigger the
  // reconnect timers.
  setSendStartErrorForShard(N2, E::OK);
  setSendStartErrorForShard(N3, E::OK);
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  down = small_shardset_t{N2, N3};
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        down,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N2, N3);

  // This time N2, N3 send records.
  onDataRecord(N3, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4), lsn(1, 5));

  triggerScheduledRewind();
  buffer_max = calc_buffer_max(lsn(1, 5) + 1, buffer_size_);

  // ClientReadStream should slide the window and rewind the streams to remove
  // N2, N3 from the known down list.
  ASSERT_START_MESSAGES(lsn(1, 6),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);
}

// Same as ScdFailoverWhenSendStartFails but we cannot send START to *all*
// nodes. Verify that we do eventually reconnect when connectivity comes back.
TEST_P(ClientReadStreamTest, ScdSendStartFailsOnAllNodes) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = true;

  // Make it so that we will fail to send START to all nodes synchronously.
  setSendStartErrorForShard(N0, E::UNREACHABLE);
  setSendStartErrorForShard(N1, E::UNREACHABLE);
  setSendStartErrorForShard(N2, E::UNREACHABLE);
  setSendStartErrorForShard(N3, E::UNREACHABLE);
  start();

  // We could not send START to anyone.
  ASSERT_NO_START_MESSAGES();

  // Trigger the timer that takes care of trying rewinding the streams.
  // However, it sees that all nodes are down. It will try to rewind the streams
  // in all send all mode.
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());

  // sendStartMessage() still returns E::UNREACHABLE for all nodes. We could not
  // send START to anyone.
  ASSERT_NO_START_MESSAGES();

  // Now the reconnect timer should be active for all nodes.
  ASSERT_TRUE(reconnectTimerIsActive(N0));
  ASSERT_TRUE(reconnectTimerIsActive(N1));
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  // Trigger the reconnect timer on all nodes.
  reconnectTimerCallback(N0);
  reconnectTimerCallback(N1);
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  // sendStart() should still return E::UNREACHABLE. The reconnect timers should
  // still be active to try again later.
  ASSERT_TRUE(reconnectTimerIsActive(N0));
  ASSERT_TRUE(reconnectTimerIsActive(N1));
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  // Let's make it so that now we can connect to N2, N3, and let's trigger the
  // reconnect timers. N0, N1 still unreachable.
  setSendStartErrorForShard(N2, E::OK);
  setSendStartErrorForShard(N3, E::OK);
  reconnectTimerCallback(N0);
  reconnectTimerCallback(N1);
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  // N2, N3 successfully respond.
  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N2, N3);

  // Let's make it so that now we can connect to N0, N1 as well, and let's
  // trigger the reconnect timers. The reconnect timers for N2, N3 should be
  // inactive since we successfully connected to them.
  setSendStartErrorForShard(N0, E::OK);
  setSendStartErrorForShard(N1, E::OK);
  ASSERT_FALSE(reconnectTimerIsActive(N2));
  ASSERT_FALSE(reconnectTimerIsActive(N3));
  reconnectTimerCallback(N0);
  reconnectTimerCallback(N1);

  // N0, N1 successfully respond.
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1);
  ON_STARTED(filter_version_t{2}, N0, N1);

  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  // Eventually the window will be slid and then ClientReadStream will go back
  // to SCD mode...
}

// Extremely unlikely situation:
// Imagine that there is a record with lsn 5 but the storage node that was
// supposed to send it does not have it for some reason. Usually we detect
// this because all nodes send a gap with lsn > 5. However we might not detect
// it if that record was the last record in the log and the log is not taking
// any data anymore. There exists a timer whose purpose is to failover to
// all send all mode whenever we don't make any progress for a given amount of
// time so that we can guarantee eventual delivery for rare cases like this.
TEST_P(ClientReadStreamTest, ScdNoMoreDataFailoverToAllSendAll) {
  state_.shards.resize(4);
  buffer_size_ = 30;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  // Simulate all_send_all_failover_timer_ ticking() twice without being able to
  // make any progress. When that happens, we should failover to all send all
  // mode.
  scdAllSendAllFailoverTimerCallback();
  scdAllSendAllFailoverTimerCallback();
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Now the storage node that stores the other replica of record with lsn 5
  // will send it...
  onDataRecord(N2, mockRecord(lsn(1, 5)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));
  ASSERT_RECV(lsn(1, 5));
  ASSERT_GAP_MESSAGES();

  // N0 sends a bridge record, we move to the next epoch and slide the window.
  onDataRecord(
      N0, mockRecord(lsn(1, 6), RECORD_Header::HOLE | RECORD_Header::BRIDGE));
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(1, 6), lsn(1, ESN_MAX.val_)});

  // Because we slid the window we move to SCD.
  triggerScheduledRewind();
  lsn_t next_lsn = lsn(2, 0);
  buffer_max = calc_buffer_max(next_lsn, buffer_size_);
  ASSERT_START_MESSAGES(next_lsn,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // Simulate all_send_all_failover_timer_ ticking() twice again.
  scdAllSendAllFailoverTimerCallback();
  scdAllSendAllFailoverTimerCallback();
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(next_lsn,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{4},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{4}, N0, N1, N2, N3);
}

// Same as previous test but one of the nodes sent a gap
// (not sure if it's possible in practice).
TEST_P(ClientReadStreamTest, ScdNoMoreDataFailoverToAllSendAllWithGap) {
  state_.shards.resize(4);
  buffer_size_ = 30;
  replication_factor_ = 2;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onGap(N1, mockGap(N1, lsn(1, 3), lsn(1, 4)));
  onDataRecord(N2, mockRecord(lsn(1, 3)));
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  scdAllSendAllFailoverTimerCallback();
  scdAllSendAllFailoverTimerCallback();
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
}

/*********** Tests with NodeSet feature support ***********/

TEST_P(ClientReadStreamTest, StartReadingWithNodeSet) {
  buffer_size_ = 4;
  state_.disable_default_metadata = true;
  start();
  // shouldn't send any START until we figure out the first nodeset
  ASSERT_NO_START_MESSAGES();
  ASSERT_STOP_MESSAGES();
  ASSERT_NO_WINDOW_MESSAGES();
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));

  // deliver the first nodeset {1, 2} with epoch 3
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(5),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  // should started reading from {1, 2}
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1,
                        N2);
}

/**
 * epoch bump with disjoint nodeset
 * (1) read stream is reading epoch 3 with nodeset {1, 2}
 * (2) {1, 2} stop sending records because records of the current epoch reach
 *     the end, and they are not in the nodeset of the next epoch
 * (3) eventually {1, 2} will received release for records in higher epoch and
 *     will send a gap upto last_released_lsn whose epoch is > 3
 * (4) without knowing the nodeset for the next epoch 4, client readstream does
 *     not deliver the epoch-bump gap. Instead, it requests metadata for epoch 4
 *     and will not progress until it receives the metadata
 * (5) the read stream eventually receives metadata for epoch 4 which has
 *     a nodeset of {0, 3}, and it starts reading from new nodes, and uses
 *     records/gaps received from the new epoch to resume gap detection
 */
TEST_P(ClientReadStreamTest, NextEpochWithDisjointNodeSet) {
  start_lsn_ = lsn(3, 4);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(N1, mockRecord(lsn(3, 4)));
  onDataRecord(N2, mockRecord(lsn(3, 5)));
  ASSERT_RECV(lsn(3, 4), lsn(3, 5));
  onGap(N2, mockGap(N2, lsn(3, 6), lsn(8, 9)));
  ASSERT_METADATA_REQ();
  onGap(N1, mockGap(N1, lsn(3, 5), lsn(5, 0)));

  // no gap is delivered until new nodeset is figured out
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(epoch_t(4));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  onEpochMetaData(epoch_t(4),
                  epoch_t(4),
                  epoch_t(4),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N3});
  ASSERT_STOP_MESSAGES(N1, N2);
  // the window has not yet slided
  ASSERT_START_MESSAGES(lsn(3, 6),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N3);

  // set states to READING for new nodes
  overrideConnectionStates(ConnectionState::READING);
  // new nodes should send gaps since they don't have records in the current
  // window
  onGap(N3, mockGap(N3, lsn(3, 6), lsn(4, 6)));
  onGap(N0, mockGap(N0, lsn(3, 6), lsn(4, 5)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 6), lsn(4, 5)});
  ASSERT_RECV();
  // windows should have slided
  lsn_t next_lsn = lsn(4, 6);

  ASSERT_WINDOW_MESSAGES(
      next_lsn, calc_buffer_max(next_lsn, buffer_size_), N0, N3);

  onDataRecord(N0, mockRecord(lsn(4, 6)));
  onDataRecord(N3, mockRecord(lsn(4, 7)));
  ASSERT_RECV(lsn(4, 6), lsn(4, 7));
  ASSERT_GAP_MESSAGES();
}

/**
 * epoch bump with overlapping nodesets
 * (1) read stream is reading epoch 3 with nodeset {1, 2} (effective until
 *     epoch 3), reading approches the end of epoch (ESN_MAX)
 * (2) at one time, node 1 started to send records for epoch 4,
 *     e.g., lsn(4, 5), because it is also part of the nodeset for epoch 4
 * (3) node 2, on the other hand, does not belong to the nodeset for epoch 4,
 *     eventually it sends a gap with a higher epoch, say epoch 5
 * (4) the read stream should not deliver any record or gap in such case
 *     but request and wait for metadata for epoch 4
 * (5) the read stream eventually receives metadata for epoch 4 which has
 *     a nodeset of {0, 1}, it should start reading from node 1 and stop reading
 *     from node 2.
 * (6) node 0 sends records with lsn(4, 4), we expect the read stream to deliver
 *     a brige gap from epoch 3 to lsn(4, 3), and deliver record (4, 4) and
 *     (4, 5)
 */

TEST_P(ClientReadStreamTest, NextEpochWithOverlappingNodeSet) {
  start_lsn_ = lsn(3, ESN_MAX.val_ - 16);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(N1, mockRecord(start_lsn_));
  onDataRecord(N2, mockRecord(start_lsn_ + 1));
  ASSERT_RECV(start_lsn_, start_lsn_ + 1);
  lsn_t next_lsn = start_lsn_ + 2;
  // a record for the next epoch but still with in the window
  onDataRecord(N1, mockRecord(lsn(4, 5)));
  onGap(N2, mockGap(N2, next_lsn, lsn(5, 3)));
  // requesting metadata for epoch 4
  ASSERT_METADATA_REQ(epoch_t(4));
  // but shouldn't deliver anything
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();
  // window should stay the same
  ASSERT_NO_WINDOW_MESSAGES();
  // next nodeset is {0, 1}
  onEpochMetaData(epoch_t(4),
                  epoch_t(4),
                  epoch_t(4),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1});

  ASSERT_STOP_MESSAGES(N2);

  ASSERT_START_MESSAGES(next_lsn,
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);

  // set states to READING for new nodes
  overrideConnectionStates(ConnectionState::READING);
  // node 0 sends (4, 4)
  onDataRecord(N0, mockRecord(lsn(4, 4)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, next_lsn, lsn(4, 3)});

  ASSERT_RECV(lsn(4, 4), lsn(4, 5));
}

// test the case that the read stream detects a potential gap which spans
// multiple epochs
TEST_P(ClientReadStreamTest, NodeSetGapSpansMultipleEpochs) {
  start_lsn_ = lsn(3, 4);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(N1, mockRecord(lsn(3, 4)));
  onDataRecord(N2, mockRecord(lsn(3, 5)));
  ASSERT_RECV(lsn(3, 4), lsn(3, 5));
  onGap(N2, mockGap(N2, lsn(3, 6), lsn(18, 9)));
  ASSERT_METADATA_REQ();
  onGap(N1, mockGap(N1, lsn(3, 5), lsn(15, 0)));

  for (int i = 4; i < 15; ++i) {
    ASSERT_GAP_MESSAGES();
    ASSERT_METADATA_REQ(epoch_t(i));
    onEpochMetaData(epoch_t(i),
                    epoch_t(3),
                    epoch_t(i),
                    1,
                    NodeLocationScope::NODE,
                    StorageSet{N1, N2});
  }
  // metadata changes for epoch 15
  ASSERT_METADATA_REQ(epoch_t(15));
  onEpochMetaData(epoch_t(15),
                  epoch_t(15),
                  epoch_t(15),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N3});

  ASSERT_STOP_MESSAGES(N1, N2);
  ASSERT_START_MESSAGES(lsn(3, 6),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N3);

  // set states to READING for new nodes
  overrideConnectionStates(ConnectionState::READING);
  onGap(N0, mockGap(N0, lsn(3, 6), lsn(15, 1)));
  onGap(N3, mockGap(N3, lsn(3, 6), lsn(15, 2)));
  onDataRecord(N0, mockRecord(lsn(15, 2)));
  onDataRecord(N3, mockRecord(lsn(15, 3)));
  ASSERT_RECV(lsn(15, 2), lsn(15, 3));
}

// Similar to the last test, but the nodeset remains the same
TEST_P(ClientReadStreamTest, NodeSetGapSpansMultipleEpochs2) {
  start_lsn_ = lsn(3, 4);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(N1, mockRecord(lsn(3, 4)));
  onDataRecord(N2, mockRecord(lsn(3, 5)));
  ASSERT_RECV(lsn(3, 4), lsn(3, 5));
  onGap(N2, mockGap(N2, lsn(3, 6), lsn(18, 9)));
  ASSERT_METADATA_REQ();
  onGap(N1, mockGap(N1, lsn(3, 5), lsn(15, 0)));

  for (int i = 4; i < 16; ++i) {
    ASSERT_GAP_MESSAGES();
    ASSERT_METADATA_REQ(epoch_t(i));
    onEpochMetaData(epoch_t(i),
                    epoch_t(3),
                    epoch_t(i),
                    1,
                    NodeLocationScope::NODE,
                    StorageSet{N1, N2});
  }

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 6), lsn(15, 0)});

  ASSERT_WINDOW_MESSAGES(lsn(15, 1), lsn(15, buffer_size_), N1, N2);

  onDataRecord(N1, mockRecord(lsn(15, 1)));
  onDataRecord(N1, mockRecord(lsn(15, 2)));
  ASSERT_RECV(lsn(15, 1), lsn(15, 2));
}

/**
 * start reading from an epoch whose metadata is unconfirmed
 * (1) last epoch metadata in the nodeset is of epoch 5 and read stream
 *     starts reading from epoch 8
 * (2) the read stream gets its initial nodeset from epoch metadata 5 and starts
 *     reading from it
 * (3) however, the initial metadata may or may not be correct, in this case,
 *     epoch metadata for epoch 8, along with its data log records, is
 *     not yet written at the time
 * (4) we expect the client stream request epoch metadata for epoch 8 once again
 *     as soon as it encounters a gap in epoch 8
 */

TEST_P(ClientReadStreamTest, StartReadingWithUnconfirmedEpochs) {
  epoch_t epoch(8);
  start_lsn_ = lsn(epoch.val_, 1);
  buffer_size_ = 4;
  state_.disable_default_metadata = true;
  start();
  ASSERT_NO_START_MESSAGES();

  // allows soft entries for metadata cache when first starts reading
  ASSERT_TRUE(state_.require_consistent_from_cache.hasValue());
  ASSERT_FALSE(state_.require_consistent_from_cache.value());
  // request metadata for epoch 8 for the first time
  ASSERT_METADATA_REQ(epoch_t(8));

  // epoch metadata: epoch 5, until 8
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(8),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2},
                  RecordSource::LAST);
  // should populate a soft entry in the cache
  ASSERT_EQ(1, state_.cache_entries_.size());
  ASSERT_EQ(epoch_t(8), state_.cache_entries_.back().epoch);
  ASSERT_EQ(epoch_t(8), state_.cache_entries_.back().until);
  ASSERT_EQ(RecordSource::CACHED_SOFT, state_.cache_entries_.back().source);

  overrideConnectionStates(ConnectionState::READING);
  // should started reading from {1, 2}
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1,
                        N2);

  // node 1 delivers (8, 2), node 2 delivers a gap to (9, 2)
  onDataRecord(N1, mockRecord(lsn(8, 2)));
  onGap(N2, mockGap(N2, lsn(8, 1), lsn(9, 2)));
  // no gap or records should be delivered, read stream should re-request
  // epoch metadata for epoch 8
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();

  // this time it should require only consistent entries from metadata cache
  ASSERT_TRUE(state_.require_consistent_from_cache.value());
  ASSERT_METADATA_REQ(epoch_t(8));
  // new nodeset for epoch 8 is {1, 3}
  onEpochMetaData(epoch,
                  epoch_t(8),
                  epoch_t(8),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N3});

  // should overwrite the existing soft entry
  ASSERT_EQ(epoch_t(8), state_.cache_entries_.back().epoch);
  ASSERT_EQ(epoch_t(8), state_.cache_entries_.back().until);
  ASSERT_EQ(
      RecordSource::CACHED_CONSISTENT, state_.cache_entries_.back().source);

  ASSERT_STOP_MESSAGES(N2);
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N3);

  overrideConnectionStates(ConnectionState::READING);
  // node 3 sends record (8, 1)
  onDataRecord(N3, mockRecord(lsn(8, 1)));
  // (8, 1) should be delivered and reading continues on the new epoch
  ASSERT_RECV(lsn(8, 1), lsn(8, 2));
}

/**
 * With nodeset, when ignoreReleasedStatus() is set, the read stream should
 * not request more epoch metadata as soon as it gets the final epoch metadata
 * from the metadata log
 */
TEST_P(ClientReadStreamTest, IgnoreReleasedStatusWithNodeSet) {
  start_lsn_ = lsn(3, 2);
  until_lsn_ = LSN_MAX - 100;
  state_.disable_default_metadata = true;
  ignore_released_status_ = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(2),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2},
                  RecordSource::LAST);
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(N1, mockRecord(lsn(3, 2)));
  onDataRecord(N2, mockRecord(lsn(3, 3)));
  ASSERT_RECV(lsn(3, 2), lsn(3, 3));
  onGap(N1, mockGap(N2, lsn(3, 3), until_lsn_));
  onGap(N2, mockGap(N2, lsn(3, 4), until_lsn_));
  // read stream should just conclude with a gap to until_lsn
  ASSERT_METADATA_REQ();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 4), until_lsn_});
  ASSERT_TRUE(state_.disposed);
}

/**
 * With nodeset support and metadata log, ClientReadStream gets nodeset updates
 * when it fetched metadata for an epoch, rather than when configuration
 * changes. However, if the newly added node was once part of the nodeset
 * used by the current epoch (e.g., result of cluster shrunk). The read stream
 * should start reading from the node.
 *
 * Expected steps:
 * (1) add a new node to the cluster;
 * (2) if the new node belongs to the current nodeset, then the read stream
 *     should connect to the node and start reading
 */
TEST_P(ClientReadStreamTest, ExpandClusterWithNodeInNodeSet) {
  const epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.disable_default_metadata = true;
  // initially only one node
  state_.shards.resize(1);
  start();
  ASSERT_METADATA_REQ(epoch);
  // epoch metadata: epoch 5, until 8
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(8),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::LAST);

  // should started reading from only node 0 despite nodeset is {0, 1}
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);

  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear(); // clear initial START messages

  // expand the cluster by adding one more node
  state_.shards.push_back(N1);
  updateConfig();

  read_stream_->noteConfigurationChanged();

  ASSERT_METADATA_REQ();

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1);
}

// similar to the previous test, but the new node is not in the current
// nodeset, read stream should not start reading it
TEST_P(ClientReadStreamTest, ExpandClusterWithNodeNotInNodeSet) {
  const epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.disable_default_metadata = true;
  state_.shards.resize(1);
  start();
  ASSERT_METADATA_REQ(epoch);
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N2},
                  RecordSource::LAST);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);

  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  // expand the cluster by adding one more node
  state_.shards.push_back(N1);
  updateConfig();
  read_stream_->noteConfigurationChanged();
  ASSERT_METADATA_REQ();
  // should not start reading from the new node because it is not in the current
  // nodeset.
  ASSERT_NO_START_MESSAGES();

  // N0 sends a gap, because N2 is not in the config, ClientReadStream should
  // try to issue it now, but we don't have the metadata for epoch 6...
  onGap(N0, mockGap(N0, start_lsn_, lsn(6, 10), GapReason::NO_RECORDS));
  ASSERT_METADATA_REQ(epoch_t(epoch.val_ + 1));
  onEpochMetaData(epoch_t(6),
                  epoch_t(6),
                  epoch_t(6),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2},
                  RecordSource::LAST);

  // We received the metadata for epoch 6... it now contains node 1 which was
  // just added to the config, we should start reading from it now.
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1);
}

/**
 * Similarily, a node should be removed in the current read set if it
 * is removed from config and is present in the current read set.
 */
TEST_P(ClientReadStreamTest, ShrinkCluster) {
  const epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.disable_default_metadata = true;
  state_.shards.resize(2);
  start();
  ASSERT_METADATA_REQ(epoch);
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::LAST);

  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1);

  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  // shrink the cluster by one node
  state_.shards.resize(1);
  updateConfig();
  read_stream_->noteConfigurationChanged();
  ASSERT_METADATA_REQ();
  // should stop reading from node 1
  ASSERT_STOP_MESSAGES(N1);
}

// Test the scenario that read stream gets an epoch metadata with a nodeset of
// {0, 1, 2, 3} but N3 is no longer in config. Later the cluster is expanded and
// N3 is added.
// Check that if the cluster is expanded while there is a node in the known down
// list, the START message that will be sent to the new node contains the up to
// date known down list.
TEST_P(ClientReadStreamTest, ScdExpandClusterWhileNodeKnownDownWithNodeSet) {
  const epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  replication_factor_ = 3;
  buffer_size_ = 10;
  scd_enabled_ = true;
  state_.disable_default_metadata = true;
  // initially only one node
  state_.shards.resize(3);
  start();
  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_METADATA_REQ(epoch);
  // epoch metadata: epoch 5, until 5, replication 3, nodeset {0, 1, 2, 3}
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(5),
                  3,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2, N3},
                  RecordSource::LAST);
  // start reading from NO, N1, N2
  ASSERT_START_MESSAGES(start_lsn_,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2);

  ON_STARTED(filter_version_t{1}, N0, N1, N2);
  onDataRecord(N0, mockRecord(lsn(5, 2)));
  onDataRecord(N1, mockRecord(lsn(5, 3)));

  SET_NODE_ALIVE(node_index_t{2}, false);

  // failover takes place, N2 is added to the known down list.
  scdShardsDownFailoverTimerCallback();
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(start_lsn_,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2);

  // The cluster is expanded: N3 is added.
  state_.shards.push_back(N3);
  updateConfig();
  read_stream_->noteConfigurationChanged();

  // START should be sent to N3 with the last version of the known down list.
  ASSERT_START_MESSAGES(start_lsn_,
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N3);
}

TEST_P(ClientReadStreamTest, SkipPartiallyTrimmedSectionsWithNodeSet) {
  epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.shards.resize(2);
  buffer_size_ = 128;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(5));
  // epoch metadata: epoch 5, until 5
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::LAST);
  ON_STARTED(filter_version_t{1}, N0, N1);

  onDataRecord(N0, mockRecord(lsn(5, 1)));
  // the trim gap spans across to epoch 7
  onGap(N1, mockGap(N1, lsn(5, 1), lsn(7, 10), GapReason::TRIM));
  ASSERT_RECV(lsn(5, 1));
  ASSERT_GAP_MESSAGES();

  onDataRecord(N0, mockRecord(lsn(5, 4)));

  // there should be no gap delivered since we don't have epoch metadata for
  // epoch 6 and 7
  ASSERT_GAP_MESSAGES();
  // the read stream, when fast forwarding, should requeset epoch metadata for
  // epoch 6
  ASSERT_METADATA_REQ(epoch_t(6));
  // epoch metadata: epoch 6, until 7, replication 1, nodeset {0, 1}
  onEpochMetaData(epoch_t(6),
                  epoch_t(6),
                  epoch_t(7),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::LAST);

  // now a gap can be delivered
  // This should result in a gap through LSN (7,10) despite N0 sending some
  // records in the interval.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(5, 2), lsn(7, 10)});

  onDataRecord(N0, mockRecord(lsn(7, 6)));
  onDataRecord(N0, mockRecord(lsn(7, 12)));
  onDataRecord(N1, mockRecord(lsn(7, 11)));
  ASSERT_RECV(lsn(7, 11), lsn(7, 12));

  // gap detection should still work in the new epoch
  onGap(N1, mockGap(N1, lsn(7, 12), lsn(7, 18), GapReason::NO_RECORDS));
  onGap(N0, mockGap(N0, lsn(7, 13), lsn(7, 21), GapReason::NO_RECORDS));

  ASSERT_METADATA_REQ();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(7, 13), lsn(7, 18)});
}

// test the implementation of map based buffer for handling sparse LSNs with
// a huge capacity
TEST_F(ClientReadStreamTest, OrderedMapBufferTest) {
  start_lsn_ = lsn(3, 2);
  until_lsn_ = LSN_MAX - 100;
  state_.shards.resize(2);
  buffer_type_ = ClientReadStreamBufferType::ORDERED_MAP;
  // the buffer can accept records in the next 16 epochs
  buffer_size_ = 16ull * (static_cast<size_t>(ESN_MAX.val_) + 1);
  flow_control_threshold_ = 0.5;
  start();

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1);

  // the buffer should be able to accept LSNs in the huge window
  onDataRecord(N0, mockRecord(lsn(3, 2)));
  onDataRecord(N1, mockRecord(lsn(4, 2)));

  onGap(N0, mockGap(N0, lsn(3, 3), lsn(9, 1)));
  onDataRecord(N0, mockRecord(lsn(9, 2)));
  onGap(N1, mockGap(N1, lsn(4, 3), lsn(15, 6)));
  onDataRecord(N1, mockRecord(lsn(15, 7)));
  ASSERT_RECV(lsn(3, 2), lsn(4, 2), lsn(9, 2));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 3), lsn(4, 1)},
                      GapMessage{GapType::BRIDGE, lsn(4, 3), lsn(9, 1)});

  // no WINDOW message should be issued as the threshold is not crossed
  ASSERT_NO_WINDOW_MESSAGES();

  onDataRecord(N0, mockRecord(lsn(18, 9)));
  ASSERT_RECV(lsn(15, 7));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(9, 3), lsn(15, 6)});

  // WINDOW should slide now since the threshold is reached
  ASSERT_WINDOW_MESSAGES(lsn(15, 8), lsn(31, 7), N0, N1);
}

TEST_P(ClientReadStreamTest, StartReadingWithUnprovisionedMetaDataLog) {
  start_lsn_ = lsn(5, 8);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  // shouldn't send any START until we figure out the first nodeset
  ASSERT_NO_START_MESSAGES();
  ASSERT_STOP_MESSAGES();
  ASSERT_NO_WINDOW_MESSAGES();
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));

  // metadata log is not provisioned
  read_stream_->onEpochMetaData(
      E::NOTFOUND,
      MetaDataLogReader::Result{LOG_ID,
                                lsn_to_epoch(start_lsn_),
                                lsn_to_epoch(start_lsn_),
                                RecordSource::LAST,
                                LSN_INVALID,
                                std::chrono::milliseconds(0),
                                nullptr});

  ASSERT_METADATA_REQ();
  ASSERT_NO_START_MESSAGES();

  // the backoff timer should be started
  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getRetryReadMetaDataTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE(timer->isActive());

  // simulate timer expires
  timer->trigger();

  // should requesting metadata on the starting epoch again
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));

  // deliver the first nodeset {1, 2} with epoch 3
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(5),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});

  ASSERT_EQ(nullptr, getRetryReadMetaDataTimer());

  overrideConnectionStates(ConnectionState::READING);
  // should start reading from {1, 2}
  ASSERT_START_MESSAGES(lsn_t(start_lsn_),
                        LSN_MAX,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1,
                        N2);
}

// test the optimization of maintaining last released lsn for speeding
// up fetching metadata
TEST_P(ClientReadStreamTest, LogWithManyEmptyEpochs) {
  epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.shards.resize(3);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(5));
  // epoch metadata: epoch 5, until 9
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(9),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::NOT_LAST);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1);
  ON_STARTED_FULL(filter_version_t{1}, E::OK, lsn(100, 1), N0, N1);

  onDataRecord(N0, mockRecord(lsn(5, 1)));
  ASSERT_RECV(lsn(5, 1));
  onGap(N0, mockGap(N0, lsn(5, 2), lsn(1000, 1), GapReason::NO_RECORDS));
  onGap(N1, mockGap(N1, lsn(5, 1), lsn(1000, 2), GapReason::NO_RECORDS));
  // last_epoch_with_metadata_ should still be 5 since the read stream does
  // know about the last released epoch at the time of the request
  ASSERT_EQ(epoch_t(5), getLastEpochWithMetaData());
  // read stream still needs to fetch metadata for epoch [6, 1000]
  ASSERT_METADATA_REQ(epoch_t(6));

  // read the metadata again and this time we have a new record for epoch 6
  onEpochMetaData(epoch_t(6),
                  epoch_t(6),
                  epoch_t(6),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2},
                  RecordSource::LAST);
  // last_epoch_with_metadata_ should advance to 1000 since we got gap in that
  // epoch
  ASSERT_EQ(epoch_t(1000), getLastEpochWithMetaData());
  ASSERT_STOP_MESSAGES(N0);
  ASSERT_START_MESSAGES(lsn(5, 2),
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N2);
  // N2 reports that the log is released up to epoch 3000
  ON_STARTED_FULL(filter_version_t{1}, E::OK, lsn(9000, 0), N2);
  onGap(N2, mockGap(N2, lsn(5, 2), lsn(1000, 1), GapReason::NO_RECORDS));
  // a gap should be delivered upto epoch 1000
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(5, 2), lsn(1000, 1)});
  onDataRecord(N2, mockRecord(lsn(1000, 2)));
  onDataRecord(N1, mockRecord(lsn(1000, 3)));
  ASSERT_RECV(lsn(1000, 2), lsn(1000, 3));
  onGap(N1, mockGap(N1, lsn(1000, 4), lsn(1500, 3), GapReason::NO_RECORDS));
  onGap(N2, mockGap(N2, lsn(1000, 3), lsn(1500, 4), GapReason::NO_RECORDS));
  ASSERT_EQ(epoch_t(1000), getLastEpochWithMetaData());
  ASSERT_METADATA_REQ(epoch_t(1001));
  // still the same metadata log entry
  onEpochMetaData(epoch_t(1001),
                  epoch_t(10),
                  epoch_t(1001),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2},
                  RecordSource::LAST);
  // but this time we know the epoch is effective until 9000
  ASSERT_EQ(epoch_t(9000), getLastEpochWithMetaData());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1000, 4), lsn(1500, 3)});
  onDataRecord(N1, mockRecord(lsn(1500, 4)));
  onDataRecord(N2, mockRecord(lsn(1500, 5)));
  ASSERT_RECV(lsn(1500, 4), lsn(1500, 5));
  onGap(N1, mockGap(N1, lsn(1500, 5), lsn(5000, 7), GapReason::NO_RECORDS));
  onGap(N2, mockGap(N2, lsn(1500, 6), lsn(5000, 9), GapReason::NO_RECORDS));
  // no epoch metadata request should have been made
  ASSERT_METADATA_REQ();
  // a gap should be delivered immediately
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(1500, 6), lsn(5000, 7)});
}

// If callbacks reject data or gaps, need to redeliver
TEST_P(ClientReadStreamTest, Redelivery) {
  state_.shards.resize(1);
  buffer_size_ = 2;
  flow_control_threshold_ = 1.0;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));

  state_.callbacks_accepting = false;
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2));
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  // Must not slide window if delivery failed
  ASSERT_NO_WINDOW_MESSAGES();

  state_.callbacks_accepting = true;
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();
  ASSERT_RECV(lsn(1, 2));
  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());
  ASSERT_WINDOW_MESSAGES(lsn(1, 3), lsn(1, 4), N0);

  // Similar test with gaps.
  onGap(N0, mockGap(N0, lsn(1, 3), lsn(1, 3), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 3), lsn(1, 3)});

  state_.callbacks_accepting = false;
  onGap(N0, mockGap(N0, lsn(1, 4), lsn(1, 4), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 4), lsn(1, 4)});
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  ASSERT_NO_WINDOW_MESSAGES();

  state_.callbacks_accepting = true;
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 4), lsn(1, 4)});
  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());
  ASSERT_WINDOW_MESSAGES(lsn(1, 5), lsn(1, 6), N0);
}

// If callbacks reject data, but client receives a TRIM gap in the meantime,
// it should still redeliver the record.
TEST_P(ClientReadStreamTest, NoFastForwardWhileRedelivering) {
  state_.shards.resize(1);
  buffer_size_ = 2;
  flow_control_threshold_ = 1.0;
  start();

  onDataRecord(N0, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));

  state_.callbacks_accepting = false;
  onDataRecord(N0, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2));
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  // Must not slide window if delivery failed
  ASSERT_NO_WINDOW_MESSAGES();

  onGap(N0, mockGap(N0, lsn(1, 3), lsn(1, 3), GapReason::TRIM));

  // we shouldn't deliver anything yet, because redelivery timer is
  // still in progress
  ASSERT_RECV();

  // simulate trigger redelivery
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();

  // callback is still rejecting record.
  ASSERT_RECV(lsn(1, 2));
  ASSERT_TRUE(getRedeliveryTimer()->isActive());
  ASSERT_NO_WINDOW_MESSAGES();

  state_.callbacks_accepting = true;
  // simulate trigger redelivery
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();
  // make sure we do redeliver the record before the trim gap
  ASSERT_RECV(lsn(1, 2));
  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());
  ASSERT_WINDOW_MESSAGES(lsn(1, 3), lsn(1, 4), N0);
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 3), lsn(1, 3)});
}

// Verifies a fix for an issue described in T30725519
TEST_P(ClientReadStreamTest, NoFastForwardFixForT30725519) {
  state_.shards.resize(4);
  buffer_size_ = 20;
  replication_factor_ = 2;
  flow_control_threshold_ = 1.0;
  scd_enabled_ = true;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  // Simulate a TRIM gap arriving, but the consumer rejects it.
  state_.callbacks_accepting = false;
  onGap(N1, mockGap(N0, lsn(1, 1), lsn(1, 30), GapReason::TRIM));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 30)});
  ASSERT_TRUE(getRedeliveryTimer()->isActive());

  // Simulate the redelivery timer triggering, we should now have issued the
  // TRIM gap.
  state_.callbacks_accepting = true;
  dynamic_cast<MockBackoffTimer*>(getRedeliveryTimer())->trigger();

  ASSERT_FALSE(getRedeliveryTimer() && getRedeliveryTimer()->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(1, 1), lsn(1, 30)});
}

// In this test, there are 4 nodes, N0 is in one region, while
// N1, N2, N3 are in another region, replication factor is 2, and
// records were written with synchronous replication scope of REGION
// (cross-region replication). The client readstream should still be
// able to read and correctly identify gaps when one entire region is
// down.
TEST_P(ClientReadStreamTest, FailureDomainGapDetection) {
  state_.shards.resize(4);
  node_locations_[N0.node()] = "region1....";
  node_locations_[N1.node()] = "region2.dc1...";
  node_locations_[N2.node()] = "region2.dc2.cl3.row4.rack5";
  node_locations_[N3.node()] = "region2.dc2.cl3.row4.rack6";
  buffer_size_ = 4096;
  replication_factor_ = 2;
  sync_replication_scope_ = NodeLocationScope::REGION;
  start();

  // N0 sent record 4, which is enough for us to determine a gap
  // of [1, 3]
  onDataRecord(N0, mockRecord(lsn(1, 4)));
  // no gap yet, but timer should be activated
  ASSERT_GAP_MESSAGES();

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);

  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());

  // simulate grace period expiring
  timer->getCallback()();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 3)});

  ASSERT_RECV(lsn(1, 4));
  ASSERT_FALSE(timer->isActive());

  onDataRecord(N1, mockRecord(lsn(1, 7)));
  onDataRecord(N2, mockRecord(lsn(1, 8)));
  ASSERT_FALSE(timer->isActive());
  onDataRecord(N3, mockRecord(lsn(1, 9)));

  ASSERT_TRUE(timer->isActive());
  timer->getCallback()();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 6)});
  ASSERT_RECV(lsn(1, 7), lsn(1, 8), lsn(1, 9));
}

// Test connection health check can take into account of failure domain
// information.
TEST_P(ClientReadStreamTest, FailureDomainConnectionHealth) {
  state_.shards.resize(4);
  node_locations_[N0.node()] = "region1....";
  node_locations_[N1.node()] = "region2.dc1...";
  node_locations_[N2.node()] = "region2.dc2.cl3.row4.rack5";
  node_locations_[N3.node()] = "region2.dc2.cl3.row4.rack6";
  buffer_size_ = 4096;
  replication_factor_ = 2;
  sync_replication_scope_ = NodeLocationScope::REGION;

  start();

  // by default all nodes are in READING state, connection should be healthy
  ASSERT_TRUE(state_.connection_healthy);
  // N0, N1 changed to CONNECTING state, connection should become unhealthy
  overrideConnectionStates(ConnectionState::CONNECTING, {N0, N1});
  ASSERT_FALSE(state_.connection_healthy);

  // N0 changed to READING while N1, N2, N3 is in CONNECTING, the connection
  // should be healthy
  overrideConnectionStates(ConnectionState::CONNECTING, {N2, N3});
  ASSERT_FALSE(state_.connection_healthy);
  overrideConnectionStates(ConnectionState::READING, {N0});
  ASSERT_TRUE(state_.connection_healthy);

  // connection is healthy if N2, N3, N4 is in READING
  overrideConnectionStates(ConnectionState::CONNECTING, {N0});
  ASSERT_FALSE(state_.connection_healthy);
  overrideConnectionStates(ConnectionState::READING, {N1, N2, N3});
  ASSERT_TRUE(state_.connection_healthy);
}

TEST_P(ClientReadStreamTest, TrimAndBridgeOutsideWindowT8698308) {
  buffer_size_ = 3;
  replication_factor_ = 2;
  state_.disable_default_metadata = true;
  start();

  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(1),
                  epoch_t(1),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1});

  for (auto n : {N0, N1}) {
    onGap(n, mockGap(n, lsn(1, 1), lsn(2, 1), GapReason::TRIM));
    onGap(n, mockGap(n, lsn(2, 2), lsn(4, 0), GapReason::NO_RECORDS));
  }
  for (int e : {2, 3, 4}) {
    ASSERT_METADATA_REQ(epoch_t(e));
    onEpochMetaData(epoch_t(e),
                    epoch_t(1),
                    epoch_t(e),
                    2,
                    NodeLocationScope::NODE,
                    StorageSet{N0, N1});
  }

  GapMessage a{GapType::TRIM, lsn(1, 1), lsn(2, 1)};
  GapMessage b{GapType::BRIDGE, lsn(2, 2), lsn(4, 0)};
  ASSERT_GAP_MESSAGES(a, b);
}

// Read when some nodes are seen as rebuilding in the event log. Verify we do
// make progress if all non rebuilding nodes chime in and verify the interaction
// with sliding the window and moving on to a different epoch / nodeset.
TEST_P(ClientReadStreamTest, RebuildingWithNodesDown) {
  state_.shards.resize(4);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  start_lsn_ = lsn(3, 5);
  until_lsn_ = lsn(4, 15);
  state_.disable_default_metadata = true;
  start();

  // Got the first nodeset {0, 1, 2} with epoch 3.
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2, N3});
  overrideConnectionStates(ConnectionState::START_SENT);

  // Should send START to all nodes.
  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        buffer_max,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // N2 starts and sends a record.
  ON_STARTED(filter_version_t{1}, N2);
  onDataRecord(N2, mockRecord(lsn(3, 7)));

  // Event log says that N1 and N3 start rebuilding.
  addToRebuildingSet(N1);
  markUnrecoverable(N1);
  addToRebuildingSet(N3);
  markUnrecoverable(N3);

  ASSERT_FALSE(state_.connection_healthy);

  // Shouldn't deliver anything, still waiting for N0 to respond.
  ASSERT_RECV();

  // N0 replies.
  ON_STARTED(filter_version_t{1}, N0);
  ASSERT_TRUE(state_.connection_healthy);
  onDataRecord(N0, mockRecord(lsn(3, 7)));

  // Should deliver the record preceded with a DATALOSS gap because
  // all nodes that are not rebuilding chimed in.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, start_lsn_, lsn(3, 6)});
  ASSERT_RECV(lsn(3, 7));

  // N2 sends another record.
  onDataRecord(N2, mockRecord(lsn(3, 9)));
  ASSERT_GAP_MESSAGES();

  // N0 sends it as well.
  onDataRecord(N0, mockRecord(lsn(3, 9)));

  // Should deliver it with the DATALOSS gap.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(3, 8), lsn(3, 8)});
  ASSERT_RECV(lsn(3, 9));

  // N2, N0 sends a gap outside the window. We slide the window on N2.
  onGap(N2, mockGap(N2, lsn(3, 10), lsn(3, 10000)));
  onGap(N0, mockGap(N0, lsn(3, 10), lsn(3, 10000)));
  ASSERT_WINDOW_MESSAGES(
      lsn(3, 10001), calc_buffer_max(lsn(3, 10001), buffer_size_), N0, N2);
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(3, 10), lsn(3, 10000)});

  // N2, N0 sends a gap to another epoch.
  ASSERT_METADATA_REQ();
  onGap(N2, mockGap(N2, lsn(3, 10001), lsn(4, 14)));
  onGap(N0, mockGap(N0, lsn(3, 10001), lsn(4, 14)));
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(epoch_t(4));
  ASSERT_RECV();

  // Got metadata for epoch 4.
  onEpochMetaData(epoch_t(4),
                  epoch_t(4),
                  epoch_t(4),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2, N3});
  ASSERT_STOP_MESSAGES(N0);
  // The window should be slid.
  ASSERT_WINDOW_MESSAGES(lsn(4, 15), until_lsn_, N2);

  // Gap should be delivered.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 10001), lsn(4, 14)});

  // N1's START timed out, we should send another one.
  startedTimerCallback(N1);
  ASSERT_START_MESSAGES(lsn(4, 15),
                        until_lsn_,
                        until_lsn_,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1);

  // N2 sends the last record.
  onDataRecord(N2, mockRecord(lsn(4, 15)));

  // We should deliver it and die.
  ASSERT_RECV(lsn(4, 15));
  ASSERT_TRUE(state_.disposed);
}

// If you create a ClientReadStream for a log that's not in the config it should
// issue a NOTINCONFIG gap for the whole range.
TEST_P(ClientReadStreamTest, StartReadingLogNotInConfig) {
  start_lsn_ = lsn(5, 8);
  until_lsn_ = lsn(42, 3);
  buffer_size_ = 1024;
  start(logid_t(42)); // Log 42 is not in config
  ASSERT_GAP_MESSAGES(GapMessage{GapType::NOTINCONFIG, start_lsn_, until_lsn_});
}

// Verify that the health callback is called with the appropriate values as the
// connection state and authoritative state of some nodes change.
TEST_P(ClientReadStreamTest, HealthWhenMixNodesRebuildingAndInRepair) {
  state_.shards.resize(6);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  until_lsn_ = lsn(8, ESN_MAX.val_ - 1);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  MockBackoffTimer* stall_grace_timer =
      dynamic_cast<MockBackoffTimer*>(getStallGracePeriodTimer());

  ASSERT_TRUE(stall_grace_timer->isActive());
  ASSERT_TRUE((bool)stall_grace_timer->getCallback());

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);

  // No one has sent STARTED yet.
  ASSERT_FALSE(state_.connection_healthy);

  ON_STARTED(filter_version_t{1}, N2);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N3);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N4);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_FALSE(state_.connection_healthy);

  // A node is added to the rebuilding set.
  addToRebuildingSet(N0);
  markUnrecoverable(N0);
  ASSERT_FALSE(state_.connection_healthy);

  // Another one is added to the rebuilding set.
  const lsn_t lsn = addToRebuildingSet(N1);
  markUnrecoverable(N1);
  // At this point, all non rebuilding nodes are reading, so the connection
  // becomes healthy.
  ASSERT_TRUE(state_.connection_healthy);

  // We simulate the socket for N5 to be closed, causing the connection to
  // become unhealthy.
  (*state_.on_close[N5])(E::PEER_CLOSED, Address(NodeID(5)));

  // simulate grace period expiring
  stall_grace_timer->getCallback()();
  ASSERT_FALSE(state_.connection_healthy);

  // Simulate the reconnect timer triggering. N5 reconnects. The connection
  // should be back healthy.
  reconnectTimerCallback(N5);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N5);
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_TRUE(state_.connection_healthy);

  // We are notified that all donors rebuilt N0 and N1.
  onAllDonorsFinishRebuilding(lsn);
  // N0, N2 are now AUTHORITATIVE_EMPTY.
  ASSERT_TRUE(state_.connection_healthy);
  // Because the authoritative status of some nodes changed to AUTH_EMPTY,
  // a rewind has been scheduled.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);
  ON_STARTED(filter_version_t{2}, N2);
  ON_STARTED(filter_version_t{2}, N3);
  ON_STARTED(filter_version_t{2}, N4);
  ON_STARTED(filter_version_t{2}, N5);
  ASSERT_TRUE(state_.connection_healthy);

  // We lose the connection to N1 which told us it's rebuilding.
  (*state_.on_close[N1])(E::PEER_CLOSED, Address(NodeID(N1.node())));
  ASSERT_TRUE(state_.connection_healthy);
  reconnectTimerCallback(N1);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N1);
  ON_STARTED(filter_version_t{2}, N1);
  // Now we have 2 nodes AUTHORITATIVE_EMPTY and all the other nodes are
  // reading.
  ASSERT_TRUE(state_.connection_healthy);

  // Connection to N5 is closed. We should still have a healthy connection
  // because we still have an f-majority within the non AUTHORITATIVE_EMPTY
  // nodes.
  (*state_.on_close[N5])(E::PEER_CLOSED, Address(NodeID(N5.node())));
  ASSERT_TRUE(state_.connection_healthy);

  // Connection to N2 is closed. We have an unhealthy connection because we have
  // two AUTHORITATIVE_EMPTY nodes and 2 nodes down.
  (*state_.on_close[N2])(E::PEER_CLOSED, Address(NodeID(2)));
  // grace period expires
  stall_grace_timer->getCallback()();

  ASSERT_FALSE(state_.connection_healthy);
  // N2 reconnects. Connection is back healthy.
  reconnectTimerCallback(N2);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N2);
  ON_STARTED(filter_version_t{2}, N2);
  ASSERT_TRUE(state_.connection_healthy);
}

// If 2 nodes are in repair, check that we just need an f-majority of nodes
// within the remaining nodes to chime in order to make progress.
TEST_P(ClientReadStreamTest, TwoNodesInRepairCanMakeProgress) {
  state_.shards.resize(6);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  start_lsn_ = lsn(1, 30);
  until_lsn_ = lsn(8, ESN_MAX.val_ - 1);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);

  // No one has sent STARTED yet.
  ASSERT_FALSE(state_.connection_healthy);

  ON_STARTED(filter_version_t{1}, N2);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N3);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N4);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_FALSE(state_.connection_healthy);

  // N0 is added to the rebuilding set.
  addToRebuildingSet(N0);
  ASSERT_FALSE(state_.connection_healthy);
  // N1 is added to the rebuilding set.
  const lsn_t version = addToRebuildingSet(N1);
  ASSERT_FALSE(state_.connection_healthy);

  // N0 is marked unrecoverable.
  markUnrecoverable(N0);
  ASSERT_FALSE(state_.connection_healthy);

  // N1 is marked unrecoverable as well, which should make the connection
  // healthy.
  markUnrecoverable(N1);
  ASSERT_TRUE(state_.connection_healthy);

  // All donors rebuilt N0, N1
  onAllDonorsFinishRebuilding(version);
  ASSERT_TRUE(state_.connection_healthy);

  // All these authoritative status changed cause a rewind.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);
  ON_STARTED(filter_version_t{2}, N2);
  ON_STARTED(filter_version_t{2}, N3);
  ON_STARTED(filter_version_t{2}, N4);
  ON_STARTED(filter_version_t{2}, N5);

  // At this point N0 and N1 are AUTHORITATIVE_EMPTY and all the other nodes are
  // in ConnectionState::READING. We just need 3 nodes to chime in out of the 4
  // remaining nodes.

  // N2 sends a gap up to e4n42
  onDataRecord(N2, mockRecord(lsn(1, 42)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N3 sends a gap up to e4n44
  onDataRecord(N3, mockRecord(lsn(1, 44)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N4 sends a gap up to e4n41
  onDataRecord(N4, mockRecord(lsn(1, 41)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // The grace period should be active because we have more nodes that could
  // chime in. Let's trigger it and check that a DATALOSS gap is issued.
  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());
  timer->getCallback()();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, start_lsn_, lsn(1, 40)});
}

// A node says it's rebuilding while the event log says it's not. Verify that
// we ignore the node's response. Once the node disconnects we try re-connecting
// again and this time it says it is fully authoritative.
TEST_P(ClientReadStreamTest, NodeSaysItsRebuildingButEventLogSaysItsNot) {
  state_.shards.resize(6);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  start_lsn_ = lsn(1, 30);
  until_lsn_ = lsn(8, ESN_MAX.val_ - 1);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);

  // No one has sent STARTED yet.
  ASSERT_FALSE(state_.connection_healthy);

  ON_STARTED(filter_version_t{1}, N2);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N3);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N4);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_FALSE(state_.connection_healthy);

  // N1 is rebuilding
  ON_STARTED_REBUILDING(filter_version_t{1}, N1);
  ASSERT_FALSE(state_.connection_healthy);

  // N0 is rebuilding
  addToRebuildingSet(N0);
  markUnrecoverable(N0);

  // We discarded N1's response.
  ASSERT_FALSE(state_.connection_healthy);

  // Connection to N1 is closed.
  (*state_.on_close[N1])(E::PEER_CLOSED, Address(NodeID(N1.node())));
  // We try to send a new START to N1.
  reconnectTimerCallback(N1);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1);

  // N1 replies. The connection is healthy because N0 is rebuilding and all the
  // other nodes are up.
  ON_STARTED(filter_version_t{1}, N1);
  ASSERT_TRUE(state_.connection_healthy);
}

// A node says it's rebuilding while the event log says it's empty. This should
// not change its authoritative status.
TEST_P(ClientReadStreamTest, NodeSaysItsRebuildingButEventLogSaysItsEmpty) {
  state_.shards.resize(6);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  start_lsn_ = lsn(1, 30);
  until_lsn_ = lsn(8, ESN_MAX.val_ - 1);
  start();

  // mock grace period timer
  MockBackoffTimer* const timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE((bool)timer->getCallback());

  MockBackoffTimer* stall_grace_timer =
      dynamic_cast<MockBackoffTimer*>(getStallGracePeriodTimer());

  ASSERT_TRUE((bool)stall_grace_timer->getCallback());

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);

  // No one has sent STARTED yet.
  ASSERT_FALSE(state_.connection_healthy);
  EXPECT_TRUE(stall_grace_timer->isActive());

  ON_STARTED(filter_version_t{1}, N1);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N2);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N3);
  ASSERT_FALSE(state_.connection_healthy);
  ON_STARTED(filter_version_t{1}, N4);
  ASSERT_FALSE(state_.connection_healthy);
  EXPECT_TRUE(stall_grace_timer->isActive());
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_TRUE(state_.connection_healthy);
  EXPECT_FALSE(stall_grace_timer->isActive());

  // N0 is rebuilding according to event log.
  const lsn_t version = addToRebuildingSet(N0);
  // We still have an f-majority of nodes that sent STARTED, so the connection
  // is healthy.
  ASSERT_TRUE(state_.connection_healthy);
  // N0 is now AUTHORITATIVE_EMPTY.
  onAllDonorsFinishRebuilding(version);
  ASSERT_TRUE(state_.connection_healthy);

  // The transition to AUTHORITATIVE_EMPTY causes a rewind.
  triggerScheduledRewind();
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{2},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);
  ON_STARTED(filter_version_t{2}, N1);
  ON_STARTED(filter_version_t{2}, N2);
  ON_STARTED(filter_version_t{2}, N3);
  ON_STARTED(filter_version_t{2}, N4);
  ON_STARTED(filter_version_t{2}, N5);
  ASSERT_TRUE(state_.connection_healthy);
  EXPECT_FALSE(stall_grace_timer->isActive());

  // Now... N0 responds STARTED(E::REBUILDING)
  ON_STARTED_REBUILDING(filter_version_t{2}, N0);
  ASSERT_TRUE(state_.connection_healthy);

  // If we lose the connection to another node, the connection should still be
  // healthy because we have an f-majority within all the nodes minus N0 which
  // is considered AUTHORITATIVE_EMPTY.
  (*state_.on_close[N3])(E::PEER_CLOSED, Address(NodeID(N3.node())));
  ASSERT_TRUE(state_.connection_healthy);

  // We also lose the connection to N0. It doesn't change anything because N0
  // is AUTHORITATIVE_EMPTY.
  (*state_.on_close[N0])(E::PEER_CLOSED, Address(NodeID(0)));
  EXPECT_TRUE(state_.connection_healthy);
  EXPECT_FALSE(stall_grace_timer->isActive());

  reconnectTimerCallback(N0);
  ASSERT_TRUE(state_.connection_healthy);
  EXPECT_FALSE(stall_grace_timer->isActive());
}

TEST_P(ClientReadStreamTest, EmptyNodeReconnectsAndSendsRecords) {
  state_.shards.resize(6);
  buffer_size_ = 100;
  replication_factor_ = 2;
  scd_enabled_ = false;
  start_lsn_ = lsn(1, 30);
  until_lsn_ = lsn(8, ESN_MAX.val_ - 1);
  start();

  // Undo start() overriding states to READING.  We want to simulate N1
  // replying STARTED later than N0.
  overrideConnectionStates(ConnectionState::START_SENT);

  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3,
                        N4,
                        N5);

  // All nodes connect.
  ON_STARTED(filter_version_t{1}, N0);
  ON_STARTED(filter_version_t{1}, N1);
  ON_STARTED(filter_version_t{1}, N2);
  ON_STARTED(filter_version_t{1}, N3);
  ON_STARTED(filter_version_t{1}, N4);
  ON_STARTED(filter_version_t{1}, N5);
  ASSERT_TRUE(state_.connection_healthy);

  // N0 is rebuilding according to event log.
  const lsn_t version = addToRebuildingSet(N0);
  // N0 is now AUTHORITATIVE_EMPTY.
  onAllDonorsFinishRebuilding(version);
  ASSERT_TRUE(state_.connection_healthy);

  // Now... even though the event log says that N0 is AUTHORITATIVE_EMPTY,
  // ClientReadStream should act as if it's FULLY_AUTHORITATIVE because it has a
  // connection to it.
  //
  // If N0, N1, N2, N3, N4 send a record, then we should have an f-majority and
  // make progress.

  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE((bool)timer->getCallback());

  onDataRecord(N0, mockRecord(lsn(1, 42)));
  onDataRecord(N1, mockRecord(lsn(1, 43)));
  onDataRecord(N2, mockRecord(lsn(1, 39)));
  onDataRecord(N3, mockRecord(lsn(1, 59)));
  onDataRecord(N4, mockRecord(lsn(1, 49)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // The grace period should be active.
  ASSERT_TRUE(timer->isActive());
  timer->getCallback()();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, start_lsn_, lsn(1, 38)});
}

// start reading from esn 2, got a gap whose right end falls inside sequencer's
// sliding window
TEST_P(ClientReadStreamTest, DoNotReportDatalossBeginningOfEpoch) {
  state_.shards.resize(2);
  replication_factor_ = 1;
  buffer_size_ = 1024;
  sequencer_window_size_ = 32;
  start_lsn_ = lsn(2, 2);
  start();

  onGap(N0, mockGap(N0, lsn(2, 2), lsn(2, 23)));
  onGap(N1, mockGap(N1, lsn(2, 2), lsn(2, 17)));

  // even the two ends of gap are in the same epoch, this gap is a BRIDGE
  // instead of DATALOSS
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(2, 2), lsn(2, 17)});

  // however, the next potential dataloss gap will be delivered as DATALOSS,
  // even it is still within the first sequencer window
  onGap(N1, mockGap(N1, lsn(2, 18), lsn(2, 23)));

  // even the two ends of gap are in the same epoch, this gap is a BRIDGE
  // instead of DATALOSS
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(2, 18), lsn(2, 23)});
}

// similar to the previous test, but report dataloss since the right end of
// gap is outside of the first window
TEST_P(ClientReadStreamTest, ReportDatalossBeginningOfEpochOutsideWindow) {
  state_.shards.resize(2);
  replication_factor_ = 1;
  buffer_size_ = 1024;
  sequencer_window_size_ = 32;
  start_lsn_ = lsn(2, 2);
  start();

  onGap(N0, mockGap(N0, lsn(2, 2), lsn(2, 65)));
  onGap(N1, mockGap(N1, lsn(2, 2), lsn(2, 34)));
  // even the two ends of gap are in the same epoch, this gap is a BRIDGE
  // instead of DATALOSS
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(2, 2), lsn(2, 34)});
}

// Grace period test. There are 4 nodes, N0 and N1 in region 1, N1 and N2 in
// region 2. Replication factor is 2 and records are written with REGION scope.
TEST_P(ClientReadStreamTest, GracePeriod) {
  state_.settings.grace_counter_limit = 2;
  state_.shards.resize(4);
  node_locations_[N0.node()] = "region1.dc1...";
  node_locations_[N1.node()] = "region1.dc2...";
  node_locations_[N2.node()] = "region2.dc1...";
  node_locations_[N3.node()] = "region2.dc2...";
  buffer_size_ = 4096;
  replication_factor_ = 2;
  sync_replication_scope_ = NodeLocationScope::REGION;
  start();

  // mock grace period timer
  MockBackoffTimer* const timer =
      dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer());
  ASSERT_NE(nullptr, timer);
  ASSERT_TRUE((bool)timer->getCallback());

  // record 1 from N0, N2, gap from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  onGap(N1, mockGap(N1, lsn(1, 0), lsn(1, 1)));
  onGap(N3, mockGap(N3, lsn(1, 0), lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1))

  // record 3 from N0, N3; record 4 from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N3, mockRecord(lsn(1, 4)));

  // fire grace period timer. N2 does not chime-in in time. Expect dataloss for
  // LSN 2 and receival of records 3 and 4. Grace counter of N2 is now 1.
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(timer->isActive());
  timer->getCallback()();
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 2), lsn(1, 2)});
  ASSERT_RECV(lsn(1, 3), lsn(1, 4));

  // record 6 from N0, N3; record 7 from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  onDataRecord(N3, mockRecord(lsn(1, 6)));
  onDataRecord(N1, mockRecord(lsn(1, 7)));
  onDataRecord(N3, mockRecord(lsn(1, 7)));

  // fire grace period timer, grace counter of N2 is now 2
  // expect dataloss for LSN 5 and receival of records 6 and 7
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(timer->isActive());
  timer->getCallback()();
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6), lsn(1, 7));

  // record 9 from N0, N3; record 10 from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 9)));
  onDataRecord(N3, mockRecord(lsn(1, 9)));
  onDataRecord(N1, mockRecord(lsn(1, 10)));
  onDataRecord(N3, mockRecord(lsn(1, 10)));

  // fire grace period timer, grace counter of N2 is now 3 (disgraced)
  // expect dataloss for LSN 8 and receival of records 9 and 10
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(timer->isActive());
  timer->getCallback()();
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 8), lsn(1, 8)});
  ASSERT_RECV(lsn(1, 9), lsn(1, 10));

  // record 12 from N0, N3; record 13 from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 12)));
  onDataRecord(N3, mockRecord(lsn(1, 12)));
  onDataRecord(N1, mockRecord(lsn(1, 13)));
  onDataRecord(N3, mockRecord(lsn(1, 13)));

  // N2 is disgraced, expect dataloss for LSN 11 and receival of records 12, 13
  // without even firing the grace period timer
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 11), lsn(1, 11)});
  ASSERT_RECV(lsn(1, 12), lsn(1, 13));

  // record 11 arrives late from N2, expect nothing delivered but grace counter
  // of N2 should have been reset to 0
  onDataRecord(N2, mockRecord(lsn(1, 11)));
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV();

  // record 15 from N0, N3; record 16 from N1, N3
  onDataRecord(N0, mockRecord(lsn(1, 15)));
  onDataRecord(N3, mockRecord(lsn(1, 15)));
  onDataRecord(N1, mockRecord(lsn(1, 16)));
  onDataRecord(N3, mockRecord(lsn(1, 16)));

  // N2 no longer disgraced, so should not see dataloss before timer fires
  ASSERT_TRUE(timer->isActive());
  ASSERT_GAP_MESSAGES();

  // dataloss as soon as timer fires
  timer->getCallback()();
  ASSERT_FALSE(timer->isActive());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 14), lsn(1, 14)});
  ASSERT_RECV(lsn(1, 15), lsn(1, 16));
}

// report data loss gap if all nodes of a nodeset were removed from config
TEST_P(ClientReadStreamTest, EmptyNodeset) {
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));
  // deliver empty nodeset for epochs [3, MAX] with source LAST
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  EPOCH_MAX,
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{ShardID(50, 0)},
                  RecordSource::LAST);
  // ClientReadStream should retry reading metadata log after a timeout
  fireReadMetadataRetryTimer();

  ASSERT_NO_START_MESSAGES();
  ASSERT_STOP_MESSAGES();
  ASSERT_NO_WINDOW_MESSAGES();
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));

  // this time deliver a NOT_LAST metadata for epochs [3, 5]
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{ShardID(50, 0)},
                  RecordSource::NOT_LAST);

  // TODO 16227919: temporarily hide such dataloss with BRIDGE gaps
  // should deliver bridge gap until the end of epoch 3

  // Note: the effective until for this record is epoch 3, which is the
  // effective_since epoch of the record delivered
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, start_lsn_, lsn(3, ESN_MAX.val_)});

  // Because there is no way for the client read stream to get the last
  // released epoch from storage nodes, we have to request metadata epoch
  // by epoch for [4, 5]
  ASSERT_METADATA_REQ(epoch_t(4));
  onEpochMetaData(epoch_t(4),
                  epoch_t(3),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{ShardID(50, 0)},
                  RecordSource::NOT_LAST);
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(4, 0), lsn(4, ESN_MAX.val_)});

  ASSERT_METADATA_REQ(epoch_t(5));
  onEpochMetaData(epoch_t(5),
                  epoch_t(3),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{ShardID(50, 0)},
                  RecordSource::NOT_LAST);
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(5, 0), lsn(5, ESN_MAX.val_)});
}

// all nodes of a nodeset were removed from config, but we happen to know from
// previous epochs that trim point is in even higher epoch;
// expect trim gap, not dataloss
TEST_P(ClientReadStreamTest, EmptyNodesetTrimmed) {
  start_lsn_ = LSN_MIN;
  buffer_size_ = 20;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(lsn_to_epoch(start_lsn_));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(1),
                  epoch_t(1),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1},
                  RecordSource::NOT_LAST);
  ASSERT_START_MESSAGES(start_lsn_,
                        until_lsn_,
                        calc_buffer_max(start_lsn_, buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N1);
  onGap(N1, mockGap(N1, start_lsn_, lsn(4, 10), GapReason::TRIM));
  ASSERT_GAP_MESSAGES();
  ASSERT_METADATA_REQ(epoch_t(2));
  onEpochMetaData(epoch_t(2),
                  epoch_t(2),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{ShardID(50, 0)},
                  RecordSource::NOT_LAST);
  ASSERT_NO_START_MESSAGES();
  ASSERT_STOP_MESSAGES(N1);
  ASSERT_NO_WINDOW_MESSAGES();
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::TRIM, start_lsn_, lsn(3, ESN_MAX.val_)});
  ASSERT_METADATA_REQ(epoch_t(4));
  onEpochMetaData(epoch_t(4),
                  epoch_t(4),
                  EPOCH_MAX,
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N2},
                  RecordSource::LAST);
  ASSERT_START_MESSAGES(lsn(4, 0),
                        until_lsn_,
                        calc_buffer_max(lsn(4, 0), buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N2);
  onDataRecord(N2, mockRecord(lsn(4, 11)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::TRIM, lsn(4, 0), lsn(4, 10)});
  ASSERT_RECV(lsn(4, 11));
}

/////////////////////// Bridge records //////////////////////////////

TEST_P(ClientReadStreamTest, EpochBumpWithBridgeRecord) {
  // 3 nodes, replication factor 1
  state_.shards.resize(3);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 4);
  start();

  onDataRecord(
      N0, mockRecord(lsn(3, 4), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  // expect a bridge gap of [e3n4, e3nESN_MAX]
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(3, 4), lsn(3, ESN_MAX.val_)});

  // window should be moved and re-broadcast to nodes in the read set
  ASSERT_WINDOW_MESSAGES(lsn(4, 0), lsn(4, 0) + buffer_size_ - 1, N0, N1, N2);
}

// bridge gap should stop at until_lsn_ if it is before the end of epoch
TEST_P(ClientReadStreamTest, BridgeToUntilLSN) {
  state_.shards.resize(3);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 4);
  until_lsn_ = lsn(3, 17);
  start();

  onDataRecord(
      N0, mockRecord(lsn(3, 4), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  // expect a bridge gap of [e3n4, e3nESN_MAX]
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 4), until_lsn_});
  ASSERT_NO_WINDOW_MESSAGES();
}

TEST_P(ClientReadStreamTest, BridgeAtEsnMAX) {
  state_.shards.resize(3);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, ESN_MAX.val_ - 1);
  start();

  onDataRecord(N0, mockRecord(start_lsn_));
  ASSERT_RECV(start_lsn_);

  onDataRecord(N1,
               mockRecord(lsn(3, ESN_MAX.val_),
                          RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(3, ESN_MAX.val_), lsn(3, ESN_MAX.val_)});
  ASSERT_NO_WINDOW_MESSAGES();
}

// start reading from before the beginning of the epoch, should deliver a
// bridge gap to the first record of an epoch
TEST_P(ClientReadStreamTest, StartReadingWithEpochBegin) {
  state_.shards.resize(5);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 1);
  start();

  // epoch starts with lsn(3, 7)
  onDataRecord(N0, mockRecord(lsn(3, 9)));
  ASSERT_RECV();

  onDataRecord(N1, mockRecord(lsn(3, 7), RECORD_Header::EPOCH_BEGIN));

  // the read stream should immediately deliver a bridge gap
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(3, 6)});
  ASSERT_RECV(lsn(3, 7));
}

// test the normal case where epoch transitions can happen with both bridge
// record and epoch begin
TEST_P(ClientReadStreamTest, EpochBumpWithBridgeRecordAndEpochBegin) {
  // 3 nodes, replication factor 1
  state_.shards.resize(3);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 4);
  start();

  onDataRecord(N0, mockRecord(start_lsn_));
  ASSERT_RECV(start_lsn_);

  onDataRecord(N1, mockRecord(lsn(3, 7), RECORD_Header::HOLE));
  ASSERT_RECV();

  onDataRecord(
      N2, mockRecord(lsn(3, 5), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  // expect a bridge gap of [e3n5, e3nESN_MAX]
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(3, 5), lsn(3, ESN_MAX.val_)});

  // lsn(3, 7) will never be delivered
  ASSERT_RECV();
  ASSERT_WINDOW_MESSAGES(lsn(4, 0), lsn(4, 0) + buffer_size_ - 1, N0, N1, N2);

  onDataRecord(N0, mockRecord(lsn(4, 6)));
  onDataRecord(N1, mockRecord(lsn(4, 5), RECORD_Header::EPOCH_BEGIN));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(4, 0), lsn(4, 4)});
  ASSERT_RECV(lsn(4, 5), lsn(4, 6));
}

// log recovery may store a hole plug in eXn1 despite that EPOCH_BEGIN may be
// associated with a record with larger ESN. make sure the stream can handle
// that.
TEST_P(ClientReadStreamTest, EsnMINAndEpochBegin) {
  state_.shards.resize(5);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 0);
  start();

  // a hole plug in (3,1) will cause a bridge gap to be immediately delivered
  onDataRecord(N1, mockRecord(lsn(3, 1), RECORD_Header::HOLE));

  // the hole will also be delivered
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(3, 0), lsn(3, 0)},
                      GapMessage{GapType::HOLE, lsn(3, 1), lsn(3, 1)});

  ASSERT_RECV();

  // after that, EPOCH_BEGIN, on the other end, will be treated as a normal
  // record
  onDataRecord(N1, mockRecord(lsn(3, 3), RECORD_Header::EPOCH_BEGIN));

  // will wait for (3, 2)
  ASSERT_RECV();

  onDataRecord(N0, mockRecord(lsn(3, 2), RECORD_Header::HOLE));

  // the hole will also be delivered
  ASSERT_GAP_MESSAGES(GapMessage{GapType::HOLE, lsn(3, 2), lsn(3, 2)});
  ASSERT_RECV(lsn(3, 3));
}

// test the case that bridge record is at eXn1
TEST_P(ClientReadStreamTest, BridgeAtEsnMIN) {
  state_.shards.resize(5);
  buffer_size_ = 1024;
  start_lsn_ = lsn(3, 0);
  start();

  // a hole plug in (3,1) will cause a bridge gap to be immediately delivered
  onDataRecord(
      N1, mockRecord(lsn(3, 1), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(3, 0), lsn(3, 0)},
      GapMessage{GapType::BRIDGE, lsn(3, 1), lsn(3, ESN_MAX.val_)});

  ASSERT_RECV();

  // after that, EPOCH_BEGIN, on the other end, will be treated as a normal
  // record
  onDataRecord(N1, mockRecord(lsn(4, 1), RECORD_Header::EPOCH_BEGIN));

  ASSERT_RECV(lsn(4, 1));
}

// test bridge gap of beginning and end of epoch with requesting epoch metadata
TEST_P(ClientReadStreamTest, BridgeGapWithMetaData) {
  start_lsn_ = lsn(3, 4);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(3));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(3),
                  epoch_t(3),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2});
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  onDataRecord(
      N1, mockRecord(lsn(3, 4), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(3, 4), lsn(3, ESN_MAX.val_)});

  // next_lsn_to_deliver_ is at lsn(4, 0), the read stream should request
  // metadata for epoch 4 so that it can update its window to other nodes
  ASSERT_METADATA_REQ(epoch_t(4));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // window has already been slit at this time
  ASSERT_WINDOW_MESSAGES(
      lsn(4, 0), calc_buffer_max(lsn(4, 0), buffer_size_), N1, N2);

  onEpochMetaData(epoch_t(4),
                  epoch_t(4),
                  epoch_t(4),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1});

  ASSERT_STOP_MESSAGES(N2);
  ASSERT_START_MESSAGES(lsn(4, 0),
                        LSN_MAX,
                        calc_buffer_max(lsn(4, 0), buffer_size_),
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0);

  ASSERT_NO_WINDOW_MESSAGES();
  overrideConnectionStates(ConnectionState::READING);

  // N0 sends a record with EPOCH_BEGIN
  onDataRecord(N0, mockRecord(lsn(4, 17), RECORD_Header::EPOCH_BEGIN));

  ASSERT_METADATA_REQ();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(4, 0), lsn(4, 16)});
  ASSERT_RECV(lsn(4, 17));
}

TEST_P(ClientReadStreamTest, Epoch0IsSkippedWithBridgeGap) {
  start_lsn_ = lsn(0, 1);
  until_lsn_ = lsn(1, 2);
  state_.disable_default_metadata = true;
  start();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(1, 0)});
  ASSERT_METADATA_REQ(epoch_t(1));
  onEpochMetaData(epoch_t(1),
                  epoch_t(1),
                  epoch_t(1),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1});
  onDataRecord(N1, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2));
}

TEST_P(ClientReadStreamTest, Epoch0IsSkippedWithBridgeGapRejectedManyTimes) {
  start_lsn_ = lsn(0, 1);
  until_lsn_ = lsn(1, 2);
  state_.disable_default_metadata = true;

  state_.callbacks_accepting = false;
  start();
  ASSERT_TRUE(state_.connection_healthy);

  auto timer = dynamic_cast<MockBackoffTimer*>(getReattemptStartTimer());
  MockBackoffTimer* stall_grace_timer =
      dynamic_cast<MockBackoffTimer*>(getStallGracePeriodTimer());

  // startContinuation() should have tried to issue a bridge gap to epoch 1
  // Let's kick off the reattempt timer.
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(1, 0)});
  ASSERT_TRUE(timer->isActive());
  timer->trigger();
  ASSERT_TRUE(state_.connection_healthy);
  ASSERT_FALSE(stall_grace_timer->isActive());

  // Again...
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(1, 0)});
  ASSERT_TRUE(timer->isActive());
  timer->trigger();
  ASSERT_TRUE(state_.connection_healthy);
  ASSERT_FALSE(stall_grace_timer->isActive());

  // Again... but this time we accept the gap
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(1, 0)});
  ASSERT_TRUE(timer->isActive());
  state_.callbacks_accepting = true;
  timer->trigger();
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, start_lsn_, lsn(1, 0)});

  // Now the stall grace period should be active since we don't have
  // EpochMetaData yet.
  ASSERT_TRUE(stall_grace_timer->isActive());
  ASSERT_TRUE(state_.connection_healthy);
}

TEST_P(ClientReadStreamTest, S150164_NonauthoritativeGapAndBridgeRecord) {
  start_lsn_ = lsn(2, 9);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(2));
  // request epoch 2, got the last record with effective_since 1
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(1),
                  epoch_t(2),
                  3,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2, N3, N4},
                  RecordSource::LAST);
  overrideConnectionStates(ConnectionState::READING);
  state_.start.clear();
  // should start reading epoch 2 with the unconfirmed nodeset
  onDataRecord(N1, mockRecord(lsn(2, 9)));
  ASSERT_RECV(lsn(2, 9));

  // f-majority of nodes send us gap to the next epoch
  onGap(N0, mockGap(N0, lsn(2, 9), lsn(3, 8)));
  onGap(N2, mockGap(N2, lsn(2, 9), lsn(3, 7)));
  onGap(N3, mockGap(N3, lsn(2, 9), lsn(3, 9)));

  // read stream still wouldn't deliver the bridge gap until epoch metadata
  // for epoch 2 is confirmed. Requesting epoch metadata for epoch 2 again
  ASSERT_METADATA_REQ(epoch_t(2));

  // before the result come back, N1 delivers a (under-replicated) bridge
  // record of epoch 2
  onDataRecord(
      N1, mockRecord(lsn(2, 10), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  // the bridge should advance the read stream to epoch 3, and the read stream
  // should request epoch metadata for epoch 3 despite that it hasn't gotten
  // metadata for epoch 2 yet
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(2, 10), lsn(2, ESN_MAX.val_)});
  ASSERT_METADATA_REQ(epoch_t(3));

  // the code should make sure that the new request will override the previous
  // one and we should never the result of epoch 2
  onEpochMetaData(epoch_t(3),
                  epoch_t(3),
                  epoch_t(3),
                  2,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2},
                  RecordSource::LAST);

  // read stream should start reading epoch 3 normally, let's try a
  // dataloss detection
  onDataRecord(N1, mockRecord(lsn(3, 5)));
  onDataRecord(N2, mockRecord(lsn(3, 7)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(3, 0), lsn(3, 4)});
  ASSERT_RECV(lsn(3, 5));
}

// check that bridge record and EPOCH_BEGIN can prevent read stream from
// failing out of SCD
TEST_P(ClientReadStreamTest, ScdEpochBumpWithBridgeRecords) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(1, 2)));
  onDataRecord(N3, mockRecord(lsn(1, 3)));
  onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));

  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());

  onDataRecord(
      N2, mockRecord(lsn(1, 4), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)});

  // we are still in SCD
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
  // no stream rewinds
  ASSERT_NO_START_MESSAGES();
  // window update should be received.
  ASSERT_WINDOW_MESSAGES(lsn(2, 0), lsn(2, 9), N0, N1, N2, N3);

  onDataRecord(N0, mockRecord(lsn(2, 7), RECORD_Header::EPOCH_BEGIN));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(2, 0), lsn(2, 6)});
  ASSERT_RECV(lsn(2, 7));

  // we are still in SCD
  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());
  ASSERT_NO_START_MESSAGES();
}

TEST_P(ClientReadStreamTest, GracePeriodForStalledReads) {
  replication_factor_ = 2;
  state_.shards.resize(4);
  start();
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1));
  ASSERT_TRUE(state_.connection_healthy);
  read_stream_->onConnectionHealthChange(N0, false);
  read_stream_->onConnectionHealthChange(N1, false);
  ASSERT_TRUE(state_.connection_healthy);
  MockBackoffTimer* timer =
      dynamic_cast<MockBackoffTimer*>(getStallGracePeriodTimer());

  ASSERT_TRUE(timer->isActive());
  ASSERT_TRUE((bool)timer->getCallback());

  // simulate grace period expiring
  timer->getCallback()();
  ASSERT_FALSE(state_.connection_healthy);
  read_stream_->onConnectionHealthChange(N0, true);

  ASSERT_TRUE(state_.connection_healthy);
}

// Check that nodes reporting under-replicated GAPS do not satisfy
// the f-majority for gap detection.
TEST_P(ClientReadStreamTest, UnderReplicatedGapSCD) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  state_.settings.read_stream_guaranteed_delivery_efficiency = true;

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  // Records [1, 4] get delivered.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  // N2 sends UNDER_REPLICATED gap [1, 8]. No rewind yet.
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 8), GapReason::UNDER_REPLICATED));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N0 sends records 6 and 8. Nothing happens, waiting for esn 5.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N3 sends record 5. Records 5 and 6 get delivered. Waiting for esn 7 now.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 5)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV(lsn(1, 5), lsn(1, 6));
  ASSERT_GAP_MESSAGES();

  // N1 sends a gap.
  read_stream_->onGap(N1, mockGap(N1, lsn(1, 5), lsn(1, 12)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N3 sends a record past the gap. We have a complete f-majority but there's
  // still a gap. Rewind.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 9)));

  {
    SCOPED_TRACE("here");
    triggerScheduledRewind();
  }

  ASSERT_START_MESSAGES(lsn(1, 7),
                        LSN_MAX,
                        buffer_max + 6,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // All storage nodes send their gaps/records again.

  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 7), lsn(1, 8), GapReason::UNDER_REPLICATED));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 9)));
  read_stream_->onGap(N1, mockGap(N1, lsn(1, 7), lsn(1, 12)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));

  // Now we have a complete f-majority and can deliver the gap.
  EXPECT_FALSE(rewindScheduled());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 7), lsn(1, 7)});
  ASSERT_RECV(lsn(1, 8), lsn(1, 9));

  // N2 sends a record without under replicated flag. The record gets delivered,
  // and a rewind is scheduled.
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 10)));
  ASSERT_RECV(lsn(1, 10));
  EXPECT_TRUE(rewindScheduled());

  // Before rewind happens, N2 sends another under replicated record.
  // Current implementation of ClientReadStream will proceed with the rewind,
  // will remove N2 from known down anyway, and the rewind will remove all
  // traces of this under replicated record.

  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 12), RECORD_Header::UNDER_REPLICATED_REGION));

  {
    SCOPED_TRACE("here");
    triggerScheduledRewind();
  }

  ASSERT_START_MESSAGES(lsn(1, 11),
                        LSN_MAX,
                        buffer_max + 6,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // N2 sends the record again, but this time without under replicated flag.
  // This can happen if (a) mini-rebuilding just completed, or (b) the higher
  // start LSN in the new START message changed the outcome, or (c)
  // underreplicated region detection is flaky in some way.
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 12)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N0 sends record 11. Records 11 and 12 get delivered.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 11)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_RECV(lsn(1, 11), lsn(1, 12));
  ASSERT_GAP_MESSAGES();
}

// Check that an under-replicated gap immediately after a no-records gap
// excludes the sending node from the f-majority for gap detection.
TEST_P(ClientReadStreamTest, GapBeforeUnderReplicatedGapNoSCD) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = false;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 4)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 4)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 1), lsn(1, 5), GapReason::NO_RECORDS));

  // N3 sends a record past this gap. We now have an authoritative
  // incomplete f-majority for lsn 5. The grace period timer should start.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer()->isActive());

  // N2 sends an adjacent under-replicated gap. This will also promote the
  // gap from N2 that covered lsn 5 to be under-replicated (side effect
  // of the current implementation). The grace period timer should be
  // cancelled.
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 6), lsn(1, 6), GapReason::UNDER_REPLICATED));

  // There is now no f-majority for lsns 5-6: One authoritative gap (N3) and
  // one non-authoritative gap (N2). The grace period timer should no longer
  // be active.
  ASSERT_FALSE(getGracePeriodTimer()->isActive());

  // N1 sends a record for lsn 5. This represents silent under-replication
  // (one of N2 or N3 should have had a copy), but the record should be
  // delivered.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 5)));
  ASSERT_GAP_MESSAGES();
  ASSERT_RECV(lsn(1, 5));

  // Still no f-majority for lsn 6.
  ASSERT_FALSE(getGracePeriodTimer()->isActive());

  // N0 sends a record past the gap. Now an incomplete f-majority
  // for lsn6. No gap, but grace period timer active.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer()->isActive());
}

// Check that an under-replicated gap immediately after a no-records gap
// excludes the sending node from the f-majority for gap detection.
TEST_P(ClientReadStreamTest, GapBeforeUnderReplicatedGapSCD) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  state_.settings.read_stream_guaranteed_delivery_efficiency = true;

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::NO_RECORDS));
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 6), lsn(1, 6), GapReason::UNDER_REPLICATED));
  EXPECT_FALSE(rewindScheduled());

  // N0, N1, N3 send a record past the gap. The last one triggers a rewind.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 7)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  triggerScheduledRewind();

  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Everyone sends the same gaps/records again.

  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::NO_RECORDS));
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 6), lsn(1, 6), GapReason::UNDER_REPLICATED));
  EXPECT_FALSE(rewindScheduled());

  // N3 sends a record past both gaps. We now have an incomplete f-majority
  // for lsn 5, but will not send a gap.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N0 sends a record past the gap. Still an incomplete f-majority
  // for lsn5. No gap.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N1 sends a record past the gap. Now we have a complete f-majority
  // and can deliver the gap.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));
  EXPECT_FALSE(rewindScheduled());
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
}

// Same as UnderRepliatedGap, but the node with under replication issues
// sends a record which implies a gap.
TEST_P(ClientReadStreamTest, UnderReplicatedRecord) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  state_.settings.read_stream_guaranteed_delivery_efficiency = true;

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 6), RECORD_Header::UNDER_REPLICATED_REGION));
  EXPECT_FALSE(rewindScheduled());

  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 6), RECORD_Header::UNDER_REPLICATED_REGION));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 6)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  triggerScheduledRewind();

  // ClientReadStream should have rewound as soon as it received the
  // UNDER_REPLICATED record.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // N2 sends the record again for e1n6
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 6), RECORD_Header::UNDER_REPLICATED_REGION));

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N3 sends a record past the gap. Still no complete f-majority
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N0 sends a record past the gap. Now we have an incomplete f-majority,
  // but will not issue a gap.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // N1 sends a record past the gap. Now we have a complete f-majority
  // and can deliver the gap.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));

  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6));
}

// Verify f-majority behavior in all-send-all mode with a node
// reporting itself as possibly missing records via an UNDER_REPLICATED
// Gap.
TEST_P(ClientReadStreamTest, AllSendAllUnderReplicatedGap) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 9), GapReason::UNDER_REPLICATED));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  triggerScheduledRewind();

  // ClientReadStream should have rewound as soon as it received the
  // UNDER_REPLICATED record from Node 2.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Nodes resend records.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 9), GapReason::UNDER_REPLICATED));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));

  // Still missing lsn 5.
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // checkNeedsFailoverToAllSendAll() should have scheduled a rewind.
  EXPECT_TRUE(rewindScheduled());
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());

  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // Same records received.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));

  // The under replicated record is not enough to get us to
  // an authoritative incomplete f-majority.
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 9), GapReason::UNDER_REPLICATED));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer() == nullptr ||
              !getGracePeriodTimer()->isActive());

  // Once we have an authoritative incomplete f-majority, the grace
  // period timer should start.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_NE(nullptr, getGracePeriodTimer());
  ASSERT_TRUE(getGracePeriodTimer()->isActive());

  // Now non-authoritative but complete so we detect the gap.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // Stream will rewind once more.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{4},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{4}, N0, N1, N2, N3);

  // Same records received again.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 9), GapReason::UNDER_REPLICATED));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer() == nullptr ||
              !getGracePeriodTimer()->isActive());

  // Once we have an authoritative incomplete f-majority, the grace
  // period timer should start again.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_NE(nullptr, getGracePeriodTimer());
  ASSERT_TRUE(getGracePeriodTimer()->isActive());

  // Now non-authoritative but complete so records and gaps can be issued.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6), lsn(1, 7), lsn(1, 8));
}

// Same as AllSendAllUnderReplicateGap, but using an UNDER_REPLICATED_REGION
// record.
TEST_P(ClientReadStreamTest, AllSendAllUnderReplicatedRecord) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));
  ASSERT_GAP_MESSAGES();

  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 9), RECORD_Header::UNDER_REPLICATED_REGION));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  triggerScheduledRewind();

  // ClientReadStream should have rewound as soon as it received the
  // UNDER_REPLICATED record from Node 2.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // Nodes resend records.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 9), RECORD_Header::UNDER_REPLICATED_REGION));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));

  // Still missing lsn 5.
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // checkNeedsFailoverToAllSendAll() should have scheduled a rewind.
  EXPECT_TRUE(rewindScheduled());
  triggerScheduledRewind();
  ASSERT_FALSE(isCurrentlyInSingleCopyDeliveryMode());

  // ClientReadStream should have rewound as soon as it received the
  // UNDER_REPLICATED record from Node 2.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{3}, N0, N1, N2, N3);

  // Same records received.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));

  // The under replicated record is not enough to get us to
  // an authoritative incomplete f-majority.
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 9), RECORD_Header::UNDER_REPLICATED_REGION));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer() == nullptr ||
              !getGracePeriodTimer()->isActive());

  // Once we have an authoritative incomplete f-majority, the grace
  // period timer should start.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_NE(nullptr, getGracePeriodTimer());
  ASSERT_TRUE(getGracePeriodTimer()->isActive());

  // Now non-authoritative but complete so we detect the gap.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // Stream will rewind once more.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{4},
                        false,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{4}, N0, N1, N2, N3);

  // Same records received again.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 9), RECORD_Header::UNDER_REPLICATED_REGION));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_TRUE(getGracePeriodTimer() == nullptr ||
              !getGracePeriodTimer()->isActive());

  // Once we have an authoritative incomplete f-majority, the grace
  // period timer should start again.
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 8)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();
  ASSERT_NE(nullptr, getGracePeriodTimer());
  ASSERT_TRUE(getGracePeriodTimer()->isActive());

  // Now non-authoritative but complete so records and gaps can be issued.
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 7)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6), lsn(1, 7), lsn(1, 8), lsn(1, 9));
}

// Verify that a node is removed from the known down list on the next
// window after it sends records without the UNDER_REPLICATED_REGION
// flag set.
TEST_P(ClientReadStreamTest, UnderReplicatedExitOnWindow) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  state_.settings.read_stream_guaranteed_delivery_efficiency = true;

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 1)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4));

  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::UNDER_REPLICATED));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 6)));
  EXPECT_FALSE(rewindScheduled());
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));

  triggerScheduledRewind();

  // ClientReadStream should have rewound as soon as it received the
  // UNDER_REPLICATED gap.
  ASSERT_START_MESSAGES(lsn(1, 5),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N2},
                        N0,
                        N1,
                        N2,
                        N3);

  // All storage nodes respond to the START message.
  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // All nodes resend.
  read_stream_->onGap(
      N2, mockGap(N2, lsn(1, 5), lsn(1, 5), GapReason::UNDER_REPLICATED));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 6)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 6)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 5), lsn(1, 5)});
  ASSERT_RECV(lsn(1, 6));

  ASSERT_WINDOW_MESSAGES(lsn(1, 7), lsn_t(7 + buffer_max - 1), N0, N1, N2, N3);

  // Issue some more records.
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 9)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 7)));
  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 8), RECORD_Header::UNDER_REPLICATED_REGION));
  ASSERT_RECV(lsn(1, 7), lsn(1, 8), lsn(1, 9));

  // Node 2 returns to fully authoritative behavior.
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 10)));
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 11)));
  ASSERT_RECV(lsn(1, 10), lsn(1, 11));
  triggerScheduledRewind();

  // Stream should rewind with an empty known down list.
  buffer_max = calc_buffer_max(lsn(1, 12), buffer_size_);
  ASSERT_START_MESSAGES(lsn(1, 12),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
}

TEST_P(ClientReadStreamTest, ReproT17673244) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 3;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start_lsn_ = lsn(1, 1);
  state_.disable_default_metadata = true;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_METADATA_REQ(epoch_t(1));
  onEpochMetaData(epoch_t(1),
                  epoch_t(1),
                  epoch_t(1),
                  3,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1, N2, N3},
                  RecordSource::LAST);
  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));

  ASSERT_TRUE(isCurrentlyInSingleCopyDeliveryMode());

  read_stream_->onDataRecord(
      N2, mockRecord(lsn(1, 4), RECORD_Header::HOLE | RECORD_Header::BRIDGE));

  ASSERT_METADATA_REQ(epoch_t(2));

  // This causes ClientReadStreamScd::pending_immediate_scd_failover_ to be
  // populated with N1.
  ON_STARTED_FAILED(filter_version_t{1}, N1);

  // Before pending_immediate_scd_failover_ is used to transform filtered_out,
  // the nodeset changes, N1 is removed from it.
  onEpochMetaData(epoch_t(2),
                  epoch_t(2),
                  epoch_t(2),
                  3,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N2, N3},
                  RecordSource::LAST);

  // A rewind was scheduled, but N1 is not added to known down list since it's
  // not in the nodeset anymore.
  triggerScheduledRewind();

  ASSERT_RECV();
  ASSERT_GAP_MESSAGES(
      GapMessage{GapType::BRIDGE, lsn(1, 4), lsn(1, ESN_MAX.val_)});

  buffer_max = calc_buffer_max(lsn(2, 0), buffer_size_);
  ASSERT_START_MESSAGES(lsn(2, 0),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{},
                        N0,
                        N2,
                        N3);

  ON_STARTED(filter_version_t{2}, N0, N2, N3);

  // a window update should be received.
  ASSERT_WINDOW_MESSAGES(lsn(2, 0), lsn(2, 9), N0, N1, N2, N3);

  read_stream_->onDataRecord(
      N0, mockRecord(lsn(2, 7), RECORD_Header::EPOCH_BEGIN));
}

// The socket to N1 closes, so a rewind is scheduled. During the rewind, we
// cannot send START to one of the nodes so another rewind is scheduled.
// This code exercises a code path where a rewind is scheduled within a rewind.
TEST_P(ClientReadStreamTest, ScdTwoRewindsInARow) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 2;
  scd_enabled_ = true;
  flow_control_threshold_ = 0.5;
  start();

  const lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1,
                        N2,
                        N3);
  ON_STARTED(filter_version_t{1}, N0, N1, N2, N3);

  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 2)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 3)));
  read_stream_->onDataRecord(N2, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 5)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV();
  ASSERT_GAP_MESSAGES();

  // Socket for N1 closes
  auto& cb = *state_.on_close[N1];
  cb(E::PEER_CLOSED, Address(NodeID(N1.node())));

  // Next time we try to send START to N2, it will fail.
  setSendStartErrorForShard(N2, E::NOBUFS);
  triggerScheduledRewind();

  // ClientReadStream should rewind with N1 in the known down list.
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{2},
                        true,
                        small_shardset_t{N1},
                        N0,
                        N1,
                        N3);

  // But because we could not send it to N2, another rewind is scheduled.
  triggerScheduledRewind();

  auto filtered_out = small_shardset_t{N1, N2};
  ASSERT_START_MESSAGES(lsn(1, 4),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{3},
                        true,
                        filtered_out,
                        N0,
                        N1,
                        N3);

  // The stale STARTED messages for version 2 are received but discarded.
  ON_STARTED(filter_version_t{2}, N0, N3);

  ON_STARTED(filter_version_t{3}, N0, N3);

  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 4)));
  read_stream_->onDataRecord(N0, mockRecord(lsn(1, 5)));
  read_stream_->onDataRecord(N3, mockRecord(lsn(1, 6)));
  ASSERT_RECV(lsn(1, 4), lsn(1, 5), lsn(1, 6));
  ASSERT_GAP_MESSAGES();
}

// Simulate situation when initialization of log takes long time
TEST_P(ClientReadStreamTest, NotReady) {
  state_.shards.resize(4);
  buffer_size_ = 10;
  replication_factor_ = 2;
  scd_enabled_ = false;

  // Make it so that all nodes are not ready
  setSendStartErrorForShard(N0, E::NOTREADY);
  setSendStartErrorForShard(N1, E::NOTREADY);
  setSendStartErrorForShard(N2, E::NOTREADY);
  setSendStartErrorForShard(N3, E::NOTREADY);
  start();

  // Now the reconnect timer should be active for all nodes.
  ASSERT_TRUE(reconnectTimerIsActive(N0));
  ASSERT_TRUE(reconnectTimerIsActive(N1));
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  // Trigger the reconnect timer on all nodes.
  reconnectTimerCallback(N0);
  reconnectTimerCallback(N1);
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  ASSERT_TRUE(reconnectTimerIsActive(N0));
  ASSERT_TRUE(reconnectTimerIsActive(N1));
  ASSERT_TRUE(reconnectTimerIsActive(N2));
  ASSERT_TRUE(reconnectTimerIsActive(N3));

  setSendStartErrorForShard(N0, E::OK);
  setSendStartErrorForShard(N1, E::OK);
  setSendStartErrorForShard(N2, E::OK);
  setSendStartErrorForShard(N3, E::OK);

  reconnectTimerCallback(N0);
  reconnectTimerCallback(N1);
  reconnectTimerCallback(N2);
  reconnectTimerCallback(N3);

  ON_STARTED(filter_version_t{2}, N0, N1, N2, N3);

  // This time N2, N3 send records.
  onDataRecord(N3, mockRecord(lsn(1, 1)));
  onDataRecord(N2, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 1)));
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  onDataRecord(N0, mockRecord(lsn(1, 3)));
  onDataRecord(N1, mockRecord(lsn(1, 4)));
  onDataRecord(N1, mockRecord(lsn(1, 5)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3), lsn(1, 4), lsn(1, 5));
}

// test the scenario where last released is bumped after metadata log read
// request is made
TEST_P(ClientReadStreamTest, S148699) {
  epoch_t epoch(5);
  start_lsn_ = lsn(epoch.val_, 1);
  state_.shards.resize(3);
  buffer_size_ = 1024;
  state_.disable_default_metadata = true;
  start();
  ASSERT_METADATA_REQ(epoch_t(5));
  onEpochMetaData(lsn_to_epoch(start_lsn_),
                  epoch_t(5),
                  epoch_t(5),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N0, N1},
                  RecordSource::LAST);
  overrideConnectionStates(ConnectionState::READING);
  onGap(N0, mockGap(N0, lsn(5, 1), lsn(8, 3), GapReason::NO_RECORDS));
  onGap(N1, mockGap(N1, lsn(5, 1), lsn(8, 4), GapReason::NO_RECORDS));
  // should request metadata for epoch 6 while last_releaed epoch is 8
  ASSERT_METADATA_REQ(epoch_t(6));
  onEpochMetaData(epoch_t(6),
                  epoch_t(6),
                  epoch_t(28),
                  1,
                  NodeLocationScope::NODE,
                  StorageSet{N1, N2},
                  RecordSource::NOT_LAST);
  ASSERT_EQ(epoch_t(8), getLastEpochWithMetaData());
  ASSERT_GAP_MESSAGES();

  // set states to READING for new nodes
  overrideConnectionStates(ConnectionState::READING);
  onGap(N2, mockGap(N2, lsn(5, 1), lsn(8, 2), GapReason::NO_RECORDS));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::BRIDGE, lsn(5, 1), lsn(8, 2)});

  onGap(N1, mockGap(N1, lsn(8, 5), lsn(10, 9), GapReason::NO_RECORDS));
  onGap(N2, mockGap(N2, lsn(8, 3), lsn(10, 3), GapReason::NO_RECORDS));
  // should still request metadata for epoch 9 despite that data log is releaesd
  // to epoch 10
  ASSERT_METADATA_REQ(epoch_t(9));
}

/**
 * Simple test for FILTERED_OUT gap. Receive FILTERED_OUT gap from different
 * nodes.
 */
TEST_P(ClientReadStreamTest, SimpleFilteredOut) {
  start();
  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 1), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 1), lsn(1, 1)});
  onGap(N1, mockGap(N1, lsn(1, 2), lsn(1, 2), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 2), lsn(1, 2)});
  onGap(N1, mockGap(N1, lsn(1, 3), lsn(1, 3), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 3), lsn(1, 3)});
  onGap(N1, mockGap(N1, lsn(1, 4), lsn(1, 4), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 4), lsn(1, 4)});
  onGap(N0, mockGap(N0, lsn(1, 5), lsn(1, 5), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 5), lsn(1, 5)});
}

/**
 * Simple test for FILTERED_OUT gap mixed with Record messages as well as
 * DATALOSS gap.
 */
TEST_P(ClientReadStreamTest, FilteredOutGapMixedWithOthers) {
  state_.shards.resize(2);
  start();
  onGap(N0, mockGap(N0, lsn(1, 1), lsn(1, 1), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 1), lsn(1, 1)});
  onDataRecord(N1, mockRecord(lsn(1, 2)));
  ASSERT_RECV(lsn(1, 2));
  onGap(N1, mockGap(N1, lsn(1, 3), lsn(1, 5)));
  onGap(N0, mockGap(N0, lsn(1, 3), lsn(1, 5)));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 3), lsn(1, 5)}, );
  onDataRecord(N0, mockRecord(lsn(1, 6)));
  ASSERT_RECV(lsn(1, 6));
  onGap(N0, mockGap(N0, lsn(1, 7), lsn(1, 7), GapReason::FILTERED_OUT));
  ASSERT_GAP_MESSAGES(GapMessage{GapType::FILTERED_OUT, lsn(1, 7), lsn(1, 7)});
}

TEST_P(ClientReadStreamTest, UnderReplicatedTransitionAboveWindow) {
  for (int second_gap_is_above_next_window_too = 0;
       second_gap_is_above_next_window_too < 2;
       ++second_gap_is_above_next_window_too) {
    SCOPED_TRACE(second_gap_is_above_next_window_too);
    state_ = TestState();
    state_.shards.resize(2);
    buffer_size_ = 10;
    replication_factor_ = 2;
    scd_enabled_ = false;
    flow_control_threshold_ = 0.7;
    start();

    lsn_t second_gap_end = second_gap_is_above_next_window_too ? 25 : 15;

    lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

    ASSERT_START_MESSAGES(lsn(1, 1),
                          LSN_MAX,
                          buffer_max,
                          filter_version_t{1},
                          false,
                          small_shardset_t{},
                          N0,
                          N1);
    ON_STARTED(filter_version_t{1}, N0, N1);

    // Send an underreplicated gap to above window, then a non-underreplicated
    // gap entirely above window. Not sure if this can happen in real life, but
    // it at least shouldn't break ClientReadStream.
    read_stream_->onGap(
        N0, mockGap(N0, lsn(1, 1), lsn(1, 11), GapReason::UNDER_REPLICATED));
    read_stream_->onGap(N0, mockGap(N0, lsn(1, 12), lsn(1, second_gap_end)));
    ASSERT_RECV();
    ASSERT_GAP_MESSAGES();
    ASSERT_NO_WINDOW_MESSAGES();

    read_stream_->onDataRecord(N1, mockRecord(lsn(1, 9)));
    ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 1), lsn(1, 8)});
    ASSERT_RECV(lsn(1, 9));
    ASSERT_WINDOW_MESSAGES(lsn(1, 10), lsn(1, 19), N0, N1);
    ASSERT_TRUE(getGracePeriodTimer() == nullptr ||
                !getGracePeriodTimer()->isActive());

    read_stream_->onDataRecord(N1, mockRecord(lsn(1, 13)));
    // Current implementataion ships the data loss as two gaps in this
    // situation, because N0 shipped the gap as two gaps.
    ASSERT_GAP_MESSAGES(GapMessage{GapType::DATALOSS, lsn(1, 10), lsn(1, 11)},
                        GapMessage{GapType::DATALOSS, lsn(1, 12), lsn(1, 12)});
    ASSERT_RECV(lsn(1, 13));
    ASSERT_NO_WINDOW_MESSAGES();
    ASSERT_TRUE(getGracePeriodTimer() != nullptr);
    ASSERT_TRUE(getGracePeriodTimer()->isActive());

    dynamic_cast<MockBackoffTimer*>(getGracePeriodTimer())->getCallback()();
    ASSERT_GAP_MESSAGES(
        GapMessage{GapType::DATALOSS, lsn(1, 14), lsn(1, second_gap_end)});
    ASSERT_RECV();
    if (second_gap_is_above_next_window_too) {
      ASSERT_WINDOW_MESSAGES(
          lsn(1, second_gap_end + 1), lsn(1, second_gap_end + 10), N0, N1);
    } else {
      ASSERT_NO_WINDOW_MESSAGES();
    }
  }
}

TEST_P(ClientReadStreamTest, UnderReplicatedRecord2) {
  state_.shards.resize(2);
  buffer_size_ = 10;
  replication_factor_ = 1;
  scd_enabled_ = true;
  start();

  lsn_t buffer_max = calc_buffer_max(start_lsn_, buffer_size_);

  ASSERT_START_MESSAGES(lsn(1, 1),
                        LSN_MAX,
                        buffer_max,
                        filter_version_t{1},
                        true,
                        small_shardset_t{},
                        N0,
                        N1);
  ON_STARTED(filter_version_t{1}, N0, N1);

  read_stream_->onDataRecord(
      N0, mockRecord(lsn(1, 2), RECORD_Header::UNDER_REPLICATED_REGION));
  read_stream_->onDataRecord(
      N0, mockRecord(lsn(1, 3), RECORD_Header::UNDER_REPLICATED_REGION));
  read_stream_->onGap(N0, mockGap(N0, lsn(1, 4), lsn(1, 6)));
  ASSERT_RECV();
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 1)));
  ASSERT_RECV(lsn(1, 1), lsn(1, 2), lsn(1, 3));
  ASSERT_GAP_MESSAGES();
  read_stream_->onDataRecord(N1, mockRecord(lsn(1, 4)));
  ASSERT_RECV(lsn(1, 4));
  ASSERT_GAP_MESSAGES();
}

}} // namespace facebook::logdevice
