/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

static const size_t kNumNodes{5};
static const size_t kNumShards{2};

namespace {

std::shared_ptr<UpdateableConfig> config;

struct Update {
  lsn_t version;
  EventLogRebuildingSet state;
  std::shared_ptr<EventLogRecord> delta;
};

/**
 * Keeps track of the records being received.
 */
class Subscriber {
 public:
  explicit Subscriber(EventLogStateMachine& m) {
    handle_ = m.subscribe([&](EventLogRebuildingSet state,
                              const EventLogRecord* record,
                              lsn_t lsn) { onUpdate(state, record, lsn); });
  }

  void onUpdate(EventLogRebuildingSet state,
                const EventLogRecord* record,
                lsn_t lsn) {
    Update update;
    update.version = lsn;
    update.state = state;

    if (record) {
      switch (record->getType()) {
        case EventType::SHARD_NEEDS_REBUILD:
          update.delta = copy<SHARD_NEEDS_REBUILD_Event>(lsn, *record);
          break;
        case EventType::SHARD_ABORT_REBUILD:
          update.delta = copy<SHARD_ABORT_REBUILD_Event>(lsn, *record);
          break;
        case EventType::SHARD_IS_REBUILT:
          update.delta = copy<SHARD_IS_REBUILT_Event>(lsn, *record);
          break;
        case EventType::SHARD_ACK_REBUILT:
          update.delta = copy<SHARD_ACK_REBUILT_Event>(lsn, *record);
          break;
        case EventType::SHARD_UNRECOVERABLE:
          update.delta = copy<SHARD_UNRECOVERABLE_Event>(lsn, *record);
          break;
        default:
          ld_check(false);
      }
    }

    updates_.push_back(update);
  }

  // Copies the given record to `data`.
  template <typename T>
  std::shared_ptr<T> copy(lsn_t /*lsn*/, const EventLogRecord& record) {
    const auto ptr = dynamic_cast<const T*>(&record);
    return std::make_shared<T>(*ptr);
  }

  Update retrieveNextUpdate() {
    EXPECT_TRUE(!updates_.empty());
    if (updates_.empty()) {
      return Update{};
    }
    auto u = updates_[0];
    updates_.pop_front();
    return u;
  }

  void assertNoUpdate() {
    ASSERT_TRUE(updates_.empty());
  }

  std::unique_ptr<EventLogStateMachine::SubscriptionHandle> handle_;
  std::deque<Update> updates_;
};

std::shared_ptr<UpdateableConfig> buildConfig() {
  configuration::Nodes nodes;
  for (node_index_t nid = 0; nid < kNumNodes; ++nid) {
    Configuration::Node& node = nodes[nid];
    node.address = Sockaddr("::1", folly::to<std::string>(4440 + nid));
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(kNumShards);
  }
  Configuration::NodesConfig nodes_config(std::move(nodes));
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes_config.getNodes().size(), 2);
  auto config = std::make_shared<UpdateableConfig>();
  config->updateableServerConfig()->update(
      ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
  return config;
}

class EventLogTest;

class MockEventLogStateMachine : public EventLogStateMachine {
 public:
  explicit MockEventLogStateMachine(EventLogTest* owner,
                                    UpdateableSettings<Settings> settings)
      : EventLogStateMachine(settings) {
    config_ = buildConfig();
    owner_ = owner;
    writeDeltaHeader();
  }

  const std::shared_ptr<ServerConfig> getServerConfig() const override {
    return config_->getServerConfig();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  void getDeltaLogTailLSN() override;
  void getSnapshotLogTailLSN() override;
  read_stream_id_t createBasicReadStream(
      logid_t /*logid*/,
      lsn_t /*start_lsn*/,
      lsn_t /*until_lsn*/,
      std::function<bool(std::unique_ptr<DataRecord>&)> /*on_record*/,
      std::function<bool(const GapRecord&)> /*on_gap*/,
      std::function<void(bool)> /*health_cb*/) override {
    return read_stream_id_t{1};
  }
  void resumeReadStream(read_stream_id_t /*id*/) override {}
  void updateWorkerShardStatusMap() override {}
  void publishRebuildingSet() override {}
  void trim() override {}
  void trimNotSnapshotted(lsn_t /*lsn*/) override {}
  void postRequestWithRetrying(std::unique_ptr<Request>& /*rq*/) override {}
  void postAppendRequest(logid_t logid,
                         std::string payload,
                         std::chrono::milliseconds timeout,
                         std::function<void(Status st, lsn_t lsn)> cb) override;
  void activateGracePeriodForFastForward() override;
  void cancelGracePeriodForFastForward() override {}
  bool isGracePeriodForFastForwardActive() override;
  void activateStallGracePeriod() override {}
  void cancelStallGracePeriod() override {}
  void activateConfirmTimer(boost::uuids::uuid /*uuid*/) override {}
  void activateGracePeriodForSnapshotting() override {}
  void postWriteDeltaRequest(
      std::string delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode,
      folly::Optional<lsn_t> base_version) override {
    // directly call writeDelta instead of posting a request
    Parent::writeDelta(
        std::move(delta), std::move(cb), mode, std::move(base_version));
  }

 private:
  std::shared_ptr<UpdateableConfig> config_;
  EventLogTest* owner_;
};

class EventLogTest : public ::testing::TestWithParam<bool> {
 public:
  EventLogTest() {
    dbg::assertOnData = true;
    config_ = buildConfig();
  }

  void SetUp() override {
    settings_ = UpdateableSettings<Settings>();
    settings_updater_ = std::make_unique<SettingsUpdater>();
    settings_updater_->registerSettings(settings_);

    const std::string val = GetParam() ? "true" : "false";
    settings_updater_->setFromAdminCmd("--event-log-snapshot-compression", val);
  }

  void init() {
    evlog_ = std::make_unique<MockEventLogStateMachine>(this, settings_);
    subscriber_ = std::make_unique<Subscriber>(*evlog_);
  }

  // generate a DataRecord object contains the given EventLogRecord in payload
  std::unique_ptr<DataRecord>
  genDeltaRecord(const EventLogRecord& event,
                 lsn_t lsn,
                 boost::uuids::uuid uuid = boost::uuids::uuid()) {
    const int size = event.toPayload(nullptr, 0);
    ld_check(size > 0);
    std::string buf;
    buf.resize(size);
    const int rv = event.toPayload(&buf[0], size);
    ld_check(rv == size);

    EventLogStateMachine::DeltaHeader h;
    std::copy(uuid.begin(), uuid.end(), std::begin(h.uuid));
    std::string delta_payload = evlog_->createDeltaPayload(std::move(buf), h);

    void* malloced = malloc(delta_payload.size());
    memcpy(malloced, &delta_payload[0], delta_payload.size());

    return std::make_unique<DataRecordOwnsPayload>(
        configuration::InternalLogs::EVENT_LOG_DELTAS,
        Payload(malloced, delta_payload.size()),
        lsn,
        std::chrono::milliseconds{100},
        0 // flags
    );
  }

  std::unique_ptr<DataRecord>
  genSnapshotRecord(const EventLogRebuildingSet& set, lsn_t v, lsn_t lsn) {
    auto buf = evlog_->createSnapshotPayload(
        set, v, settings_->rsm_include_read_pointer_in_snapshot);
    void* malloced = malloc(buf.size());
    memcpy(malloced, &buf[0], buf.size());

    return std::make_unique<DataRecordOwnsPayload>(
        configuration::InternalLogs::EVENT_LOG_SNAPSHOTS,
        Payload(malloced, buf.size()),
        lsn,
        std::chrono::milliseconds{100},
        0 // flags
    );
  }

  const std::shared_ptr<ServerConfig> getServerConfig() const {
    return config_->getServerConfig();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  struct DeltaAppendHandle {
    EventLogTest* owner;
    std::unique_ptr<EventLogRecord> delta;
    // Callback that the test must use to confirm the append.
    std::function<void(Status st, lsn_t lsn)> confirm_append_cb;
    // Populated when the state machine confirms the delta.
    folly::Optional<std::pair<Status, lsn_t>> result;
    boost::uuids::uuid uuid;

    // Simulate the append completing.
    void confirmAppend(Status st, lsn_t lsn) {
      ASSERT_NE(confirm_append_cb, nullptr);
      confirm_append_cb(st, lsn);
      confirm_append_cb = nullptr; // Ensure we don't call it twice.
    }

    // Simulate the appended record being received.
    void onReceived(lsn_t lsn) {
      auto p = owner->genDeltaRecord(*delta, lsn, uuid);
      owner->evlog_->onDeltaRecord(p);
      ASSERT_EQ(owner->evlog_->getState().getLastUpdate(),
                owner->evlog_->getVersion());
    }

    void onConfirmTimeout() {
      // Timing out should happen after the append succeeded.
      ld_check(!confirm_append_cb);
      owner->evlog_->onDeltaConfirmationTimeout(uuid);
    }

    void assertCallbackNotCalled() {
      ASSERT_FALSE(result.hasValue()) << error_name(result.value().first) << " "
                                      << lsn_to_string(result.value().second);
    }

    void assertCallbackCalled(Status st, lsn_t lsn) {
      ASSERT_TRUE(result.hasValue());
      ASSERT_EQ(st, result.value().first) << error_name(result.value().first);
      ASSERT_EQ(lsn, result.value().second)
          << lsn_to_string(result.value().second);
    }
  };

  using WriteMode = EventLogStateMachine::WriteMode;

  DeltaAppendHandle
  writeDelta(std::unique_ptr<EventLogRecord> delta,
             WriteMode mode = WriteMode::CONFIRM_APPLIED,
             folly::Optional<lsn_t> base_version = folly::none) {
    DeltaAppendHandle h;
    h.owner = this;
    h.delta = std::move(delta);
    auto cb = [&](Status st, lsn_t lsn, const std::string& /* unused */) {
      h.result = std::make_pair(st, lsn);
    };
    EXPECT_EQ(nullptr, last_posted_append_rq_);
    evlog_->writeDelta(*h.delta, cb, mode, base_version);
    h.confirm_append_cb = std::move(last_posted_append_rq_);
    h.uuid = evlog_->getLastUUID();
    return h;
  }

  UpdateableSettings<Settings> settings_;
  std::unique_ptr<SettingsUpdater> settings_updater_;
  bool use_snapshot_log_{true};

  std::shared_ptr<UpdateableConfig> config_;
  std::unique_ptr<MockEventLogStateMachine> evlog_;
  std::unique_ptr<Subscriber> subscriber_;
  lsn_t delta_log_tail_lsn_{LSN_OLDEST};
  lsn_t snapshot_log_tail_lsn_{LSN_OLDEST};

  // Last callback that was posted by the state machine for an append.
  std::function<void(Status st, lsn_t lsn)> last_posted_append_rq_;

  bool grace_period_for_fast_forward_active_{false};
};

void MockEventLogStateMachine::getDeltaLogTailLSN() {
  onGotDeltaLogTailLSN(E::OK, owner_->delta_log_tail_lsn_);
}

void MockEventLogStateMachine::getSnapshotLogTailLSN() {
  onGotSnapshotLogTailLSN(
      E::OK, owner_->delta_log_tail_lsn_, owner_->snapshot_log_tail_lsn_);
}

void MockEventLogStateMachine::postAppendRequest(
    logid_t /*logid*/,
    std::string /*payload*/,
    std::chrono::milliseconds /*timeout*/,
    std::function<void(Status st, lsn_t lsn)> cb) {
  owner_->last_posted_append_rq_ = std::move(cb);
}

void MockEventLogStateMachine::activateGracePeriodForFastForward() {
  owner_->grace_period_for_fast_forward_active_ = true;
}

bool MockEventLogStateMachine::isGracePeriodForFastForwardActive() {
  return owner_->grace_period_for_fast_forward_active_;
}

} // namespace

#define ASSERT_EVENTS_EQ(expected, got, T)                                \
  {                                                                       \
    ASSERT_EQ(                                                            \
        *dynamic_cast<const T*>(expected), *dynamic_cast<const T*>(got)); \
  }

/**
 * Check that two lists of type EventLogRecordList have the same content.
 */
#define ASSERT_EVENT_LOG_RECORD_LISTS_EQ(expected, got)                   \
  {                                                                       \
    ASSERT_EQ(expected.size(), got.size());                               \
    for (int i = 0; i < expected.size(); ++i) {                           \
      ASSERT_EQ(expected[i].first, got[i].first);                         \
      ASSERT_EQ(expected[i].second->getType(), got[i].second->getType()); \
      switch (expected[i].second->getType()) {                            \
        case EventType::SHARD_NEEDS_REBUILD:                              \
          ASSERT_EVENTS_EQ(expected[i].second.get(),                      \
                           got[i].second.get(),                           \
                           SHARD_NEEDS_REBUILD_Event);                    \
          break;                                                          \
        case EventType::SHARD_IS_REBUILT:                                 \
          ASSERT_EVENTS_EQ(expected[i].second.get(),                      \
                           got[i].second.get(),                           \
                           SHARD_IS_REBUILT_Event);                       \
          break;                                                          \
        case EventType::SHARD_ACK_REBUILT:                                \
          ASSERT_EVENTS_EQ(expected[i].second.get(),                      \
                           got[i].second.get(),                           \
                           SHARD_ACK_REBUILT_Event);                      \
          break;                                                          \
        default:                                                          \
          ld_check(false);                                                \
      }                                                                   \
    }                                                                     \
  }

#define UPDATE(set, version, name, ...)                                      \
  {                                                                          \
    name##_Header h = {__VA_ARGS__};                                         \
    auto e = std::make_shared<name##_Event>(h);                              \
    set.update(                                                              \
        version, std::chrono::milliseconds(), *e, *getNodesConfiguration()); \
  }

#define DELTA(lsn, name, ...)                   \
  {                                             \
    name##_Header h = {__VA_ARGS__};            \
    auto e = std::make_shared<name##_Event>(h); \
    auto p = genDeltaRecord(*e, lsn);           \
    evlog_->onDeltaRecord(p);                   \
  }

#define SNAPSHOT(set, version, lsn)                \
  {                                                \
    auto p = genSnapshotRecord(set, version, lsn); \
    evlog_->onSnapshotRecord(p);                   \
  }

#define DELTA_GAP(type, lo, hi)                                                \
  {                                                                            \
    auto gap = GapRecord{                                                      \
        configuration::InternalLogs::EVENT_LOG_DELTAS, GapType::type, lo, hi}; \
    evlog_->onDeltaGap(gap);                                                   \
  }

#define SNAPSHOT_GAP(type, lo, hi)                                             \
  {                                                                            \
    auto gap = GapRecord{                                                      \
        configuration::InternalLogs::EVENT_LOG_DELTAS, GapType::type, lo, hi}; \
    evlog_->onSnapshotGap(gap);                                                \
  }

#define ASSERT_SHARD_STATUS(set, nid, sid, status)                        \
  {                                                                       \
    const auto nc = buildConfig()                                         \
                        ->getServerConfig()                               \
                        ->getNodesConfigurationFromServerConfigSource();  \
    auto map = set.toShardStatusMap(*nc);                                 \
    EXPECT_EQ(AuthoritativeStatus::status, map.getShardStatus(nid, sid)); \
  }

#define D(name, ...) std::make_unique<name##_Event>(name##_Header{__VA_ARGS__})

// Simply verify that we can read a few deltas in the mode that does not use the
// snapshot log.
TEST_P(EventLogTest, SimpleDeltaLogOnlyNoBacklog) {
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(LSN_OLDEST, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);

  DELTA(lsn_t{2}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{2}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{3}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{3}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{4}, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{4}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  subscriber_->assertNoUpdate();
}

// Similar to the test above but this time there is a backlog of records.
// We verify that that backlog is read, updates in the backlog are aggregated
// and we only notify the subscriber once we finish reading it.
TEST_P(EventLogTest, SimpleDeltaLogOnlyWithBacklog) {
  delta_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  // These 3 updates are in the backlog, the subscriber should not be notified
  // yet.
  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  subscriber_->assertNoUpdate();
  DELTA(lsn_t{2}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  subscriber_->assertNoUpdate();
  DELTA(lsn_t{3}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  subscriber_->assertNoUpdate();

  // This last update concludes reading the backlog, we have our initial state.
  DELTA(lsn_t{4}, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{4}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Now, updates should be delivered as they arrive.

  DELTA(lsn_t{5}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{5}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

TEST_P(EventLogTest, SimpleWithSnapshotLogWithBacklog) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding.
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{33}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});

  // Simulate a gap in the delta log up to lsn 41. The tail is lsn 42 so we
  // should still not deliver anything to subscribers.
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{41});
  subscriber_->assertNoUpdate();

  // Simulate a delta arriving where N3 is added to the rebuilding set.
  // We should now retrieve the initial state where both N2/N3 are rebuilding.
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  DELTA(lsn_t{43}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

TEST_P(EventLogTest, VerifyVersionsInSync) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding.
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{33}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});

  // Simulate a gap in the delta log up to lsn 41. The tail is lsn 42 so we
  // should still not deliver anything to subscribers.
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{41});
  subscriber_->assertNoUpdate();

  // Simulate a delta arriving where N3 is added to the rebuilding set.
  // We should now retrieve the initial state where both N2/N3 are rebuilding.
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();
  ASSERT_EQ(u.state.getLastUpdate(), evlog_->getVersion());

  DELTA(lsn_t{43}, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{3}, lsn_t{2});
  // No update is delivered since the delta fails to apply
  subscriber_->assertNoUpdate();
  ASSERT_EQ(evlog_->getState().getLastUpdate(), evlog_->getVersion());
}

// simulate a scenario where the last snapshot contains all the deltas
// this should result in an EventLogRebuildingSet that has last_update_
// and last_seen_lsn_ properly set. this was not the case in the issue
// found in T18951874.
TEST_P(EventLogTest, SnapshotAtTail) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding and it contains
  // the deltas up to the tail
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{42}, lsn_t{4});

  // simulate the tail delta that should be skipped anyway
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});

  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(u.state.getLastUpdate(), lsn_t{42});
  ASSERT_EQ(u.state.getLastSeenLSN(), lsn_t{42});
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// A new snapshot arrives, making us skip some deltas and fast forward the state
// machine. We verify that ReplicatedStateMachine waits for a grace period
// to give chance for missed deltas to arrive before accepting the snapshot.
// We also verify that if this fast-forward prevents us from confirming a delta
// append, we call the user-provided callback for the write with E::FAILED but
// we do provide the LSN of the appended delta.
TEST_P(EventLogTest, SnapshotFastForward) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding.
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{33}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});

  // Simulate a gap in the delta log up to lsn 41. The tail is lsn 42 so we
  // should still not deliver anything to subscribers.
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{41});
  subscriber_->assertNoUpdate();

  // Simulate a delta arriving where N3 is added to the rebuilding set.
  // We should now retrieve the initial state where both N2/N3 are rebuilding.
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  DELTA(lsn_t{43}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Simulate the user writing a delta with the CONFIRM_APPLIED flag.
  auto append_handle =
      writeDelta(D(SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0}),
                 WriteMode::CONFIRM_APPLIED);
  append_handle.assertCallbackNotCalled();
  append_handle.confirmAppend(E::OK, lsn_t{52});
  append_handle.assertCallbackNotCalled();

  // Create a new snapshot with higher version.
  UPDATE(set, lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  UPDATE(set, lsn_t{43}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  UPDATE(set, lsn_t{52}, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  SNAPSHOT_GAP(HOLE, lsn_t{5}, lsn_t{51});
  subscriber_->assertNoUpdate();

  // ReplicatedStateMachine should not accept the snapshot immediately and
  // instead activate the grace period.
  SNAPSHOT(set, lsn_t{52}, lsn_t{5});
  subscriber_->assertNoUpdate();
  append_handle.assertCallbackNotCalled();
  ASSERT_TRUE(evlog_->isGracePeriodForFastForwardActive());

  // Simulate the grace period completing.
  grace_period_for_fast_forward_active_ = false;

  // Try issuing the snapshot again, this time it should work.
  SNAPSHOT(set, lsn_t{52}, lsn_t{5});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{52}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // The callback for our write should be called with E::FAILED because
  // ReplicatedStateMachine could not determine if the delta was applied (since
  // it fast-forwarded), however the callback does contain the LSN of the delta
  // since the append succeeded.
  append_handle.assertCallbackCalled(E::FAILED, lsn_t{52});

  // Now, a delta that was taken into account in that snapshot arrives, but
  // should be discarded since we already fast forwarded past them thanks to the
  // snapshot.
  DELTA(lsn_t{52}, SHARD_UNRECOVERABLE, node_index_t{1}, uint32_t{0});
  subscriber_->assertNoUpdate();

  // Now, a delta past the last snapshot arrives and should be applied.
  DELTA(lsn_t{53}, SHARD_ABORT_REBUILD, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{53}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// We use the state machine to write a delta with the CONFIRM_APPLIED flag,
// verify that the state machine waits for the delta to be received locally
// before calling the user provided callback.
TEST_P(EventLogTest, WriteDeltaConfirmAppliedSimple) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Let's use the state machine to write a delta.
  auto append_handle =
      writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                 WriteMode::CONFIRM_APPLIED);
  append_handle.assertCallbackNotCalled();

  // Let's simulate the AppendRequest succeeding. Because we used the
  // CONFIRM_APPLIED flag, the delta should still not be confirmed because we
  // have not read it yet.
  append_handle.confirmAppend(E::OK, lsn_t{48});
  append_handle.assertCallbackNotCalled();

  // We see a gap in the delta log, we still have not read our delta so the
  // callback should still not be called.
  DELTA_GAP(BRIDGE, lsn_t{43}, lsn_t{47});
  append_handle.assertCallbackNotCalled();

  // Our delta arrives, we should confirm it now.
  append_handle.onReceived(lsn_t{48});
  append_handle.assertCallbackCalled(E::OK, lsn_t{48});

  // The subscriber should also be notified.
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{48}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// We time out confirming a delta written CONFIRM_APPLIED. However we do receive
// the delta afterwards.
TEST_P(EventLogTest, WriteDeltaConfirmAppliedTimeout) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Let's use the state machine to write a delta.
  auto append_handle =
      writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                 WriteMode::CONFIRM_APPLIED);
  append_handle.assertCallbackNotCalled();

  // Let's simulate the AppendRequest succeeding. Because we used the
  // CONFIRM_APPLIED flag, the delta should still not be confirmed because we
  // have not read it yet.
  append_handle.confirmAppend(E::OK, lsn_t{48});
  append_handle.assertCallbackNotCalled();

  // We time out. The callback should be called with E::TIMEDOUT.
  append_handle.onConfirmTimeout();
  append_handle.assertCallbackCalled(E::TIMEDOUT, lsn_t{48});

  // We are able to read the record after the timeout.
  append_handle.onReceived(lsn_t{48});

  // The subscriber should be notified.
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{48}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// This test verifies that the EventLogStateMachine does not allow
// writing deltas with CONFIRM_APPLIED while syncing the delta log.
TEST_P(EventLogTest, WriteDeltaConfirmAppliedWhileSyncing) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{38}); // we haven't reached the tail yet
  subscriber_->assertNoUpdate();

  // Write a delta. The state machine hasn't reached the tail yet, so it
  // should decline the operation.
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  // Callback should be immediately executed with E::AGAIN status
  h1.assertCallbackCalled(E::AGAIN, LSN_INVALID);

  // Insert a bridge gap to reach the tail
  DELTA_GAP(BRIDGE, lsn_t{39}, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Now write the delta again. since we reached the tail, it should succeed.
  auto h2 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h2.assertCallbackNotCalled();

  // Let's simulate the AppendRequest succeeding.
  h2.confirmAppend(E::OK, lsn_t{48});
  // Callback should only be executed when it reads the delta
  h2.assertCallbackNotCalled();
  // Simulate reading the delta
  h2.onReceived(lsn_t{48});
  h2.assertCallbackCalled(E::OK, lsn_t{48});

  // The subscriber should also be notified.
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{48}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// This test verifies that the EventLogStateMachine does not allow
// writing deltas with CONFIRM_APPLIED when the delta log read stream is
// not healthy.
TEST_P(EventLogTest, WriteDeltaConfirmAppliedWhileUnhealthy) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Write a delta to make sure that it succeeds
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h1.confirmAppend(E::OK, lsn_t{43});
  h1.onReceived(lsn_t{43});
  h1.assertCallbackCalled(E::OK, lsn_t{43});

  // The subscriber should also be notified.
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Now simulate unhealthy read stream
  evlog_->onDeltaLogReadStreamHealthChange(false);

  // Try to write a delta. The state machine should decline the operation.
  auto h2 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  // Callback should be immediately executed with E::AGAIN status
  h2.assertCallbackCalled(E::AGAIN, LSN_INVALID);

  // Update the delta log tail lsn to account for the delta that were written
  delta_log_tail_lsn_ = lsn_t{43};
  // Simulate read stream healthy again
  evlog_->onDeltaLogReadStreamHealthChange(true);

  // When the read stream gets healthy again, the EventLogStateMachine switches
  // to the SYNC_DELTA state. This means the next update is a full state update.
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{2}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_EQ(nullptr, u.delta);

  // Try again to write the delta to validate that we can resume writing
  auto h3 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  // Callback should not called, which indicates the append was accepted.
  h3.assertCallbackNotCalled();

  // Simulate append success
  h3.confirmAppend(E::OK, lsn_t{49});
  h3.assertCallbackNotCalled();

  // Simulate reading the delta
  h3.onReceived(lsn_t{49});
  // success
  h3.assertCallbackCalled(E::OK, lsn_t{49});

  // Now make sure subscribers get the delta
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{49}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// This test verifies that the EventLogStateMachine syncs with the delta log
// when the read stream becomes healthy and does not allow wrinting delta while
// doing so.
TEST_P(EventLogTest, CatchupAfterBeingUnhealthy) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Simulate unhealthy read stream
  evlog_->onDeltaLogReadStreamHealthChange(false);

  // Make sure we keep getting records as we are still tailing at this point
  DELTA(lsn_t{43}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Update the delta log tail lsn to simulate deltas that we haven't received
  delta_log_tail_lsn_ = lsn_t{49};
  // Simulate read stream healthy again
  evlog_->onDeltaLogReadStreamHealthChange(true);

  // We are now syncing deltas again but we are not at the new tail yet
  // so there shouldn't be any updates
  subscriber_->assertNoUpdate();

  DELTA(lsn_t{44}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});

  // Still not at the tail
  subscriber_->assertNoUpdate();

  // Now try to write a delta. it should be declined.
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  // Callback should be immediately executed with E::AGAIN status
  h1.assertCallbackCalled(E::AGAIN, LSN_INVALID);

  // now we get a gap that takes us to the tail
  DELTA_GAP(BRIDGE, lsn_t{45}, lsn_t{49});

  // We reached the new tail. We should the full state update
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{44}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{3}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// This test verifies that if the EventLogStateMachine receives a GAP prior to
// transitioning to unhealthy then healthy, it resyncs with the delta log
// but there nothing to fetch and it transitions to TAILING immediately.
TEST_P(EventLogTest, CatchupAfterGapAndBeingUnhealthy) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Simulate unhealthy read stream
  evlog_->onDeltaLogReadStreamHealthChange(false);

  // Simulate read stream healthy again
  evlog_->onDeltaLogReadStreamHealthChange(true);

  // Now try to write a delta. it should succeed
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);

  // Callback should not called, which indicates the append was accepted.
  h1.assertCallbackNotCalled();

  // Simulate append success
  h1.confirmAppend(E::OK, lsn_t{43});
  h1.assertCallbackNotCalled();

  // Simulate reading the delta
  h1.onReceived(lsn_t{43});
  // success
  h1.assertCallbackCalled(E::OK, lsn_t{43});

  // Due to the health transition, the state machine redelivers the
  // previous state
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_EQ(nullptr, u.delta);
  // and then we should get the newly written delta
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// We write a mix of deltas that use the CONFIRM_APPLIED flag and deltas that
// use the CONFIRM_APPEND_ONLY flag.
TEST_P(EventLogTest, WriteManyDeltas) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Delta 1 - LSN 44
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h1.assertCallbackNotCalled();

  // Delta 2 - This delta is stale and should not be applied. - LSN 45
  auto h2 =
      writeDelta(D(SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t{3}),
                 WriteMode::CONFIRM_APPLIED);
  h2.assertCallbackNotCalled();

  // Delta 3 - LSN 46
  auto h3 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}),
                       WriteMode::CONFIRM_APPEND_ONLY);
  h3.assertCallbackNotCalled();

  // Delta 4 - LSN 47
  auto h4 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h4.assertCallbackNotCalled();

  // NOTE: because we append all these deltas from the same thread, the state
  // machine expects their lsn to be in-order. However, confirmation order can
  // by anything.

  // Delta 4 is appended but not yet confirmed.
  h4.confirmAppend(E::OK, lsn_t{47});
  h4.assertCallbackNotCalled();

  // Delta 3 is appended and confirmed immediately since CONFIRM_APPEND_ONLY was
  // used.
  h3.confirmAppend(E::OK, lsn_t{46});
  h3.assertCallbackCalled(E::OK, lsn_t{46});

  // Delta 1 is appended but not yet confirmed.
  h1.confirmAppend(E::OK, lsn_t{44});
  h1.assertCallbackNotCalled();

  DELTA_GAP(HOLE, lsn_t{42}, lsn_t{43});

  // Delta 1 is read, we can confirm it.
  h1.onReceived(lsn_t{44});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{44}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  h1.assertCallbackCalled(E::OK, lsn_t{44});

  // Delta 2 is appended but not yet confirmed.
  h2.confirmAppend(E::OK, lsn_t{45});
  h2.assertCallbackNotCalled();

  // Delta 2 is read, we can confirm it.
  h2.onReceived(lsn_t{45});
  h2.assertCallbackCalled(E::FAILED, lsn_t{45});
  subscriber_->assertNoUpdate();

  // Delta 3 is read, we already confirmed it since it used CONFIRM_APPEND_ONLY.
  h3.onReceived(lsn_t{46});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{46}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  // Delta 4 is read, we can confirm it.
  h4.onReceived(lsn_t{47});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{47}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  h4.assertCallbackCalled(E::OK, lsn_t{47});
}

// We receive a DATALOSS gap in the delta log. Verify that the state machine
// stalls and does not apply any more deltas until we receive a snapshot with
// higher version. In prod, if this ever happens, we'll expect the oncall to
// manually write a snapshot.
TEST_P(EventLogTest, DATALOSSInDeltaLog) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding.
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{33}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});

  // Simulate a gap in the delta log up to lsn 41. The tail is lsn 42 so we
  // should still not deliver anything to subscribers.
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{41});
  subscriber_->assertNoUpdate();

  // Simulate a delta arriving where N3 is added to the rebuilding set.
  // We should now retrieve the initial state where both N2/N3 are rebuilding.
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Simulate a DATALOSS gap from LSN 43 to LSN 64.
  DELTA_GAP(DATALOSS, lsn_t{43}, lsn_t{64});
  subscriber_->assertNoUpdate();

  // Now we receive a delta with LSN 65, that delta should be discarded because
  // the state machine saw the DATALOSS gap and thus expects a snapshot with
  // version >= 64 before moving forward with the delta log.
  DELTA(lsn_t{65}, SHARD_UNRECOVERABLE, node_index_t{3}, uint32_t{0});
  subscriber_->assertNoUpdate();

  // The oncall manually writes a snapshot to fix the issue. It looks like we
  // lost a delta for SHARD_NEEDS_REBUILD(3, 0).
  UPDATE(set, lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  UPDATE(set, lsn_t{49}, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0});
  SNAPSHOT(set, lsn_t{64}, lsn_t{5});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{64}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Now the delta arrives again and should be accepted.
  DELTA(lsn_t{65}, SHARD_UNRECOVERABLE, node_index_t{3}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{65}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// Many deltas are written with CONFIRM_APPLIED flag, and many of them are even
// read before the AppendRequest completes. Because we uniquely identify each
// delta with a UUID, we can confirm them even before the AppendRequest confirms
// their LSN.
TEST_P(EventLogTest, ManyDeltasReadBeforeAppended) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  EventLogRebuildingSet set;
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{42});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{33}, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // Delta 1 - LSN 44
  auto h1 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h1.assertCallbackNotCalled();

  // Delta 2 - This delta is stale and should not be applied. - LSN 45
  auto h2 =
      writeDelta(D(SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t{3}),
                 WriteMode::CONFIRM_APPLIED);
  h2.assertCallbackNotCalled();

  // Delta 3 - LSN 46
  auto h3 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0}),
                       WriteMode::CONFIRM_APPEND_ONLY);
  h3.assertCallbackNotCalled();

  // Delta 4 - LSN 47
  auto h4 = writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0}),
                       WriteMode::CONFIRM_APPLIED);
  h4.assertCallbackNotCalled();

  DELTA_GAP(HOLE, lsn_t{42}, lsn_t{43});

  // Delta 1 is read, we can confirm it.
  h1.onReceived(lsn_t{44});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{44}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  h1.assertCallbackCalled(E::OK, lsn_t{44});

  // Delta 2 is read.
  h2.onReceived(lsn_t{45});
  subscriber_->assertNoUpdate();
  h2.assertCallbackCalled(E::FAILED, lsn_t{45});

  // Delta 3 is read.
  h3.onReceived(lsn_t{46});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{46}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  // Delta 4 is appended but not yet confirmed.
  h4.confirmAppend(E::OK, lsn_t{47});
  h4.assertCallbackNotCalled();

  // Delta 3 is appended and confirmed immediately since CONFIRM_APPEND_ONLY was
  // used.
  h3.confirmAppend(E::OK, lsn_t{46});
  h3.assertCallbackCalled(E::OK, lsn_t{46});

  // Delta 2 is appended but it was already confirmed since it was read already.
  h2.confirmAppend(E::OK, lsn_t{45});

  // Delta 1 is appended but it was already confirmed since it was read already.
  h1.confirmAppend(E::OK, lsn_t{44});

  // Delta 4 is read, we can confirm it.
  h4.onReceived(lsn_t{47});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{47}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
  h4.assertCallbackCalled(E::OK, lsn_t{47});
}

TEST_P(EventLogTest, ShardIsRebuiltCompat) {
  // this test verifies that the EventLogRecord fromPayload() method can
  // deserialize both the new format (that includes the new flags member)
  // and the old format of the SHARD_IS_REBUILT event.
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(LSN_OLDEST, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);

  // This is the old format, it does not include the flags member...
  struct OLD_SHARD_IS_REBUILT_Header {
    node_index_t donorNodeIdx;
    uint32_t shardIdx;
    lsn_t version;
    std::string describe() const {
      return "OLD_SHARD_IS_REBUILT";
    }
  } __attribute__((__packed__));

  using OLD_SHARD_IS_REBUILT_Event =
      FixedEventLogRecord<OLD_SHARD_IS_REBUILT_Header,
                          EventType::SHARD_IS_REBUILT>;

  static_assert(
      sizeof(OLD_SHARD_IS_REBUILT_Header) != sizeof(SHARD_IS_REBUILT_Header),
      "expecting different size");

  DELTA(lsn_t{2}, SHARD_NEEDS_REBUILD, node_index_t{4}, uint32_t{3});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{2}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{4}, uint32_t{3}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  // ... let's see if the deserializer can deserialize it.
  DELTA(lsn_t{3}, OLD_SHARD_IS_REBUILT, node_index_t{1}, uint32_t{3}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{3}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{4}, uint32_t{3}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{4}, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{3}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{4}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{4}, uint32_t{3}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);
}

// N1 is added to the rebuilding set then rebuilt completely. It becomes
// AUTHORITATIVE_EMPTY. Then, 2 shards N2 and N3 are added to the rebuilding
// set. The rebuilding is non authoritative. Rebuilding completes but the shards
// stay UNAVAILABLE. Both shards are marked unrecoverable which should make them
// transition to AUTHORITATIVE_EMPTY.
TEST_P(EventLogTest, Test) {
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(LSN_OLDEST, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);

  DELTA(lsn_t{2}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{2}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{3}, SHARD_IS_REBUILT, node_index_t{2}, uint32_t{0}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{3}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{4}, SHARD_IS_REBUILT, node_index_t{0}, uint32_t{0}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{4}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{5}, SHARD_IS_REBUILT, node_index_t{3}, uint32_t{0}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{5}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{6}, SHARD_IS_REBUILT, node_index_t{4}, uint32_t{0}, lsn_t{2});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{6}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{7}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{7}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{8}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{8}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{9}, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{9}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{10},
        SHARD_IS_REBUILT,
        node_index_t{0},
        uint32_t{0},
        lsn_t{10},
        SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE);
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{10}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{11},
        SHARD_IS_REBUILT,
        node_index_t{4},
        uint32_t{0},
        lsn_t{10},
        SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE);
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{11}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  DELTA(lsn_t{12}, SHARD_UNRECOVERABLE, node_index_t{3}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{12}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{2}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{3}, uint32_t{0}, AUTHORITATIVE_EMPTY);
  ASSERT_NE(nullptr, u.delta);

  subscriber_->assertNoUpdate();
}

TEST_P(EventLogTest, ShardNeedsRebuildDiscardedBecauseAlreadyRebuilding) {
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(LSN_OLDEST, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);

  // N1 is being rebuilt...
  DELTA(lsn_t{2}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{2}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_NE(nullptr, u.delta);

  // Let's trigger rebuilding of N1 again...
  auto append_handle =
      writeDelta(D(SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0}),
                 WriteMode::CONFIRM_APPLIED);
  append_handle.assertCallbackNotCalled();
  append_handle.confirmAppend(E::OK, lsn_t{3});

  // ... but EventLogStateMachine should reject it because N1 is already
  // rebuilding. It should not notify subscribers of any update.
  append_handle.onReceived(lsn_t{3});
  append_handle.assertCallbackCalled(E::ALREADY, lsn_t{3});
  subscriber_->assertNoUpdate();
}

TEST_P(EventLogTest, ShardNeedsRebuildDiscardedBecauseAlreadyDrained) {
  settings_updater_->setFromCLI({{"event-log-snapshotting", "false"}});
  init();
  evlog_->start();

  DELTA_GAP(BRIDGE, LSN_OLDEST, LSN_OLDEST);
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(LSN_OLDEST, u.version);
  ASSERT_TRUE(u.state.empty());
  ASSERT_EQ(nullptr, u.delta);

  // N1 is being drained...
  DELTA(lsn_t{2},
        SHARD_NEEDS_REBUILD,
        node_index_t{1},
        uint32_t{0},
        {},
        {},
        SHARD_NEEDS_REBUILD_Header::DRAIN);
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{2}, u.version);
  ASSERT_SHARD_STATUS(
      u.state, node_index_t{1}, uint32_t{0}, FULLY_AUTHORITATIVE);
  ASSERT_NE(nullptr, u.delta);

  // Let's trigger a drain of N1 again...
  auto append_handle = writeDelta(D(SHARD_NEEDS_REBUILD,
                                    node_index_t{1},
                                    uint32_t{0},
                                    {},
                                    {},
                                    SHARD_NEEDS_REBUILD_Header::DRAIN),
                                  WriteMode::CONFIRM_APPLIED);
  append_handle.assertCallbackNotCalled();
  append_handle.confirmAppend(E::OK, lsn_t{3});

  // ... but EventLogStateMachine should reject it because N1 is already
  // draining. It should not notify subscribers of any update.
  append_handle.onReceived(lsn_t{3});
  append_handle.assertCallbackCalled(E::ALREADY, lsn_t{3});
  subscriber_->assertNoUpdate();
}

// simulate a case where a delta cannot be applied, and verify that it doesn't
// change the state version.
TEST_P(EventLogTest, ApplyDeltaFailed) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"event-log-snapshotting", "true"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding and it contains
  // the deltas up to the tail
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{42}, lsn_t{4});

  // simulate the tail delta that should be skipped anyway
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});

  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(u.state.getLastUpdate(), lsn_t{42});
  ASSERT_EQ(u.state.getLastSeenLSN(), lsn_t{42});
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  // read an event that can't be applied becasue there's no matching
  // SHARD_NEEDS_REBUILD
  DELTA(lsn_t{43}, SHARD_UNDRAIN, node_index_t{1}, uint32_t{1});
  subscriber_->assertNoUpdate();
  // make sure the version didn't change
  ASSERT_EQ(evlog_->getVersion(), lsn_t{42});

  // read an event that is applied
  DELTA(lsn_t{44}, SHARD_NEEDS_REBUILD, node_index_t{3}, uint32_t{1});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{44}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{3}, uint32_t{1}, UNAVAILABLE);
  ASSERT_EQ(u.state.getLastUpdate(), lsn_t{44});
  ASSERT_EQ(u.state.getLastSeenLSN(), lsn_t{44});
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
  // make sure the version was bumped
  ASSERT_EQ(evlog_->getVersion(), lsn_t{44});
}

TEST_P(EventLogTest, TimeBasedSnapsho) {
  delta_log_tail_lsn_ = lsn_t{42};
  snapshot_log_tail_lsn_ = lsn_t{4};
  settings_updater_->setFromCLI({{"eventlog-snapshotting-period", "10ms"}});
  init();
  evlog_->start();

  // Simulate a snapshot arriving where N2 is rebuilding.
  EventLogRebuildingSet set;
  UPDATE(set, lsn_t{33}, SHARD_NEEDS_REBUILD, node_index_t{2}, uint32_t{0});
  SNAPSHOT(set, lsn_t{33}, lsn_t{4});

  // Simulate a gap in the delta log up to lsn 41. The tail is lsn 42 so we
  // should still not deliver anything to subscribers.
  DELTA_GAP(BRIDGE, LSN_OLDEST, lsn_t{41});
  subscriber_->assertNoUpdate();

  // Simulate a delta arriving where N3 is added to the rebuilding set.
  // We should now retrieve the initial state where both N2/N3 are rebuilding.
  DELTA(lsn_t{42}, SHARD_NEEDS_REBUILD, node_index_t{1}, uint32_t{0});
  auto u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{42}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNAVAILABLE);
  ASSERT_EQ(nullptr, u.delta);
  subscriber_->assertNoUpdate();

  DELTA(lsn_t{43}, SHARD_UNRECOVERABLE, node_index_t{2}, uint32_t{0});
  u = subscriber_->retrieveNextUpdate();
  ASSERT_EQ(lsn_t{43}, u.version);
  ASSERT_SHARD_STATUS(u.state, node_index_t{1}, uint32_t{0}, UNAVAILABLE);
  ASSERT_SHARD_STATUS(u.state, node_index_t{2}, uint32_t{0}, UNDERREPLICATION);
  ASSERT_NE(nullptr, u.delta);
  subscriber_->assertNoUpdate();
}

// Run all tests with and without snapshot compression.
INSTANTIATE_TEST_CASE_P(T, EventLogTest, ::testing::Values(false, true));

}} // namespace facebook::logdevice
