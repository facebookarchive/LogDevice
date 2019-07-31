/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingCoordinator.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/rebuilding/RebuildingPlan.h"
#include "logdevice/server/rebuilding/ShardRebuildingV1.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

// Convenient shortcuts for writting ShardIDs.
#define N0S1 ShardID(0, 1)
#define N1S1 ShardID(1, 1)
#define N2S1 ShardID(2, 1)
#define N2S3 ShardID(2, 3)
#define N3S1 ShardID(3, 1)
#define N3S3 ShardID(3, 3)
#define N4S1 ShardID(4, 1)

namespace facebook { namespace logdevice {

using ms = std::chrono::milliseconds;

static const NodeID my_node_id{0, 1};

struct Start {
  logid_t logid;
  RebuildingSet rebuilding_set;
  lsn_t until_lsn;
  RecordTimestamp end;
  bool operator==(const Start& other) const {
    auto as_tuple = [](const Start& m) {
      return std::tie(m.logid, m.end, m.rebuilding_set);
    };
    return as_tuple(*this) == as_tuple(other);
  }
  static bool cmp(Start const& a, Start const& b) {
    return a.logid < b.logid;
  }
};

struct DonorProgress {
  uint32_t shard;
  RecordTimestamp next;
  lsn_t version;
  bool operator==(const DonorProgress& other) const {
    auto as_tuple = [](const DonorProgress& m) {
      return std::tie(m.shard, m.next, m.version);
    };
    return as_tuple(*this) == as_tuple(other);
  }
  static bool cmp(DonorProgress const& a, DonorProgress const& b) {
    if (a.shard == b.shard) {
      return a.version < b.version;
    } else {
      return a.shard < b.shard;
    }
  }
};

struct ShardRebuilt {
  uint32_t shard_idx;
  lsn_t version;
  bool operator==(const ShardRebuilt& other) const {
    auto as_tuple = [](const ShardRebuilt& m) {
      return std::tie(m.shard_idx, m.version);
    };
    return as_tuple(*this) == as_tuple(other);
  }
  static bool cmp(ShardRebuilt const& a, ShardRebuilt const& b) {
    if (a.shard_idx == b.shard_idx) {
      return a.version < b.version;
    } else {
      return a.shard_idx < b.shard_idx;
    }
  }
};

struct MoveWindow {
  logid_t logid;
  RecordTimestamp end;
  bool operator==(const MoveWindow& other) const {
    auto as_tuple = [](const MoveWindow& m) {
      return std::tie(m.logid, m.end);
    };
    return as_tuple(*this) == as_tuple(other);
  }
  static bool cmp(MoveWindow const& a, MoveWindow const& b) {
    return a.logid < b.logid;
  }
};

struct ShardNeedsRebuild {
  SHARD_NEEDS_REBUILD_flags_t flags;
  lsn_t cond_version;
};

std::ostream& operator<<(std::ostream& os, const Start& m) {
  os << "Start{";
  os << "logid: " << m.logid.val_ << ", ";
  os << "rebuilding_set: " << m.rebuilding_set.describe() << ", ";
  os << "until_lsn: " << m.until_lsn << ", ";
  os << "ts: " << m.end.toMilliseconds().count();
  os << "}";
  return os;
};

std::ostream& operator<<(std::ostream& os, const MoveWindow& m) {
  os << "MoveWindow{";
  os << "logid: " << m.logid.val_ << ", ";
  os << "end: " << m.end.toMilliseconds().count();
  os << "}";
  return os;
};

std::ostream& operator<<(std::ostream& os, const DonorProgress& m) {
  os << "DonorProgress{";
  os << "shard: " << m.shard << ", ";
  os << "ts: " << m.next.toMilliseconds().count() << ", ";
  os << "version: " << m.version;
  os << "}";
  return os;
};

std::ostream& operator<<(std::ostream& os, const ShardRebuilt& m) {
  os << "ShardRebuilt{";
  os << "shard: " << m.shard_idx << ", ";
  os << "version: " << m.version;
  os << "}";
  return os;
};

/**
 * Keeps track of calls to sendLogRebuildingMoveWindowRequest() and
 * sendStartLogRebuildingRequest().
 */
struct ReceivedMessages {
  std::vector<Start> start;
  std::vector<DonorProgress> donor_progress;
  std::vector<ShardRebuilt> shard_rebuilt;
  std::vector<MoveWindow> move_window;
  std::vector<uint32_t> shard_ack_rebuilt;
  std::vector<logid_t> abort;
  std::vector<logid_t> restart;
  std::unordered_map<uint32_t, ShardNeedsRebuild> shard_needs_rebuild;
  std::unordered_map<uint32_t, lsn_t> abort_for_my_shard;
  std::vector<uint32_t> mark_shard_unrecoverable;

  void clear() {
    start.clear();
    donor_progress.clear();
    shard_rebuilt.clear();
    move_window.clear();
    shard_ack_rebuilt.clear();
    abort.clear();
    restart.clear();
    shard_needs_rebuild.clear();
    abort_for_my_shard.clear();
  }
};

static ReceivedMessages received;
static std::unordered_set<std::pair<logid_t, shard_index_t>>
    restart_timer_activated;
static std::unordered_set<uint32_t> stall_timer_activated;
static std::unordered_set<uint32_t> restart_scheduled;
static bool planning_timer_activated{false};
static thrift::RemoveMaintenancesRequest remove_maintenance;
static std::vector<thrift::MaintenanceDefinition> apply_maintenances;

class MockedRebuildingCoordinator;

/**
 * Inherits from RebuildingCoordinator and keeps
 * track of calls to sendLogRebuildingMoveWindowRequest() and
 * sendStartLogRebuildingRequest().
 */
class MockedShardRebuildingV1 : public ShardRebuildingV1 {
 public:
  using ShardRebuildingV1::ShardRebuildingV1;

  ~MockedShardRebuildingV1() override;

  int getWorkerCount() override {
    return 1;
  }

  bool isPartitionedStore() override {
    return true;
  }

  void sendLogRebuildingMoveWindowRequest(int /*worker_id*/,
                                          logid_t logid,
                                          RecordTimestamp end) override {
    ld_info("Moving window for log %lu on shard %u: %s",
            logid.val_,
            shard_,
            format_time(end).c_str());
    received.move_window.push_back(MoveWindow{logid, end});
  }

  /**
   * Create and send a StartLogRebuildingRequest. Mocked by tests.
   */
  void sendStartLogRebuildingRequest(
      int /*worker_id*/,
      logid_t logid,
      std::shared_ptr<const RebuildingSet> rebuilding_set,
      RebuildingPlan plan,
      RecordTimestamp end,
      lsn_t /*version*/) override {
    ld_info("Rebuilding log %lu on shard %u for nodes %s with plan=%s, end=%s",
            logid.val_,
            shard_,
            rebuilding_set->describe().c_str(),
            plan.toString().c_str(),
            format_time(end).c_str());
    received.start.push_back(Start{logid, *rebuilding_set, plan.untilLSN, end});
  }

  void sendAbortLogRebuildingRequest(int /*worker_id*/,
                                     logid_t logid) override {
    ld_info("Aborting rebuilding of log %lu on shard %u", logid.val_, shard_);
    received.abort.push_back(logid);
  }

  void sendRestartLogRebuildingRequest(int /*unused*/, logid_t logid) override {
    ld_info("Restarting rebuilding of log %lu on shard %u", logid.val_, shard_);
    received.restart.push_back(logid);
  }

  void activateRestartTimerForLog(logid_t logid) override {
    restart_timer_activated.insert(std::make_pair(logid, shard_));
  }

  bool isRestartTimerActiveForLog(logid_t logid) override {
    return restart_timer_activated.count(std::make_pair(logid, shard_));
  }

  void activateStallTimer() override {
    stall_timer_activated.insert(shard_);
  }

  bool isStallTimerActive() override {
    return stall_timer_activated.count(shard_);
  }

  void cancelStallTimer() override {
    ld_check(stall_timer_activated.count(shard_));
    stall_timer_activated.erase(shard_);
  }

  void cancelRestartTimerForLog(logid_t logid) override {
    ld_check(restart_timer_activated.count(std::make_pair(logid, shard_)));
    restart_timer_activated.erase(std::make_pair(logid, shard_));
  }

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;

  MockedRebuildingCoordinator* owner = nullptr;
};

class RebuildingCoordinatorTest;

class MockMaintenanceLogWriter : public MaintenanceLogWriter {
 public:
  explicit MockMaintenanceLogWriter(RebuildingCoordinatorTest* test)
      : MaintenanceLogWriter(nullptr), test_(test) {}

  virtual void writeDelta(
      const MaintenanceDelta& delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      ClusterMaintenanceStateMachine::WriteMode mode =
          ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none) override {
    if (delta.getType() == MaintenanceDelta::Type::apply_maintenances) {
      apply_maintenances = delta.get_apply_maintenances();
    } else {
      remove_maintenance = delta.get_remove_maintenances();
    }
  }

  virtual void writeDelta(std::unique_ptr<MaintenanceDelta> delta) override {
    if (delta->getType() == MaintenanceDelta::Type::apply_maintenances) {
      apply_maintenances = delta->get_apply_maintenances();
    } else {
      remove_maintenance = delta->get_remove_maintenances();
    }
  }

  RebuildingCoordinatorTest* test_;
};

class MockedRebuildingCoordinator : public RebuildingCoordinator {
 public:
  class MockedRebuildingPlanner : public RebuildingPlanner {
   public:
    MockedRebuildingPlanner(
        ParametersPerShard parameters,
        RebuildingSets rebuilding_sets,
        UpdateableSettings<RebuildingSettings> rebuilding_settings,
        std::shared_ptr<UpdateableConfig> config,
        uint32_t max_num_shards,
        bool rebuild_internal_logs,
        Listener* listener,
        MockedRebuildingCoordinator* owner)
        : RebuildingPlanner(parameters,
                            std::move(rebuilding_sets),
                            std::move(rebuilding_settings),
                            std::move(config),
                            max_num_shards,
                            rebuild_internal_logs,
                            listener),
          params_(parameters),
          owner_(owner) {
      for (auto& kv : parameters) {
        auto sidx = kv.first;
        EXPECT_EQ(0, owner_->rebuildingPlanners.count(sidx));
        owner_->rebuildingPlanners[sidx] = this;
      }
    }

    void start() override {}

    ~MockedRebuildingPlanner() override {
      if (!owner_) {
        return;
      }
      for (auto& kv : params_) {
        auto sidx = kv.first;
        EXPECT_EQ(this, owner_->rebuildingPlanners[sidx]);
        owner_->rebuildingPlanners.erase(sidx);
      }
    }

    ParametersPerShard params_;
    MockedRebuildingCoordinator* owner_;
  };

  MockedRebuildingCoordinator(
      const std::shared_ptr<UpdateableConfig>& config,
      shard_size_t num_shards,
      UpdateableSettings<RebuildingSettings> settings,
      UpdateableSettings<AdminServerSettings> admin_settings,
      bool my_shard_has_data_intact,
      DirtyShardMap* dirty_shard_cache)
      : RebuildingCoordinator(config,
                              nullptr,
                              nullptr,
                              settings,
                              admin_settings,
                              nullptr),
        num_shards_(num_shards),
        my_shard_has_data_intact_(my_shard_has_data_intact),
        dirty_shard_cache_(dirty_shard_cache) {}

  ~MockedRebuildingCoordinator() override {
    RebuildingCoordinator::shutdown();

    for (auto& it : rebuildingPlanners) {
      it.second->owner_ = nullptr;
    }
  }

  void wakeUpReadStreams(uint32_t) override {}

  int checkMarkers() override {
    return 0;
  }

  void populateDirtyShardCache(DirtyShardMap& map) override {
    map.clear();
    if (dirty_shard_cache_) {
      map = *dirty_shard_cache_;
    }
  }

  void writeMarkerForShard(uint32_t shard, lsn_t version) override {
    onMarkerWrittenForShard(shard, version, E::OK);
  }

  size_t numShards() override {
    return num_shards_;
  }
  void notifyProcessorShardRebuilt(uint32_t /*shard*/) override {}

  StatsHolder* getStats() override {
    return nullptr;
  }

  void subscribeToEventLog() override {}
  NodeID getMyNodeID() override {
    return my_node_id;
  }

  std::shared_ptr<RebuildingPlanner>
  createRebuildingPlanner(RebuildingPlanner::ParametersPerShard params,
                          RebuildingSets rebuildingSets) override {
    return std::make_shared<MockedRebuildingPlanner>(
        std::move(params),
        std::move(rebuildingSets),
        rebuildingSettings_,
        config_,
        numShards(),
        /* rebuilding_internal_logs = */ !rebuildUserLogsOnly_,
        this,
        this);
  }

  std::unique_ptr<ShardRebuildingInterface> createShardRebuilding(
      shard_index_t shard,
      lsn_t version,
      lsn_t restart_version,
      std::shared_ptr<const RebuildingSet> rebuilding_set,
      UpdateableSettings<RebuildingSettings> rebuilding_settings) override {
    auto r = std::make_unique<MockedShardRebuildingV1>(shard,
                                                       version,
                                                       restart_version,
                                                       rebuilding_set,
                                                       nullptr,
                                                       rebuilding_settings,
                                                       config_,
                                                       this);
    r->owner = this;
    return r;
  }

  void notifyShardDonorProgress(uint32_t shard,
                                RecordTimestamp ts,
                                lsn_t version,
                                double progress_estimate) override {
    ld_info(
        "Next timestamp for shard %u is %s", shard, format_time(ts).c_str());
    received.donor_progress.push_back(DonorProgress{shard, ts, version});
  }

  void markMyShardUnrecoverable(uint32_t shard) override {
    received.mark_shard_unrecoverable.push_back(shard);
  }

  bool myShardHasDataIntact(uint32_t /*shard_idx*/) const override {
    return my_shard_has_data_intact_;
  }

  void setDataIntact(bool dataIntact) {
    my_shard_has_data_intact_ = dataIntact;
  }

  void setShardUnrecoverable(uint32_t shard) {
    unrecoverable_shards_.insert(shard);
  }

  bool shouldMarkMyShardUnrecoverable(uint32_t shard) const override {
    return unrecoverable_shards_.count(shard);
  }

  void notifyShardRebuilt(uint32_t shard,
                          lsn_t version,
                          bool /*is_authoritative*/) override {
    ld_info("Notified that we rebuilt shard %u", shard);
    received.shard_rebuilt.push_back(ShardRebuilt{shard, version});
  }

  void notifyAckMyShardRebuilt(uint32_t shard, lsn_t /*version*/) override {
    ld_info("Notified that we acknowledge rebuilding of my shard %u", shard);
    received.shard_ack_rebuilt.push_back(shard);
  }

  void restartForMyShard(uint32_t shard,
                         SHARD_NEEDS_REBUILD_flags_t f,
                         RebuildingRangesMetadata* rrm,
                         lsn_t conditional_version) override {
    if (rrm && !rrm->empty()) {
      f |= SHARD_NEEDS_REBUILD_Header::TIME_RANGED;
    }
    received.shard_needs_rebuild[shard] =
        ShardNeedsRebuild{f, conditional_version};
  }

  void abortForMyShard(uint32_t shard,
                       lsn_t version,
                       const EventLogRebuildingSet::NodeInfo*,
                       const char*) override {
    received.abort_for_my_shard[shard] = version;
  }

  bool restartIsScheduledForShard(uint32_t shard_idx) override {
    return restart_scheduled.count(shard_idx);
  }

  void scheduleRestartForShard(uint32_t shard_idx) override {
    restart_scheduled.insert(shard_idx);
  }

  void normalizeTimeRanges(uint32_t, RecordTimeIntervals&) override {}

  std::map<shard_index_t, MockedRebuildingPlanner*> rebuildingPlanners;

  std::map<shard_index_t, MockedShardRebuildingV1*> shardRebuildings;

  void activatePlanningTimer() override {
    planning_timer_activated = true;
  }

 private:
  shard_size_t num_shards_;
  bool my_shard_has_data_intact_;
  std::unordered_set<uint32_t> unrecoverable_shards_;
  DirtyShardMap* dirty_shard_cache_;
};

void MockedShardRebuildingV1::start(
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan) {
  ld_check(!owner->shardRebuildings.count(shard_));
  owner->shardRebuildings[shard_] = this;
  ShardRebuildingV1::start(std::move(plan));
}
MockedShardRebuildingV1::~MockedShardRebuildingV1() {
  // Let the base class shut down log rebuildings while
  // sendAbortLogRebuildingRequest() is still mocked.
  ShardRebuildingV1::destroy();

  ld_check(owner->shardRebuildings.at(shard_) == this);
  owner->shardRebuildings.erase(shard_);
}

/**
 * Test fixture.
 */
class RebuildingCoordinatorTest : public ::testing::Test {
 public:
  RebuildingCoordinatorTest()
      : settings(create_default_settings<RebuildingSettings>()),
        admin_settings_(create_default_settings<AdminServerSettings>()),
        config_(std::make_shared<UpdateableConfig>()),
        rebuilding_set_(std::make_unique<EventLogRebuildingSet>(my_node_id)) {
    settings.local_window = std::chrono::seconds(10);
    settings.global_window = std::chrono::milliseconds::max();
    settings.max_logs_in_flight = 100;
    settings.max_batch_bytes = 100;
    settings.max_records_in_flight = 100;
    settings.use_legacy_log_to_shard_mapping_in_rebuilding = false;
    settings.shard_is_rebuilt_msg_delay = {
        std::chrono::seconds(0), std::chrono::seconds(0)};

    // Clean up possible leftovers from previous test run.
    received.clear();
    restart_timer_activated.clear();
    stall_timer_activated.clear();
    restart_scheduled.clear();

    dbg::assertOnData = true;
  }

  void TearDown() override {
    received = ReceivedMessages{};
  }

  void updateConfig() {
    Configuration::Nodes nodes;
    for (int i = 0; i < num_nodes; ++i) {
      Configuration::Node& node = nodes[i];
      node.address = Sockaddr("::1", folly::to<std::string>(4440 + i));
      node.generation = 1;
      node.addSequencerRole();
      node.addStorageRole(/*num_shards*/ 2);
    }

    auto logs_config = std::make_unique<configuration::LocalLogsConfig>();

    logsconfig::LogAttributes log_attrs;
    log_attrs.set_replicationFactor(3);
    log_attrs.set_scdEnabled(true);

    logs_config->insert(
        boost::icl::right_open_interval<logid_t::raw_type>(1, num_logs + 1),
        "mylogs",
        log_attrs);

    // Event log.
    configuration::InternalLogs internal_logs;
    const auto log_group = internal_logs.insert("event_log_deltas", log_attrs);
    ld_check(log_group);

    Configuration::NodesConfig nodes_config(std::move(nodes));
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);

    config_->updateableServerConfig()->update(
        ServerConfig::fromDataTest(__FILE__, nodes_config, meta_config));
    logs_config->setInternalLogsConfig(internal_logs);
    logs_config->markAsFullyLoaded();
    config_->updateableLogsConfig()->update(std::move(logs_config));
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  void triggerScheduledRestarts() {
    for (shard_index_t s : restart_scheduled) {
      coordinator_->restartForShard(s, *rebuilding_set_);
      coordinator_->executePlanningRequests();
    }
    restart_scheduled.clear();
  }

  void start() {
    updateConfig();

    coordinator_ = std::make_unique<MockedRebuildingCoordinator>(
        config_,
        num_shards,
        UpdateableSettings<RebuildingSettings>(settings),
        UpdateableSettings<AdminServerSettings>(admin_settings_),
        my_shard_has_data_intact,
        &dirty_shard_cache);

    // Make it so that RebuildingCoordinator does not rebuild metadata logs or
    // internal logs, so that tests only need to worry about the user logs they
    // configure themselves.
    coordinator_->rebuildUserLogsOnly();
    coordinator_->start();
    coordinator_->maintenance_log_writer_ =
        std::make_unique<MockMaintenanceLogWriter>(this);
    coordinator_->onUpdate(
        *rebuilding_set_, nullptr, rebuilding_set_->getLastSeenLSN());
    triggerScheduledRestarts();
  }

  void onRebuildingRestarted(node_index_t /*node_idx*/,
                             uint32_t shard_idx,
                             lsn_t version,
                             folly::Optional<lsn_t> until_lsn,
                             bool i_am_a_donor) {
    if (!i_am_a_donor) {
      EXPECT_EQ(0, coordinator_->rebuildingPlanners.count(shard_idx));
      return;
    }

    std::vector<logid_t> logs;
    for (size_t i = 1; i <= num_logs; i++) {
      logs.push_back(logid_t(i));
    }
    if (!logs.empty()) {
      auto* planner = coordinator_->rebuildingPlanners[shard_idx];
      if (!planner) {
        ADD_FAILURE();
      } else {
        ASSERT_GT(planner->parameters_.count(shard_idx), 0);
        EXPECT_EQ(version, planner->parameters_[shard_idx].version);
      }
    }

    if (until_lsn.hasValue()) {
      size_t log1 = shard_idx ? shard_idx : num_shards;
      for (size_t i = log1; i <= num_logs; i += num_shards) {
        onRetrievedPlanForLog(i, shard_idx, until_lsn.value(), version);
      }
      onFinishedRetrievingPlans(shard_idx);
    }
  }

  lsn_t onShardNeedsRebuild(node_index_t node_idx,
                            uint32_t shard_idx,
                            SHARD_NEEDS_REBUILD_flags_t flags,
                            folly::Optional<lsn_t> until_lsn = LSN_MAX,
                            bool i_am_a_donor = true,
                            RebuildingRangesMetadata* rrm = nullptr) {
    SHARD_NEEDS_REBUILD_Header h = {node_idx, shard_idx, "", "", flags};
    auto e = std::make_shared<SHARD_NEEDS_REBUILD_Event>(h, rrm);
    const lsn_t version = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        version, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), version);
      triggerScheduledRestarts();
      onRebuildingRestarted(
          node_idx, shard_idx, version, until_lsn, i_am_a_donor);
    }
    return version;
  }

  lsn_t onShardAbortRebuild(node_index_t node_idx,
                            uint32_t shard_idx,
                            lsn_t version,
                            folly::Optional<lsn_t> until_lsn = LSN_MAX,
                            bool i_am_a_donor = true) {
    SHARD_ABORT_REBUILD_Header h = {node_idx, shard_idx, version};
    auto e = std::make_shared<SHARD_ABORT_REBUILD_Event>(h);
    const lsn_t new_version = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        new_version, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), new_version);
      triggerScheduledRestarts();
      onRebuildingRestarted(
          node_idx, shard_idx, new_version, until_lsn, i_am_a_donor);
    }
    return new_version;
  }

  void onShardIsRebuilt(node_index_t donor_node_idx,
                        uint32_t shard_idx,
                        lsn_t version,
                        bool is_authoritative = true) {
    SHARD_IS_REBUILT_Header h = {
        donor_node_idx,
        shard_idx,
        version,
        is_authoritative ? 0 : SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE};
    auto e = std::make_shared<SHARD_IS_REBUILT_Event>(h);
    const lsn_t lsn = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        lsn, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), lsn);
    }
  }

  void onShardUndrain(node_index_t node_idx, uint32_t shard_idx) {
    SHARD_UNDRAIN_Header h = {node_idx, shard_idx};
    auto e = std::make_shared<SHARD_UNDRAIN_Event>(h);
    const lsn_t lsn = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        lsn, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), lsn);
    }
  }

  void onShardAckRebuilt(node_index_t node_idx,
                         uint32_t shard_idx,
                         lsn_t version) {
    SHARD_ACK_REBUILT_Header h = {node_idx, shard_idx, version};
    auto e = std::make_shared<SHARD_ACK_REBUILT_Event>(h);
    const lsn_t lsn = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        lsn, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), lsn);
    }
  }

  void onShardDonorProgress(node_index_t donor_node_idx,
                            uint32_t shard_idx,
                            int64_t end,
                            lsn_t version) {
    SHARD_DONOR_PROGRESS_Header h = {donor_node_idx, shard_idx, end, version};
    auto e = std::make_shared<SHARD_DONOR_PROGRESS_Event>(h);
    const lsn_t lsn = rebuilding_set_->getLastSeenLSN() + 1;
    rebuilding_set_->update(
        lsn, std::chrono::milliseconds(), *e, *getNodesConfiguration());
    if (coordinator_) {
      coordinator_->onUpdate(*rebuilding_set_, e.get(), lsn);
    }
  }

  void onEventLogTrimmed(lsn_t hi) {
    rebuilding_set_->clear(hi);
    if (coordinator_) {
      coordinator_->onEventLogTrimmed(hi);
    }
  }

  MockedShardRebuildingV1*
  getShard(shard_index_t shard_idx,
           folly::Optional<lsn_t> restart_version = folly::none) {
    ld_check(coordinator_->shardRebuildings.count(shard_idx));
    auto reb = coordinator_->shardRebuildings.at(shard_idx);
    if (restart_version.hasValue()) {
      ld_check_eq(restart_version.value(), reb->getRestartVersion());
    }
    return reb;
  }

  void onLogRebuildingWindowEnd(logid_t::raw_type logid,
                                shard_index_t shard_idx,
                                int64_t ts,
                                lsn_t version,
                                size_t size = 0) {
    getShard(shard_idx, version)
        ->onLogRebuildingWindowEnd(
            logid_t(logid), RecordTimestamp::from(ms(ts)), size);
  }

  void onLogRebuildingComplete(logid_t::raw_type logid,
                               shard_index_t shard_idx,
                               lsn_t version) {
    getShard(shard_idx, version)->onLogRebuildingComplete(logid_t(logid));
  }

  void onLogRebuildingReachedUntilLsn(logid_t::raw_type logid,
                                      lsn_t version,
                                      size_t size) {
    shard_index_t shard_idx =
        getLegacyShardIndexForLog(logid_t(logid), num_shards);
    getShard(shard_idx, version)
        ->onLogRebuildingReachedUntilLsn(logid_t(logid), size);
  }

  void onLogRebuildingSizeUpdate(logid_t::raw_type logid,
                                 lsn_t version,
                                 size_t size) {
    getShard(getLegacyShardIndexForLog(logid_t(logid), num_shards), version)
        ->onLogRebuildingSizeUpdate(logid_t(logid), size);
  }

  void onRetrievedPlanForLog(logid_t::raw_type log,
                             shard_index_t shard_idx,
                             lsn_t until_lsn,
                             lsn_t version) {
    auto plan = std::make_unique<RebuildingPlan>(RecordTimestamp::min());
    auto metadata = std::make_shared<EpochMetaData>(
        StorageSet{N0S1, N1S1, N2S1, N2S3, N3S1, N3S3, N4S1},
        ReplicationProperty(3, NodeLocationScope::NODE));
    plan->addEpochRange(EPOCH_MIN, EPOCH_MAX, std::move(metadata));
    plan->untilLSN = until_lsn;
    coordinator_->onRetrievedPlanForLog(
        logid_t(log), shard_idx, std::move(plan), true, version);
  }

  void onFinishedRetrievingPlans(shard_index_t shard_idx,
                                 lsn_t version = LSN_INVALID) {
    if (version == LSN_INVALID) {
      version = rebuilding_set_->getLastSeenLSN();
    }
    coordinator_->onFinishedRetrievingPlans(shard_idx, version);
  }

  void noteConfigurationChanged() {
    coordinator_->noteConfigurationChanged();
  }

  void setDataIntact(bool dataIntact) {
    coordinator_->setDataIntact(dataIntact);
  }

  void setShardUnrecoverable(uint32_t shard) {
    coordinator_->setShardUnrecoverable(shard);
  }

  void fireRestartTimer(logid_t logid) {
    shard_index_t shard_idx = getLegacyShardIndexForLog(logid, num_shards);
    auto reb = getShard(shard_idx);
    ld_check(reb->isRestartTimerActiveForLog(logid));
    restart_timer_activated.erase(std::make_pair(logid, shard_idx));
    reb->onRestartTimerExpiredForLog(logid);
  }

  void fireStallTimer(uint32_t shard_idx) {
    auto reb = getShard(shard_idx);
    ld_check(reb->isStallTimerActive());
    stall_timer_activated.erase(shard_idx);
    reb->onStallTimerExpired();
  }

  size_t num_nodes = 4;
  shard_size_t num_shards = 2;
  size_t num_logs = 10;
  logid_t event_log{42};
  RebuildingSettings settings;
  AdminServerSettings admin_settings_;
  bool my_shard_has_data_intact{false};
  RebuildingCoordinator::DirtyShardMap dirty_shard_cache;

  std::shared_ptr<UpdateableConfig> config_;
  std::unique_ptr<EventLogRebuildingSet> rebuilding_set_;
  std::unique_ptr<MockedRebuildingCoordinator> coordinator_;
};

/**
 * Useful test macros.
 */

// ... is list of logs, e.g. {1,3,5,7,9}
#define ASSERT_STARTED(nids, ts_end, ...)                          \
  {                                                                \
    std::stable_sort(                                              \
        received.start.begin(), received.start.end(), Start::cmp); \
    std::vector<Start> tmp;                                        \
    for (auto log : {__VA_ARGS__}) {                               \
      tmp.push_back(Start{logid_t(log), nids, LSN_MAX, ts_end});   \
    }                                                              \
    ASSERT_EQ(tmp, received.start);                                \
    received.start.clear();                                        \
  }

#define ASSERT_RESTARTED(...)                                           \
  {                                                                     \
    std::stable_sort(received.restart.begin(), received.restart.end()); \
    std::vector<logid_t> tmp;                                           \
    for (auto log : {__VA_ARGS__}) {                                    \
      tmp.push_back(logid_t(log));                                      \
    }                                                                   \
    ASSERT_EQ(tmp, received.restart);                                   \
    received.restart.clear();                                           \
  }

#define ASSERT_RESTART_TIMER_ACTIVATED(log, shard_idx)      \
  {                                                         \
    restart_timer_activated.count(                          \
        std::make_pair(logid_t(log), uint32_t(shard_idx))); \
  }

#define ASSERT_STALL_TIMER_ACTIVATED(shard) \
  { stall_timer_activated.count(shard); }

// logs is list of (log, until_lsn) pairs, e.g. ({{1, LSN_MAX}, {3, 42}})
// (enclosed in parentheses so that the comma inside braces is not parsed as
//  macro arguments separator)
#define ASSERT_STARTED2(nids, ts_end, logs)                                  \
  {                                                                          \
    std::stable_sort(                                                        \
        received.start.begin(), received.start.end(), Start::cmp);           \
    std::vector<Start> tmp;                                                  \
    for (auto log : std::vector<std::pair<logid_t::raw_type, lsn_t>> logs) { \
      tmp.push_back(Start{logid_t(log.first), nids, log.second, ts_end});    \
    }                                                                        \
    ASSERT_EQ(tmp, received.start);                                          \
    received.start.clear();                                                  \
  }

#define ASSERT_NO_STARTED() \
  { EXPECT_TRUE(received.start.empty()); }

#define ASSERT_MOVE_WINDOW(ts_next, ...)                \
  {                                                     \
    std::stable_sort(received.move_window.begin(),      \
                     received.move_window.end(),        \
                     MoveWindow::cmp);                  \
    std::vector<MoveWindow> tmp;                        \
    for (auto log : {__VA_ARGS__}) {                    \
      tmp.push_back(MoveWindow{logid_t(log), ts_next}); \
    }                                                   \
    ASSERT_EQ(tmp, received.move_window);               \
    received.move_window.clear();                       \
  }

#define ASSERT_NO_MOVE_WINDOW() \
  { EXPECT_TRUE(received.move_window.empty()); }

#define ASSERT_DONOR_PROGRESS(shard, ts_next, version)                     \
  {                                                                        \
    auto s =                                                               \
        DonorProgress{shard, RecordTimestamp::from(ms(ts_next)), version}; \
    std::vector<DonorProgress> tmp;                                        \
    tmp.push_back(s);                                                      \
    ASSERT_EQ(tmp, received.donor_progress);                               \
    received.donor_progress.clear();                                       \
  }

#define ASSERT_NO_DONOR_PROGRESS() \
  { EXPECT_TRUE(received.donor_progress.empty()); }

#define ASSERT_SHARD_REBUILT(shard, version) \
  {                                          \
    auto s = ShardRebuilt{shard, version};   \
    std::vector<ShardRebuilt> tmp;           \
    tmp.push_back(s);                        \
    ASSERT_EQ(tmp, received.shard_rebuilt);  \
    received.shard_rebuilt.clear();          \
  }

#define ASSERT_NO_SHARD_REBUILT() \
  { EXPECT_TRUE(received.shard_rebuilt.empty()); }

#define ASSERT_SHARD_ACK_REBUILT(shard)         \
  {                                             \
    std::vector<uint32_t> tmp;                  \
    tmp.push_back(shard);                       \
    ASSERT_EQ(tmp, received.shard_ack_rebuilt); \
    received.shard_ack_rebuilt.clear();         \
  }

#define ASSERT_NO_SHARD_ACK_REBUILT() \
  { EXPECT_TRUE(received.shard_ack_rebuilt.empty()); }

#define ASSERT_LOG_REBUILDING_ABORTED(...)                          \
  {                                                                 \
    std::stable_sort(received.abort.begin(), received.abort.end()); \
    std::vector<logid_t> tmp;                                       \
    for (uint64_t log : std::vector<uint64_t>({__VA_ARGS__})) {     \
      tmp.push_back(logid_t(log));                                  \
    }                                                               \
    ASSERT_EQ(tmp, received.abort);                                 \
    received.abort.clear();                                         \
  }

#define ASSERT_SHARD_NEEDS_REBUILD(shard, flags_, cond_version_) \
  {                                                              \
    auto it = received.shard_needs_rebuild.find(shard);          \
    ASSERT_TRUE(it != received.shard_needs_rebuild.end());       \
    ASSERT_EQ(it->second.flags, flags_);                         \
    ASSERT_EQ(it->second.cond_version, cond_version_);           \
    received.shard_needs_rebuild.erase(it);                      \
  }

#define ASSERT_ABORT_FOR_MY_SHARD(shard, version)         \
  {                                                       \
    auto it = received.abort_for_my_shard.find(shard);    \
    ASSERT_TRUE(it != received.abort_for_my_shard.end()); \
    ASSERT_EQ(it->second, version);                       \
    received.abort_for_my_shard.erase(it);                \
    ASSERT_TRUE(received.abort_for_my_shard.empty());     \
  }

#define ASSERT_REMOVE_MAINTENANCE(shard)                                  \
  {                                                                       \
    ASSERT_EQ(remove_maintenance.get_user(), maintenance::INTERNAL_USER); \
    auto filter = remove_maintenance.get_filter();                        \
    ASSERT_TRUE(filter.get_group_ids().size() == 1);                      \
    ASSERT_EQ(filter.get_group_ids()[0], toString(shard));                \
  }

#define ASSERT_APPLY_MAINTENANCE(shard)                                      \
  {                                                                          \
    ASSERT_EQ(apply_maintenances.size(), 1);                                 \
    ASSERT_EQ(apply_maintenances[0].get_user(), maintenance::INTERNAL_USER); \
    ASSERT_EQ(apply_maintenances[0].get_shards().size(), 1);                 \
    ASSERT_EQ(apply_maintenances[0].get_shards()[0], shard);                 \
    ASSERT_EQ(apply_maintenances[0].get_shard_target_state(),                \
              ShardOperationalState::DRAINED);                               \
  }
/**
 * Tests.
 */

// Check that we complete rebuilding if there are no logs to rebuild.
TEST_F(RebuildingCoordinatorTest, NoLogsToRebuild) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 0;
  num_shards = 15;

  start();

  // We rebuild shard 6 of node 2. (we are node 0).
  lsn_t version = onShardNeedsRebuild(2, 6, 0 /* flags */, folly::none);
  onFinishedRetrievingPlans(6);
  // But there are no logs on that shard, so rebuilding should complete
  // immediately.
  ASSERT_SHARD_REBUILT(6, version);
}

// All logs are rebuilt at the same time and there is a global timestamp window.
TEST_F(RebuildingCoordinatorTest, GlobalWindowAllLogsSimultaneously) {
  settings.local_window_uses_partition_boundary = false;
  settings.global_window = std::chrono::seconds(30);
  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t version = onShardNeedsRebuild(2, 1, 0 /* flags */);

  // There are 10 logs. We should start rebuilding logs 1, 3, 5, 7, 9.
  // LogRebuilding state machines are instructed to not read records with
  // timestamps less than or equal ms::min(), which means all state machines
  // will simply read the timestamp of the first record and inform us that they
  // reached the end of the window.
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::min(), 1, 3, 5, 7, 9);

  // All LogRebuilding state machines inform of the timestamp of the first
  // record.
  onLogRebuildingWindowEnd(3, 1, 1337, version);
  onLogRebuildingWindowEnd(5, 1, 72, version);
  onLogRebuildingWindowEnd(7, 1, 639, version);
  onLogRebuildingWindowEnd(1, 1, 42, version);
  ASSERT_NO_DONOR_PROGRESS();
  onLogRebuildingWindowEnd(9, 1, 20300, version);

  // RebuildingCoordinator should write to the event log that it reached the end
  // of the local window (which is initially RecordTimestamp::min()) and its
  // next timestamp is 42.
  ASSERT_DONOR_PROGRESS(1, 42, version)

  // Now we wait for all other nodes to inform that they reached the end of the
  // global window as well so that we can slide it.
  // Note that node 2 is not expected to intervene because it's rebuilding in
  // RESTORE mode.
  onShardDonorProgress(0, 1, 42, version); // This is what we wrote
  onShardDonorProgress(1, 1, 53, version);
  ASSERT_NO_MOVE_WINDOW();
  onShardDonorProgress(3, 1, 32, version);

  // Now that we know that all nodes reached the end of their view of the global
  // window, we can slide it and wake up all LogRebuildings that are still in
  // the window (logs 1, 3, 5, 7). Log 9's next timestamp is past the local
  // window.
  auto local_window_end = RecordTimestamp::from(ms(42)) + settings.local_window;
  auto global_window_end =
      RecordTimestamp::from(ms(32)) + settings.global_window;
  EXPECT_EQ(local_window_end, getShard(1)->getLocalWindowEnd());
  EXPECT_EQ(global_window_end, coordinator_->getGlobalWindowEnd(1));
  ASSERT_MOVE_WINDOW(local_window_end, 1, 3, 5, 7);

  // Logs 1, 3, 5, 7 complete rebuilding.
  onLogRebuildingComplete(1, 1, version);
  onLogRebuildingComplete(7, 1, version);
  onLogRebuildingComplete(5, 1, version);
  ASSERT_NO_MOVE_WINDOW();
  onLogRebuildingComplete(3, 1, version);

  // Because log 9 is the last log remaining, we can slide the local window to
  // the very end of the global window.
  ASSERT_MOVE_WINDOW(global_window_end, 9);
  // Also we notify in the event log that our next timestamp is 20300, the next
  // timestamp of the last remaining log 9.
  ASSERT_DONOR_PROGRESS(1, 20300, version);
  onShardDonorProgress(0, 1, 20300, version); // This is what we wrote

  // All other nodes move forward.
  onShardDonorProgress(1, 1, 1000000, version);
  ASSERT_NO_MOVE_WINDOW();
  onShardDonorProgress(3, 1, 1500000, version);

  // Log 9 comes back.
  onLogRebuildingWindowEnd(9, 1, 1000300, version);
  // This causes us to write our next timestamp in the event log.
  ASSERT_DONOR_PROGRESS(1, 1000300, version);
  // Which we then read.
  onShardDonorProgress(0, 1, 1000300, version);
  // We slide the global window and thus the local window.
  local_window_end = RecordTimestamp(ms(1000300) + settings.local_window);
  global_window_end = RecordTimestamp(ms(1000000) + settings.global_window);
  EXPECT_EQ(local_window_end, getShard(1)->getLocalWindowEnd());
  EXPECT_EQ(global_window_end, coordinator_->getGlobalWindowEnd(1));
  // Log 9 is woken up.
  ASSERT_MOVE_WINDOW(local_window_end, 9);

  // Rebuilding completes for log 9.
  onLogRebuildingComplete(9, 1, version);

  // We should notify in the event log that we rebuilt the shard.
  ASSERT_SHARD_REBUILT(1, version);
}

// Same as GlobalWindowAllLogsSimultaneously but there is a second node
// rebuilding the same shard in RELOCATE mode. This code also checks the code
// path where one donor node finishing rebuilding of the shard triggers sliding
// the timestamp windows.
TEST_F(RebuildingCoordinatorTest, GlobalWindowAllLogsSimultaneously2) {
  settings.local_window_uses_partition_boundary = false;
  settings.global_window = std::chrono::seconds(30);
  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);

  // There are 10 logs. We should start rebuilding logs 1, 3, 5, 7, 9.
  // LogRebuilding state machines are instructed to not read records with
  // timestamps less than or equal ms::min(), which means all state machines
  // will simply read the timestamp of the first record and inform us that they
  // reached the end of the window.
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::min(), 1, 3, 5, 7, 9);

  // We also start rebuilding that shard for node 1, in RELOCATE mode which
  // means that this node will still be a donor.
  lsn_t v2 = onShardNeedsRebuild(1, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);

  // All LogRebuilding state machines are aborted and restarted.
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7, 9);
  set.shards.emplace(N1S1, RebuildingNodeInfo(RebuildingMode::RELOCATE));
  ASSERT_STARTED(set, RecordTimestamp::min(), 1, 3, 5, 7, 9);

  // All LogRebuilding state machines inform of the timestamp of the first
  // record.
  onLogRebuildingWindowEnd(3, 1, 1337, v2);
  onLogRebuildingWindowEnd(5, 1, 72, v2);
  onLogRebuildingWindowEnd(7, 1, 639, v2);
  onLogRebuildingWindowEnd(1, 1, 42, v2);
  ASSERT_NO_DONOR_PROGRESS();
  onLogRebuildingWindowEnd(9, 1, 20300, v2);

  // RebuildingCoordinator should write to the event log that it reached the end
  // of the local window (which is initially RecordTimestamp::min()) and its
  // next timestamp is 42.
  ASSERT_DONOR_PROGRESS(1, 42, v2);

  // Now we wait for all other nodes to inform that they reached the end of the
  // global window as well so that we can slide it.
  // Note that node 2 is not expected to intervene because it's rebuilding in
  // RESTORE mode.
  onShardDonorProgress(0, 1, 42, v2); // This is what we wrote
  onShardDonorProgress(1, 1, 53, v2);
  ASSERT_NO_MOVE_WINDOW();
  // Node 3 finishes rebuilding the shard. Here we are checking that the
  // onShardIsRebuilt event triggers sliding of the global window.
  onShardIsRebuilt(3, 1, v2);

  // Now that we know that all nodes reached the end of their view of the global
  // window, we can slide it and wake up all LogRebuildings that are still in
  // the window (logs 1, 3, 5, 7). Log 9's next timestamp is past the local
  // window.
  RecordTimestamp local_window_end(ms(42) + settings.local_window);
  RecordTimestamp global_window_end(ms(42) + settings.global_window);
  EXPECT_EQ(local_window_end, getShard(1)->getLocalWindowEnd());
  EXPECT_EQ(global_window_end, coordinator_->getGlobalWindowEnd(1));
  ASSERT_MOVE_WINDOW(local_window_end, 1, 3, 5, 7);

  // Logs 1, 3, 5, 7 complete rebuilding.
  onLogRebuildingComplete(1, 1, v2);
  onLogRebuildingComplete(7, 1, v2);
  onLogRebuildingComplete(5, 1, v2);
  ASSERT_NO_MOVE_WINDOW();
  onLogRebuildingComplete(3, 1, v2);

  // Because log 9 is the last log remaining, we can slide the local window to
  // the very end of the global window.
  ASSERT_MOVE_WINDOW(global_window_end, 9);
  // Also we notify in the event log that our next timestamp is 20300, the next
  // timestamp of the last remaining log 9.
  ASSERT_DONOR_PROGRESS(1, 20300, v2);
  onShardDonorProgress(0, 1, 20300, v2); // This is what we wrote

  // All other nodes move forward.
  onShardDonorProgress(1, 1, 1000000, v2);
  ASSERT_NO_MOVE_WINDOW();

  // Log 9 comes back.
  onLogRebuildingWindowEnd(9, 1, 1000300, v2);
  // This causes us to write our next timestamp in the event log.
  ASSERT_DONOR_PROGRESS(1, 1000300, v2);
  // Which we then read.
  onShardDonorProgress(0, 1, 1000300, v2);
  // We slide the global window and thus the local window.
  local_window_end = RecordTimestamp(ms(1000300) + settings.local_window);
  global_window_end = RecordTimestamp(ms(1000000) + settings.global_window);
  EXPECT_EQ(local_window_end, getShard(1)->getLocalWindowEnd());
  EXPECT_EQ(global_window_end, coordinator_->getGlobalWindowEnd(1));
  // Log 9 is woken up.
  ASSERT_MOVE_WINDOW(local_window_end, 9);

  // Rebuilding completes for log 9.
  onLogRebuildingComplete(9, 1, v2);

  // We should notify in the event log that we rebuilt the shard.
  ASSERT_SHARD_REBUILT(1, v2);
}

// There is no global timestamp window (RebuildingCoordinator lets
// LogRebuilding state machine make progress as fast as they can) but there is
// a limit on the number of LogRebuilding state machines at a time.
TEST_F(RebuildingCoordinatorTest, NoGlobalWindowAndMaxLogsInFlightLimit) {
  std::mt19937 rng(0xbabababa241ef19e);
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 20;
  num_shards = 2;

  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);

  // But we only start 5 logs because max_logs_in_flight=5.
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1, 3, 5, 7, 9);

  std::vector<uint64_t> to_start = {11, 13, 15, 17, 19};
  std::vector<uint64_t> to_complete = {1, 3, 5, 7, 9};

  while (!to_complete.empty()) {
    int pos =
        std::uniform_int_distribution<int>(0, to_complete.size() - 1)(rng);
    uint64_t log = to_complete[pos];
    // A log completes...
    onLogRebuildingComplete(log, 1, v1);
    to_complete.erase(to_complete.begin() + pos);
    if (!to_start.empty()) {
      // ... and this should trigger starting rebuilding of another log.
      ASSERT_EQ(1, received.start.size());
      auto new_log = received.start[0].logid;
      auto it = std::find(to_start.begin(), to_start.end(), new_log.val_);
      ASSERT_TRUE(it != to_start.end());
      to_start.erase(it);
      ASSERT_STARTED(set, RecordTimestamp::max(), new_log.val_);
      to_complete.push_back(new_log.val_);
    }
  }

  ASSERT_NO_STARTED();
  ASSERT_NO_MOVE_WINDOW();
  ASSERT_SHARD_REBUILT(1, v1);
}

// One of my shards is being rebuilt by the other nodes in the cluster. All I
// have to do is wait until all donors rebuilt it and acknowledge.
TEST_F(RebuildingCoordinatorTest, MyShardAck) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 0. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  ASSERT_NO_STARTED(); // We should not rebuild anything ourselves.
  // All other nodes complete rebuilding of the shard.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  ASSERT_NO_SHARD_ACK_REBUILT();
  onShardIsRebuilt(3, 1, v1);
  // We ack.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// Same as MyShardAck but the same shard is being restored on another node
// as well. Verify that we acknowledge that our shard was rebuilt when all nodes
// minus these two rebuilt.
TEST_F(RebuildingCoordinatorTest, MyShardAck2) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 0. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  ASSERT_NO_STARTED(); // We should not rebuild anything ourselves.
  // But this shard also needs to be rebuilt for node 1.
  lsn_t v2 = onShardNeedsRebuild(1, 1, 0 /* flags */, folly::none, false);
  ASSERT_NO_STARTED(); // We should not rebuild anything ourselves.
  // All other nodes (2, 3) complete rebuilding of the shard.
  onShardIsRebuilt(2, 1, v2);
  ASSERT_NO_SHARD_ACK_REBUILT();
  onShardIsRebuilt(3, 1, v2);
  // We ack.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// Same as MyShardAck2 but with a different order: we first heard that we need
// to rebuild shard 1 on node 1 so we started some log rebuilding state
// machines. As soon as we hear that we should rebuild the same shard for
// ourself, we abort these state machines because we are not supposed to
// participate since we are rebuilding in RESTORE mode.
TEST_F(RebuildingCoordinatorTest, MyShardAck3) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 1 (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  // So we should kick of rebuilding of log 1.
  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);
  // But we start rebuilding the same shard for ourself as well.
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // This causes us to abort the log rebuilding we started.
  ASSERT_LOG_REBUILDING_ABORTED(1);
  // All other nodes (2, 3) complete rebuilding of the shard.
  onShardIsRebuilt(2, 1, v2);
  ASSERT_NO_SHARD_ACK_REBUILT();
  onShardIsRebuilt(3, 1, v2);
  // We ack.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// Same as MyShardAck3 but the second node rebuilding the same shard
// rebuilds in RELOCATE mode so we should wait for it to inform us that it
// participated before doing the ack.
TEST_F(RebuildingCoordinatorTest, MyShardAck4) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 1 (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(1, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  // So we should kick of rebuilding of log 1.
  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);
  // But we start rebuilding the same shard for ourself as well.
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // This causes us to abort the log rebuilding we started.
  ASSERT_LOG_REBUILDING_ABORTED(1);
  // All other nodes (2, 3) complete rebuilding of the shard.
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(3, 1, v2);
  ASSERT_NO_SHARD_ACK_REBUILT();
  // Because the shard is being rebuilt in RELOCATE mode for node 1, node 1 is
  // expected to participate as well. So we (node 0) don't acknowledge until
  // node 1 informs it did its part.
  onShardIsRebuilt(1, 1, v2);
  // We ack.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// Check that local window is not slid if it would make effective window
// smaller than half of local window length.
TEST_F(RebuildingCoordinatorTest, SmallLocalWindow) {
  settings.local_window = RecordTimestamp::duration(100);
  settings.global_window = RecordTimestamp::duration(200);
  settings.local_window_uses_partition_boundary = false;
  num_logs = 1;
  num_shards = 2;
  num_nodes = 3;

  start();
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::min(), 1);

  const uint64_t base_ts = 10000000000l;
  ASSERT_NO_DONOR_PROGRESS();

  onLogRebuildingWindowEnd(1, 1, base_ts, v1);
  ASSERT_NO_MOVE_WINDOW();
  ASSERT_DONOR_PROGRESS(1, base_ts, v1);

  onShardDonorProgress(0, 1, base_ts, v1);
  onShardDonorProgress(2, 1, base_ts - 30, v1);
  ASSERT_NO_DONOR_PROGRESS();
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 100)), 1);

  onLogRebuildingWindowEnd(1, 1, base_ts + 140, v1);
  ASSERT_NO_MOVE_WINDOW();
  ASSERT_DONOR_PROGRESS(1, base_ts + 140, v1);

  onShardDonorProgress(0, 1, base_ts + 140, v1);
  onShardDonorProgress(2, 1, base_ts, v1);
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 200)), 1);
  ASSERT_NO_DONOR_PROGRESS();

  onShardDonorProgress(2, 1, base_ts + 100000, v1);
  ASSERT_NO_DONOR_PROGRESS();

  onLogRebuildingWindowEnd(1, 1, base_ts + 201, v1);
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 301)), 1);
  ASSERT_DONOR_PROGRESS(1, base_ts + 201, v1);

  onLogRebuildingComplete(1, 1, v1);

  onShardDonorProgress(0, 1, base_ts + 201, v1);
  ASSERT_NO_STARTED();
  ASSERT_NO_MOVE_WINDOW();
  ASSERT_NO_DONOR_PROGRESS();
  ASSERT_SHARD_REBUILT(1, v1);
}

TEST_F(RebuildingCoordinatorTest, UntilLsn) {
  settings.local_window = RecordTimestamp::duration(100);
  settings.global_window = RecordTimestamp::duration(100000);
  settings.local_window_uses_partition_boundary = false;
  settings.max_logs_in_flight = 2;
  settings.max_get_seq_state_in_flight = 2;
  num_logs = 12;
  num_shards = 2;
  num_nodes = 3;

  start();
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */, folly::none);
  ASSERT_NO_STARTED();

  onRetrievedPlanForLog(3, 1, 1300, v1);
  onRetrievedPlanForLog(9, 1, 1400, v1);
  onRetrievedPlanForLog(1, 1, 1500, v1);
  onRetrievedPlanForLog(7, 1, 1100, v1);
  onRetrievedPlanForLog(5, 1, 1000, v1);
  onRetrievedPlanForLog(11, 1, 1600, v1);
  ASSERT_NO_STARTED();
  onFinishedRetrievingPlans(1, v1);

  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  // 2000-01-01 00:00:00
  const uint64_t base_ts = 946713600000;

  ASSERT_STARTED2(set, RecordTimestamp::min(), ({{1, 1500}, {3, 1300}}));
  onLogRebuildingComplete(3, 1, v1);
  ASSERT_STARTED2(set, RecordTimestamp::min(), ({{5, 1000}}));
  onLogRebuildingWindowEnd(5, 1, base_ts + 10, v1);
  ASSERT_STARTED2(set, RecordTimestamp::min(), ({{7, 1100}}));
  onLogRebuildingWindowEnd(7, 1, base_ts + 5, v1);
  ASSERT_STARTED2(set, RecordTimestamp::min(), ({{9, 1400}}));
  onLogRebuildingWindowEnd(9, 1, base_ts, v1);
  ASSERT_STARTED2(set, RecordTimestamp::min(), ({{11, 1600}}));
  onLogRebuildingWindowEnd(1, 1, base_ts + 15, v1);
  onShardDonorProgress(2, 1, base_ts + 100000, v1);
  ASSERT_NO_STARTED();
  ASSERT_NO_DONOR_PROGRESS();

  onLogRebuildingComplete(11, 1, v1);
  ASSERT_DONOR_PROGRESS(1, base_ts, v1);
  ASSERT_NO_MOVE_WINDOW();

  onShardDonorProgress(0, 1, base_ts, v1);
  // nextTimestamp-base_ts for logs:
  //  1: 15
  //  3: complete
  //  5: 10
  //  7: 5
  //  9: 0
  //  11: complete
  // Even though logs 7, 9 have the lowest timestamp, we wake up logs 1, 5
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 100)), 1, 5);

  onLogRebuildingWindowEnd(1, 1, base_ts + 510, v1);
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 100)), 7);
  onLogRebuildingComplete(5, 1, v1);
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 100)), 9);
  onLogRebuildingComplete(9, 1, v1);
  ASSERT_NO_MOVE_WINDOW();
  onLogRebuildingWindowEnd(7, 1, base_ts + 500, v1);
  ASSERT_DONOR_PROGRESS(1, base_ts + 500, v1);

  onShardDonorProgress(0, 1, base_ts + 500, v1);
  ASSERT_MOVE_WINDOW(RecordTimestamp(ms(base_ts + 600)), 1, 7);
  onLogRebuildingComplete(1, 1, v1);
  ASSERT_NO_SHARD_REBUILT();
  onLogRebuildingComplete(7, 1, v1);

  ASSERT_NO_STARTED();
  ASSERT_NO_MOVE_WINDOW();
  ASSERT_NO_DONOR_PROGRESS();
  ASSERT_SHARD_REBUILT(1, v1);
}

// Let's imagine that the event log is not trimmed and we read a backlog of
// events. This should cause rebuilding state machines to be started and
// aborted as we receive each message.
TEST_F(RebuildingCoordinatorTest, EventLogBacklog) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);
  onShardIsRebuilt(3, 1, v1);
  onShardIsRebuilt(0, 1, v1);
  // We received on own SHARD_IS_REBUILT message, we should abort LogRebuilding
  // state machines.
  ASSERT_LOG_REBUILDING_ABORTED(1);
  onShardIsRebuilt(1, 1, v1);
  onShardAckRebuilt(2, 1, v1);

  // We rebuild shard 1 of node 3. (we are node 0).
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  RebuildingSet set2({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set2, RecordTimestamp::max(), 1);
  onShardIsRebuilt(0, 1, v2);
  // We received on own SHARD_IS_REBUILT message, we should abort LogRebuilding
  // state machines.
  ASSERT_LOG_REBUILDING_ABORTED(1);
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(1, 1, v2);
  onShardAckRebuilt(3, 1, v2);

  // Our own shard 1 is being rebuilt.
  lsn_t v3 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  ASSERT_NO_STARTED(); // We are in the rebuilding set, so we are not a donor.
  onShardIsRebuilt(3, 1, v3);
  onShardIsRebuilt(1, 1, v3);
  onShardIsRebuilt(2, 1, v3);
  onShardAckRebuilt(0, 1, v3);

  // Receiving SHARD_IS_REBUILT from all donors causes us to send
  // SHARD_ACK_REBUILT event though there already was such a message...
  ASSERT_SHARD_ACK_REBUILT(1);

  // We rebuild shard 1 of node 1. (we are node 0).
  lsn_t v4 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  RebuildingSet set4({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set4, RecordTimestamp::max(), 1);
  onShardIsRebuilt(3, 1, v4);
  onShardIsRebuilt(2, 1, v4);
  onShardIsRebuilt(0, 1, v4);
  // We received on own SHARD_IS_REBUILT message, we should abort LogRebuilding
  // state machines.
  ASSERT_LOG_REBUILDING_ABORTED(1);
  onShardAckRebuilt(1, 1, v4);

  // ... we read the duplicate SHARD_ACK_REBUILT here but it should be
  // discarded.
  onShardAckRebuilt(0, 1, v4);

  ASSERT_NO_SHARD_REBUILT();
  ASSERT_NO_DONOR_PROGRESS();
  ASSERT_NO_SHARD_ACK_REBUILT();
  ASSERT_NO_MOVE_WINDOW();
}

// If all nodes are in the rebuilding set, each node should acknowledge
// rebuilding immediately as there is no donor.
TEST_F(RebuildingCoordinatorTest, AllNodesRebuilding) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  auto set =
      RebuildingSet({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);

  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  set.shards.emplace(N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  ASSERT_LOG_REBUILDING_ABORTED(1);
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);

  lsn_t v3 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  ASSERT_LOG_REBUILDING_ABORTED(1);

  lsn_t v4 = onShardNeedsRebuild(1, 1, 0 /* flags */, LSN_MAX, false);

  // At this point, we realize that all nodes are in the rebuilding set so there
  // is no donor. There is nothing else to do but to acknowledge that our shard
  // was rebuilt.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// We (as a donor) finished rebuilding a shard with rebuilding set S.
// But before the rebuilding was acknowledged another we got SHARD_NEEDS_REBUILD
// for another node N0 (us) of the same shard. The new rebuilding should have
// rebuilding set S+{N0}, not just {N0}.
TEST_F(RebuildingCoordinatorTest, RewindAfterDone) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  ld_info("Requesting rebuilding of N1.");
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  auto set =
      RebuildingSet({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);

  ld_info("Requesting rebuilding of N2.");
  lsn_t v2 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  set.shards.emplace(N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  ASSERT_LOG_REBUILDING_ABORTED(1);
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);

  ld_info("Calling onLogRebuildingComplete() for version 2.");
  onLogRebuildingComplete(1, 1, v2);
  ASSERT_SHARD_REBUILT(1, v2);

  ld_info("Requesting rebuilding of N3.");
  lsn_t v3 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  set.shards.emplace(N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  ASSERT_LOG_REBUILDING_ABORTED();
  ASSERT_STARTED(set, RecordTimestamp::max(), 1);

  ld_info("Calling onShardIsRebuilt() by N0 version 2.");
  onShardIsRebuilt(0, 1, v2);

  ld_info("Calling onShardIsRebuilt() by N1 version 3.");
  onShardIsRebuilt(1, 1, v3);
  ASSERT_NO_STARTED();
  ASSERT_NO_SHARD_REBUILT();

  ld_info("Calling onLogRebuildingComplete() for version 3.");
  onLogRebuildingComplete(1, 1, v3);
  ASSERT_SHARD_REBUILT(1, v3);

  ld_info("Calling onShardIsRebuilt() by N0 version 3.");
  onShardIsRebuilt(0, 1, v3);

  // Rebuilding set should be {0, 1, 2, 3} because the rebuilding of {1, 2, 3}
  // wasn't acknowledged.
  ld_info("Requesting rebuilding of N0.");
  lsn_t v4 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  ASSERT_SHARD_ACK_REBUILT(1);
}

TEST_F(RebuildingCoordinatorTest, Trim) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 4;
  num_shards = 4;
  num_nodes = 4;

  start();

  ld_info("Requesting rebuilding of N1 shard 1.");
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  auto set1 =
      RebuildingSet({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set1, RecordTimestamp::max(), 1);

  ld_info("Requesting rebuilding of N3 shard 3.");
  lsn_t v2 = onShardNeedsRebuild(3, 3, 0 /* flags */);
  auto set3 =
      RebuildingSet({{N3S3, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set3, RecordTimestamp::max(), 3);

  ld_info("Reporting trimmed event log.");
  onEventLogTrimmed(v2 + 1);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3);

  ld_info("Requesting rebuilding of N2 shard 1.");
  lsn_t v3 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  set1 = RebuildingSet({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set1, RecordTimestamp::max(), 1);

  ld_info("Calling onLogRebuildingComplete().");
  onLogRebuildingComplete(1, 1, v3);
  ASSERT_SHARD_REBUILT(1, v3);
}

TEST_F(RebuildingCoordinatorTest, AbortOnLogRemoval) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 8;
  num_shards = 4;
  num_nodes = 4;

  start();

  ld_info("Requesting rebuilding of N3 shard 3.");
  lsn_t v1 = onShardNeedsRebuild(2, 3, 0 /* flags */);
  auto set1 =
      RebuildingSet({{N2S3, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED2(set1, RecordTimestamp::max(), ({{3, LSN_MAX}, {7, LSN_MAX}}));

  ld_info("Requesting rebuilding of N4 shard 1.");
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */, folly::none);

  // Simulate removal of log
  num_logs = 2;
  num_nodes = 4;
  num_shards = 4;
  updateConfig();
  noteConfigurationChanged();

  ASSERT_LOG_REBUILDING_ABORTED(3, 7);

  onRetrievedPlanForLog(1, 1, 100, 2);
  onFinishedRetrievingPlans(1, 2);
  auto set2 =
      RebuildingSet({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED2(set2, RecordTimestamp::max(), ({{1, 100}}));

  ld_info("Calling onLogRebuildingComplete() for shard 1.");
  onLogRebuildingComplete(1, 1, v2);
  ASSERT_SHARD_REBUILT(1, v2);

  ld_info("Calling onLogRebuildingComplete() for shard 3.");
  onLogRebuildingComplete(3, 3, v1);
  onLogRebuildingComplete(7, 3, v1);
  ASSERT_SHARD_REBUILT(3, v1);
}

TEST_F(RebuildingCoordinatorTest, PartitionedWindowSlider) {
  // Vector of vectors of timestamps that getPartitionTimestamps() would return
  using ts_vector = std::vector<std::vector<size_t>>;

  class MockPartitionedLocalWindowSlider
      : public ShardRebuildingV1::PartitionedLocalWindowSlider {
   public:
    explicit MockPartitionedLocalWindowSlider(const ts_vector& queue)
        : PartitionedLocalWindowSlider(nullptr), queue_(queue) {}
    void getPartitionTimestamps() override {
      partition_timestamps_.clear();
      ld_check(cur_queue_pos_ < queue_.size());
      for (auto timestamp : queue_[cur_queue_pos_]) {
        partition_timestamps_.push_back(RecordTimestamp::from(ms(timestamp)));
      }
      ++cur_queue_pos_;
    }
    RecordTimestamp getGlobalWindowEnd() override {
      return RecordTimestamp::max();
    }
    size_t cur_queue_pos_{0};

   private:
    const ts_vector& queue_;
  };

  // Vector of calls to getNewLocalWindowEnd, where the first element of every
  // pair is the next_ts, and the second element is the expected local window
  // end.
  struct TestRun {
    size_t input_msec;
    bool expected_result;
    size_t expected_output_msec;
  };
  using run_vector = std::vector<TestRun>;

#define RUN_TEST(q, r)                                                    \
  {                                                                       \
    MockPartitionedLocalWindowSlider slider(q);                           \
    for (auto& run : (r)) {                                               \
      RecordTimestamp output;                                             \
      ld_info("Sliding to %lu", run.input_msec);                          \
      auto res = slider.getNewLocalWindowEnd(                             \
          RecordTimestamp::from(ms(run.input_msec)), &output);            \
      ASSERT_EQ(run.expected_result, res);                                \
      if (res) {                                                          \
        ASSERT_EQ(                                                        \
            RecordTimestamp::from(ms(run.expected_output_msec)), output); \
      }                                                                   \
    }                                                                     \
    ASSERT_EQ(q.size(), slider.cur_queue_pos_);                           \
  }

  ts_vector q{
      {100, 200, 500, 1000, 2000},
  };

  run_vector r{
      {0, true, 100},
      {50, true, 100},
      {100, true, 100},
      {101, true, 200},
      {200, true, 200},
      {1000, true, 1000},
      {1001, true, 2000},
      {2000, true, 2000},
      {2000, true, 2000},

      // Rewind shouldn't move the local window back. This doesn't really matter
      // in practice because RebuildingCoordinator creates a new
      // LocalWindowSlider when rewinding
      {100, true, 2000},
  };

  RUN_TEST(q, r);

  q = {
      {std::chrono::milliseconds::max().count()},
  };

  r = {
      {0, true, std::chrono::milliseconds::max().count()},
      {100, true, std::chrono::milliseconds::max().count()},
      {200, true, std::chrono::milliseconds::max().count()},
  };

  q = {
      {100, 200, 500, 1000, 2000, 0, 3000},
  };

  r = {
      {0, true, 100},
      {50, true, 100},
      {100, true, 100},
      {101, true, 200},
      {200, true, 200},
      {1000, true, 1000},
      {1001, true, 2000},
      {2000, true, 2000},
      {2000, true, 2000},
      {2500, true, 3000},

      // Rewind shouldn't move the local window back
      {2400, true, 3000},

      // Seeking beyond all partitions should return false
      {3001, false},
  };

  RUN_TEST(q, r);
}

// Verify the behavior when we abort rebuilding of a shard. The node for which
// the shard is aborted should be removed from the rebuilding set and rebuilding
// should be restarted.
TEST_F(RebuildingCoordinatorTest, shardAbortRebuilding) {
  std::mt19937 rng(0xbabababa241ef19e);
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 20;
  num_shards = 2;

  start();

  // We rebuild N2:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  // But we only start 5 logs because max_logs_in_flight=5.
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1, 3, 5, 7, 9);

  // We rebuild N3:S1. (we are node 0). Both N2:S1 and N3:S1 should be in the
  // rebuilding set.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7, 9);
  RebuildingSet set2({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set2, RecordTimestamp::max(), 1, 3, 5, 7, 9);

  // Let's abort rebuilding of shard 1 of node 2. Rebuilding set should now
  // contain only N3:S1.
  lsn_t v3 = onShardAbortRebuild(2, 1, v2);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7, 9);
  RebuildingSet set3({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set3, RecordTimestamp::max(), 1, 3, 5, 7, 9);
}

// We drain N0:S1 while also rebuilding N3:S1. Check that N0 participates but N3
// does not. Check that N0 waits for a SHARD_UNDRAIN message before acking.
TEST_F(RebuildingCoordinatorTest, DrainOneNodeWhileRebuildAnotherOne) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;

  start();

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // We rebuild N3:S1. (we are node 0). Both N2:S1 and N3:S1 should be in the
  // rebuilding set.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
  RebuildingSet set2({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(
      set2, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  onLogRebuildingComplete(9, 1, v2);
  onLogRebuildingComplete(17, 1, v2);
  onLogRebuildingComplete(5, 1, v2);
  onLogRebuildingComplete(19, 1, v2);
  onLogRebuildingComplete(15, 1, v2);
  onLogRebuildingComplete(1, 1, v2);
  onLogRebuildingComplete(3, 1, v2);
  onLogRebuildingComplete(11, 1, v2);
  onLogRebuildingComplete(13, 1, v2);
  onLogRebuildingComplete(7, 1, v2);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v2);

  // All nodes but N3 complete rebuilding.
  onShardIsRebuilt(1, 1, v2);
  onShardIsRebuilt(4, 1, v2);
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(0, 1, v2);

  // Rebuilding should be acked only after receiving a SHARD_UNDRAIN message.
  ASSERT_NO_SHARD_ACK_REBUILT();
  onShardUndrain(0, 1);
  ASSERT_SHARD_ACK_REBUILT(1);
}

// We start draining of N0:S1 in RELOCATE mode. During the drain, N0:S1 actually
// goes down so the FD starts rebuilding in RESTORE mode. However the shard
// comes back up so rebuilding is again restarted in RELOCATE mode instead of
// being aborted.
TEST_F(RebuildingCoordinatorTest, RestoreModeDuringDrain) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  start();

  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
  onLogRebuildingComplete(9, 1, v1);
  onLogRebuildingComplete(17, 1, v1);
  onLogRebuildingComplete(5, 1, v1);
  onLogRebuildingComplete(19, 1, v1);
  onLogRebuildingComplete(15, 1, v1);
  onLogRebuildingComplete(1, 1, v1);
  onLogRebuildingComplete(3, 1, v1);
  onLogRebuildingComplete(11, 1, v1);
  onLogRebuildingComplete(13, 1, v1);
  onLogRebuildingComplete(7, 1, v1);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v1);

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RESTORE mode (triggered by the FD for instance
  // if N0 was down for some time).
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  // In RESTORE mode, N0 is not a donor so nothing is started.
  ASSERT_NO_STARTED();

  // Let's assume N0 is back up, and because the local data is intact (we set
  // my_shard_has_data_intact=true), RebuildingCoordinator should immediately
  // send a new SHARD_NEEDS_REBUILD to restart rebuilding in RELOCATE mode.
  flags = SHARD_NEEDS_REBUILD_Header::RELOCATE |
      SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
  ASSERT_SHARD_NEEDS_REBUILD(1, flags, v2);

  // The message arrives.
  lsn_t v3 = onShardNeedsRebuild(0, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // Rebuilding completes.
  onLogRebuildingComplete(9, 1, v3);
  onLogRebuildingComplete(17, 1, v3);
  onLogRebuildingComplete(5, 1, v3);
  onLogRebuildingComplete(19, 1, v3);
  onLogRebuildingComplete(15, 1, v3);
  onLogRebuildingComplete(1, 1, v3);
  onLogRebuildingComplete(3, 1, v3);
  onLogRebuildingComplete(11, 1, v3);
  onLogRebuildingComplete(13, 1, v3);
  onLogRebuildingComplete(7, 1, v3);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v3);
  onShardIsRebuilt(0, 1, v3);
  onShardIsRebuilt(1, 1, v3);
  onShardIsRebuilt(2, 1, v3);
  onShardIsRebuilt(3, 1, v3);
  onShardIsRebuilt(4, 1, v3);

  // RebuildingCoordinator remembers that the intent was to drain, so it does
  // not acknowledge when rebuilding completes.
  ASSERT_NO_SHARD_ACK_REBUILT();
}

// We start draining of N0:S1 in RELOCATE mode. During the drain, N0:S1 actually
// goes down so the FD starts rebuilding in RESTORE mode. However the shard
// comes back up so rebuilding is again restarted in RELOCATE mode instead of
// being aborted.
TEST_F(RebuildingCoordinatorTest, RestoreModeDuringDrainUsingMaintenanceLog) {
  admin_settings_.enable_cluster_maintenance_state_machine = true;
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  start();

  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
  onLogRebuildingComplete(9, 1, v1);
  onLogRebuildingComplete(17, 1, v1);
  onLogRebuildingComplete(5, 1, v1);
  onLogRebuildingComplete(19, 1, v1);
  onLogRebuildingComplete(15, 1, v1);
  onLogRebuildingComplete(1, 1, v1);
  onLogRebuildingComplete(3, 1, v1);
  onLogRebuildingComplete(11, 1, v1);
  onLogRebuildingComplete(13, 1, v1);
  onLogRebuildingComplete(7, 1, v1);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v1);

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RESTORE mode (triggered by the FD for instance
  // if N0 was down for some time).
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  // In RESTORE mode, N0 is not a donor so nothing is started.
  ASSERT_NO_STARTED();

  // Let's assume N0 is back up, and because the local data is intact (we set
  // my_shard_has_data_intact=true), RebuildingCoordinator should immediately
  // send a new SHARD_NEEDS_REBUILD to restart rebuilding in RELOCATE mode.
  flags = SHARD_NEEDS_REBUILD_Header::RELOCATE |
      SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
  ASSERT_SHARD_NEEDS_REBUILD(1, flags, v2);
  // Since ClusterMaintenanceStateMachine is enabled, we should also have
  // written to maintenance log to remove the Restore mode rebuilding request
  auto shard = ShardID(0, 1);
  ASSERT_REMOVE_MAINTENANCE(shard);
  // The message arrives.
  lsn_t v3 = onShardNeedsRebuild(0, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // Rebuilding completes.
  onLogRebuildingComplete(9, 1, v3);
  onLogRebuildingComplete(17, 1, v3);
  onLogRebuildingComplete(5, 1, v3);
  onLogRebuildingComplete(19, 1, v3);
  onLogRebuildingComplete(15, 1, v3);
  onLogRebuildingComplete(1, 1, v3);
  onLogRebuildingComplete(3, 1, v3);
  onLogRebuildingComplete(11, 1, v3);
  onLogRebuildingComplete(13, 1, v3);
  onLogRebuildingComplete(7, 1, v3);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v3);
  onShardIsRebuilt(0, 1, v3);
  onShardIsRebuilt(1, 1, v3);
  onShardIsRebuilt(2, 1, v3);
  onShardIsRebuilt(3, 1, v3);
  onShardIsRebuilt(4, 1, v3);

  // RebuildingCoordinator remembers that the intent was to drain, so it does
  // not acknowledge when rebuilding completes.
  ASSERT_NO_SHARD_ACK_REBUILT();
}

// Verify that mini rebuilding gets upgraded to full shard rebuilding
// if data is missing and a new internal maintenance is requested
TEST_F(RebuildingCoordinatorTest,
       RequestInternalMaintenanceIfShardUnrecoverable) {
  admin_settings_.enable_cluster_maintenance_state_machine = true;
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = rrm;
  start();

  // RebuildingCoordinator should have requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);

  // Receive the message.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  // Some nodes complete rebuilding
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);

  // Now mark shard unrecoverable
  // also shards have lost data. Rebuilding coordinator
  // should convert mini rebuilding to full shard rebuilding
  setShardUnrecoverable(1);
  setDataIntact(false);
  // Now another node is added to rebuilding set. (This is just to
  // restart rebuilding on N0)
  lsn_t v3 = onShardNeedsRebuild(1, 1, 0 /* flags */, LSN_MAX, true);

  auto nodeid = thrift::NodeID();
  nodeid.set_node_index(0);
  auto shardid = thrift::ShardID();
  shardid.set_node(nodeid);
  shardid.set_shard_index(1);
  ASSERT_APPLY_MAINTENANCE(shardid);
}

// We start rebuilding a node in RESTORE mode. Then a drain is started.
// During the drain, N0:S1 comes back up so rebuilding is again restarted
//  in RELOCATE mode instead of being aborted.
TEST_F(RebuildingCoordinatorTest, RestoreModeAfterDrain) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = false;

  start();

  // We start rebuilding N0:S1 in RESTORE mode (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  // In RESTORE mode, N0 is not a donor so nothing is started.
  ASSERT_NO_STARTED();

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RELOCATE mode (triggered by someone starting
  // a drain) but since we are rebuilding in RESTORE mode, we should preserve
  // it.
  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;
  lsn_t v2 = onShardNeedsRebuild(0, 1, flags, LSN_MAX, false);
  ASSERT_NO_STARTED();

  setDataIntact(true);

  // Now another node is added to rebuilding set. (This is just to
  // restart rebuilding on N0) but we have our data intact this time.
  // RebuildingCoordinator should immediately send a new SHARD_NEEDS_REBUILD
  // to restart rebuilding in RELOCATE mode.
  lsn_t v3 = onShardNeedsRebuild(1, 1, 0 /* flags */, LSN_MAX, false);
  flags = SHARD_NEEDS_REBUILD_Header::RELOCATE |
      SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
  ASSERT_SHARD_NEEDS_REBUILD(1, flags, v3);

  // The message arrives.
  lsn_t v4 = onShardNeedsRebuild(0, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  RebuildingSet set2({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)},
                      {N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(
      set2, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // Rebuilding completes.
  onLogRebuildingComplete(9, 1, v4);
  onLogRebuildingComplete(17, 1, v4);
  onLogRebuildingComplete(1, 1, v4);
  onLogRebuildingComplete(5, 1, v4);
  onLogRebuildingComplete(19, 1, v4);
  onLogRebuildingComplete(15, 1, v4);
  onLogRebuildingComplete(3, 1, v4);
  onLogRebuildingComplete(11, 1, v4);
  onLogRebuildingComplete(13, 1, v4);
  onLogRebuildingComplete(7, 1, v4);
  ASSERT_NO_STARTED();
  ASSERT_SHARD_REBUILT(1, v4);
  onShardIsRebuilt(0, 1, v4);
  onShardIsRebuilt(2, 1, v4);
  onShardIsRebuilt(3, 1, v4);
  onShardIsRebuilt(4, 1, v4);

  // RebuildingCoordinator remembers that the intent was to drain, so it does
  // not acknowledge when rebuilding completes.
  ASSERT_NO_SHARD_ACK_REBUILT();
}

// Verify that DRAIN works.
TEST_F(RebuildingCoordinatorTest, Drain) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  start();

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  // Rebuilding should be started in RELOCATE mode since we have
  // requested a drain and there is no  rebuilding
  // active for this shard.
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // All nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);
  onShardIsRebuilt(0, 1, v1);

  // RebuildingCoordinator remembers that the intent was to drain, so it does
  // not acknowledge when rebuilding completes.
  ASSERT_NO_SHARD_ACK_REBUILT();
}

// Verify that rebuilding is started for a dirty shard.
TEST_F(RebuildingCoordinatorTest, DirtyShard) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = rrm;

  start();

  // RebuildingCoordinator should have requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);

  // Receive the message.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);

  ASSERT_SHARD_ACK_REBUILT(1);
}

// Verify that we do not ack a complete, but non-authoritative rebuilding
// of a dirty shard.
TEST_F(RebuildingCoordinatorTest, DirtyShardNonAuthoritative) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = rrm;

  start();

  // RebuildingCoordinator should have requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);

  // Receive the message.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  // Make sure there is at least one outstanding recoverable shard.
  lsn_t v2 = onShardNeedsRebuild(2, 1, 0, folly::none, true, &rrm);

  onShardIsRebuilt(1, 1, v2, /*is_authoritative*/ false);
  onShardIsRebuilt(3, 1, v2, /*is_authoritative*/ false);

  ASSERT_NO_SHARD_ACK_REBUILT();
}

// Verify that a dirty shard that is draining is left draining
TEST_F(RebuildingCoordinatorTest, DirtyShardDrain) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = rrm;

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  updateConfig();

  // We drain N0:S1.
  onShardNeedsRebuild(my_node_id.index(), 1, flags, folly::none);

  start();

  // Drain should still be in effect without any time ranges in RESTORE
  // mode (we cannot be a donor since we have some data lost).
  ASSERT_SHARD_NEEDS_REBUILD(1, 0, LSN_INVALID);
  onShardNeedsRebuild(my_node_id.index(), 1, 0, folly::none, false);
  ASSERT_EQ(rebuilding_set_->getRebuildingShards()
                .at(1)
                .nodes_.at(my_node_id.index())
                .mode,
            RebuildingMode::RESTORE);

  // If we stop draining, we should convert back to doing a time ranged
  // rebuild.
  onShardUndrain(0, 1);
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);
  auto drain = rebuilding_set_->getRebuildingShards()
                   .at(1)
                   .nodes_.at(my_node_id.index())
                   .drain;
  ASSERT_FALSE(drain);
}

// Verify that a dirty shard that was fully rebuilt before the node started
// results in an acked rebuild and the dirty shard info is cleared.
TEST_F(RebuildingCoordinatorTest, DirtyShardAlreadyRebuilt) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = rrm;

  updateConfig();

  // We rebuild S1.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(), 1, 0, folly::none, false);
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);

  start();

  // Dirty Shard is OBE.
  ASSERT_SHARD_ACK_REBUILT(1);
  ASSERT_EQ(rebuilding_set_->getRebuildingShards()
                .at(1)
                .nodes_.at(my_node_id.index())
                .auth_status,
            AuthoritativeStatus::AUTHORITATIVE_EMPTY);

  onShardAckRebuilt(my_node_id.index(), 1, v1);
  ASSERT_TRUE(rebuilding_set_->getRebuildingShards().empty());
}

// If a drain is ongoing and we receive a SHARD_UNDRAIN message, this will
// effectively remove the DRAIN flag and cause RebuildingCoordinator to send a
// SHARD_ABORT_REBUILD. If, however, a new SHARD_NEEDS_REBUILD comes up before
// that SHARD_ABORT_REBUILD is receive, the latter will be discarded.
// We expect that when the new SHARD_NEEDS_REBUILD message is received,
// RebuildingCoordinator will see that rebuilding should be aborted again
// because 1/ the data is intact, 2/ the DRAIN flag is not set.
TEST_F(RebuildingCoordinatorTest, ReproT18780256) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 50;
  num_logs = 20;
  num_shards = 2;

  start();
  setDataIntact(true);

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  ASSERT_STARTED(
      set, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // SHARD_UNDRAIN is received for N0:S1. It should write a
  // SHARD_ABORT_REBUILD message.
  onShardUndrain(0, 1);
  ASSERT_ABORT_FOR_MY_SHARD(1, v1);

  // We start rebuilding N3:S1 before the above SHARD_ABORT_REBUILD gets
  // received. Because the rebuilding version changed, the SHARD_ABORT_REBUILD
  // will get discarded.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
  RebuildingSet set2({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(
      set2, RecordTimestamp::max(), 1, 3, 5, 7, 9, 11, 13, 15, 17, 19);

  // Verify that RebuildingCoordinator wrote another SHARD_ABORT_REBUILD for its
  // shard 1 after rebuilding version changed.
  ASSERT_ABORT_FOR_MY_SHARD(1, v2);
}

TEST_F(RebuildingCoordinatorTest, NonDurableLogRebuilding) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 2;
  num_logs = 10;
  num_shards = 2;

  start();

  // Rebuilding N3:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(3, 1, 0 /*flags*/);
  RebuildingSet set({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1, 3);

  // Log 1 isn't durable. Timer for shard 1 should be started
  onLogRebuildingReachedUntilLsn(1, v1, 100);
  ASSERT_RESTART_TIMER_ACTIVATED(1, 1);

  // 1 slot is available, Next log should be started
  ASSERT_EQ(1, received.start.size());
  ASSERT_STARTED(set, RecordTimestamp::max(), 5);

  // Restart timer for Log 1 fires. But there is no slot
  // Restart should be sent once a slot becomes available
  fireRestartTimer(logid_t(1));
  onLogRebuildingComplete(3, 1, v1);
  ASSERT_RESTARTED(1);

  onLogRebuildingComplete(5, 1, v1);
  ASSERT_STARTED(set, RecordTimestamp::max(), 7);
  onLogRebuildingComplete(7, 1, v1);
  ASSERT_STARTED(set, RecordTimestamp::max(), 9);
  onLogRebuildingComplete(9, 1, v1);
  ASSERT_NO_STARTED();
  onLogRebuildingComplete(1, 1, v1);
  ASSERT_SHARD_REBUILT(1, v1);

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(0, 1, v1);
}

TEST_F(RebuildingCoordinatorTest, NonDurableLogRebuilding2) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 5;
  num_logs = 10;
  num_shards = 2;

  start();

  // Rebuilding N3:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(3, 1, 0 /*flags*/);
  RebuildingSet set({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1, 3, 5, 7, 9);

  // None of them are durable. Timer should be started
  std::vector<uint64_t> logs = {1, 3, 5, 7, 9};
  auto it = logs.begin();
  while (it != logs.end()) {
    onLogRebuildingReachedUntilLsn(*it, v1, 10);
    ASSERT_RESTART_TIMER_ACTIVATED(*it, 1);
    it++;
  }

  // All timers fire
  it = logs.begin();
  while (it != logs.end()) {
    fireRestartTimer(logid_t(*it));
    ASSERT_RESTARTED(*it);
    it++;
  }
  ASSERT_NO_STARTED();

  // All complete non-durably
  it = logs.begin();
  while (it != logs.end()) {
    onLogRebuildingReachedUntilLsn(*it, v1, 10);
    ASSERT_RESTART_TIMER_ACTIVATED(*it, 1);
    it++;
  }

  // Say Memtable flush causes all to send complete
  // before timer expires
  it = logs.begin();
  while (it != logs.end()) {
    onLogRebuildingComplete(*it, 1, v1);
    it++;
  }

  ASSERT_SHARD_REBUILT(1, v1);

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(0, 1, v1);
}

TEST_F(RebuildingCoordinatorTest, StallRebuilding) {
  settings.local_window = RecordTimestamp::duration(100);
  settings.global_window = RecordTimestamp::duration::max();
  settings.local_window_uses_partition_boundary = false;
  settings.max_logs_in_flight = 2;
  settings.total_log_rebuilding_size_per_shard_mb = 2;
  num_logs = 12;
  num_shards = 2;

  const uint64_t base_ts = 10000000000l;
  start();

  // Rebuilding N3:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(3, 1, 0 /*flags*/);
  RebuildingSet set({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::min(), 1, 3);

  // Log 1 isn't durable. Timer should be started
  onLogRebuildingReachedUntilLsn(1, v1, 1024 * 1024);
  ASSERT_RESTART_TIMER_ACTIVATED(1, 1);

  // 1 slot is available, Next log should be started
  ASSERT_EQ(1, received.start.size());
  ASSERT_STARTED(set, RecordTimestamp::min(), 5);

  // Restart timer for Log 1 fires. But there is no slot
  // Restart should be sent once a slot becomes available
  fireRestartTimer(logid_t(1));
  onLogRebuildingWindowEnd(3, 1, base_ts + 15, v1, 1024 * 1024);
  ASSERT_RESTARTED(1);

  onLogRebuildingWindowEnd(5, 1, base_ts + 15, v1, 1024 * 1024);
  // We have exceed the memory limit. Stall timer should be activated.
  ASSERT_STALL_TIMER_ACTIVATED(1);
  ASSERT_NO_STARTED();

  // While rebuilding is stalled,  a memtable flush causes
  // Log 1 to complete.
  onLogRebuildingComplete(1, 1, v1);
  onLogRebuildingSizeUpdate(5, v1, 0);

  // This should cancel stall timer and start new log rebuildings
  ASSERT_STARTED(set, RecordTimestamp::min(), 7, 9);

  onLogRebuildingWindowEnd(7, 1, base_ts + 5, v1, 1024 * 1024);
  ASSERT_STALL_TIMER_ACTIVATED(1);

  onLogRebuildingComplete(9, 1, v1);
  // Log 11 should not start since stall timer is active
  ASSERT_NO_STARTED();

  // Stall timer fires
  fireStallTimer(1);

  ASSERT_RESTARTED(3, 7);

  onLogRebuildingComplete(5, 1, v1);
  onLogRebuildingComplete(7, 1, v1);
  ASSERT_STARTED(set, RecordTimestamp::min(), 11);
  onLogRebuildingComplete(3, 1, v1);

  onLogRebuildingReachedUntilLsn(11, v1, 1024 * 1024);
  onLogRebuildingComplete(11, 1, v1);

  ASSERT_SHARD_REBUILT(1, v1);

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(0, 1, v1);
}

TEST_F(RebuildingCoordinatorTest, SizeUpdateAfterRestart) {
  settings.local_window = RecordTimestamp::duration(100);
  settings.global_window = RecordTimestamp::duration::max();
  settings.local_window_uses_partition_boundary = false;
  settings.max_logs_in_flight = 2;
  settings.total_log_rebuilding_size_per_shard_mb = 2;
  num_logs = 10;
  num_shards = 2;

  const uint64_t base_ts = 10000000000l;
  start();

  // Rebuilding N3:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(3, 1, 0 /*flags*/);
  RebuildingSet set({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::min(), 1, 3);

  // Log 1 isn't durable. Timer should be started
  onLogRebuildingReachedUntilLsn(1, v1, 1024 * 1024);
  ASSERT_RESTART_TIMER_ACTIVATED(1, 1);

  // 1 slot is available, Next log should be started
  ASSERT_EQ(1, received.start.size());
  ASSERT_STARTED(set, RecordTimestamp::min(), 5);

  // Restart timer for Log 1 fires. But there is no slot
  // Restart should be sent once a slot becomes available
  fireRestartTimer(logid_t(1));
  onLogRebuildingComplete(3, 1, v1);
  ASSERT_RESTARTED(1);

  onLogRebuildingWindowEnd(5, 1, base_ts + 15, v1, 1024 * 1024);
  // We have exceed the memory limit. Stall timer should be activated.
  ASSERT_STALL_TIMER_ACTIVATED(1);

  // While rebuilding is stalled,  a memtable flush causes
  // Log 1 to complete.
  onLogRebuildingComplete(1, 1, v1);

  // This should cancel stall timer and start new lor rebuildings
  ASSERT_STARTED(set, RecordTimestamp::min(), 7, 9);

  onLogRebuildingWindowEnd(7, 1, base_ts + 5, v1, 1024 * 1024);
  ASSERT_STALL_TIMER_ACTIVATED(1);

  onLogRebuildingComplete(9, 1, v1);
  ASSERT_NO_STARTED();

  // Stall timer fires
  fireStallTimer(1);

  ASSERT_RESTARTED(5, 7);
  // Now we receive Size update for log 5
  // that was sent before RC restarted the log
  onLogRebuildingSizeUpdate(5, v1, 1024);

  onLogRebuildingComplete(5, 1, v1);
  onLogRebuildingComplete(7, 1, v1);
  ASSERT_SHARD_REBUILT(1, v1);

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(0, 1, v1);
}

TEST_F(RebuildingCoordinatorTest, RebuildingRestartsWhileSomeTimersAreActive) {
  settings.local_window = RecordTimestamp::duration::max();
  settings.global_window = RecordTimestamp::duration::max();
  settings.max_logs_in_flight = 3;
  num_logs = 10;
  num_shards = 2;

  start();

  // Rebuilding N3:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(3, 1, 0 /*flags*/);
  RebuildingSet set({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set, RecordTimestamp::max(), 1, 3, 5);

  // Log 1 isn't durable. Timer should be started
  onLogRebuildingReachedUntilLsn(1, v1, 10);
  ASSERT_RESTART_TIMER_ACTIVATED(1, 1);

  // Log 7 should start
  ASSERT_STARTED(set, RecordTimestamp::max(), 7);

  // We rebuild N4:S1. (we are node 0). Both N4:S1 and N3:S1 should be in the
  // rebuilding set.
  lsn_t v2 = onShardNeedsRebuild(4, 1, 0 /* flags */);
  ASSERT_LOG_REBUILDING_ABORTED(1, 3, 5, 7);
  RebuildingSet set2({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)},
                      {N4S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  ASSERT_STARTED(set2, RecordTimestamp::max(), 1, 3, 5);

  // 1 completes non durably
  onLogRebuildingReachedUntilLsn(1, v2, 10);
  ASSERT_RESTART_TIMER_ACTIVATED(1, 1);

  // Log 7 should start
  ASSERT_STARTED(set2, RecordTimestamp::max(), 7);

  onLogRebuildingComplete(7, 1, v2);
  ASSERT_STARTED(set2, RecordTimestamp::max(), 9);

  // 1 Completes before timer expires
  onLogRebuildingComplete(1, 1, v2);

  ASSERT_NO_STARTED()

  onLogRebuildingComplete(5, 1, v2);
  onLogRebuildingComplete(3, 1, v2);
  onLogRebuildingComplete(9, 1, v2);
  ASSERT_NO_STARTED();

  ASSERT_SHARD_REBUILT(1, v2);

  onShardIsRebuilt(1, 1, v2);
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(0, 1, v2);
}

}} // namespace facebook::logdevice
