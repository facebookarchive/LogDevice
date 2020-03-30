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

using namespace facebook::logdevice::maintenance;

// Convenient shortcuts for writing ShardIDs.
#define N0S1 ShardID(0, 1)
#define N1S1 ShardID(1, 1)
#define N2S1 ShardID(2, 1)
#define N2S3 ShardID(2, 3)
#define N3S1 ShardID(3, 1)
#define N3S3 ShardID(3, 3)
#define N4S1 ShardID(4, 1)

namespace facebook { namespace logdevice {

class RebuildingCoordinatorTest;

namespace {

using ms = std::chrono::milliseconds;

static const NodeID my_node_id{0, 1};

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

struct ShardNeedsRebuild {
  SHARD_NEEDS_REBUILD_flags_t flags;
  lsn_t cond_version;
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
 * Keeps track of various calls from RebuildingCoordinator.
 */
struct ReceivedMessages {
  std::vector<DonorProgress> donor_progress;
  std::vector<ShardRebuilt> shard_rebuilt;
  std::vector<uint32_t> shard_ack_rebuilt;
  std::unordered_map<uint32_t, ShardNeedsRebuild> shard_needs_rebuild;
  std::unordered_map<uint32_t, lsn_t> abort_for_my_shard;
  std::vector<uint32_t> mark_shard_unrecoverable;

  void clear() {
    shard_rebuilt.clear();
    shard_ack_rebuilt.clear();
    shard_needs_rebuild.clear();
    abort_for_my_shard.clear();
  }
};

static ReceivedMessages received;
static std::unordered_set<uint32_t> restart_scheduled;
static bool planning_timer_activated{false};
static thrift::RemoveMaintenancesRequest remove_maintenance;
static std::vector<thrift::MaintenanceDefinition> apply_maintenances;

class MockedRebuildingCoordinator;

class MockedShardRebuilding : public ShardRebuildingInterface {
 public:
  MockedShardRebuilding(shard_index_t _shard,
                        lsn_t _restart_version,
                        const RebuildingSet& _rebuilding_set)
      : shard(_shard),
        restart_version(_restart_version),
        rebuilding_set(_rebuilding_set) {}
  ~MockedShardRebuilding() override;

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;

  virtual void advanceGlobalWindow(RecordTimestamp new_window_end) override {
    global_window = new_window_end;
  }

  virtual void noteConfigurationChanged() override {}
  virtual void noteRebuildingSettingsChanged() override {}

  // Fills the current row of @param table with debug information about the
  // state of rebuilding for this shard. Used by admin commands.
  virtual void getDebugInfo(InfoRebuildingShardsTable& table) const override {}

  MockedRebuildingCoordinator* owner = nullptr;
  const shard_index_t shard;
  const lsn_t restart_version;
  const RebuildingSet rebuilding_set;
  RecordTimestamp global_window = RecordTimestamp::min();
};

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

  void noteRangesPublished(uint32_t shard,
                           RebuildingRangesVersion /*version*/) override {
    (*dirty_shard_cache_)[shard].first.setPublished(true);
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
    auto r = std::make_unique<MockedShardRebuilding>(
        shard, restart_version, *rebuilding_set);
    r->owner = this;
    return r;
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

  void notifyShardDonorProgress(uint32_t shard,
                                RecordTimestamp ts,
                                lsn_t version) override {
    ld_info("Notified about donor progress for my shard %u, ts %s",
            shard,
            ts.toString().c_str());
    received.donor_progress.push_back(DonorProgress{shard, ts, version});
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
                       const EventLogRebuildingSet::NodeInfo* node_info,
                       const char* reason) override {
    received.abort_for_my_shard[shard] = version;
    if (myShardIsDirty(shard)) {
      RebuildingCoordinator::abortForMyShard(shard, version, node_info, reason);
    }
  }

  bool restartIsScheduledForShard(uint32_t shard_idx) override {
    return restart_scheduled.count(shard_idx);
  }

  void scheduleRestartForShard(uint32_t shard_idx) override {
    restart_scheduled.insert(shard_idx);
  }

  void normalizeTimeRanges(uint32_t, RecordTimeIntervals&) override {}

  std::map<shard_index_t, MockedRebuildingPlanner*> rebuildingPlanners;

  std::map<shard_index_t, MockedShardRebuilding*> shardRebuildings;

  void activatePlanningTimer() override {
    planning_timer_activated = true;
  }

 private:
  shard_size_t num_shards_;
  bool my_shard_has_data_intact_;
  std::unordered_set<uint32_t> unrecoverable_shards_;
  DirtyShardMap* dirty_shard_cache_;
};

void MockedShardRebuilding::start(
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan) {
  ld_check(!owner->shardRebuildings.count(shard));
  owner->shardRebuildings[shard] = this;
}
MockedShardRebuilding::~MockedShardRebuilding() {
  ld_check(owner->shardRebuildings.at(shard) == this);
  owner->shardRebuildings.erase(shard);
}

} // namespace

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
    settings.global_window = std::chrono::milliseconds::max();
    settings.use_legacy_log_to_shard_mapping_in_rebuilding = false;
    settings.shard_is_rebuilt_msg_delay = {
        std::chrono::seconds(0), std::chrono::seconds(0)};
    settings.new_to_old = false;

    // Clean up possible leftovers from previous test run.
    received.clear();
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

    auto log_attrs =
        logsconfig::LogAttributes().with_replicationFactor(3).with_scdEnabled(
            true);

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

    if (until_lsn.has_value()) {
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

  MockedShardRebuilding*
  getShard(shard_index_t shard_idx,
           folly::Optional<lsn_t> restart_version = folly::none) {
    ld_check(coordinator_->shardRebuildings.count(shard_idx));
    auto reb = coordinator_->shardRebuildings.at(shard_idx);
    if (restart_version.has_value()) {
      ld_check_eq(restart_version.value(), reb->restart_version);
    }
    return reb;
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

  void onShardRebuildingProgress(uint32_t shard, int64_t next_ts) {
    coordinator_->onShardRebuildingProgress(shard,
                                            RecordTimestamp::from(ms(next_ts)),
                                            /*progress_estimate*/ 0.5);
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

// Asserts that the shard is locally dirty and has had its dirty
// range information published to the event log.
#define ASSERT_PUBLISHED(shard)                 \
  {                                             \
    ASSERT_TRUE(coordinator_);                  \
    RebuildingCoordinator::DirtyShardMap map;   \
    coordinator_->populateDirtyShardCache(map); \
    auto it = map.find(shard);                  \
    ASSERT_TRUE(it != map.end());               \
    ASSERT_TRUE(it->second.first.published());  \
  }

// Asserts that the shard is locally dirty, but has not had its dirty
// range information published to the event log.
#define ASSERT_NOT_PUBLISHED(shard)             \
  {                                             \
    ASSERT_TRUE(coordinator_);                  \
    RebuildingCoordinator::DirtyShardMap map;   \
    coordinator_->populateDirtyShardCache(map); \
    auto it = map.find(shard);                  \
    ASSERT_TRUE(it != map.end());               \
    ASSERT_FALSE(it->second.first.published()); \
  }

/**
 * Tests.
 */

// Check that we complete rebuilding if there are no logs to rebuild.
TEST_F(RebuildingCoordinatorTest, NoLogsToRebuild) {
  num_logs = 0;
  num_shards = 15;

  start();

  // We rebuild shard 6 of node 2. (we are node 0).
  lsn_t version = onShardNeedsRebuild(2, 6, 0 /* flags */, folly::none);
  onFinishedRetrievingPlans(6);
  // But there are no logs on that shard, so rebuilding should complete
  // immediately, without even creating a ShardRebuilding.
  ASSERT_SHARD_REBUILT(6, version);
}

// All logs are rebuilt at the same time and there is a global timestamp window.
TEST_F(RebuildingCoordinatorTest, GlobalWindow1) {
  settings.global_window = std::chrono::seconds(30);
  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t version = onShardNeedsRebuild(2, 1, 0 /* flags */);

  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // Simulate ShardRebuilding informing coordinator of the timestamp of the
  // first record.
  onShardRebuildingProgress(1, 42);
  ASSERT_DONOR_PROGRESS(1, 42, version);

  // Now we wait for all other nodes to inform that they reached the end of the
  // global window as well so that we can slide it.
  // Note that node 2 is not expected to intervene because it's rebuilding in
  // RESTORE mode.
  onShardDonorProgress(0, 1, 42, version); // This is what we wrote
  onShardDonorProgress(1, 1, 53, version);
  EXPECT_EQ(RecordTimestamp::min(), getShard(1)->global_window);
  onShardDonorProgress(3, 1, 32, version);

  // Now that we know that all nodes reached the end of their view of the global
  // window, we can slide the global window.
  auto global_window_end =
      RecordTimestamp::from(ms(32)) + settings.global_window;
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Report progress timestamp smaller than the previous one (ShardRebuilding is
  // allowed to do that). RebuildingCoordinator should ignore it.
  onShardRebuildingProgress(1, 40);
  ASSERT_NO_DONOR_PROGRESS();
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Report progress timestamp only slightly higher than the last time.
  // RebuildingCoordinator shouldn't write to the event log when the
  // difference is to small.
  onShardRebuildingProgress(1, 45);
  ASSERT_NO_DONOR_PROGRESS();
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Report some progress timestamp ~2/3 of the way through the global window.
  onShardRebuildingProgress(1, 20300);
  ASSERT_DONOR_PROGRESS(1, 20300, version);

  // All other nodes move forward.
  onShardDonorProgress(0, 1, 20300, version); // This is what we wrote
  onShardDonorProgress(1, 1, 1000000, version);
  EXPECT_EQ(global_window_end, getShard(1)->global_window);
  onShardDonorProgress(3, 1, 1500000, version);

  // Global window should move because min from all nodes increased enough.
  global_window_end = RecordTimestamp::from(ms(20300)) + settings.global_window;
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Our ShardRebuilding makes more progress.
  onShardRebuildingProgress(1, 1000300);
  ASSERT_DONOR_PROGRESS(1, 1000300, version);
  // Read what we wrote.
  onShardDonorProgress(0, 1, 1000300, version);

  // Global window should slide.
  global_window_end = RecordTimestamp(ms(1000000) + settings.global_window);
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // ShardRebuilding completes.
  coordinator_->onShardRebuildingComplete(1);

  // We should notify in the event log that we rebuilt the shard.
  ASSERT_SHARD_REBUILT(1, version);
}

// Same as GlobalWindow1 but there is a second node
// rebuilding the same shard in RELOCATE mode. This code also checks the code
// path where one donor node finishing rebuilding of the shard triggers sliding
// the timestamp windows.
TEST_F(RebuildingCoordinatorTest, GlobalWindow2) {
  settings.global_window = std::chrono::seconds(30);
  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);

  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // We also start rebuilding that shard for node 1, in RELOCATE mode which
  // means that this node will still be a donor.
  lsn_t v2 = onShardNeedsRebuild(1, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);

  // ShardRebuilding should restart with the new rebuilding set.
  set.shards.emplace(N1S1, RebuildingNodeInfo(RebuildingMode::RELOCATE));
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // Simulate ShardRebuilding informing coordinator of the timestamp of the
  // first record.
  onShardRebuildingProgress(1, 42);
  ASSERT_DONOR_PROGRESS(1, 42, v2);

  // Now we wait for all other nodes to inform that they reached the end of the
  // global window as well so that we can slide it.
  // Note that node 2 is not expected to intervene because it's rebuilding in
  // RESTORE mode.
  onShardDonorProgress(0, 1, 42, v2); // This is what we wrote
  onShardDonorProgress(1, 1, 53, v2);
  EXPECT_EQ(RecordTimestamp::min(), getShard(1)->global_window);
  // Node 3 finishes rebuilding the shard. Here we are checking that the
  // onShardIsRebuilt event triggers sliding of the global window.
  onShardIsRebuilt(3, 1, v2);

  // Now that we know that all nodes reached the end of their view of the global
  // window, we can slide it.
  RecordTimestamp global_window_end(ms(42) + settings.global_window);
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Simulate some more ShardRebuilding progress.
  onShardRebuildingProgress(1, 20300);
  ASSERT_DONOR_PROGRESS(1, 20300, v2);

  // All nodes move forward.
  onShardDonorProgress(0, 1, 20300, v2); // This is what we wrote
  onShardDonorProgress(1, 1, 1000000, v2);

  // Global window moves.
  global_window_end = RecordTimestamp(ms(20300) + settings.global_window);
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // Simulate some more ShardRebuilding progress.
  onShardRebuildingProgress(1, 1000300);
  ASSERT_DONOR_PROGRESS(1, 1000300, v2);
  // Which we then read.
  onShardDonorProgress(0, 1, 1000300, v2);
  // We slide the global window.
  global_window_end = RecordTimestamp(ms(1000000) + settings.global_window);
  EXPECT_EQ(global_window_end, getShard(1)->global_window);

  // ShardRebuilding completes.
  coordinator_->onShardRebuildingComplete(1);

  // We should notify in the event log that we rebuilt the shard.
  ASSERT_SHARD_REBUILT(1, v2);
}

// One of my shards is being rebuilt by the other nodes in the cluster. All I
// have to do is wait until all donors rebuilt it and acknowledge.
TEST_F(RebuildingCoordinatorTest, MyShardAck) {
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 0. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // We should not rebuild anything ourselves.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
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
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 0. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // We should not rebuild anything ourselves.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
  // But this shard also needs to be rebuilt for node 1.
  lsn_t v2 = onShardNeedsRebuild(1, 1, 0 /* flags */, folly::none, false);
  // We should not rebuild anything ourselves.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
  // All other nodes (2, 3) complete rebuilding of the shard.
  onShardIsRebuilt(2, 1, v2);
  ASSERT_NO_SHARD_ACK_REBUILT();
  onShardIsRebuilt(3, 1, v2);
  // We ack.
  ASSERT_SHARD_ACK_REBUILT(1);
}

// Same as MyShardAck2 but with a different order: we first heard that we need
// to rebuild shard 1 on node 1 so we started a shard rebuilding state machine.
// As soon as we hear that we should rebuild the same shard for
// ourself, we abort that state machine because we are not supposed to
// participate since we are rebuilding in RESTORE mode.
TEST_F(RebuildingCoordinatorTest, MyShardAck3) {
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 1 (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  // So we should kick of rebuilding of log 1.
  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);
  // But we start rebuilding the same shard for ourself as well.
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // This causes us to abort the shard rebuilding we started.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
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
  num_logs = 2;
  num_shards = 2;

  start();
  // We rebuild shard 1 of node 1 (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(1, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  // So we should kick of rebuilding of log 1.
  RebuildingSet set({{N1S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);
  // But we start rebuilding the same shard for ourself as well.
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  // This causes us to abort the log rebuilding we started.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
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

// Let's imagine that the event log is not trimmed and we read a backlog of
// events. This should cause rebuilding state machines to be started and
// aborted as we receive each message.
TEST_F(RebuildingCoordinatorTest, EventLogBacklog) {
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  // We rebuild shard 1 of node 2. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);
  onShardIsRebuilt(3, 1, v1);
  onShardIsRebuilt(0, 1, v1);
  // We received our own SHARD_IS_REBUILT message, we should abort
  // ShardRebuilding state machine.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
  onShardIsRebuilt(1, 1, v1);
  onShardAckRebuilt(2, 1, v1);

  // We rebuild shard 1 of node 3. (we are node 0).
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  RebuildingSet set2({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set2, getShard(1)->rebuilding_set);
  onShardIsRebuilt(0, 1, v2);
  // We received our own SHARD_IS_REBUILT message, we should abort
  // ShardRebuilding state machine.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(1, 1, v2);
  onShardAckRebuilt(3, 1, v2);

  // Our own shard 1 is being rebuilt.
  lsn_t v3 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
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
  EXPECT_EQ(set4, getShard(1)->rebuilding_set);
  onShardIsRebuilt(3, 1, v4);
  onShardIsRebuilt(2, 1, v4);
  onShardIsRebuilt(0, 1, v4);
  // We received our own SHARD_IS_REBUILT message, we should abort
  // ShardRebuilding state machine.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
  onShardAckRebuilt(1, 1, v4);

  // ... we read the duplicate SHARD_ACK_REBUILT here but it should be
  // discarded.
  onShardAckRebuilt(0, 1, v4);

  ASSERT_NO_SHARD_REBUILT();
  ASSERT_NO_DONOR_PROGRESS();
  ASSERT_NO_SHARD_ACK_REBUILT();
}

// If all nodes are in the rebuilding set, each node should acknowledge
// rebuilding immediately as there is no donor.
TEST_F(RebuildingCoordinatorTest, AllNodesRebuilding) {
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  auto set =
      RebuildingSet({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  set.shards.emplace(N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  lsn_t v3 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

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
  num_logs = 2;
  num_shards = 2;
  num_nodes = 4;

  start();

  ld_info("Requesting rebuilding of N1.");
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  auto set =
      RebuildingSet({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  ld_info("Requesting rebuilding of N2.");
  lsn_t v2 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  set.shards.emplace(N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  ld_info("Calling onShardRebuildingComplete() for version 2.");
  coordinator_->onShardRebuildingComplete(1);
  ASSERT_SHARD_REBUILT(1, v2);

  ld_info("Requesting rebuilding of N3.");
  lsn_t v3 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  set.shards.emplace(N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE));
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  ld_info("Calling onShardIsRebuilt() by N0 version 2.");
  onShardIsRebuilt(0, 1, v2);

  ld_info("Calling onShardIsRebuilt() by N1 version 3.");
  onShardIsRebuilt(1, 1, v3);
  ASSERT_NO_SHARD_REBUILT();

  ld_info("Calling onShardRebuildingComplete() for version 3.");
  coordinator_->onShardRebuildingComplete(1);
  ASSERT_SHARD_REBUILT(1, v3);

  ld_info("Calling onShardIsRebuilt() by N0 version 3.");
  onShardIsRebuilt(0, 1, v3);

  // Rebuilding set should be {0, 1, 2, 3} because the rebuilding of {1, 2, 3 }
  // wasn't acknowledged.
  ld_info("Requesting rebuilding of N0.");
  lsn_t v4 = onShardNeedsRebuild(0, 1, 0 /* flags */, folly::none, false);
  ASSERT_SHARD_ACK_REBUILT(1);
}

TEST_F(RebuildingCoordinatorTest, Trim) {
  num_logs = 4;
  num_shards = 4;
  num_nodes = 4;

  start();

  ld_info("Requesting rebuilding of N1 shard 1.");
  lsn_t v1 = onShardNeedsRebuild(1, 1, 0 /* flags */);
  auto set1 =
      RebuildingSet({{N1S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set1, getShard(1)->rebuilding_set);

  ld_info("Requesting rebuilding of N3 shard 3.");
  lsn_t v2 = onShardNeedsRebuild(3, 3, 0 /* flags */);
  auto set3 =
      RebuildingSet({{N3S3, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set3, getShard(3)->rebuilding_set);

  ld_info("Reporting trimmed event log.");
  onEventLogTrimmed(v2 + 1);
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

  ld_info("Requesting rebuilding of N2 shard 1.");
  lsn_t v3 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  set1 = RebuildingSet({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set1, getShard(1)->rebuilding_set);

  ld_info("Calling onShardRebuildingComplete().");
  coordinator_->onShardRebuildingComplete(1);
  ASSERT_SHARD_REBUILT(1, v3);
}

// Verify the behavior when we abort rebuilding of a shard. The node for which
// the shard is aborted should be removed from the rebuilding set and rebuilding
// should be restarted.
TEST_F(RebuildingCoordinatorTest, ShardAbortRebuilding) {
  std::mt19937 rng(0xbabababa241ef19e);
  num_logs = 20;
  num_shards = 2;

  start();

  // We rebuild N2:S1. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(2, 1, 0 /* flags */);
  RebuildingSet set({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // We rebuild N3:S1. (we are node 0). Both N2:S1 and N3:S1 should be in the
  // rebuilding set.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  RebuildingSet set2({{N2S1, RebuildingNodeInfo(RebuildingMode::RESTORE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set2, getShard(1)->rebuilding_set);

  // Let's abort rebuilding of shard 1 of node 2. Rebuilding set should now
  // contain only N3:S1.
  lsn_t v3 = onShardAbortRebuild(2, 1, v2);
  RebuildingSet set3({{N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set3, getShard(1)->rebuilding_set);
}

// We drain N0:S1 while also rebuilding N3:S1. Check that N0 participates but N3
// does not. Check that N0 waits for a SHARD_UNDRAIN message before acking.
TEST_F(RebuildingCoordinatorTest, DrainOneNodeWhileRebuildAnotherOne) {
  num_logs = 20;
  num_shards = 2;

  start();

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // We rebuild N3:S1. (we are node 0). Both N2:S1 and N3:S1 should be in the
  // rebuilding set.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  RebuildingSet set2({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set2, getShard(1)->rebuilding_set);

  coordinator_->onShardRebuildingComplete(1);
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
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  start();

  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);
  coordinator_->onShardRebuildingComplete(1);
  ASSERT_SHARD_REBUILT(1, v1);

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RESTORE mode (triggered by the FD for instance
  // if N0 was down for some time).
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  // In RESTORE mode, N0 is not a donor so nothing is started.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

  // Let's assume N0 is back up, and because the local data is intact (we set
  // my_shard_has_data_intact=true), RebuildingCoordinator should immediately
  // send a new SHARD_NEEDS_REBUILD to restart rebuilding in RELOCATE mode.
  flags = SHARD_NEEDS_REBUILD_Header::RELOCATE |
      SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
  ASSERT_SHARD_NEEDS_REBUILD(1, flags, v2);

  // The message arrives.
  lsn_t v3 = onShardNeedsRebuild(0, 1, SHARD_NEEDS_REBUILD_Header::RELOCATE);
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // Rebuilding completes.
  coordinator_->onShardRebuildingComplete(1);
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));
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
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  start();

  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);
  coordinator_->onShardRebuildingComplete(1);
  ASSERT_SHARD_REBUILT(1, v1);

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RESTORE mode (triggered by the FD for instance
  // if N0 was down for some time).
  lsn_t v2 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  // In RESTORE mode, N0 is not a donor so nothing is started.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

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
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // Rebuilding completes.
  coordinator_->onShardRebuildingComplete(1);
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
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));
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
// in RELOCATE mode instead of being aborted.
TEST_F(RebuildingCoordinatorTest, RestoreModeAfterDrain) {
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = false;

  start();

  // We start rebuilding N0:S1 in RESTORE mode (we are node 0).
  lsn_t v1 = onShardNeedsRebuild(0, 1, 0 /* flags */, LSN_MAX, false);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  // In RESTORE mode, N0 is not a donor so nothing is started.
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

  // Some nodes complete rebuilding.
  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(4, 1, v1);
  onShardIsRebuilt(2, 1, v1);

  // Rebuilding is restarted in RELOCATE mode (triggered by someone starting
  // a drain) but since we are rebuilding in RESTORE mode, we should preserve
  // it.
  auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;
  lsn_t v2 = onShardNeedsRebuild(0, 1, flags, LSN_MAX, false);
  EXPECT_FALSE(coordinator_->shardRebuildings.count(1));

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
  EXPECT_EQ(set2, getShard(1)->rebuilding_set);

  // Rebuilding completes.
  coordinator_->onShardRebuildingComplete(1);
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
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

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
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

  start();

  // RebuildingCoordinator should have requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);
  ASSERT_NOT_PUBLISHED(1);

  // Receive the message.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  ASSERT_PUBLISHED(1);

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);

  ASSERT_SHARD_ACK_REBUILT(1);
}

// Verify that we do not ack a complete, but non-authoritative rebuilding
// of a dirty shard.
TEST_F(RebuildingCoordinatorTest, DirtyShardNonAuthoritative) {
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
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

  start();

  // RebuildingCoordinator should have requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);
  ASSERT_NOT_PUBLISHED(1);

  // Receive the message.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  ASSERT_PUBLISHED(1);

  // Make sure there is at least one outstanding recoverable shard.
  lsn_t v2 = onShardNeedsRebuild(2, 1, 0, folly::none, true, &rrm);

  onShardIsRebuilt(1, 1, v2, /*is_authoritative*/ false);
  onShardIsRebuilt(3, 1, v2, /*is_authoritative*/ false);

  ASSERT_NO_SHARD_ACK_REBUILT();
}

// Verify that a dirty shard that is draining is left draining
TEST_F(RebuildingCoordinatorTest, DirtyShardDrain) {
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
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

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
  ASSERT_NOT_PUBLISHED(1);
}

// Verify that a dirty shard that was fully rebuilt before the node started
// results in an acked rebuild and the dirty shard info is cleared.
TEST_F(RebuildingCoordinatorTest, DirtyShardAlreadyRebuilt) {
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
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

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

// Verify that dirty ranges are not republished if identical to
// those in the event log and the store has persisted the published flag.
TEST_F(RebuildingCoordinatorTest, ReDirtyShard) {
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  // Store identical RebuildingRangesMetadata locally and in
  // the event log.
  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

  updateConfig();

  // Pre-populate rebuilding set with the dirty info for our shard.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);

  start();

  // RebuildingCoordinator should have re-requested rebuilding of our
  // shard.
  ASSERT_SHARD_NEEDS_REBUILD(
      1, SHARD_NEEDS_REBUILD_Header::TIME_RANGED, LSN_INVALID);
  ASSERT_NOT_PUBLISHED(1);

  // Receive the message.
  lsn_t v2 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);
  ASSERT_GT(v2, v1);
  ASSERT_PUBLISHED(1);
  onShardIsRebuilt(1, 1, v2);
  onShardIsRebuilt(2, 1, v2);
  onShardIsRebuilt(3, 1, v2);

  ASSERT_SHARD_ACK_REBUILT(1);
}

// Verify that dirty ranges are republished, even if identical to
// those in the event log, if the store has cleared the published flag
// (ranges have been re-dirtied).
TEST_F(RebuildingCoordinatorTest, DoNotReDirtyShard) {
  num_logs = 20;
  num_shards = 2;
  my_shard_has_data_intact = true;

  // Store identical RebuildingRangesMetadata locally and in
  // the event log.
  auto now = RecordTimestamp::now();
  auto dirtyStart = RecordTimestamp(now - std::chrono::minutes(10));
  auto dirtyEnd = RecordTimestamp(now - std::chrono::minutes(5));
  RebuildingRangesMetadata rrm;
  rrm.modifyTimeIntervals(TimeIntervalOp::ADD,
                          DataClass::APPEND,
                          RecordTimeInterval(dirtyStart, dirtyEnd));
  rrm.setPublished(true);
  dirty_shard_cache[1] = std::pair(rrm, RebuildingRangesVersion(0, 0));

  updateConfig();

  // Pre-populate rebuilding set with the dirty info for our shard.
  lsn_t v1 = onShardNeedsRebuild(my_node_id.index(),
                                 1,
                                 SHARD_NEEDS_REBUILD_Header::TIME_RANGED,
                                 folly::none,
                                 false,
                                 &rrm);

  start();

  auto it = received.shard_needs_rebuild.find(1);
  ASSERT_TRUE(it == received.shard_needs_rebuild.end());

  onShardIsRebuilt(1, 1, v1);
  onShardIsRebuilt(2, 1, v1);
  onShardIsRebuilt(3, 1, v1);

  ASSERT_SHARD_ACK_REBUILT(1);
}

// If a drain is ongoing and we receive a SHARD_UNDRAIN message, this will
// effectively remove the DRAIN flag and cause RebuildingCoordinator to send a
// SHARD_ABORT_REBUILD. If, however, a new SHARD_NEEDS_REBUILD comes up before
// that SHARD_ABORT_REBUILD is receive, the latter will be discarded.
// We expect that when the new SHARD_NEEDS_REBUILD message is received,
// RebuildingCoordinator will see that rebuilding should be aborted again
// because 1/ the data is intact, 2/ the DRAIN flag is not set.
TEST_F(RebuildingCoordinatorTest, ReproT18780256) {
  num_logs = 20;
  num_shards = 2;

  start();
  setDataIntact(true);

  const auto flags = SHARD_NEEDS_REBUILD_Header::DRAIN;

  // We drain N0:S1. (we are node 0).
  // Logs 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 need to be rebuilt.
  lsn_t v1 = onShardNeedsRebuild(0, 1, flags);
  RebuildingSet set({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)}});
  EXPECT_EQ(set, getShard(1)->rebuilding_set);

  // SHARD_UNDRAIN is received for N0:S1. It should write a
  // SHARD_ABORT_REBUILD message.
  onShardUndrain(0, 1);
  ASSERT_ABORT_FOR_MY_SHARD(1, v1);

  // We start rebuilding N3:S1 before the above SHARD_ABORT_REBUILD gets
  // received. Because the rebuilding version changed, the SHARD_ABORT_REBUILD
  // will get discarded.
  lsn_t v2 = onShardNeedsRebuild(3, 1, 0 /* flags */);
  RebuildingSet set2({{N0S1, RebuildingNodeInfo(RebuildingMode::RELOCATE)},
                      {N3S1, RebuildingNodeInfo(RebuildingMode::RESTORE)}});
  EXPECT_EQ(set2, getShard(1)->rebuilding_set);

  // Verify that RebuildingCoordinator wrote another SHARD_ABORT_REBUILD for its
  // shard 1 after rebuilding version changed.
  ASSERT_ABORT_FOR_MY_SHARD(1, v2);
}

}} // namespace facebook::logdevice
