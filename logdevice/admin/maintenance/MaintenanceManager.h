/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Map.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"
#include "logdevice/admin/maintenance/SafetyCheckScheduler.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/event_log/EventLogWriter.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/work_model/SerialWorkContext.h"

namespace facebook { namespace logdevice { namespace maintenance {

class MaintenanceManager;
class ShardWorkflow;
class SequencerWorkflow;

/*
 * Dependencies of MaintenanceManager isolated
 * into a separate class for ease of testing
 */
class MaintenanceManagerDependencies {
 public:
  MaintenanceManagerDependencies(
      UpdateableSettings<AdminServerSettings> settings,
      ClusterMaintenanceStateMachine* cluster_maintenance_state_machine,
      EventLogStateMachine* event_log)
      : settings_(settings),
        cluster_maintenance_state_machine_(cluster_maintenance_state_machine),
        event_log_state_machine_(event_log) {}

  class ShardAuthoritativeStateUpdateHandle
      : public ShardAuthoritativeStatusSubscriber {
   public:
    explicit ShardAuthoritativeStateUpdateHandle(
        MaintenanceManagerDependencies* dep)
        : dep(dep) {}

    void onShardStatusChanged() override {
      dep->onShardStatusChanged();
    }
    MaintenanceManagerDependencies* dep;
  };

  virtual ~MaintenanceManagerDependencies();

  // Sets up all necessary subscription handles
  virtual void startSubscription();
  // Resets subscription handles
  virtual void stopSubscription();

  /*
   * Posts safety check request
   *
   * @param shard_wf Set of references to ShardWorkflow for
   *                 the shards for which we need to run safety check
   * @param seq_wf   Set of references to SequencerWorkflow for
   *                 the sequencer nodes for which we need to run safety check
   *
   * @return folly::SemiFuture<SafetyCheckResult> A future whose promise
   * is fulfiled once we have results for all workflows
   */

  virtual folly::SemiFuture<SafetyCheckScheduler::Result>
  postSafetyCheckRequest(const std::set<const ShardWorkflow&> shard_wf,
                         const std::set<const SequencerWorkflow&> seq_wf);

  using NodesConfigurationUpdateResult = std::tuple<
      Status,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>>;

  // calls `update` on the NodesConfigManager
  virtual folly::SemiFuture<NodesConfigurationUpdateResult>
  postNodesConfigurationUpdate(
      const folly::F14NodeMap<ShardID, membership::ShardState::Update>&
          shard_update,
      const folly::F14NodeMap<NodeID, membership::SequencerNodeState::Update>&
          sequencer_update);

  virtual void writeToEventLog(std::unique_ptr<EventLogRecord> event);

  virtual void setOwner(MaintenanceManager* owner) {
    owner_ = owner;
  }

 private:
  MaintenanceManager* owner_;

  UpdateableSettings<AdminServerSettings> settings_;

  // A replicated state machine that tails the maintenance logs
  const ClusterMaintenanceStateMachine* cluster_maintenance_state_machine_;

  // A replicated state machine that tails the event log
  const EventLogStateMachine* event_log_state_machine_;


      // Subscription handle for ClusterMaintenanceStateMachine.
      // calls the onClusterMaintenanceStateUpdate callback when updated
      // state is available from ClusterMaintenanceStateMachine
      std::unique_ptr<ClusterMaintenanceStateMachine::SubscriptionHandle>
          cms_update_handle_;

  // Subscription handle for Settings
  UpdateableSettings<AdminServerSettings>::SubscriptionHandle settings_handle_;

  // Subscription handle for ShardAuthoritativeStatus.
  std::unique_ptr<ShardAuthoritativeStateUpdateHandle> sas_update_handle_;

  std::unique_ptr<EventLogWriter> event_log_writer_;

  // TODO: This will be a handle to the safety check scheduler
  std::unique_ptr<SafetyCheckScheduler> safety_check_scheduler_;

  // Callback that gets called when there is a new update from the
  // ClusterMaintenanceStateMachine.
  // sets `run_evaluate_` on owner to signal the need to run `evaluate` again
  void
  onClusterMaintenanceStateUpdate(const thrift::ClusterMaintenanceState& state,
                                  const MaintenanceDelta* delta,
                                  lsn_t version);

  // Subscription callback for ClusterMembership
  // sets `run_evaluate_` on owner to signal the need to run `evaluate` again
  void onNodesConfigurationUpdated();

  // Callback that gets called when there is a new ShardAuthoritativeStatus
  // update.
  // sets `run_evaluate_` on owner to signal the need to run `evaluate` again
  void onShardStatusChanged();

  // Subscription callback for Settings update.
  // Starts/Stops the MaintenanceManager depending on settings
  void onSettingsUpdated();

  virtual const ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap();
};

/*
 * MaintenanceManager is the entity that manages all
 * the maintenances on a cluster. The main task of cluster maintenance
 * manager is to evaluate the maintenances in ClusterMaintenanceStateWrapper
 * and create and run workflows and schedule safety checks and request
 * NodesConfig updates
 */
class MaintenanceManager : public SerialWorkContext {
 public:
  MaintenanceManager(UpdateableSettings<Settings> settings,
                     std::unique_ptr<MaintenanceManagerDependencies> deps);

  virtual ~MaintenanceManager();

  /**
   * Starts the dependencies and sets up internal fields
   */
  void start();

  /**
   * Sets shutting_down_ to true which signals the evaluate method
   * to terminate. Promise will be fulfiled once evaluate terminates
   * and all internal fields reset
   */
  folly::SemiFuture<bool> stop();

  // Set to true every time we get one of the subscription
  // callback indicating the need to call `evaluate()`
  std::atomic<bool> run_evaluate_{false};

  // Returns the NodeMaintenanceState for given node
  using NodeState = thrift::NodeState;
  NodeState getNodeMaintenanceState(NodeID node);

  // Return the ShardMaintenanceState for the given shard
  using ShardState = thrift::ShardState;
  ShardState getShardMaintenanceState(ShardID shard);

  // Getters

  // Getter that returns ShardOperationalState for the given shard
  ShardOperationalState getShardOperationalState(ShardID shard) const;
  // Getter that returns ShardDataHealth for the given shard
  ShardDataHealth getShardDataHealth(ShardID shard) const;
  // Getter that returns SequencingState for the gievn node
  SequencingState getSequencingState(node_index_t node) const;
  // Getter that returns the NodesConfiguration
  virtual const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
  getNodesConfiguration() const;
  // Getter that returns the StorageState for a shard
  membership::StorageState getStorageState(ShardID shard);

 protected:
  // Used only in tests
  ShardWorkflow* getActiveShardWorkflow(ShardID shard);
  SequencerWorkflow* getActiveSequencerWorkflow(NodeID node);

 private:
  std::unique_ptr<MaintenanceManagerDependencies> deps_;

  bool started_{false};

  // Set when `stop()` is called. Signals evaluate
  // method to terminate and call shutdown
  std::atomic<bool> shutting_down_{false};

  // Promise that is fulfiled at end of shutdown() method
  // indicating MaintenanceManager has been stopped
  folly::SharedPromise<bool> stop_promise_;

  // The main method that evaluate the current ClusterMaintenanceStateWrapper,
  // creates and runs workflows and requests NodesConfiguration update
  // and safety checks
  void evaluate();

  // Reset all internal fields and fulfils the stop_promise_
  void shutdown();

  // A map of shard to the currently running maintenance worlflow
  folly::F14NodeMap<ShardID, std::unique_ptr<ShardWorkflow>>
      active_shard_workflow_;

  // A map of node to the currently running sequencer maintenance worlflow
  folly::F14NodeMap<NodeID, std::unique_ptr<SequencerWorkflow>>
      active_sequencer_workflow_;
};

}}} // namespace facebook::logdevice::maintenance
