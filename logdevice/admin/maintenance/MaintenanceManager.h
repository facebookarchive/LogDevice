/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"
#include "logdevice/admin/maintenance/EventLogWriter.h"
#include "logdevice/admin/maintenance/SafetyCheckScheduler.h"
#include "logdevice/admin/maintenance/SequencerWorkflow.h"
#include "logdevice/admin/maintenance/ShardWorkflow.h"
#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/work_model/SerialWorkContext.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

namespace facebook { namespace logdevice { namespace maintenance {

using NCUpdateResult = folly::Expected<
    std::shared_ptr<const configuration::nodes::NodesConfiguration>,
    Status>;

using SafetyCheckResult = folly::Expected<SafetyCheckScheduler::Result, Status>;
using NodeState = thrift::NodeState;
using ShardState = thrift::ShardState;
using SequencerState = thrift::SequencerState;
using MaintenanceClash = thrift::MaintenanceClash;

/*
 * Dependencies of MaintenanceManager isolated
 * into a separate class for ease of testing
 */
class MaintenanceManagerDependencies {
 public:
  MaintenanceManagerDependencies(
      Processor* processor,
      UpdateableSettings<AdminServerSettings> admin_settings,
      ClusterMaintenanceStateMachine* cluster_maintenance_state_machine,
      EventLogStateMachine* event_log,
      std::unique_ptr<SafetyCheckScheduler> safety_check_scheduler)
      : processor_(processor),
        admin_settings_(std::move(admin_settings)),
        cluster_maintenance_state_machine_(cluster_maintenance_state_machine),
        event_log_state_machine_(event_log),
        safety_check_scheduler_(std::move(safety_check_scheduler)) {}

  virtual ~MaintenanceManagerDependencies() {}

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

  virtual folly::SemiFuture<SafetyCheckResult> postSafetyCheckRequest(
      const ClusterMaintenanceWrapper& maintenance_state,
      const ShardAuthoritativeStatusMap& status_map,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_config,
      const std::vector<const ShardWorkflow*>& shard_wf,
      const std::vector<const SequencerWorkflow*>& seq_wf);

  // calls `update` on the NodesConfigManager
  virtual folly::SemiFuture<NCUpdateResult> postNodesConfigurationUpdate(
      std::unique_ptr<configuration::nodes::StorageConfig::Update>
          shards_update,
      std::unique_ptr<configuration::nodes::SequencerConfig::Update>
          sequencers_update);

  void setOwner(MaintenanceManager* owner);

  EventLogStateMachine* getEventLog();

  // Returns the NodesConfiguration attached to processor_
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  ClusterMaintenanceStateMachine* getStateMachine() const {
    return cluster_maintenance_state_machine_;
  }

  Processor* getProcessor() const {
    return processor_;
  }

  std::shared_ptr<const AdminServerSettings> settings() const {
    return admin_settings_.get();
  }

 private:
  // Handle to processor for getting the NodesConfig
  Processor* processor_;

  UpdateableSettings<AdminServerSettings> admin_settings_;

  // The MaintenanceManager instance this is attached to
  MaintenanceManager* owner_{nullptr};

  // A replicated state machine that tails the maintenance logs
  ClusterMaintenanceStateMachine* cluster_maintenance_state_machine_;

  // A replicated state machine that tails the event log
  EventLogStateMachine* event_log_state_machine_;

  // Scheduler object to schedule safety checks
  std::unique_ptr<SafetyCheckScheduler> safety_check_scheduler_;

  // Subscription handle for ClusterMaintenanceStateMachine.
  // calls the onClusterMaintenanceStateUpdate callback when updated
  // state is available from ClusterMaintenanceStateMachine
  std::unique_ptr<ClusterMaintenanceStateMachine::SubscriptionHandle>
      cms_update_handle_;

  // Subscription handle for ShardAuthoritativeStatus.
  std::unique_ptr<EventLogStateMachine::SubscriptionHandle> el_update_handle_;

  // Subscription handle for NodesConfig update
  std::unique_ptr<ConfigSubscriptionHandle> nodes_config_update_handle_;
};

/*
 * MaintenanceManager is the entity that manages all
 * the maintenances on a cluster. The main task of cluster maintenance
 * manager is to evaluate the maintenances in ClusterMaintenanceStateWrapper
 * and create and run workflows and schedule safety checks and request
 * NodesConfig updates. This class is not thread-safe and any work that
 * needs to happen should be added to the work context
 */
class MaintenanceManager : public SerialWorkContext {
 public:
  MaintenanceManager(folly::Executor* executor,
                     std::unique_ptr<MaintenanceManagerDependencies> deps);

  virtual ~MaintenanceManager();

  /**
   * Schedules work on this object to call `startInternal`
   */
  void start();

  /**
   * Schedules work on this object to call `stopInternal`
   *
   * @return A future whose value will be avilable
   *         once maintenance manager has truly stopped
   */
  folly::SemiFuture<folly::Unit> stop();

  // Getters

  /*
   * This returns a clone of the latest in-memory ClusterMaintenanceState from
   * the replicated state machine. This state is augmented with information from
   * the state of maintenance manager. We are particularly interested in setting
   * the last check impact state.
   *
   *    Status can be set to:
   *      E::NOTREADY if we don't have a state or the state machine is not
   *      running
   *      E::NOBUFS if we cannot enqueue work on the workers.
   *      E::SHUTDOWN if processor is shutting down
   */
  folly::SemiFuture<
      folly::Expected<std::vector<MaintenanceDefinition>, MaintenanceError>>
  getLatestMaintenanceState();

  /**
   * Copies the maintenances of the input and augment the safety check results
   * and other progress information wherever we have a valid value for it.
   */
  folly::SemiFuture<std::vector<MaintenanceDefinition>>
  augmentWithProgressInfo(std::vector<MaintenanceDefinition> input);

  // Getter that returns a SemiFututre with NodeState for a given node
  folly::SemiFuture<folly::Expected<NodeState, Status>>
  getNodeState(node_index_t node);

  /*
   * Takes a filter, it will match the nodes, combine the results
   * and get the state from the maintenance manager.
   */
  folly::SemiFuture<
      folly::Expected<thrift::NodesStateResponse, MaintenanceError>>
  getNodesState(thrift::NodesFilter filter);

  // Getter that returns a SemiFuture with ShardState for a given shard
  folly::SemiFuture<folly::Expected<ShardState, Status>>
  getShardState(ShardID shard);
  // Getter that returns a SemiFuture with SequencerState for a given node
  folly::SemiFuture<folly::Expected<SequencerState, Status>>
  getSequencerState(node_index_t node);
  // Getter that returns a SemiFuture with ShardOperationalState for the given
  // shard
  folly::SemiFuture<folly::Expected<ShardOperationalState, Status>>
  getShardOperationalState(ShardID shard);
  // Getter that returns a SemiFuture with ShardDataHealth for the given shard
  folly::SemiFuture<folly::Expected<ShardDataHealth, Status>>
  getShardDataHealth(ShardID shard);
  // Getter that returns a SemiFuture with SequencingState for the gievn node
  folly::SemiFuture<folly::Expected<SequencingState, Status>>
  getSequencingState(node_index_t node);
  // Getter that returns a SemiFuture with StorageState for a shard from
  // NodesConfig
  folly::SemiFuture<folly::Expected<membership::StorageState, Status>>
  getStorageState(ShardID shard);
  // Getter that returns a SemiFuture with MetaDataStorageState for a shard from
  // NodesConfig
  folly::SemiFuture<folly::Expected<membership::MetaDataStorageState, Status>>
  getMetaDataStorageState(ShardID shard);
  // Getter that returns a SemiFuture with the shard's target operational state
  folly::SemiFuture<
      folly::Expected<folly::F14FastSet<ShardOperationalState>, Status>>
  getShardTargetStates(ShardID shard);
  // Getter that returns a SemiFuture with node's target sequencing state
  folly::SemiFuture<folly::Expected<SequencingState, Status>>
  getSequencerTargetState(node_index_t node_index);

  // Callback that gets called when there is a new update from the
  // ClusterMaintenanceStateMachine. Schedules work on this object to
  // update `cluster_maintenance_state_` and calls `scheduleRun`
  void onClusterMaintenanceStateUpdate(ClusterMaintenanceState state,
                                       lsn_t version);

  // Subscription callback for ClusterMembership update
  // Schedules work on this object to call `scheduleRun`
  void onNodesConfigurationUpdated();

  // Callback that gets called when there is a EventLogRebuildingSet
  // update. Schedules work on this object to update
  // `event_log_rebuilding_set_` and calls `scheduleRun`
  void onEventLogRebuildingSetUpdate(EventLogRebuildingSet set, lsn_t version);

  enum class MMStatus {
    // MaintenanceManager has been started
    // all necessary subscrption and is waiting
    // for initial state to be available
    STARTING = 0,
    // MaintenanceManager is completed a run of evaluation
    // and is now waiting for a state change in form of
    // ClusterMaintenanceState, EventLogRebuildingSet or
    // NodesConfiguration update
    AWAITING_STATE_CHANGE,
    // MaintenanceManager is currently running the active
    // workflows and waiting on results of run
    RUNNING_WORKFLOWS,
    // MaintenanceManager is waiting for a callback
    // after posting a NC update
    AWAITING_NODES_CONFIG_UPDATE,
    // MaintenanceManager is waiting for results after
    // scheduling a safety check
    AWAITING_SAFETY_CHECK_RESULTS,
    // MaintenanceManager stop was called. If we
    // are still in middle of evaluation chain, we will
    // not interrupt but evaluation will terminate once
    // future is complete
    STOPPING,
    // MaintenanceManager has been stopped. Its safe
    // to destroy this object
    STOPPED,
    // MaintenanceManager object has been created but
    // not started yet
    NOT_STARTED
  };

  // Returns the current status_
  // Useful for testing
  folly::SemiFuture<MMStatus> getStatus();

  /**
   * Returns the WorkerType that this state machine should be running on
   */
  static WorkerType workerType(Processor* processor) {
    // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
    // on whether we have background workers.
    if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
      return WorkerType::BACKGROUND;
    }
    return WorkerType::GENERAL;
  }

 protected:
  // Used only in tests
  ShardWorkflow* FOLLY_NULLABLE getActiveShardWorkflow(ShardID shard) const;
  SequencerWorkflow* FOLLY_NULLABLE
  getActiveSequencerWorkflow(node_index_t node) const;

 private:
  std::unique_ptr<MaintenanceManagerDependencies> deps_;

  // Set to true every time we get one of the subscription
  // callback indicating the need to call `evaluate()`
  bool run_evaluate_{false};

  // State delivered by the ClusterMaintenanceStateMachine
  std::unique_ptr<ClusterMaintenanceState> cluster_maintenance_state_;

  // Version of the last ClusterMaintenanceState delivered
  lsn_t last_cms_version_{LSN_INVALID};

  // State delivered by the EventLogStateMachine
  std::unique_ptr<EventLogRebuildingSet> event_log_rebuilding_set_;

  // Version of the last EventLogRebuildingSet delivered
  lsn_t last_ers_version_{LSN_INVALID};

  /**
   * Starts the dependencies and sets up internal fields
   */
  void startInternal();

  /**
   * Stops the subscriptions and updates the status_
   */
  void stopInternal();

  /**
   * Getter that returns NodeState for a given shard
   *
   * @param   node_index_t Index of the node for which to get the NodeState
   * @param   ClusterState* A pointer to cluster state that hold gossip
   *          information.
   * @return  folly::Expected<NodeState, Status> Valid NodeState if node is
   *          in NodesConfig. Status can be E::NOTREADY if EventLogRebuildingSet
   *          or ClusterMaintenanceWrapper is not initialized or E::NOTFOUND if
   *          node or its shards are not in the NodesConfig
   */
  folly::Expected<NodeState, Status>
  getNodeStateInternal(node_index_t node,
                       const ClusterState* cluster_state) const;
  /**
   * Getter that returns ShardState for a given shard
   *
   * @param   shard ShardID for which to get ShardState
   * @return  folly::Expected<ShardState, Status> If shard is found,
   *          valid ShardState. Otherwise Status can be E::NOTFOUND, if
   *          shard is not in config or E::NOTREADY is
   *          ClusterMaintenanceWrapper is not initialized
   */
  folly::Expected<ShardState, Status>
  getShardStateInternal(ShardID shard) const;
  // Getter that returns SequencerState for a given node
  SequencerState getSequencerStateInternal(node_index_t node) const;
  // Getter that returns ShardOperationalState for the given shard
  folly::Expected<ShardOperationalState, Status>
  getShardOperationalStateInternal(ShardID shard) const;
  /**
   * Returns the calculated maintenance progress for a given maintenance. This
   * will return MaintenanceProgress::UNKNOWN in case we cannot determine
   * the progress.
   *
   * This will return COMPLETED iff, all shards and sequencers in the
   * maintenance have reached the target state, and this maintenance is
   * already considered in the existing in-memory workflows.
   *
   * The maintenance is considered BLOCKED_UNTIL_SAFE if any of the shards or
   * sequencers are blocked on not on the target state already.
   *
   * Otherwise, the maintenance is considered IN_PROGRESS.
   */
  thrift::MaintenanceProgress
  getMaintenanceProgressInternal(const MaintenanceDefinition& def) const;
  /**
   * Getter that returns ShardDataHealth for the given shard
   *
   * @param   shard ShardID for which to get ShardDataHealth
   * @return  folly::Expected<ShardDataHealth, Status>
   *          ShardDataHealth or E::NOTREADY if EventLogRebuildingSet
   *          is not initalized
   */
  folly::Expected<ShardDataHealth, Status>
  getShardDataHealthInternal(ShardID shard) const;
  // Getter that returns SequencingState for the gievn node
  SequencingState getSequencingStateInternal(node_index_t node) const;
  /**
   * Getter that returns StorageState for a shard from NodesConfig
   *
   * @param   shard ShardID for which to get the StorageState from NodesConfig
   * @return  membership::StorageState if shard exists in NodesConfig, otherwise
   *          Status is set to E::NOTFOUND
   */
  folly::Expected<membership::StorageState, Status>
  getStorageStateInternal(ShardID shard) const;
  /**
   * Getter that returns MetaDataStorageState for a shard from
   * NodesConfig
   * @param   shard ShardID for which to get the MetadataStorageSTate from
   * NodesConfig
   * @return  folly::Expected<membership::MetaDataStorageState, Status>
   *          membership::MetaDataStorageState if shard exists in NodesConfig,
   * otherwise Status is set to E::NOTFOUND
   */
  folly::Expected<membership::MetaDataStorageState, Status>
  getMetaDataStorageStateInternal(ShardID shard) const;
  /**
   * Getter that returns the shard's target operational state
   *
   * @param   shard ShardID for which to get the target states
   * @return  folly::Expected<folly::F14FastSet<ShardOperationalState>, Status>
   *          Set of ShardOperationalState for shard if
   *          ClusterMaintenanceWrapper is initialized. Otherwise Status is
   *          E::NOTREADY
   */
  folly::Expected<folly::F14FastSet<ShardOperationalState>, Status>
  getShardTargetStatesInternal(ShardID shard) const;
  /**
   * Getter that returns node's target sequencing state
   *
   * @param   node_index Index of the node for which to get target sequencer
   *                   state
   * @return  folly::Expected<SequencingState, Status> SequencingState from
   *          ClusterMaintenanceWrapper if it is initialized. Otherwise Status
   *          is set to E::NOTREADY
   */
  folly::Expected<SequencingState, Status>
  getSequencerTargetStateInternal(node_index_t node_index) const;
  /**
   * Getter that returns the latest result of safety check for a
   * given GroupID
   *
   * @return folly::Expected<Imapct, Status>
   *         Impact for the GroupID. Status is set to E::NOTREADY if
   *         MaintenanceManager is not initialized, E::NOTFOUND if
   *         the group id does not exist in the ClusterMaintenanceWrapper
   */
  folly::Expected<Impact, Status>
  getLatestSafetyCheckResultInternal(GroupID id) const;

  // Returns current value of `status_`
  MMStatus getStatusInternal() const;

  // Sets `run_evaluate_` to true
  void scheduleRun();

  // The main method that creates a chain of functions executed in
  // sequence.
  // 1. updates ClientMaintenanceWrapper
  // 2. Creates and runs ShardWorkflows
  // 3. Creates and runs SequencerWorkflows
  // 4. Once all workflows return, schedules NodesConfig updates
  // 5. Once NodesConfig updates complete, schedules a safety check
  // 6. Once Safety check completes, call `evaluate` again
  // If at any point in this sequence, running_ is set to false,
  // stops executing further in the chain
  void evaluate();

  // Updates the `client_maintenance_wrapper_` if the
  // version is stale
  void updateClientMaintenanceStateWrapper();

  // Iterates over all shards/nodes in the current
  // NodesConfig and creates shard/sequencer workflows
  void createWorkflows();

  // Returns true if given shard is enabled in current state
  // A shard is considered enabled if its storageState is READ_WRITE
  // and has no full rebuilding accoring to EventLogRebuildingSet
  bool isShardEnabled(const ShardID& shard);

  // Returns true if given node has Sequencing enabled in the
  // current `nodes_config_`
  bool isSequencingEnabled(node_index_t node) const;

  // Calls `run` on active_shard_workflows_
  virtual std::pair<std::vector<ShardID>,
                    std::vector<folly::SemiFuture<MaintenanceStatus>>>
  runShardWorkflows();

  // A helper method that gets called when all futures
  // returned by `runShardWorkflows` completes
  void processShardWorkflowResult(
      const std::vector<ShardID>& shards,
      const std::vector<folly::Try<MaintenanceStatus>>& status);

  // Set to true if there are shards that need to be enabled.
  // Used to short circuit evaluation so that we can finish
  // enabling shards before running safety checks
  bool has_shards_to_enable_;

  // Returns the transition expected by shard workflow
  virtual membership::StorageStateTransition
  getExpectedStorageStateTransition(ShardID shard);

  // Returns the required conditions for given shard and transition
  membership::StateTransitionCondition
  getCondition(ShardID shard, membership::StorageStateTransition transition);

  // Remove the corresponding workflow from `active_shard_workflows_`
  void removeShardWorkflow(ShardID shard);

  // calls `run` on active_sequencer_workflows_
  virtual std::pair<std::vector<node_index_t>,
                    std::vector<folly::SemiFuture<MaintenanceStatus>>>
  runSequencerWorkflows();

  // A helper method that gets called when all futures
  // returned by `createAndRunSequencerWorkflows` completes
  void processSequencerWorkflowResult(
      const std::vector<node_index_t>& nodes,
      const std::vector<folly::Try<MaintenanceStatus>>& status);

  // Removes the corresponging sequencer workflow from
  // `active_sequencer_workflows_`
  void removeSequencerWorkflow(node_index_t node);

  // Schedule NodesConfiguration update for any workflow whose
  // status is AWAITING_NODES_CONFIG_CHANGES. Will look up shards
  // in active_shard_workflows_ to determine the transtition that
  // is to be requested
  folly::SemiFuture<NCUpdateResult> scheduleNodesConfigUpdates();

  // Schedule Safety check for any workflow whose status is
  // AWAITING_SAFETY_CHECK
  folly::SemiFuture<SafetyCheckResult> scheduleSafetyCheck();

  // A helper method that looks at the safety check results
  // and updates the status of workflows accordingly
  void processSafetyCheckResult(SafetyCheckScheduler::Result result);

  // Indicates the current status of this MaintenanceManager
  MMStatus status_{MMStatus::NOT_STARTED};

  // A map of shard to the currently running maintenance worlflow
  folly::F14NodeMap<
      ShardID,
      std::pair<std::unique_ptr<ShardWorkflow>, MaintenanceStatus>,
      ShardID::Hash>
      active_shard_workflows_;

  // A map of node to the currently running sequencer maintenance worlflow
  folly::F14NodeMap<
      node_index_t,
      std::pair<std::unique_ptr<SequencerWorkflow>, MaintenanceStatus>>
      active_sequencer_workflows_;

  // The current ClusterMaintenanceWrapper generated from the last known
  // ClusterManintenanceState and NodesConfiguration. Gets updated in
  // evaluate method if the version of current ClusterMaintenanceState
  // and the one in wrapper are different
  std::unique_ptr<ClusterMaintenanceWrapper> cluster_maintenance_wrapper_;

  // Copy of NodesConfiguration. Updated everytime `evaluate` is called or
  // as part of the NodesConfiguration update callback when MM requested
  // NC updates are successfully applied. We have a local copy here so that
  // Maintenancemanager does not have to wait for the update to be propograted
  // through subscription callback. Note that this copy could diverge from
  // what is in processor until `evaluate` called again
  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_config_;

  // Latest safety check results
  // Updated every time `evaluate` is called
  folly::F14NodeMap<GroupID, Impact> unsafe_groups_;

  folly::Promise<folly::Unit> shutdown_promise_;

  // A timer that when fired calls `scheduleRun`
  // This is useful for triggering reevaluation when no state
  // changes occur (ex: We kicked off passive drain earlier and
  // metadata has now been trimmed but there is no new state change
  // to trigger the re-evaluation)
  std::unique_ptr<Timer> reevaluation_timer_;

  virtual void activateReevaluationTimer();

  virtual void cancelReevaluationTimer();

  // Returns true if status_ is STOPPING
  bool shouldStopProcessing();

  // Fulfills the shutdown_promise_ indicating
  // shutdown of MaintenanceManager is complete
  void finishShutdown();

  // Lazily created during call to getEventLogWriter
  std::unique_ptr<EventLogWriter> event_log_writer_;

  // Returns the event_log_writer_;
  EventLogWriter* getEventLogWriter();

  // Returns the RebuildingMode if shard is in rebuilding set
  // otherwise returns RebuildingMode::INVALID
  virtual RebuildingMode getCurrentRebuildingMode(ShardID shard);

  // Answers the question whether the current operational state of a shard meets
  // a target maintenance or not.
  static bool isTargetAchieved(ShardOperationalState current,
                               ShardOperationalState target);
  // Checks if this maintenance was marked unsafe by the last safety check run
  // or not.
  bool isMaintenanceMarkedUnsafe(const GroupID& id) const;

  bool isBootstrappingCluster() const;

  friend class MaintenanceManagerTest;
};

}}} // namespace facebook::logdevice::maintenance
