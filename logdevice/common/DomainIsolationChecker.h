/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <atomic>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/NodeLocation.h"

namespace facebook { namespace logdevice {

/**
 * @file  DomainIsolationChecker checks if a _local_ location-based failure
 *        domain in a specific location scope is _isolated_. A local domain
 *        means that the failure domain determined by the location scope
 *        contains the local cluster node running this object. A domain is
 *        considered isolated if all other cluster nodes OUTSIDE of the domain
 *        are marked as DEAD. This is usually caused by the domain getting
 *        disconnected from the reset of the cluster (e.g., failure of the
 *        top-of-rack swtiches). Being able to detect such failures helps
 *        LogDevice react accordingly to achieve better availability.
 */

class DomainIsolationChecker {
 public:
  /**
   * Initialize the internal state of DomainIsolationChecker. The thread
   * that calls the init() function will be the `main thread` on which
   * majority of functions of the class are required to run.
   *
   * Not called in constructor since virtual methods are called.
   */
  void init();

  /**
   * Check if the local failure domain determined by location scope @param scope
   * is considered isolated.
   *
   * Can be called on any thread.
   *
   * @param scope   location scope that determines the local domain.
   *                Note that ROOT scope is always considered not isolated since
   *                the domain represents the entire cluster.
   */
  bool isMyDomainIsolated(NodeLocationScope scope) const;

  /**
   * Called when a cluster node indexed by @param index is _about_ to change to
   * the DEAD state. Note that the state of the node must still be ALIVE
   * (i.e., isNodeAlive(index) == true) when calling the function. Caller should
   * then mark the node DEAD after calling this function.
   *
   * Must be called on the `main thread` that called init().
   */
  void onNodeDead(node_index_t index);

  /**
   * Similar to onNodeDead(), but called when node is _about_ to change from
   * DEAD to ALIVE.
   *
   * Must be called on the `main thread` that called init().
   */
  void onNodeAlive(node_index_t index);

  /**
   * Called when the cluster configuration is changed, rebuilds the internal
   * state.
   *
   * Must be called on the `main thread` that called init().
   */
  void noteConfigurationChanged();

  /**
   * Check if any of the local failure domains changed state from/to being
   * isolated and send appropriate notification to workers so they can take
   * action
   *
   * Must be called on the `main thread` that called init().
   */
  void processIsolationUpdates();

  /**
   * Output a string containing the isolation status of local domains
   * of each node location scope. For admin commands.
   */
  std::string getDebugInfo() const;

  virtual ~DomainIsolationChecker() {}

 protected:
  ///// functions overridden in tests

  // Return the DEAD/ALIVE state of the cluster node. By default the information
  // is obtained from the failure detector.
  virtual bool isNodeAlive(node_index_t index) const;

  // get cluster nodes configuration
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  // get the node id of the node running this DomainIsolationChecker
  virtual NodeID getMyNodeID() const;

  // get the current worker thread id
  virtual worker_id_t getThreadID() const;

  // check if the local domain in @param scope has all nodes in the
  // cluster. If so, the local domain should not be considered as isolated
  bool localDomainContainsWholeCluster(NodeLocationScope scope) const;

 private:
  // descriptor of a failure domain in a node location scope
  struct DomainInfo {
    // a set of nodes belonging to the _local_ domain
    std::unordered_set<node_index_t> domain_nodes;

    // number of nodes
    int dead_nodes_outside{0};

    // the following two members keep track of updates sent to workers,
    // so that we do not switch between isolated and not isolated too
    // fast.

    // timestamp of the next update notification allowed to be sent to workers
    std::chrono::steady_clock::time_point next_notification_;

    // value of the last isolated boolean sent to workers.
    bool last_isolated_{false};
  };

  // if enabled_ is false, we lack information to make a decision, and
  // isMyDomainIsolated will always return false
  std::atomic<bool> enabled_{false};

  // set of all nodes in the cluster, according to the current configuration
  std::set<node_index_t> nodes_;

  // an array of domain descriptors for each node location scope
  std::array<DomainInfo, NodeLocation::NUM_ALL_SCOPES> scope_info_;

  // an boolean array of _isolated_ status of local failure domains in each
  // location scope. Accessed by multiple threads.
  std::array<std::atomic<bool>, NodeLocation::NUM_ALL_SCOPES> isolated_scopes_;

  // minimum time between two notifications
  std::chrono::milliseconds min_notification_interval_{1000};

  // worker thread id that calls the init() function
  worker_id_t inited_on_{-1};

  // clear and rebuild internal state
  void rebuildState();

  // set the alive state of a node
  void setNodeState(node_index_t index, bool alive);

  // function that implements onNodeDead() and onNodeAlive()
  void onNodeStateChanged(node_index_t index, bool alive);
};

}} // namespace facebook::logdevice
