/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <memory>
#include <numeric>
#include <set>
#include <vector>

#include <folly/SharedMutex.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

/**
 * @file ClusterState provides an interface to query whether a node is dead or
 * alive. It maintains a list of dead nodes that is updated either by the
 * Failure Detector on servers or by querying servers on clients.
 */

namespace facebook { namespace logdevice {

class GetClusterStateRequest;
class Processor;

/* States of a node maintained by ClusterState */
enum ClusterStateNodeState : uint8_t {
  FULLY_STARTED = 0,
  DEAD = 1,
  FAILING_OVER = 2,
  STARTING = 3,
};
/* Type of callbacks used for node state changes subscriptions */
struct ClusterStateSubscriptionList {
  using UpdateCallback =
      std::function<void(node_index_t nid, ClusterStateNodeState state)>;
  std::list<UpdateCallback> list;
  using iterator = decltype(list)::iterator;
};

class ClusterState {
 public:
  using NodeState = ClusterStateNodeState;
  using UpdateCallback = ClusterStateSubscriptionList::UpdateCallback;
  using RawSubscriptionHandle = ClusterStateSubscriptionList::iterator;
  class SubscriptionHandle {
   public:
    explicit SubscriptionHandle() : handle_() {}
    explicit SubscriptionHandle(RawSubscriptionHandle handle)
        : handle_(handle) {}

   private:
    RawSubscriptionHandle handle_;

    friend class ClusterState;
  };

  static const char* getNodeStateString(NodeState state) {
    switch (state) {
      case FULLY_STARTED:
        return "FULLY_STARTED";
      case DEAD:
        return "DEAD";
      case FAILING_OVER:
        return "FAILING_OVER";
      case STARTING:
        return "STARTING";
    }
    return "UNKNOWN";
  }

  explicit ClusterState(
      size_t cluster_size,
      Processor* processor,
      const configuration::nodes::ServiceDiscoveryConfig& sd_config)
      : processor_(processor) {
    resizeClusterState(cluster_size, false);
    updateNodesInConfig(sd_config);
  }

  virtual ~ClusterState() {}

  inline bool isNodeInConfig(node_index_t idx) const {
    folly::SharedMutex::ReadHolder read_lock(mutex_);
    return nodes_in_config_.count(idx) > 0;
  }

  static inline bool isAliveState(NodeState state) {
    return state == NodeState::FULLY_STARTED || state == NodeState::STARTING;
  }

  bool isNodeAlive(node_index_t idx) const {
    return isAliveState(getNodeState(idx));
  }

  bool isNodeFullyStarted(node_index_t idx) const {
    return getNodeState(idx) == NodeState::FULLY_STARTED;
  }

  bool isNodeStarting(node_index_t idx) const {
    return getNodeState(idx) == NodeState::STARTING;
  }

  NodeState getNodeState(node_index_t idx) const {
    folly::SharedMutex::ReadHolder read_lock(mutex_);
    // check the node list if the index falls within what we know
    // otherwise fall back to considering the node dead. if the index is beyond
    // the size of this list, it means this node is not yet in our configuration
    // anyway.
    ld_check(idx >= 0);
    if (idx < cluster_size_) {
      return node_state_list_[idx].load();
    }
    return NodeState::DEAD;
  }

  const char* getNodeStateAsStr(node_index_t idx) const {
    return getNodeStateString(getNodeState(idx));
  }

  /**
   * @return Id of the first node seen as alive. Used to determine which node
   * should perform some actions such as trim the event log.
   */
  node_index_t getFirstNodeAlive() const;

  void setNodeState(node_index_t idx, NodeState state);

  /**
   * Asynchronously updates this ClusterState object.
   * Executes a GetClusterStateRequest.
   */
  virtual void refreshClusterStateAsync();

  void onGetClusterStateDone(Status status,
                             const std::vector<uint8_t>& nodes_state,
                             std::vector<node_index_t> boycotted_nodes);

  void noteConfigurationChanged();
  void updateNodesInConfig(const configuration::nodes::ServiceDiscoveryConfig&);

  void shutdown() {
    folly::SharedMutex::WriteHolder write_lock(shutdown_mutex_);
    shutdown_.store(true);
  }

  /**
   * Registers a callback to be called when a node state changes. Callbacks are
   * maintained per-worker and are guaranteed to be called in the same worker
   * that created them.
   *
   * @return handle that may be used to unsubscribe
   */
  SubscriptionHandle subscribeToUpdates(UpdateCallback callback);

  /**
   * Deregisters given subscription
   */
  void unsubscribeFromUpdates(SubscriptionHandle subs);

  bool isNodeBoycotted(node_index_t idx) const;
  void setBoycottedNodes(std::vector<node_index_t> boycotts);

  void waitForRefresh();

 private:
  size_t getClusterSize() {
    folly::SharedMutex::ReadHolder read_lock(mutex_);
    return cluster_size_;
  }

  void resizeClusterState(size_t new_size, bool notifySubscribers);

  void postUpdateToWorkers(node_index_t node_id, NodeState state);

  void notifyRefreshComplete();

  folly::SharedMutex mutex_;
  std::unique_ptr<std::atomic<NodeState>[]> node_state_list_ { nullptr };
  std::unordered_set<node_index_t> nodes_in_config_;
  size_t cluster_size_{0};
  std::atomic<std::chrono::steady_clock::duration> last_refresh_{
      std::chrono::steady_clock::duration::zero()};
  std::atomic<bool> refresh_in_progress_{false};
  std::mutex cv_mutex_;
  std::condition_variable cv_;

  Processor* processor_;
  std::atomic<bool> shutdown_{false};
  folly::SharedMutex shutdown_mutex_;
  FastUpdateableSharedPtr<std::vector<node_index_t>> boycotted_nodes_{
      std::make_shared<std::vector<node_index_t>>()};
};
}} // namespace facebook::logdevice
