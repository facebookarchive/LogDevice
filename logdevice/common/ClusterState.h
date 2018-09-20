/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <chrono>
#include <condition_variable>
#include <vector>
#include <list>
#include "logdevice/common/NodeID.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/include/Err.h"

#include <folly/SharedMutex.h>

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
  ALIVE = 0,
  DEAD = 1,
  FAILING_OVER = 2,
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
      case ALIVE:
        return "ALIVE";
      case DEAD:
        return "DEAD";
      case FAILING_OVER:
        return "FAILING_OVER";
    }
    return "UNKNOWN";
  }

  explicit ClusterState(size_t cluster_size, Processor* processor)
      : processor_(processor) {
    resizeClusterState(cluster_size, false);
    last_refresh_.store(std::chrono::steady_clock::duration::zero());
  }

  virtual ~ClusterState() {}

  bool isNodeAlive(node_index_t idx) const {
    return getNodeState(idx) == NodeState::ALIVE;
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
  size_t cluster_size_{0};
  std::atomic<std::chrono::steady_clock::duration> last_refresh_;
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
