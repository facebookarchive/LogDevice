/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClusterState.h"

#include <folly/String.h>

#include "logdevice/common/ClusterStateUpdatedRequest.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * Maximum number of cluster state subscriptions. This is a soft limit,
 * the code will only log a warning if it goes beyond this value. in which
 * case we might need to consider different approach regarding callback
 * execution
 */
static const int MAX_SUBSCRIPTIONS_PER_WORKER = 32;

/**
 * Internal request used to propagate node state changes to workers that
 * have subscribed.
 * This request is executed in the context of worker that has subscribed and
 * calls all the registered callbacks of that particular worker.
 */
class NodeStateUpdatedRequest : public Request {
 public:
  explicit NodeStateUpdatedRequest(worker_id_t wid,
                                   node_index_t nid,
                                   ClusterState::NodeState state)
      : Request(RequestType::NODE_STATE_UPDATED),
        worker_id_(wid),
        node_id_(nid),
        state_(state) {}

  int getThreadAffinity(int) override {
    return worker_id_.val();
  }

  Execution execute() override {
    auto w = Worker::onThisThread();
    // execute all the callbacks that this worker has.
    // the number of subscriptions is expected to be very low.
    // if that assumption changes, we can revisit this loop.
    for (auto& cb : w->clusterStateSubscriptions().list) {
      cb(node_id_, state_);
    }
    return Execution::COMPLETE;
  }

 private:
  worker_id_t worker_id_{-1};
  node_index_t node_id_{-1};
  ClusterState::NodeState state_{ClusterState::NodeState::FULLY_STARTED};
};

/**
 * Inserts a callback to the subscription list of the current worker
 * and inserts its worker id to the list of subscribers if not present already
 */
ClusterState::SubscriptionHandle
ClusterState::subscribeToUpdates(UpdateCallback callback) {
  auto w = Worker::onThisThread();
  auto handle = w->clusterStateSubscriptions().list.insert(
      w->clusterStateSubscriptions().list.end(), callback);

  auto count = w->clusterStateSubscriptions().list.size();
  if (count > MAX_SUBSCRIPTIONS_PER_WORKER) {
    ld_warning("The number of cluster state subscriptions (%zu) for "
               " worker %s is greater than %d",
               count,
               w->getName().c_str(),
               MAX_SUBSCRIPTIONS_PER_WORKER);
  }

  return ClusterState::SubscriptionHandle(handle);
}

/**
 * Removes callback from current worker's subscription list and removes its
 * worker id from list of subscribers if there is no more callbacks
 */
void ClusterState::unsubscribeFromUpdates(
    ClusterState::SubscriptionHandle subs) {
  auto w = Worker::onThisThread();
  w->clusterStateSubscriptions().list.erase(subs.handle_);
}

bool ClusterState::isNodeBoycotted(node_index_t idx) const {
  const auto boycotts = boycotted_nodes_.get();
  return std::find(boycotts->begin(), boycotts->end(), idx) != boycotts->end();
}

void ClusterState::setBoycottedNodes(std::vector<node_index_t> boycotts) {
  boycotted_nodes_.update(
      std::make_shared<std::vector<node_index_t>>(std::move(boycotts)));
}

node_index_t ClusterState::getFirstNodeAlive() const {
  folly::SharedMutex::ReadHolder read_lock(mutex_);
  folly::Optional<node_index_t> first_node;

  for (node_index_t nid = 0; nid < cluster_size_; nid++) {
    if (isNodeInConfig(nid)) {
      if (isNodeAlive(nid)) {
        return nid;
      } else if (!first_node.hasValue()) {
        first_node = nid;
      }
    }
  }

  if (!first_node.hasValue()) {
    RATELIMIT_WARNING(
        std::chrono::seconds{5}, 1, "No node in service discovery config.");
    first_node = 0;
  }

  // If all nodes are seen as dead, return first node.
  return first_node.value();
}

void ClusterState::postUpdateToWorkers(node_index_t node_id, NodeState state) {
  // for each worker, execute a NodeStateUpdatedRequest in
  // the context of that worker
  processor_->applyToWorkers([node_id, state](Worker& w) {
    std::unique_ptr<Request> req =
        std::make_unique<NodeStateUpdatedRequest>(w.idx_, node_id, state);
    w.processor_->postRequest(req);
  });
}

void ClusterState::setNodeState(node_index_t idx,
                                ClusterState::NodeState state) {
  ld_check(idx >= 0);

  folly::SharedMutex::ReadHolder read_lock(mutex_);
  if (idx < cluster_size_) {
    ClusterState::NodeState prev = node_state_list_[idx].exchange(state);
    if ((prev != state) && processor_) {
      if (!processor_->settings()->server) {
        // this is a client stats
        WORKER_STAT_INCR(client.cluster_state_updates);
      }
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "State of N%d changed from %s to %s",
                     idx,
                     getNodeStateString(prev),
                     getNodeStateString(state));

      postUpdateToWorkers(idx, state);
    }
  }
}

void ClusterState::onGetClusterStateDone(
    Status status,
    const std::vector<uint8_t>& nodes_state,
    std::vector<node_index_t> boycotted_nodes) {
  SCOPE_EXIT {
    notifyRefreshComplete();
  };

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();

  folly::SharedMutex::ReadHolder read_lock(shutdown_mutex_);
  if (shutdown_) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Ignoring GetClusterStateRequest completion as the "
                   "processor is shutting down.");
    return;
  }

  if (status != E::OK) {
    ld_error("Unable to refresh cluster state: %s", error_description(status));
  } else {
    std::vector<std::string> dead;
    for (int i = 0; i < nodes_state.size(); i++) {
      setNodeState(i, static_cast<ClusterState::NodeState>(nodes_state[i]));
      if (nodes_configuration->isNodeInServiceDiscoveryConfig(i) &&
          nodes_state[i] == ClusterState::NodeState::DEAD) {
        dead.push_back("N" + std::to_string(i));
      }
    }

    std::vector<std::string> boycotted_tostring;
    boycotted_tostring.reserve(boycotted_nodes.size());
    for (auto index : boycotted_nodes) {
      boycotted_tostring.emplace_back("N" + std::to_string(index));
    }

    setBoycottedNodes(std::move(boycotted_nodes));

    ld_info("Cluster state received with %lu dead nodes (%s) and %lu boycotted "
            "nodes (%s)",
            dead.size(),
            folly::join(',', dead).c_str(),
            boycotted_tostring.size(),
            folly::join(',', boycotted_tostring).c_str());

    // notify workers of the update so they can take any action
    auto cb = [&](Worker& w) {
      std::unique_ptr<Request> req =
          std::make_unique<ClusterStateUpdatedRequest>(w.idx_);
      if (processor_->postRequest(req) != 0) {
        ld_error("error processing cluster state update on worker #%d: "
                 "postRequest() failed with status %s",
                 w.idx_.val(),
                 error_description(err));
      }
    };
    processor_->applyToWorkers(cb);
  }
}

void ClusterState::resizeClusterState(size_t new_size, bool notifySubscribers) {
  folly::SharedMutex::WriteHolder write_lock(mutex_);
  if (cluster_size_ != new_size) {
    auto new_list = new std::atomic<ClusterState::NodeState>[new_size];
    bool have_failure_detector =
        processor_ && processor_->isFailureDetectorRunning();
    for (int i = 0; i < new_size; i++) {
      if (i < cluster_size_) {
        new_list[i].store(node_state_list_[i].load());
      } else {
        /* If we have failure detector, mark new nodes as dead by default;
         * the failure detector will then change their state as needed.
         * Otherwise, treat nodes as fully started by default,
         * so that HashBasedSequencerLocator can hash to an
         * actual node and not return E::NOTFOUND
         */
        auto state = have_failure_detector
            ? ClusterState::NodeState::DEAD
            : ClusterState::NodeState::FULLY_STARTED;
        new_list[i].store(state);
        if (processor_ && notifySubscribers) {
          postUpdateToWorkers(i, state);
        }
      }
    }
    ld_info(
        "Cluster state size updated from %lu to %lu", cluster_size_, new_size);
    node_state_list_.reset(new_list);
    cluster_size_ = new_size;
  }
}

void ClusterState::updateNodesInConfig(
    const configuration::nodes::ServiceDiscoveryConfig& sd_config) {
  folly::SharedMutex::WriteHolder write_lock(mutex_);

  nodes_in_config_.clear();
  for (auto& node : sd_config) {
    nodes_in_config_.insert(node.first);
  }
}

void ClusterState::noteConfigurationChanged() {
  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  size_t new_size = nodes_configuration->getMaxNodeIndex() + 1;

  if (getClusterSize() != new_size) {
    resizeClusterState(new_size, true);
  }
  updateNodesInConfig(*nodes_configuration->getServiceDiscovery());
}

void ClusterState::refreshClusterStateAsync() {
  // making sure that this is only called from a client, to not mess up
  // with the failure detector on a server
  if (processor_->settings()->server) {
    return;
  }

  folly::SharedMutex::ReadHolder read_lock(shutdown_mutex_);
  if (shutdown_) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Ignoring refresh cluster state request as the "
                   "processor is shutting down.");
    return;
  }

  bool prev = refresh_in_progress_.exchange(true);
  if (prev) {
    ld_debug("Cluster state refresh already in progress");
    return;
  }

  auto now = std::chrono::steady_clock::now().time_since_epoch();
  auto local_settings = processor_->settings();
  auto req_interval = local_settings->cluster_state_refresh_interval;
  if (last_refresh_.load() > now - req_interval) {
    ld_debug("Cluster state refresh was recently executed (less than %lums)",
             req_interval.count());
    notifyRefreshComplete();
    return;
  }

  auto cb = [&](Status status,
                std::vector<uint8_t> node_states,
                std::vector<node_index_t> boycotted_nodes) {
    onGetClusterStateDone(status, node_states, std::move(boycotted_nodes));
  };

  std::unique_ptr<Request> req = std::make_unique<GetClusterStateRequest>(
      local_settings->get_cluster_state_timeout,
      local_settings->get_cluster_state_wave_timeout,
      std::move(cb));

  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Scheduling GetClusterStateRequest to fetch cluster state");

  auto rv = processor_->postRequest(req);
  if (rv == 0) {
    last_refresh_.store(now);
  } else {
    ld_error("Unable to schedule GetClusterStateRequest on a worker, "
             "error %d (%s) ",
             int(err),
             error_description(err));
    notifyRefreshComplete();
  }
}

void ClusterState::waitForRefresh() {
  std::unique_lock<std::mutex> lock(cv_mutex_);
  cv_.wait(lock, [this] { return !refresh_in_progress_; });
}

void ClusterState::notifyRefreshComplete() {
  {
    std::lock_guard<std::mutex> lock(cv_mutex_);
    refresh_in_progress_.store(false);
  }
  cv_.notify_one();
}

}} // namespace facebook::logdevice
