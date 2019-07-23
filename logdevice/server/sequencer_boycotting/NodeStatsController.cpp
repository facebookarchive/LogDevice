/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/NodeStatsController.h"

#include <algorithm>
#include <chrono>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/BoycottTracer.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_Message.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

void NodeStatsController::onStatsReceived(msg_id_t msg_id,
                                          NodeID from,
                                          BucketedNodeStats stats) {
  if (msg_id != current_msg_id_) {
    return;
  }

  const auto index = from.index();
  if (index < received_stats_.size()) {
    ld_check(received_stats_.size() == last_receive_time_.size());
    received_stats_[index] = std::move(stats);
    last_receive_time_[index] = last_send_time_;
  } else {
    /**
     * Received a response from a node that we didn't account for. Discard for
     * now, and it should fix itself next time we request stats due to resizing
     */
    ld_debug("Received stats from %s, which has a node_index_t larger than the "
             "current amount of tracked nodes. Discarding the stats.",
             from.toString().c_str());
  }
}

bool NodeStatsController::isStarted() {
  return is_started_;
}

void NodeStatsController::start() {
  is_started_ = true;
  activateAggregationTimer();
}

void NodeStatsController::stop() {
  cancelAggregationTimer();
  cancelResponseTimer();

  is_started_ = false;
}

void NodeStatsController::activateAggregationTimer() {
  if (!aggregation_timer_.isAssigned()) {
    aggregation_timer_.assign([&] {
      this->aggregationTimerCallback();
      this->activateAggregationTimer();
    });
  }

  aggregation_timer_.activate(getAggregationPeriod());
}

void NodeStatsController::aggregationTimerCallback() {
  const auto max_idx = getMaxNodeIndex();
  const auto node_count = max_idx + 1; /*index is 0-indexed*/
  if (received_stats_.size() != node_count) {
    received_stats_.resize(node_count);
    last_receive_time_.resize(node_count);
  }

  /**
   * Don't care about old received stats, only care about the ones that are
   * about to be received
   *
   * clear() doesn't change the capacity of the container, so this should be a
   * relatively cheap operation
   */
  received_stats_.clear();
  received_stats_.resize(node_count);

  RATELIMIT_DEBUG(
      std::chrono::seconds{10}, 1, "Collecting node stats from all nodes");
  sendCollectStatsMessage();
  activateResponseTimer();
}

void NodeStatsController::cancelAggregationTimer() {
  aggregation_timer_.cancel();
}

void NodeStatsController::activateResponseTimer() {
  if (!response_timer_.isAssigned()) {
    response_timer_.assign([&] { this->responseTimerCallback(); });
  }

  response_timer_.activate(
      Worker::settings()
          .sequencer_boycotting.node_stats_controller_response_timeout);
}

void NodeStatsController::responseTimerCallback() {
  RATELIMIT_DEBUG(
      std::chrono::seconds{10}, 1, "Aggregating stats received from all nodes");
  aggregate();

  boycott(detectOutliers());
}

void NodeStatsController::cancelResponseTimer() {
  response_timer_.cancel();
}

void NodeStatsController::aggregateThroughCallback(AggregateStatsCallback cb) {
  const auto now = std::chrono::steady_clock::now();
  const auto period_duration = getAggregationPeriod();

  const auto remove_worst_percentage = removeWorstClientPercentage();
  const auto required_client_count = getRequiredClientCount();

  size_t max_period_count = 0;
  std::unordered_set<NodeID, NodeID::Hash> all_nodes;
  std::vector<std::unordered_map<NodeID, unsigned int, NodeID::Hash>>
      node_mapping(received_stats_.size());

  ld_check(received_stats_.size() == node_mapping.size());
  // get the NodeID to index mapping and the max period count
  for (unsigned int i = 0; i < received_stats_.size(); ++i) {
    for (unsigned int node_idx = 0;
         node_idx < received_stats_[i].node_ids.size();
         ++node_idx) {
      auto node = received_stats_[i].node_ids[node_idx];
      node_mapping[i].emplace(node, node_idx);
      all_nodes.emplace(node);

      max_period_count = std::max(
          max_period_count, received_stats_[i].summed_counts->shape()[1]);
    }
  }

  /**
   * What this loop does:
   * 1)
   * It loops through all nodes that have stats reported about them.
   * 2)
   * For each node, it loops over all the periods
   * 3)
   * For each period, loop over the values received about the given node for the
   * given period. Sum the values together and track the worst clients
   * separately to later be (possibly) be removed
   * 4)
   * Once all values of a period are collected, make sure that we have enough
   * clients
   * 5)
   * Sort the worst clients and remove them according to the
   * remove_worst_percentage
   * 6)
   * Add the remaining worst clients to the sum
   * 7)
   * Give it to the outlier detector
   */
  for (const auto& node : all_nodes) {
    for (int period_idx = 0; period_idx < max_period_count; ++period_idx) {
      std::vector<BucketedNodeStats::ClientNodeStats> worst_clients;
      BucketedNodeStats::SummedNodeStats sum;
      for (unsigned int received_from_idx = 0;
           received_from_idx < received_stats_.size();
           ++received_from_idx) {
        const auto& received = received_stats_[received_from_idx];
        // shape()[1] = period count
        ld_check(received.summed_counts->shape()[1] ==
                 received.client_counts->shape()[1]);

        ld_check(received_from_idx < node_mapping.size());
        // make sure that the request contains information about the node and
        // period
        if (received.summed_counts->shape()[1] > period_idx &&
            node_mapping[received_from_idx].count(node)) {
          auto node_idx = node_mapping[received_from_idx].at(node);
          sum += (*received.summed_counts)[node_idx][period_idx];

          std::copy_if(
              (*received.client_counts)[node_idx][period_idx].begin(),
              (*received.client_counts)[node_idx][period_idx].end(),
              std::back_inserter(worst_clients),
              [](const auto& val) { return val.successes + val.fails > 0; });
        }
      }

      const auto total_client_count = worst_clients.size() + sum.client_count;

      // need at least these many clients to be allowed to boycott
      if (total_client_count < required_client_count) {
        continue;
      }

      // sort by largest fail count
      std::sort(worst_clients.begin(),
                worst_clients.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.fails > rhs.fails;
                });

      const unsigned int remove_count = std::min<unsigned int>(
          worst_clients.size(),
          std::floor(total_client_count * remove_worst_percentage));

      unsigned int total_append_count = sum.successes + sum.fails;
      for (auto& client_appends : worst_clients) {
        total_append_count += client_appends.successes + client_appends.fails;
      }

      auto hash = [](const BucketedNodeStats::ClientNodeStats val) {
        return std::hash<unsigned>()(val.successes) ^
            std::hash<unsigned>()(val.fails);
      };
      std::unordered_set<BucketedNodeStats::ClientNodeStats, decltype(hash)>
          to_be_removed(10, std::move(hash));
      for (int i = 0;
           i < worst_clients.size() && to_be_removed.size() < remove_count;
           ++i) {
        const auto& client_append_count = worst_clients[i];
        // only allow removing of a client if it doesn't represent a large chunk
        // (>= 90%) of the data for a node
        if (client_append_count.successes + client_append_count.fails <
            0.9 * total_append_count) {
          to_be_removed.emplace(client_append_count);
        }
      }

      worst_clients.erase(
          std::remove_if(worst_clients.begin(),
                         worst_clients.end(),
                         [&to_be_removed](const auto& append_counts) {
                           return to_be_removed.count(append_counts) > 0;
                         }),
          worst_clients.end());

      for (const auto& client_appends : worst_clients) {
        sum += client_appends;
      }

      AppendOutlierDetector::NodeStats stats;
      stats.append_successes = sum.successes;
      stats.append_fails = sum.fails;

      const auto append_time = now - period_idx * period_duration;
      cb(node.index(), stats, append_time);
    }
  }
}

void NodeStatsController::aggregate() {
  aggregateThroughCallback(
      [this](node_index_t node_index,
             AppendOutlierDetector::NodeStats stats,
             std::chrono::steady_clock::time_point append_time) {
        outlier_detector_->addStats(node_index, stats, append_time);
      });
}

std::vector<NodeID> NodeStatsController::detectOutliers() const {
  const auto now = std::chrono::steady_clock::now();
  auto outliers = outlier_detector_->detectOutliers(now);
  const auto& nc = getNodesConfiguration();
  std::vector<NodeID> outlier_nodes;
  outliers.reserve(outliers.size());

  // from node_index to NodeID
  for (const auto& outlier_index : outliers) {
    if (nc->isNodeInServiceDiscoveryConfig(outlier_index)) {
      outlier_nodes.emplace_back(nc->getNodeID(outlier_index));
    }
  }

  return outlier_nodes;
}

void NodeStatsController::boycott(std::vector<NodeID> outliers) {
  WORKER_STAT_SET(append_success_outliers_active, outliers.size());

  auto failure_detector = getFailureDetector();
  if (!failure_detector) {
    RATELIMIT_WARNING(
        std::chrono::seconds{10},
        1,
        "Wanted to boycott nodes, but failure_detector is not set. Ignoring");
    return;
  }

  failure_detector->setOutliers(std::move(outliers));
}

void NodeStatsController::sendCollectStatsMessage()
// TODO: T26340751 fix float-cast-overflow undefined behavior
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
    __attribute__((__no_sanitize__("float-cast-overflow")))
#endif
#endif
{
  ++current_msg_id_;
  last_send_time_ = std::chrono::steady_clock::now();

  const auto retention_time = getRetentionTime();
  const auto aggregation_period = getAggregationPeriod();

  const auto max_bucket_count = retention_time / aggregation_period;

  NODE_STATS_AGGREGATE_Header header;
  header.msg_id = current_msg_id_;
  for (const auto& target : getTargetNodes()) {
    auto buckets_since_last = std::round(
        // cast to duration with double as representation to be able to
        // round it
        static_cast<std::chrono::duration<double, std::nano>>(
            (last_send_time_ - last_receive_time_[target.index()])) /
        aggregation_period);

    header.bucket_count =
        std::min<uint16_t>(max_bucket_count, buckets_since_last);

    if (header.bucket_count) {
      Worker::onThisThread()->sender().sendMessage(
          std::make_unique<NODE_STATS_AGGREGATE_Message>(header), target);
    }
  }
}

size_t NodeStatsController::getMaxNodeIndex() const {
  return Worker::onThisThread()->getNodesConfiguration()->getMaxNodeIndex();
}

std::chrono::milliseconds NodeStatsController::getRetentionTime() const {
  return Worker::settings().sequencer_boycotting.node_stats_retention_on_nodes;
}

std::chrono::milliseconds NodeStatsController::getAggregationPeriod() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_controller_aggregation_period;
}

std::vector<NodeID> NodeStatsController::getTargetNodes() const {
  const auto& nc = getNodesConfiguration();

  std::vector<NodeID> target_nodes;
  target_nodes.reserve(nc->getServiceDiscovery()->numNodes());

  for (auto& entry : *nc->getServiceDiscovery()) {
    target_nodes.emplace_back(nc->getNodeID(entry.first));
  }

  return target_nodes;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
NodeStatsController::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

FailureDetector* NodeStatsController::getFailureDetector() const {
  return getProcessor()->failure_detector_.get();
}

ServerProcessor* NodeStatsController::getProcessor() const {
  return ServerWorker::onThisThread()->processor_;
}

double NodeStatsController::removeWorstClientPercentage() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_remove_worst_percentage;
}

unsigned int NodeStatsController::getRequiredClientCount() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_boycott_required_client_count;
}

void NodeStatsController::getDebugInfo(InfoAppendOutliersTable* table) {
  /* Only active controller nodes should report */
  if (!isStarted()) {
    return;
  }

  auto now = std::chrono::steady_clock::now();
  auto failure_detector = getFailureDetector();

  aggregateThroughCallback(
      [now, table, failure_detector](
          node_index_t node_index,
          AppendOutlierDetector::NodeStats stats,
          std::chrono::steady_clock::time_point timepoint) {
        table->next()
            .set<0>(static_cast<int>(node_index))
            .set<1>(stats.append_successes)
            .set<2>(stats.append_fails)
            .set<3>(std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - timepoint)
                        .count())
            .set<4>(failure_detector != nullptr
                        ? failure_detector->isBoycotted(node_index)
                        : false);
      });
}

folly::dynamic NodeStatsController::getStateInJson() {
  folly::dynamic res{folly::dynamic::object()};

  auto getOrInsertMapInMap = [](folly::dynamic& map,
                                std::string key) -> folly::dynamic& {
    auto iter = map.find(key);
    if (iter == map.items().end()) {
      return map[key] = folly::dynamic::object();
    }
    return iter->second;
  };

  aggregateThroughCallback(
      [&](node_index_t node_id,
          AppendOutlierDetector::NodeStats stats,
          std::chrono::steady_clock::time_point bucket_start) {
        std::string node_label =
            std::string("N") + folly::to<std::string>(node_id);
        auto& node_state = getOrInsertMapInMap(res, node_label);
        std::string time_bucket_label =
            folly::to<std::string>(msec_since(bucket_start)) + "ms ago";
        auto& time_bucket_state =
            getOrInsertMapInMap(node_state, time_bucket_label);
        time_bucket_state["fails"] = stats.append_fails;
        time_bucket_state["successes"] = stats.append_successes;
      });
  return res;
}

void NodeStatsController::traceBoycott(
    NodeID boycotted_node,
    std::chrono::system_clock::time_point boycott_start_time,
    std::chrono::milliseconds boycott_duration) {
  BoycottTracer tracer(Worker::onThisThread()->getTraceLogger());
  tracer.traceBoycott(
      boycotted_node, boycott_start_time, boycott_duration, getStateInJson());
}

}} // namespace facebook::logdevice
