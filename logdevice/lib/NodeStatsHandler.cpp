/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/NodeStatsHandler.h"

#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void NodeStatsHandler::init() {
  const auto& settings = Worker::settings();
  aggregation_timer_.assign([&] { this->onAggregation(); });

  client_timeout_timer_.assign([&] { this->onClientTimeout(); });

  retry_timer_.assign(

      [&]() { this->onRetry(); },
      settings.sequencer_boycotting.node_stats_send_retry_delay);
}

void NodeStatsHandler::start() {
  if (hasValidNodeSet()) {
    updateDestination();
    activateAggregationTimer();
  } else {
    ld_warning(
        "No nodes in config, will assume it's a test with a bad config. Did "
        "NOT start NodeStatsHandler");
  }
}

void NodeStatsHandler::onMessageSent(Status status) {
  RATELIMIT_DEBUG(std::chrono::seconds{10},
                  1,
                  "Sent message to node %s with status %s",
                  destination_.toString().c_str(),
                  error_name(status));
  switch (status) {
    case Status::OK:
      resetRetryTimer();
      activateClientTimeoutTimer();

      // now wait for a response, or for the client timeout to fire
      break;
    default:
      activateRetryTimer();
      break;
  }
}

void NodeStatsHandler::onReplyFromNode(uint64_t msg_id) {
  if (msg_id != current_msg_id_) {
    // got a response from a node that was deemed too slow by the client
    // timeout, do nothing
    return;
  }

  cancelClientTimeoutTimer();
}

void NodeStatsHandler::prepareAndSendStats() {
  // make sure not to get interrupted by other timers
  cancelRetryTimer();
  cancelClientTimeoutTimer();

  ld_debug(
      "Sending NODE_STATS_Message to node %s", destination_.toString().c_str());

  ++current_msg_id_;
  auto rv = sendStats(collectStats());
  if (rv != 0) {
    if (err != Status::PROTONOSUPPORT) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        1,
                        "Failed to send NODE_STATS message to %s: %s",
                        destination_.toString().c_str(),
                        error_description(err));
    }

    onMessageSent(err);
  }
}

int NodeStatsHandler::sendStats(std::vector<NodeStats>&& stats) {
  auto msg = createMessage(std::forward<std::vector<NodeStats>>(stats));
  return Worker::onThisThread()->sender().sendMessage(
      std::move(msg), destination_);
}

std::vector<NodeStats> NodeStatsHandler::collectStats() {
  auto worker_stats = Worker::onThisThread()->stats();
  if (!worker_stats) {
    return {};
  }

  const auto aggregation_delay = getAggregationDelay();
  std::unordered_map<NodeID, NodeStats, NodeID::Hash> stats_map;
  const auto now = std::chrono::steady_clock::now();

  const auto from = now - aggregation_delay;
  worker_stats->runForEach([&](Stats& stats) {
    for (auto& thread_node_stats :
         stats.synchronizedCopy(&Stats::per_node_stats)) {
      const auto& id = thread_node_stats.first;
      auto& timeseries = thread_node_stats.second;

      timeseries->updateCurrentTime(now);

      stats_map[id].id_ = id;
      stats_map[id].append_successes_ +=
          timeseries->sumAppendSuccess(from, now);
      stats_map[id].append_fails_ += timeseries->sumAppendFail(from, now);
    }
  });

  std::vector<NodeStats> stats;
  stats.reserve(stats_map.size());
  // used for logging
  std::vector<std::string> stats_string;
  stats_string.reserve(stats_map.size());
  for (auto& entry : stats_map) {
    stats_string.emplace_back(entry.first.toString() + " S" +
                              std::to_string(entry.second.append_successes_) +
                              " F" +
                              std::to_string(entry.second.append_fails_));
    stats.emplace_back(std::move(entry.second));
  }

  RATELIMIT_DEBUG(std::chrono::seconds(10),
                  1,
                  "Sending the following stats in a NODE_STATS_Message: %s",
                  folly::join(", ", stats_string).c_str());

  return stats;
}

void NodeStatsHandler::updateDestination() {
  auto old_destination = destination_;
  destination_ = findDestinationNode();

  ld_debug("Changed NodeStatsHandler target node from %s, to %s",
           old_destination.toString().c_str(),
           destination_.toString().c_str());
}

void NodeStatsHandler::activateAggregationTimer() {
  aggregation_timer_.activate(getAggregationDelay());
}

void NodeStatsHandler::activateRetryTimer() {
  retry_timer_.activate();
}

void NodeStatsHandler::cancelRetryTimer() {
  retry_timer_.cancel();
}

void NodeStatsHandler::resetRetryTimer() {
  retry_timer_.reset();
}

void NodeStatsHandler::activateClientTimeoutTimer() {
  client_timeout_timer_.activate(
      Worker::onThisThread()
          ->settings()
          .sequencer_boycotting.node_stats_timeout_delay);
}

void NodeStatsHandler::cancelClientTimeoutTimer() {
  client_timeout_timer_.cancel();
}

void NodeStatsHandler::onClientTimeout() {
  ld_debug("Client timeout fired for %s. Retrying.",
           destination_.toString().c_str());
  onRetry();
}

void NodeStatsHandler::onAggregation() {
  prepareAndSendStats();

  // should re-start the timer to aggregate in some time
  activateAggregationTimer();
}

void NodeStatsHandler::onRetry() {
  auto old_dest = destination_;
  updateDestination();

  ld_debug("Retrying sending node stats to a new destination. Was %s, now %s",
           old_dest.toString().c_str(),
           destination_.toString().c_str());

  onAggregation();
}

bool NodeStatsHandler::hasValidNodeSet() const {
  return Worker::onThisThread()->getNodesConfiguration()->clusterSize() > 0;
}

NodeID NodeStatsHandler::findDestinationNode() const {
  const auto& node_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto new_destination =
      RandomNodeSelector::getNode(*node_configuration, destination_);

  return new_destination;
}

std::unique_ptr<NODE_STATS_Message>
NodeStatsHandler::createMessage(std::vector<NodeStats>&& stats) const {
  std::vector<NodeID> ids;
  std::vector<uint32_t> append_successes;
  std::vector<uint32_t> append_fails;

  const auto node_count = stats.size();
  ids.reserve(node_count);
  append_successes.reserve(node_count);
  append_fails.reserve(node_count);

  for (auto& entry : stats) {
    ids.emplace_back(std::move(entry.id_));
    append_successes.emplace_back(std::move(entry.append_successes_));
    append_fails.emplace_back(std::move(entry.append_fails_));
  }

  NODE_STATS_Header header = {
      current_msg_id_, static_cast<uint16_t>(node_count)};

  return std::make_unique<NODE_STATS_Message>(std::move(header),
                                              std::move(ids),
                                              std::move(append_successes),
                                              std::move(append_fails));
}

std::chrono::milliseconds NodeStatsHandler::getAggregationDelay() const {
  return Worker::onThisThread()
      ->settings()
      .sequencer_boycotting.node_stats_send_period;
}
}} // namespace facebook::logdevice
