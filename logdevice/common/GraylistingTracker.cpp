// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/GraylistingTracker.h"

#include "logdevice/common/OutlierDetection.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/include/NodeLocationScope.h"

namespace facebook { namespace logdevice {

void GraylistingTracker::updateGraylist(Timestamp now) {
  auto latencies = getLatencyEstimationForNodes(*getNodesConfiguration());
  auto regional_latencies = groupLatenciesPerRegion(std::move(latencies));
  auto outliers = findOutlierNodes(std::move(regional_latencies));

  // if the monitoring time is over the node will get its grace period
  removeExpiredMonitoredNodes(now);
  auto confirmed_outliers = updatePotentialGraylist(now, std::move(outliers));

  for (auto node : confirmed_outliers) {
    graylist_deadlines_.emplace(
        node, now + getUpdatedGraylistingDuration(node, now));
  }

  removeExpiredGraylistedNodes(now);

  updateActiveGraylist();
}

const std::unordered_set<node_index_t>&
GraylistingTracker::getGraylistedNodes() const {
  return graylist_;
}

void GraylistingTracker::start() {
  if (timer_.isActive()) {
    return;
  }
  timer_.setCallback([this]() {
    updateGraylist(std::chrono::steady_clock::now());
    timer_.activate(Worker::settings().graylisting_refresh_interval);
  });
  timer_.activate(Worker::settings().graylisting_refresh_interval);
}

void GraylistingTracker::stop() {
  if (!timer_.isActive()) {
    return;
  }
  timer_.cancel();
}

bool GraylistingTracker::isRunning() const {
  return timer_.isActive();
}

void GraylistingTracker::onSettingsUpdated() {
  if (Worker::settings().disable_outlier_based_graylisting) {
    if (isRunning()) {
      stop();
      resetGraylist();
    }
  } else {
    if (!isRunning()) {
      start();
    }
  }
}

void GraylistingTracker::resetGraylist() {
  STAT_INCR(getStats(), graylist_reseted);
  potential_graylist_.clear();
  graylist_deadlines_.clear();
  graylist_backoffs_.clear();
  graylist_.clear();
}

const std::unordered_map<node_index_t, GraylistingTracker::Timestamp>&
GraylistingTracker::getGraylistDeadlines() const {
  return graylist_deadlines_;
}

WorkerTimeoutStats& GraylistingTracker::getWorkerTimeoutStats() {
  return Worker::onThisThread()->getWorkerTimeoutStats();
}

std::chrono::seconds
GraylistingTracker::positiveFeedbackAndGet(node_index_t node, Timestamp now) {
  ld_assert(graylist_backoffs_.count(node) == 1);
  graylist_backoffs_[node]->positiveFeedback(
      std::chrono::time_point_cast<std::chrono::milliseconds>(now));
  return graylist_backoffs_[node]->getCurrentValue();
}

void GraylistingTracker::negativeFeedback(node_index_t node) {
  ld_assert(graylist_backoffs_.count(node) == 1);
  graylist_backoffs_[node]->negativeFeedback();
}

void GraylistingTracker::removeExpiredGraylistedNodes(Timestamp now) {
  auto it = graylist_deadlines_.begin();
  while (it != graylist_deadlines_.end()) {
    if (it->second <= now) {
      // calculate positive feedback only when not in graylist
      positiveFeedbackAndGet(it->first, now);

      // set the node and time starting from which it will be monitored
      monitored_outliers_[it->first] = now;
      it = graylist_deadlines_.erase(it);
    } else {
      it++;
    }
  }
}

void GraylistingTracker::removeExpiredMonitoredNodes(Timestamp now) {
  auto monitored_period = getMonitoredPeriod();
  auto it = monitored_outliers_.begin();
  while (it != monitored_outliers_.end()) {
    if (it->second + monitored_period < now) {
      it = monitored_outliers_.erase(it);
    } else {
      it++;
    }
  }
}

std::vector<node_index_t> GraylistingTracker::updatePotentialGraylist(
    Timestamp now,
    std::vector<node_index_t> outliers) {
  // Add new nodes to the potential outliers
  std::unordered_map<node_index_t, Timestamp> new_potential;

  auto grace_period = getGracePeriod();

  std::vector<node_index_t> confirmed_outliers;
  for (const auto& node : outliers) {
    auto it = potential_graylist_.find(node);
    auto ts = now;
    if (it != potential_graylist_.end()) {
      ts = it->second;
    }

    if (now - ts > grace_period) {
      confirmed_outliers.emplace_back(node);
    } else if (monitored_outliers_.count(node) != 0) {
      confirmed_outliers.emplace_back(node);
    } else {
      new_potential[node] = ts;
    }
  }
  potential_graylist_ = std::move(new_potential);
  return confirmed_outliers;
}

std::vector<node_index_t>
GraylistingTracker::findSortedOutlierNodesPerRegion(Latencies latencies) {
  auto outlier_pairs =
      OutlierDetection::findOutliers(OutlierDetection::Method::RMSD,
                                     std::move(latencies),
                                     /* num_deviations */ 5,
                                     getMaxGraylistedNodes(),
                                     /* required_margin */ 0.75)
          .outliers;
  using LatencySample = std::pair<node_index_t, WorkerTimeoutStats::Latency>;
  std::sort(outlier_pairs.begin(),
            outlier_pairs.end(),
            [](const LatencySample& a, const LatencySample& b) {
              return a.second > b.second;
            });

  std::vector<node_index_t> outliers;
  outliers.reserve(outlier_pairs.size());
  std::transform(outlier_pairs.begin(),
                 outlier_pairs.end(),
                 std::back_inserter(outliers),
                 [](auto x) { return x.first; });
  return outliers;
}

/* static */ std::vector<node_index_t>
GraylistingTracker::roundRobinFlattenVector(
    const std::vector<std::vector<node_index_t>>& vectors,
    int64_t max_size) {
  int64_t num_elements = 0;
  for (const auto& vector : vectors) {
    num_elements += static_cast<int64_t>(vector.size());
  }

  int result_size = std::min(max_size, num_elements);
  std::vector<node_index_t> result;
  result.reserve(result_size);
  for (int idx = 0; result.size() < result_size; idx++) {
    for (int i = 0; i < vectors.size() && result.size() < result_size; i++) {
      auto& vector = vectors[i];
      if (idx >= vector.size()) {
        continue;
      }
      result.push_back(vector[idx]);
    }
  }
  ld_assert(result.size() == result_size);
  return result;
}

std::vector<node_index_t>
GraylistingTracker::findOutlierNodes(PerRegionLatencies regions) {
  std::vector<std::vector<node_index_t>> regional_outliers;
  for (auto& region : regions) {
    auto region_outliers =
        findSortedOutlierNodesPerRegion(std::move(region.second));
    regional_outliers.emplace_back(std::move(region_outliers));
  }

  return roundRobinFlattenVector(regional_outliers, getMaxGraylistedNodes());
}

std::chrono::seconds GraylistingTracker::getGracePeriod() const {
  return Worker::settings().graylisting_grace_period;
}

void GraylistingTracker::createGraylistingBackoff(node_index_t node) {
  if (graylist_backoffs_.count(node) == 0) {
    auto initial_and_min_cap = getGraylistingDuration();
    auto backoff_ptr =
        std::make_unique<ChronoExponentialBackoff>(initial_and_min_cap,
                                                   initial_and_min_cap,
                                                   getMaxGraylistingDuration(),
                                                   /* multiplier */ 2.0,
                                                   /* decrease_rate */ 1.0,
                                                   /* fuzz_factor */ 0.0);
    graylist_backoffs_[node] = std::move(backoff_ptr);
  }
}

std::chrono::seconds GraylistingTracker::getMaxGraylistingDuration() const {
  return getGraylistingDuration() * 10;
}

std::chrono::seconds GraylistingTracker::getGraylistingDuration() const {
  return Worker::settings().slow_node_retry_interval;
}

std::chrono::seconds
GraylistingTracker::getUpdatedGraylistingDuration(node_index_t node,
                                                  Timestamp now) {
  createGraylistingBackoff(node);

  return positiveFeedbackAndGet(node, now);
}

std::chrono::seconds GraylistingTracker::getMonitoredPeriod() const {
  return Worker::settings().graylisting_monitored_period;
}

GraylistingTracker::Latencies GraylistingTracker::getLatencyEstimationForNodes(
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  auto& stats = getWorkerTimeoutStats();
  GraylistingTracker::Latencies latencies;
  for (const auto& kv : *nodes_configuration.getServiceDiscovery()) {
    auto latency =
        stats.getEstimations(WorkerTimeoutStats::Levels::TEN_SECONDS, kv.first);
    if (!latency.hasValue()) {
      continue;
    }
    latencies.emplace_back(
        kv.first, latency.value()[WorkerTimeoutStats::QuantileIndexes::P95]);
  }
  return latencies;
}

void GraylistingTracker::updateActiveGraylist() {
  auto pairs = getSortedGraylistDeadlines(graylist_deadlines_);

  auto new_graylist_size =
      std::min(static_cast<int64_t>(pairs.size()), getMaxGraylistedNodes());

  auto old_graylist = graylist_;
  graylist_.clear();
  graylist_deadlines_.clear();

  for (int i = 0; i < new_graylist_size; i++) {
    auto& pair = pairs[i];
    auto node = pair.second;
    graylist_deadlines_[node] = pair.first;
    graylist_.insert(node);
    if (old_graylist.count(node) == 0) {
      negativeFeedback(node); // only if node wasn't in previous graylist
      STAT_INCR(getStats(), graylist_shard_added);
    }
  }
  ld_assert(graylist_.size() == graylist_deadlines_.size());
}

int64_t GraylistingTracker::getMaxGraylistedNodes() const {
  int num_available_nodes = 0;
  const auto& nodes_configuration = getNodesConfiguration();
  const auto& storage_membership = nodes_configuration->getStorageMembership();
  for (const auto node : *storage_membership) {
    if (storage_membership->hasWritableShard(node)) {
      // GraylistingTracker only tracks at storage node granularity instead of
      // shards. Therefore, consider the node is available if there is at least
      // one writable shard
      num_available_nodes++;
    }
  }
  auto threshold =
      static_cast<int64_t>(getGraylistNodeThreshold() * num_available_nodes);
  return threshold;
}

double GraylistingTracker::getGraylistNodeThreshold() const {
  return Worker::settings().gray_list_nodes_threshold;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GraylistingTracker::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

StatsHolder* GraylistingTracker::getStats() {
  return Worker::stats();
}

/* static */ std::vector<std::pair<GraylistingTracker::Timestamp, node_index_t>>
GraylistingTracker::getSortedGraylistDeadlines(
    std::unordered_map<node_index_t, Timestamp> deadlines) {
  std::vector<std::pair<Timestamp, node_index_t>> pairs;
  pairs.reserve(deadlines.size());
  for (const auto& node : deadlines) {
    pairs.emplace_back(node.second, node.first);
  }
  std::sort(pairs.begin(), pairs.end(), std::greater<>());
  return pairs;
}

GraylistingTracker::PerRegionLatencies
GraylistingTracker::groupLatenciesPerRegion(
    GraylistingTracker::Latencies latencies) {
  const auto& nodes_sd = getNodesConfiguration()->getServiceDiscovery();

  constexpr folly::StringPiece kUnknownRegion = "";

  PerRegionLatencies per_region_latencies;
  for (auto& node : latencies) {
    const auto* sd = nodes_sd->getNodeAttributesPtr(node.first);
    if (sd == nullptr) {
      continue;
    }

    auto region = sd->location.hasValue() && !sd->location.value().isEmpty()
        ? sd->location.value().getLabel(NodeLocationScope::REGION)
        : kUnknownRegion.toString();
    auto it = per_region_latencies.find(region);
    if (it == per_region_latencies.end()) {
      it = per_region_latencies.emplace(region, Latencies{}).first;
    }
    it->second.emplace_back(std::move(node));
  }
  return per_region_latencies;
}

}} // namespace facebook::logdevice
