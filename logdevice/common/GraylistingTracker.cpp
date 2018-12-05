// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/GraylistingTracker.h"

#include "logdevice/common/OutlierDetection.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"

namespace facebook { namespace logdevice {

void GraylistingTracker::updateGraylist(Timestamp now) {
  const auto& nodes = getNodes();

  auto latencies = getLatencyEstimationForNodes(nodes);
  auto outliers = findOutlierNodes(now, std::move(latencies));
  auto confirmed_outliers = updatePotentialGraylist(now, std::move(outliers));

  for (auto node : confirmed_outliers) {
    if (graylist_deadlines_.count(node) == 0) {
      graylist_deadlines_[node] = now + getGraylistingDuration();
    }
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
  graylist_deadlines_.clear();
  graylist_.clear();
}

WorkerTimeoutStats& GraylistingTracker::getWorkerTimeoutStats() {
  return Worker::onThisThread()->getWorkerTimeoutStats();
}

void GraylistingTracker::removeExpiredGraylistedNodes(Timestamp now) {
  auto it = graylist_deadlines_.begin();
  while (it != graylist_deadlines_.end()) {
    if (it->second <= now) {
      it = graylist_deadlines_.erase(it);
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
    } else {
      new_potential[node] = ts;
    }
  }
  potential_graylist_ = std::move(new_potential);
  return confirmed_outliers;
}

std::vector<node_index_t>
GraylistingTracker::findOutlierNodes(Timestamp now, Latencies latencies) {
  auto outlier_pairs =
      OutlierDetection::findOutliers(OutlierDetection::Method::RMSD,
                                     std::move(latencies),
                                     /* num_deviations */ 5,
                                     getMaxGraylistedNodes(),
                                     /* required_margin */ 0.75)
          .outliers;
  std::vector<node_index_t> outliers;
  outliers.reserve(outlier_pairs.size());
  std::transform(outlier_pairs.begin(),
                 outlier_pairs.end(),
                 std::back_inserter(outliers),
                 [](auto x) { return x.first; });
  return outliers;
}

const configuration::Nodes& GraylistingTracker::getNodes() const {
  return Worker::onThisThread()->getServerConfig()->getNodes();
}

std::chrono::seconds GraylistingTracker::getGracePeriod() const {
  return Worker::settings().graylisting_grace_period;
}

std::chrono::seconds GraylistingTracker::getGraylistingDuration() const {
  return Worker::settings().slow_node_retry_interval;
}

GraylistingTracker::Latencies GraylistingTracker::getLatencyEstimationForNodes(
    const configuration::Nodes& nodes) {
  auto& stats = getWorkerTimeoutStats();
  GraylistingTracker::Latencies latencies;
  for (const auto& node : nodes) {
    auto latency = stats.getEstimations(
        WorkerTimeoutStats::Levels::TEN_SECONDS, node.first);
    if (!latency.hasValue()) {
      continue;
    }
    // TODO use  WorkerTimeoutStats::QuantileIndexes when D9216245 lands
    latencies.emplace_back(node.first, latency.value()[2]);
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
    auto idx = pairs.size() - 1 - i;
    ld_assert(idx < pairs.size());
    auto& pair = pairs[pairs.size() - 1 - i];

    graylist_deadlines_[pair.second] = pair.first;
    graylist_.insert(pair.second);
    if (old_graylist.count(pair.second) == 0) {
      STAT_INCR(getStats(), graylist_shard_added);
    }
  }
  ld_assert(graylist_.size() == graylist_deadlines_.size());
}

int64_t GraylistingTracker::getMaxGraylistedNodes() const {
  int num_available_nodes = 0;
  const auto& nodes = getNodes();
  for (const auto& node : nodes) {
    if (node.second.isWritableStorageNode()) {
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
  std::sort(pairs.begin(), pairs.end());
  return pairs;
}

}} // namespace facebook::logdevice
