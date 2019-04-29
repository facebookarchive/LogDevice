/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/sequencer_boycotting/BoycottTracker.h"

#include <algorithm>
#include <chrono>
#include <queue>
#include <unordered_set>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerTraceRequest.h"

namespace facebook { namespace logdevice {

void BoycottTracker::calculateBoycotts(
    std::chrono::system_clock::time_point current_time) {
  const auto max_boycott_count = getMaxBoycottCount();

  // start by calculating the boycotts by this node, given the outliers set with
  // setLocalOutliers
  calculateBoycottsByThisNode(current_time, max_boycott_count);

  std::vector<Boycott> all_boycotts;
  all_boycotts.reserve(reported_boycotts_.size());

  const auto current_time_ns = current_time.time_since_epoch();

  removeExpiredBoycotts(current_time);

  for (auto boycott_it : reported_boycotts_) {
    auto& boycott = boycott_it.second;
    if (boycott.reset ||
        // the boycott is not yet in effect
        boycott.boycott_in_effect_time > current_time_ns) {
      continue;
    }

    all_boycotts.emplace_back(boycott);
  }

  std::sort(all_boycotts.begin(),
            all_boycotts.end(),
            [](const Boycott& lhs, const Boycott& rhs) {
              return lhs.boycott_in_effect_time < rhs.boycott_in_effect_time;
            });

  all_boycotts.resize(std::min<size_t>(all_boycotts.size(), max_boycott_count));

  auto new_boycotted_nodes =
      std::make_shared<std::unordered_set<node_index_t>>(all_boycotts.size());
  std::transform(
      all_boycotts.cbegin(),
      all_boycotts.cend(),
      std::inserter(*new_boycotted_nodes, new_boycotted_nodes->begin()),
      [](const Boycott& boycott) { return boycott.node_index; });

  ld_check(new_boycotted_nodes->size() <= max_boycott_count);
  WORKER_STAT_SET(boycotts_seen, new_boycotted_nodes->size());

  auto current_boycotted_nodes = boycotted_nodes_.get();
  boycotted_nodes_.compare_and_swap(
      current_boycotted_nodes, new_boycotted_nodes);

  removeUnusedBoycotts(current_time);
}

void BoycottTracker::updateReportedBoycotts(
    const std::vector<Boycott>& boycotts) {
  const auto spread_time = getBoycottSpreadTime();
  for (const auto& boycott : boycotts) {
    auto it = reported_boycotts_.find(boycott.node_index);
    auto new_boycott_decision_time =
        boycott.boycott_in_effect_time - spread_time;

    // Add the boycott if
    if (
        // there is no boycott for that node
        it == reported_boycotts_.end() ||
        // don't overwrite if it's the same to help with logging
        (it->second != boycott &&
         // only replace a boycott if it was performed after the previous one
         // had propagated everywhere
         (it->second.boycott_in_effect_time < new_boycott_decision_time ||
          // or if someone requested a reset at a later time. Don't care if the
          // previous decision had propagated to that node or not, just reset
          (it->second.boycott_in_effect_time < boycott.boycott_in_effect_time &&
           boycott.reset)))) {
      if (boycott.reset) {
        ld_info("Reseting boycott for N%i", boycott.node_index);
      } else {
        ld_info("Boycott N%i", boycott.node_index);
      }
      reported_boycotts_[boycott.node_index] = boycott;
    }
  }
}

void BoycottTracker::updateReportedBoycottDurations(
    const std::vector<BoycottAdaptiveDuration>& boycott_durations,
    std::chrono::system_clock::time_point now) {
  for (const auto& duration : boycott_durations) {
    const auto it = reported_boycott_durations_.find(duration.getNodeIndex());
    if (it == reported_boycott_durations_.end() ||
        it->second.getValueTimestamp() < duration.getValueTimestamp()) {
      reported_boycott_durations_[duration.getNodeIndex()] = duration;
    }
  }
  removeDefaultBoycottDurations(now);
}

const std::unordered_map<node_index_t, Boycott>&
BoycottTracker::getBoycottsForGossip() const {
  return reported_boycotts_;
}

const std::unordered_map<node_index_t, BoycottAdaptiveDuration>&
BoycottTracker::getBoycottDurationsForGossip() const {
  return reported_boycott_durations_;
}

std::vector<node_index_t>
BoycottTracker::getBoycottedNodes(std::chrono::system_clock::time_point now) {
  calculateBoycotts(now);

  const auto& boycotted_nodes = boycotted_nodes_.get();
  return {boycotted_nodes->cbegin(), boycotted_nodes->cend()};
}

void BoycottTracker::setLocalOutliers(std::vector<NodeID> outliers) {
  // simply set the outliers here and calculate later in
  // calculateBoycottsByThisNode to minimize the cross-thread dependencies
  local_outliers_.swap(outliers);
}

void BoycottTracker::resetBoycott(node_index_t node_index) {
  local_resets_.wlock()->emplace(node_index);
}

bool BoycottTracker::isBoycotted(node_index_t node) const {
  const auto& boycotted_nodes = boycotted_nodes_.get();
  return boycotted_nodes->find(node) != boycotted_nodes->end();
}

void BoycottTracker::calculateBoycottsByThisNode(
    std::chrono::system_clock::time_point current_time,
    unsigned int max_boycott_count) {
  const auto current_boycotts = boycotted_nodes_.get();
  boycotts_by_this_node_.erase(
      std::remove_if(
          boycotts_by_this_node_.begin(),
          boycotts_by_this_node_.end(),
          [&](const Boycott& boycott) {
            ld_check(boycott.boycott_duration.count() > 0);
            bool old_boycott =
                boycott.boycott_in_effect_time + boycott.boycott_duration <
                current_time.time_since_epoch();

            const auto it = reported_boycotts_.find(boycott.node_index);
            bool is_overwritten =
                it == reported_boycotts_.end() || it->second != boycott;

            if (is_overwritten) {
              ld_info(
                  "Another controller boycotted N%i at a more recent time, no "
                  "longer use ours",
                  boycott.node_index);
            }

            return old_boycott || is_overwritten;
          }),
      boycotts_by_this_node_.end());

  const auto boycott_time =
      current_time.time_since_epoch() + getBoycottSpreadTime();

  local_outliers_.withULockPtr([&](auto locked_outliers) {
    for (size_t i = 0; i < locked_outliers->size() &&
         // don't try to boycott more nodes than allowed
         boycotts_by_this_node_.size() + current_boycotts->size() <
             max_boycott_count;
         ++i) {
      const auto& cur_outlier = locked_outliers->at(i);
      const auto& node_idx = cur_outlier.index();
      const auto it = reported_boycotts_.find(node_idx);

      // don't overwrite a boycott (it could then be endlessly refreshed and not
      // respect the boycott duration)
      if (it == reported_boycotts_.end()) {
        const auto& boycott_duration = this->isUsingBoycottAdaptiveDuration()
            ? this->computeAdaptiveDuration(node_idx, false, current_time)
            : this->getBoycottDuration();
        Boycott boycott = Boycott{node_idx, boycott_time, boycott_duration};
        boycotts_by_this_node_.emplace_back(boycott);
        reported_boycotts_[boycott.node_index] = boycott;

        ld_info("Decided to boycott %s for %ld milliseconds",
                cur_outlier.toString().c_str(),
                boycott.boycott_duration.count());
        WORKER_STAT_INCR(boycotts_by_controller_total);
        // Posting a request to NodeStatsController to trace the boycotting
        // information
        std::unique_ptr<Request> req =
            std::make_unique<NodeStatsControllerTraceRequest>(
                cur_outlier,
                std::chrono::system_clock::time_point(boycott_time),
                boycott.boycott_duration);
        this->postRequest(req); // ignoring the return value, since there is
                                // nothing to do if posting fails
      }
    }

    // clear any outliers after having calculated the boycotts
    // Otherwise the same outliers will be considered every time until they are
    // updated with a new analyze of given stats, and might be outdated
    locked_outliers.moveFromUpgradeToWrite()->clear();
  });

  local_resets_.withULockPtr([&](auto locked_resets) {
    for (auto reset_node_idx : *locked_resets) {
      ld_info("Initiating reset for N%i", reset_node_idx);
      const auto& boycott_duration = this->isUsingBoycottAdaptiveDuration()
          ? this->computeAdaptiveDuration(reset_node_idx, true, current_time)
          : this->getBoycottDuration();
      reported_boycotts_[reset_node_idx] =
          Boycott{reset_node_idx, boycott_time, boycott_duration, true};
    }

    locked_resets.moveFromUpgradeToWrite()->clear();
  });

  WORKER_STAT_SET(boycotts_by_controller_active, boycotts_by_this_node_.size());
}

void BoycottTracker::removeUnusedBoycotts(
    std::chrono::system_clock::time_point current_time) {
  const auto current_boycotts = boycotted_nodes_.get();
  for (auto boycott_it = reported_boycotts_.cbegin();
       boycott_it != reported_boycotts_.cend();) {
    // remove if
    if (
        // if it was not selected as a boycott and it's not a reset, but only if
        // it has propagated everywhere
        (current_boycotts->count(boycott_it->first) == 0 &&
         !boycott_it->second.reset &&
         boycott_it->second.boycott_in_effect_time <
             current_time.time_since_epoch())) {
      boycott_it = reported_boycotts_.erase(boycott_it);
    } else {
      ++boycott_it;
    }
  }
}

void BoycottTracker::removeExpiredBoycotts(
    std::chrono::system_clock::time_point current_time) {
  for (auto boycott_it = reported_boycotts_.cbegin();
       boycott_it != reported_boycotts_.cend();) {
    ld_check(boycott_it->second.boycott_duration.count() > 0);
    if (boycott_it->second.boycott_in_effect_time +
            boycott_it->second.boycott_duration <
        current_time.time_since_epoch()) {
      ld_info("Boycott expired for N%i", boycott_it->first);
      boycott_it = reported_boycotts_.erase(boycott_it);
    } else {
      ++boycott_it;
    }
  }
}

void BoycottTracker::removeDefaultBoycottDurations(
    std::chrono::system_clock::time_point current_time) {
  for (auto it = reported_boycott_durations_.cbegin();
       it != reported_boycott_durations_.cend();) {
    if (it->second.isDefault(current_time)) {
      it = reported_boycott_durations_.erase(it);
    } else {
      ++it;
    }
  }
}

std::chrono::milliseconds BoycottTracker::getBoycottSpreadTime() const {
  /**
   * Assuming 10ms GOSSIP period
   * "Time to propagate boycott info grows a bit more than log2(node count)
   * should on average take 200ms in a cluster with 4k nodes""
   *
   * The default GOSSIP period is 100ms, so for 4k nodes it'd take 2 seconds.
   * Because this isn't a crazy high value, just use this as a constant instead
   * of re-calculating depending on the amount of nodes
   */
  return std::chrono::seconds{2};
}

unsigned int BoycottTracker::getMaxBoycottCount() const {
  return Worker::settings().sequencer_boycotting.node_stats_max_boycott_count;
}

std::chrono::milliseconds BoycottTracker::getBoycottDuration() const {
  return Worker::settings().sequencer_boycotting.node_stats_boycott_duration;
}

bool BoycottTracker::isUsingBoycottAdaptiveDuration() const {
  return Worker::settings()
      .sequencer_boycotting.node_stats_boycott_use_adaptive_duration;
}

BoycottAdaptiveDuration BoycottTracker::getDefaultBoycottDuration(
    node_index_t node_idx,
    BoycottAdaptiveDuration::TS now) const {
  const auto& settings = Worker::settings().sequencer_boycotting;
  return BoycottAdaptiveDuration(
      node_idx,
      settings.node_stats_boycott_min_adaptive_duration,
      settings.node_stats_boycott_max_adaptive_duration,
      settings.node_stats_boycott_adaptive_duration_decrease_rate,
      settings.node_stats_boycott_adaptive_duration_decrease_time_step,
      settings.node_stats_boycott_adaptive_duration_increase_factor,
      settings.node_stats_boycott_min_adaptive_duration,
      now);
}

std::chrono::milliseconds BoycottTracker::computeAdaptiveDuration(
    node_index_t node_idx,
    bool is_reset,
    std::chrono::system_clock::time_point current_time) {
  auto reported_it = reported_boycott_durations_.find(node_idx);
  if (reported_it == reported_boycott_durations_.end()) {
    reported_it =
        reported_boycott_durations_
            .emplace(
                node_idx, getDefaultBoycottDuration(node_idx, current_time))
            .first;
  }
  auto& boycott_duration = reported_it->second;
  auto computed_duration = boycott_duration.getEffectiveDuration(current_time);
  if (is_reset) {
    boycott_duration.resetIssued(computed_duration, current_time);
  } else {
    boycott_duration.negativeFeedback(computed_duration, current_time);
  }
  return computed_duration;
}

int BoycottTracker::postRequest(std::unique_ptr<Request>& rq) {
  return Worker::onThisThread()->processor_->postRequest(rq);
}

}} // namespace facebook::logdevice
