/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "BoycottTracker.h"

#include <algorithm>
#include <queue>
#include <unordered_set>

#include "logdevice/common/debug.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerTraceRequest.h"

namespace facebook { namespace logdevice {

void BoycottTracker::calculateBoycotts(
    std::chrono::system_clock::time_point current_time) {
  const auto max_boycott_count = getMaxBoycottCount();
  const auto boycott_duration = getBoycottDuration();

  // start by calculating the boycotts by this node, given the outliers set with
  // setLocalOutliers
  calculateBoycottsByThisNode(
      current_time, max_boycott_count, boycott_duration);

  std::vector<Boycott> all_boycotts;
  all_boycotts.reserve(reported_boycotts_.size());

  const auto current_time_ns = current_time.time_since_epoch();

  boycotted_nodes_.clear();

  removeExpiredBoycotts(current_time, boycott_duration);

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

  std::transform(all_boycotts.cbegin(),
                 all_boycotts.cend(),
                 std::inserter(boycotted_nodes_, boycotted_nodes_.begin()),
                 [](const Boycott& boycott) {
                   return std::make_pair(boycott.node_index, boycott);
                 });

  ld_check(boycotted_nodes_.size() <= max_boycott_count);
  WORKER_STAT_SET(boycotts_seen, boycotted_nodes_.size());

  removeUnusedBoycotts(current_time);
}

bool BoycottTracker::isBoycotted(node_index_t node) {
  return boycotted_nodes_.find(node) != boycotted_nodes_.end();
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

const std::unordered_map<node_index_t, Boycott>&
BoycottTracker::getBoycottsForGossip() const {
  return reported_boycotts_;
}

std::vector<node_index_t>
BoycottTracker::getBoycottedNodes(std::chrono::system_clock::time_point now) {
  calculateBoycotts(now);

  std::vector<node_index_t> boycotts;
  boycotts.reserve(boycotted_nodes_.size());

  std::transform(boycotted_nodes_.cbegin(),
                 boycotted_nodes_.cend(),
                 std::back_inserter(boycotts),
                 [](const auto& entry) { return entry.second.node_index; });

  return boycotts;
}

void BoycottTracker::setLocalOutliers(std::vector<NodeID> outliers) {
  // simply set the outliers here and calculate later in
  // calculateBoycottsByThisNode to minimize the cross-thread dependencies
  local_outliers_.swap(outliers);
}

void BoycottTracker::resetBoycott(node_index_t node_index) {
  local_resets_.wlock()->emplace(node_index);
}

void BoycottTracker::calculateBoycottsByThisNode(
    std::chrono::system_clock::time_point current_time,
    unsigned int max_boycott_count,
    std::chrono::milliseconds boycott_duration) {
  boycotts_by_this_node_.erase(
      std::remove_if(
          boycotts_by_this_node_.begin(),
          boycotts_by_this_node_.end(),
          [&](const Boycott& boycott) {
            bool old_boycott =
                boycott.boycott_in_effect_time + boycott_duration <
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
         boycotts_by_this_node_.size() + boycotted_nodes_.size() <
             max_boycott_count;
         ++i) {
      const auto& cur_outlier = locked_outliers->at(i);
      const auto it = reported_boycotts_.find(cur_outlier.index());

      // don't overwrite a boycott (it could then be endlessly refreshed and not
      // respect the boycott duration)
      if (it == reported_boycotts_.end()) {
        Boycott boycott{cur_outlier.index(), boycott_time};
        boycotts_by_this_node_.emplace_back(boycott);
        reported_boycotts_[boycott.node_index] = boycott;

        ld_info("Decided to boycott %s", cur_outlier.toString().c_str());
        WORKER_STAT_INCR(boycotts_by_controller_total);
        // Posting a request to NodeStatsController to trace the boycotting
        // information
        std::unique_ptr<Request> req =
            std::make_unique<NodeStatsControllerTraceRequest>(
                cur_outlier,
                std::chrono::system_clock::time_point(boycott_time),
                boycott_duration);
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
      reported_boycotts_[reset_node_idx] =
          Boycott{reset_node_idx, boycott_time, true};
    }

    locked_resets.moveFromUpgradeToWrite()->clear();
  });

  WORKER_STAT_SET(boycotts_by_controller_active, boycotts_by_this_node_.size());
}

void BoycottTracker::removeUnusedBoycotts(
    std::chrono::system_clock::time_point current_time) {
  for (auto boycott_it = reported_boycotts_.cbegin();
       boycott_it != reported_boycotts_.cend();) {
    // remove if
    if (
        // if it was not selected as a boycott and it's not a reset, but only if
        // it has propagated everywhere
        (boycotted_nodes_.count(boycott_it->first) == 0 &&
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
    std::chrono::system_clock::time_point current_time,
    std::chrono::milliseconds boycott_duration) {
  for (auto boycott_it = reported_boycotts_.cbegin();
       boycott_it != reported_boycotts_.cend();) {
    if (boycott_it->second.boycott_in_effect_time + boycott_duration <
        current_time.time_since_epoch()) {
      boycott_it = reported_boycotts_.erase(boycott_it);
    } else {
      ++boycott_it;
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

int BoycottTracker::postRequest(std::unique_ptr<Request>& rq) {
  return Worker::onThisThread()->processor_->postRequest(rq);
}

}} // namespace facebook::logdevice
