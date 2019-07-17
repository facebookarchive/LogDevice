/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeSetState.h"

#include "logdevice/common/CheckNodeHealthRequest.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

std::atomic<nodeset_state_id_t::raw_type> NodeSetState::next_id(1);

NodeSetState::NodeSetState(const StorageSet& shards,
                           logid_t log_id,
                           NodeSetState::HealthCheck healthCheck)
    : id_(NodeSetState::next_id++),
      all_shards_cnt_(shards.size()),
      healthCheck_(healthCheck),
      log_id_(log_id) {
  // create the map with all indices in nodes, keys to the map never change
  // after so the map is thread-safe after construction
  for (const ShardID shard : shards) {
    shard_states_[shard] = ShardState();
  }
  graylisting_enabled_.store(grayListingEnabledInSettings());
}

NodeSetState::~NodeSetState() {}

const char* NodeSetState::reasonString(NotAvailableReason reason) {
  static_assert((int)NotAvailableReason::Count == 8,
                "please update NodeSetState::reasonString() after updating "
                "NodeSetState::NotAvailableReason");
  switch (reason) {
    case NotAvailableReason::NONE:
      return "NONE";
    case NotAvailableReason::LOW_WATERMARK_NOSPC:
      return "LOW_WATERMARK_NOSPC";
    case NotAvailableReason::OVERLOADED:
      return "OVERLOADED";
    case NotAvailableReason::NO_SPC:
      return "NO_SPC";
    case NotAvailableReason::UNROUTABLE:
      return "UNROUTABLE";
    case NotAvailableReason::STORE_DISABLED:
      return "DISABLED";
    case NotAvailableReason::SLOW:
      return "SLOW";
    case NotAvailableReason::PROBING:
      return "PROBING";
    default:
      return "UNKNOWN";
  }
  ld_check(false);
}

std::chrono::steady_clock::time_point
NodeSetState::getNotAvailableUntil(ShardID shard) const {
  auto it = shard_states_.find(shard);
  if (it == shard_states_.end()) {
    ld_check(false);
    return std::chrono::steady_clock::time_point::min();
  }

  ShardState n = it->second.load();
  return n.getReason() == NotAvailableReason::NONE
      ? std::chrono::steady_clock::time_point::min()
      : n.getTimePoint();
}

double NodeSetState::getSpaceBasedRetentionThreshold() const {
  return Worker::settings().space_based_retention_node_threshold;
}

double NodeSetState::getGrayListThreshold() const {
  return Worker::settings().gray_list_nodes_threshold;
}

NodeSetState::NotAvailableReason
NodeSetState::getNotAvailableReason(ShardID shard) const {
  auto it = shard_states_.find(shard);
  if (it == shard_states_.end()) {
    ld_check(false);
    return NotAvailableReason::NONE;
  }
  return it->second.load().getReason();
}

void NodeSetState::postHealthCheckRequest(ShardID shard,
                                          bool perform_space_based_retention) {
  Worker* worker = Worker::onThisThread();
  const auto& nodes_configuration = worker->getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);
  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard.node())) {
    // node is no longer in config
    return;
  }

  auto health_check_rq = new CheckNodeHealthRequest(
      shard,
      getSharedPtr(),
      log_id_,
      id_,
      Worker::settings().check_node_health_request_timeout,
      perform_space_based_retention);
  std::unique_ptr<Request> rq(health_check_rq);

  worker->processor_->postWithRetrying(rq);
}

void NodeSetState::checkAndSendSpaceBasedTrims() {
  if (!isHealthCheckEnabled()) {
    return;
  }

  if (!shouldPerformSpaceBasedRetention()) {
    return;
  }

  ShardState old_state;
  std::string nodes_with_less_space;
  for (const auto& it : shard_states_) {
    old_state = it.second.load();
    if (old_state.getReason() == NotAvailableReason::LOW_WATERMARK_NOSPC ||
        old_state.getReason() == NotAvailableReason::NO_SPC) {
      nodes_with_less_space += it.first.toString() + ", ";
      postHealthCheckRequest(it.first, true);
    }
  }
  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Directed SBT for log:%lu on shards: (%s)",
                 log_id_.val(),
                 nodes_with_less_space.c_str());
}

std::chrono::steady_clock::time_point
NodeSetState::getDeadline(NotAvailableReason reason) const {
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now();

  switch (reason) {
    case NotAvailableReason::NO_SPC:
      deadline += Worker::settings().nospace_retry_interval;
      break;
    case NotAvailableReason::STORE_DISABLED:
      deadline += Worker::settings().disabled_retry_interval;
      break;
    case NotAvailableReason::OVERLOADED:
      deadline += Worker::settings().overloaded_retry_interval;
      break;
    case NotAvailableReason::UNROUTABLE:
      deadline += Worker::settings().unroutable_retry_interval;
      break;
    case NotAvailableReason::PROBING:
      deadline += Worker::settings().node_health_check_retry_interval;
      break;
    case NotAvailableReason::NONE:
      deadline = std::chrono::steady_clock::time_point::min();
      break;
    case NotAvailableReason::SLOW:
      deadline += Worker::settings().slow_node_retry_interval;
      break;
    case NotAvailableReason::LOW_WATERMARK_NOSPC:
      deadline += Worker::settings().sbr_low_watermark_check_interval;
      break;
    case NotAvailableReason::Count:
      ld_check(false);
  }

  return deadline;
}

bool NodeSetState::grayListingEnabledInSettings() const {
  const Settings* s = getSettings();
  if (!s) {
    return false;
  }

  return !s->disable_graylisting;
}

bool NodeSetState::shouldClearGrayList() const {
  if (!grayListingEnabledInSettings()) {
    return false;
  }

  double threshold = getGrayListThreshold();
  if (threshold == 1.0) {
    return false;
  }

  nodeset_ssize_t num_unavailable_nodes = 0;
  const auto& storage_membership =
      getNodesConfiguration()->getStorageMembership();
  size_t num_storage_nodes = 0;

  for (auto& ss : shard_states_) {
    if (storage_membership->shouldReadFromShard(ss.first)) {
      num_storage_nodes++;
      // The reason for considering other unavailable reasons apart from
      // just SLOW is that in cases where the number of slow nodes is
      // less than the percentage threshold, but the overall number of
      // unavailable nodes is greater than the percentage threshold,
      // we will end up with a situation where we can't pick copysets
      // and appends will fail with SEQNOBUFS
      if (!consideredAvailable(getNotAvailableReason(ss.first))) {
        num_unavailable_nodes++;
      }
    }
  }
  double threshold_nodes = threshold * double(num_storage_nodes);

  bool result = num_unavailable_nodes >= threshold_nodes;
  ld_spew("threshold exceeded:%s, log:%lu, num_unavailable_nodes:%u,"
          " num_slow_nodes:%u, threshold_nodes:%lf",
          result ? "yes" : "no",
          log_id_.val(),
          num_unavailable_nodes,
          numNotAvailableShards(NotAvailableReason::SLOW),
          threshold_nodes);

  return result;
}

bool NodeSetState::shouldPerformSpaceBasedRetention() const {
  double space_based_retention_threshold = getSpaceBasedRetentionThreshold();
  if (space_based_retention_threshold == 0) {
    return false;
  }

  nodeset_ssize_t low_wm =
      numNotAvailableShards(NotAvailableReason::LOW_WATERMARK_NOSPC);
  nodeset_ssize_t high_wm = numNotAvailableShards(NotAvailableReason::NO_SPC);
  nodeset_ssize_t num_full_nodes = low_wm + high_wm;
  double threshold_nodes =
      space_based_retention_threshold * double(shard_states_.size());

  bool result = num_full_nodes >= threshold_nodes;
  ld_spew("threshold exceeded:%s, log:%lu, num_full_nodes:%u(lo:%d, hi:%d), "
          "threshold_nodes:%lf",
          result ? "yes" : "no",
          log_id_.val(),
          num_full_nodes,
          low_wm,
          high_wm,
          threshold_nodes);
  return result;
}

std::chrono::steady_clock::time_point NodeSetState::checkNotAvailableUntil(
    ShardID shard,
    std::chrono::steady_clock::time_point now) {
  auto it = shard_states_.find(shard);
  if (it == shard_states_.end()) {
    ld_check(false);
    return std::chrono::steady_clock::time_point::min();
  }

  shard_state_atomic_t& state = it->second;
  ShardState old_state = state.load();
  NotAvailableReason old_reason;
  std::chrono::steady_clock::time_point tp;
  ShardState new_state = ShardState();

  if (now.time_since_epoch() > next_trimming_check_.load()) {
    // Attempt to initiate Space based trimming on the nodeset.
    // We need to do this irrespective of whether 'shard' is in
    // the low-space state or not.
    checkAndSendSpaceBasedTrims();
    atomic_fetch_max(next_trimming_check_,
                     getDeadline(NotAvailableReason::LOW_WATERMARK_NOSPC)
                         .time_since_epoch());
  }

  do {
    old_reason = old_state.getReason();
    if (consideredAvailable(old_reason)) {
      // already cleared
      ld_spew("Shard %s available. log:%lu",
              shard.toString().c_str(),
              log_id_.val());
      return std::chrono::steady_clock::time_point::min();
    }

    tp = old_state.getTimePoint();
    if (tp > now) {
      ld_spew("Shard %s unavailable until %lums later, reason: %s. log:%lu",
              shard.toString().c_str(),
              to_msec(tp - now).count(),
              reasonString(old_reason),
              log_id_.val());
      // not expired, no need to clear, return existing time_point
      return tp;
    }

    // we don't send PROBEs from SLOW state
    if (old_reason != NotAvailableReason::SLOW && isHealthCheckEnabled()) {
      tp = getDeadline(NotAvailableReason::PROBING);
      new_state =
          ShardState(NotAvailableReason::PROBING, tp.time_since_epoch());
    }
  } while (!state.compare_exchange_strong(old_state, new_state));

  ld_spew("Shard %s transitioned from %s to %s for log:%lu",
          shard.toString().c_str(),
          reasonString(old_reason),
          reasonString(new_state.getReason()),
          log_id_.val());

  // must have done a reset from an unavailable state
  ld_check(!consideredAvailable(old_reason));

  if (old_reason == new_state.getReason() &&
      old_reason != NotAvailableReason::PROBING) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Cannot overwrite deadline, old:%s, new:%s",
                       reasonString(old_reason),
                       reasonString(new_state.getReason()));
    ld_check(false);
    return std::chrono::steady_clock::time_point::max();
  }

  // Note: num could be negative due to a race condition:
  // 1) initially the num is 0, ShardState for index is NotAvailableReason::NONE
  // 2) Thread A executes setNotAvailableUntil(index), in which the CAS loop
  //    sets a new unavailable state from NotAvailableReason::NONE.
  // 3) before setNotAvailableUntil() increases the num counter, Thread A is
  //    put to sleep
  // 4) Thread B executes this function and clears the ShardState back to
  //    NotAvailableReason::NONE, execution reaches here with num value still be
  //    0.
  // We allow num to be negative because Thread A would eventually wake up and
  // increase the num counter, making the num correct again.

  if (old_reason != new_state.getReason()) {
    std::atomic<nodeset_ssize_t>& old_reason_unavailable_count =
        availability_counters_[static_cast<uint8_t>(old_reason)];
    old_reason_unavailable_count--;

    if (new_state.getReason() == NotAvailableReason::PROBING) {
      ld_check(isHealthCheckEnabled());
      postHealthCheckRequest(shard, false);
      std::atomic<nodeset_ssize_t>& probing_count =
          availability_counters_[static_cast<uint8_t>(
              NotAvailableReason::PROBING)];
      probing_count++;
    } else {
      tp = std::chrono::steady_clock::time_point::min();
    }
  }

  return tp;
}

void NodeSetState::resetGrayList(GrayListResetReason r) {
  for (auto& s : shard_states_) {
    ShardState cur_state = s.second.load();
    if (cur_state.getReason() == NotAvailableReason::SLOW) {
      clearNotAvailableUntil(s.first);
    }
  }

  static_assert((int)GrayListResetReason::Count == 3, "");
  switch (r) {
    case GrayListResetReason::CANT_PICK_COPYSET:
      STAT_INCR(getStats(), graylist_reset_cant_pick_copyset);
      break;
    case GrayListResetReason::THRESHOLD_REACHED:
      STAT_INCR(getStats(), graylist_reset_on_threshold_reached);
      break;
    case GrayListResetReason::DISABLED_IN_SETTINGS:
      STAT_INCR(getStats(), graylist_reset_on_disable);
      break;
    default:
      RATELIMIT_WARNING(std::chrono::seconds(5),
                        1,
                        "Invalid reset reason:%d",
                        static_cast<int>(r));
      ld_check(false);
  }
}

const Settings* FOLLY_NULLABLE NodeSetState::getSettings() const {
  if (!Worker::onThisThread(false)) {
    return nullptr;
  }
  return &Worker::onThisThread()->settings();
}

const std::shared_ptr<const NodesConfiguration>
NodeSetState::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

void NodeSetState::clearNotAvailableUntil(ShardID shard) {
  auto it = shard_states_.find(shard);
  if (it == shard_states_.end()) {
    ld_check(false);
    return;
  }

  shard_state_atomic_t& state = it->second;
  ShardState old_state = state.exchange(ShardState());
  NotAvailableReason reason = old_state.getReason();
  if (reason == NotAvailableReason::NONE) {
    return;
  }

  // must have done a reset from an unavailable state
  ld_check(!consideredAvailable(reason));
  std::atomic<nodeset_ssize_t>& num =
      availability_counters_[static_cast<uint8_t>(reason)];

  // Note: num could be negative due to a race condition:
  // 1) initialy the num is 0, ShardState for index is NotAvailableReason::NONE
  // 2) Thread A executes setNotAvailableUntil(index), in which the CAS loop
  //    sets a new unavailable state from NotAvailableReason::NONE.
  // 3) before setNotAvailableUntil() increases the num counter, Thread A is
  //    put to sleep
  // 4) Thread B executes this function and clears the ShardState back to
  //    NotAvailableReason::NONE, execution reaches here with num value still be
  //    0.
  // We allow num to be negative because Thread A would eventually wake up and
  // increase the num counter, making the num correct again.
  num--;
  return;
}

bool NodeSetState::setNotAvailableUntil(
    ShardID shard,
    NotAvailableReason new_reason,
    folly::Optional<NotAvailableReason> allow_transition_from) {
  std::chrono::steady_clock::time_point tp = getDeadline(new_reason);
  return setNotAvailableUntil(
      shard, tp, new_reason, std::move(allow_transition_from));
}

// state transition function
bool NodeSetState::setNotAvailableUntil(
    ShardID shard,
    std::chrono::steady_clock::time_point until_time,
    NotAvailableReason new_reason,
    folly::Optional<NotAvailableReason> allow_transition_from) {
  if (new_reason == NotAvailableReason::PROBING) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "setNotAvailableUntil is called to set status to "
                       "PROBING. Status should be set to PROBING only when "
                       "health check probe is sent");
    ld_check(false);
    return false;
  }

  auto it = shard_states_.find(shard);
  if (it == shard_states_.end()) {
    ld_spew("Shard:%s no longer belongs to the current nodeset. log:%lu",
            shard.toString().c_str(),
            log_id_.val());
    return false;
  }

  shard_state_atomic_t& state = it->second;
  ShardState old_state = state.load();
  // new state to store
  const ShardState new_state =
      ShardState(new_reason, until_time.time_since_epoch());
  NotAvailableReason old_reason;

  do {
    old_reason = old_state.getReason();
    if (allow_transition_from.hasValue() &&
        old_reason != *allow_transition_from) {
      return false;
    }
    switch (old_reason) {
      case NotAvailableReason::PROBING:
        ld_check(isHealthCheckEnabled());
        break;
      case NotAvailableReason::OVERLOADED:
      case NotAvailableReason::NO_SPC:
      case NotAvailableReason::UNROUTABLE:
      case NotAvailableReason::STORE_DISABLED:
      case NotAvailableReason::SLOW:
        if (new_reason != NotAvailableReason::NONE &&
            new_reason != NotAvailableReason::SLOW) {
          // 1. We don't want to override the above states with
          //    low-watermark state, as the STORED reply can come out of
          //    order, because it is not tied to a specific connection
          // 2. Check node health reply explicitly changes the state
          //    to NONE, followed by changing it to low-watermark.
          //    Reason: check node health reply comes from storage node
          //    on the same connection, so we can assume it's the
          //    latest state.
          // 3. It's ok to move to low-watermark from PROBING, because
          //    we enter probing state only after spending the timeout
          //    duration for the above unavailable states.
          ld_debug("Transition not allowed for %s, log:%lu from %s to %s",
                   shard.toString().c_str(),
                   log_id_.val(),
                   reasonString(old_reason),
                   reasonString(new_reason));
          return false;
        }
        break;
      case NotAvailableReason::NONE:
      case NotAvailableReason::LOW_WATERMARK_NOSPC:
        break;
      case NotAvailableReason::Count:
        ld_check(false);
        break;
    }
  } while (!state.compare_exchange_strong(old_state, new_state));

  ld_check(old_reason == NotAvailableReason::PROBING ||
           consideredAvailable(old_reason) ||
           new_reason == NotAvailableReason::NONE ||
           new_reason == NotAvailableReason::SLOW);

  ld_spew("Set shard %s from %s to %s for log:%lu",
          shard.toString().c_str(),
          reasonString(old_reason),
          reasonString(new_reason),
          log_id_.val());

  if (old_reason != NotAvailableReason::NONE) {
    std::atomic<nodeset_ssize_t>& old_reason_unavailable_count =
        availability_counters_[static_cast<uint8_t>(old_reason)];
    old_reason_unavailable_count--;
  }

  if (new_reason != NotAvailableReason::NONE) {
    std::atomic<nodeset_ssize_t>& new_reason_unavailable_count =
        availability_counters_[static_cast<uint8_t>(new_reason)];
    new_reason_unavailable_count++;
  }

  return true;
}

nodeset_ssize_t
NodeSetState::numNotAvailableShards(NotAvailableReason reason) const {
  ld_check(reason < NotAvailableReason::Count);
  nodeset_ssize_t rv =
      availability_counters_[static_cast<uint8_t>(reason)].load();
  return rv;
}

void NodeSetState::refreshStates() {
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

  if (now.time_since_epoch() < no_refresh_until_.load()) {
    return;
  }

  // If graylisting was enabled earlier, but disabled now,
  // clear the shards which are present in SLOW state.
  if (graylisting_enabled_.load() == true &&
      grayListingEnabledInSettings() == false) {
    graylisting_enabled_.store(false);
    resetGrayList(GrayListResetReason::DISABLED_IN_SETTINGS);
  } else if (graylisting_enabled_.load() == false &&
             grayListingEnabledInSettings() == true) {
    graylisting_enabled_.store(true);
  }

  for (auto it = shard_states_.cbegin(); it != shard_states_.cend(); ++it) {
    checkNotAvailableUntil(it->first, now);
  }

  std::chrono::steady_clock::duration next_until =
      now.time_since_epoch() +
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          Worker::settings().nodeset_state_refresh_interval);

  atomic_fetch_max(no_refresh_until_, next_until);
}

}} // namespace facebook::logdevice
