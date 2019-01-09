/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/rebuilding/NonAuthoritativeRebuildingChecker.h"

#include "logdevice/common/Request.h"

namespace facebook { namespace logdevice {

const auto MIN_SAFE_PERIOD = std::chrono::milliseconds{10000};

NonAuthoritativeRebuildingChecker::NonAuthoritativeRebuildingChecker(
    const UpdateableSettings<RebuildingSettings>& rebuilding_settings,
    EventLogStateMachine* event_log,
    node_index_t node_id)
    : rebuilding_settings_(rebuilding_settings),
      callback_helper_(this),
      event_log_(event_log),
      myNodeId_(node_id),
      active_{false} {
  writer_ = std::make_unique<EventLogWriter>(event_log);

  subhandle_ = rebuilding_settings_.subscribeToUpdates(
      [ticket = callback_helper_.ticket()]() {
        ticket.postCallbackRequest([](auto* self) {
          if (self) {
            self->updateLocalSettings();
          }
        });
      });
  updateLocalSettings();
}

void NonAuthoritativeRebuildingChecker::updateLocalSettings() {
  // update period_ with a safe value
  period_ = std::max(
      rebuilding_settings_->auto_mark_unrecoverable_timeout, MIN_SAFE_PERIOD);

  if (period_ == MIN_SAFE_PERIOD) {
    ld_warning(
        "`auto-mark-unrecoverable-timeout` has been set to a dangerously "
        "low value. Please check your settings.");
  }

  bool enabled = (period_ != std::chrono::milliseconds::max());
  if (enabled != active_) {
    if (enabled) {
      schedulePass();
    } else {
      // cancel everything
      timer_.reset();
      shardTimers_.clear();
    }
    active_ = enabled;
  }
}

void NonAuthoritativeRebuildingChecker::schedulePass() {
  if (!timer_) {
    timer_ = std::make_unique<Timer>([this] { doPass(); });
  }
  timer_->activate(period_ / 2);
}

void NonAuthoritativeRebuildingChecker::doPass() {
  auto& rbs = event_log_->getCurrentRebuildingSet();
  auto now = RecordTimestamp::now().toMilliseconds();

  schedulePass(); // schedule next instance
  if (period_ == decltype(period_)::max()) {
    return; // don't do anything, check it later
  }

  ld_debug("Checking non-authoritative rebuilding processes.");

  for (const auto& s : rbs.getRebuildingShards()) {
    for (const auto& n : s.second.nodes_) {
      ShardID sid(n.first, s.first);

      if (n.second.rebuildingIsNonAuthoritative() &&
          n.second.recoverable == true) {
        auto last_transition = n.second.non_authoritative_since_ts;
        decltype(period_) time_until_mark_unrecoverable;

        if (period_ + last_transition > now) {
          time_until_mark_unrecoverable =
              std::chrono::duration_cast<decltype(period_)>(
                  period_ + last_transition - now);
        } else {
          time_until_mark_unrecoverable = decltype(period_)::zero();
        }

        auto& timer = shardTimers_[sid];
        if (!timer) {
          timer = std::make_unique<Timer>([this, sid] { checkShard(sid); });
        }
        ld_info("Rebuilding for shard %s is non-authoritative, scheduling it "
                "to be checked at %s (%lu msecs from now).",
                sid.toString().c_str(),
                format_time(time_until_mark_unrecoverable +
                            SystemTimestamp::now().toMilliseconds())
                    .c_str(),
                time_until_mark_unrecoverable.count());
        if (timer->isActive()) {
          timer->cancel();
        }
        timer->activate(time_until_mark_unrecoverable);
      } else {
        shardTimers_.erase(sid);
      }
    }
  }
}

void NonAuthoritativeRebuildingChecker::checkShard(ShardID sid) {
  // check if current worker is `the leader'.
  auto cs = Worker::onThisThread()->getClusterState();
  ld_check(cs != nullptr);
  if (cs->getFirstNodeAlive() != myNodeId_) {
    return; // not a leader, nothing to do here
  }

  auto nidx = sid.node();
  uint32_t sidx = sid.shard();
  auto& rbs = event_log_->getCurrentRebuildingSet();
  auto& shards = rbs.getRebuildingShards();
  auto now = RecordTimestamp::now();

  if (lastMarkedUnrecoverable_.count(sid) &&
      lastMarkedUnrecoverable_.at(sid) + period_ >= now) {
    return; // it is too early to check
  }

  ld_debug(
      "Checking if shard %s has been too long in non-authoritative rebuilding.",
      sid.toString().c_str());

  if (shards.count(sidx)) {
    return;
  }

  const auto& nodes = shards.at(sidx).nodes_;
  if (nodes.count(nidx)) {
    return;
  }

  const auto& node_info = nodes.at(nidx);
  auto last_transition = RecordTimestamp(node_info.non_authoritative_since_ts);
  const auto eps = std::chrono::seconds{
      1}; // small value used in the comparison below to increase tolerance

  if (node_info.recoverable) {
    if (last_transition + period_ <= now + eps) {
      // shard has been too long in non-authoritative state, mark it as
      // unrecoverable
      lastMarkedUnrecoverable_[sid] = now;
      auto event = std::make_unique<SHARD_UNRECOVERABLE_Event>(nidx, sidx);
      writer_->writeEvent(std::move(event));
      ld_info("Shard %s will be marked as unrecoverable because it was too "
              "long in non-authoritative rebuilding.",
              sid.toString().c_str());
    } else {
      ld_debug("Shard %s was checked but its rebuilding did not spent enough "
               "time in non authoritative state (it must have flipped to "
               "authoritative state since this check was scheduled), next "
               "attempt to mark it unrecoverable will occur in %lu ms.",
               sid.toString().c_str(),
               (period_ + last_transition - now).count());
    }
  }
}

}} // namespace facebook::logdevice
