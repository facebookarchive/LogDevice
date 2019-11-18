/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <map>

#include "folly/Format.h"
#include "logdevice/common/debug.h"
namespace facebook::logdevice {

template <typename EventType>
uint8_t TimeBreakdown<EventType>::getPercentage(EventType type) const {
  return eventPercentage_[static_cast<uint64_t>(type)];
}

template <typename EventType>
void TimeBreakdown<EventType>::setPercentage(EventType type,
                                             uint8_t percentage) {
  auto type_val = static_cast<uint64_t>(type);
  eventPercentage_[type_val] = percentage;
}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
constexpr EventTracker<EventType, Clock, mergeAction>::EventTracker(
    TimeDuration window_length,
    size_t num_blocks,
    TimePoint start_time)
    : blockLength_{(window_length.count() + num_blocks - 1) / num_blocks},
      blocks_{num_blocks - 1}, // doesn't include latest_block
      latestBlockStartTime_{start_time},
      timeOfLastCommittedEvent_{start_time} {}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
void EventTracker<EventType, Clock, mergeAction>::commit(TimePoint now,
                                                         EventType event_type) {
  if (ongoingEventType_.hasValue()) {
    simpleCommit(now, ongoingEventType_.value());
    ongoingEventType_.clear();
  } else {
    simpleCommit(now, event_type);
  }
}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
void EventTracker<EventType, Clock, mergeAction>::commitOngoingEvent(
    TimePoint now,
    EventType event_type) {
  commit(now, event_type);
  ongoingEventType_ = event_type;
}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
void EventTracker<EventType, Clock, mergeAction>::simpleCommit(
    TimePoint now,
    EventType event_type) {
  if (now <= timeOfLastCommittedEvent_) {
    return;
  }
  auto event_type_val = static_cast<uint64_t>(event_type);

  // Fast forward if new event is too far in the future
  size_t max_num_blocks = blocks_.capacity() + 1; // include latest block
  if (latestBlockStartTime_ + blockLength_ * (max_num_blocks + 1) < now) {
    blocks_.clear();
    latestBlock_.fill(0);
    latestBlockStartTime_ = now - blockLength_ * max_num_blocks;
    timeOfLastCommittedEvent_ = latestBlockStartTime_;
  }

  auto latest_block_limit = latestBlockStartTime_ + blockLength_;

  // process blocks that are going to be completely filled
  while (latest_block_limit < now) {
    auto timespan = std::chrono::duration_cast<TimeDuration>(
        latest_block_limit - timeOfLastCommittedEvent_);
    latestBlock_[event_type_val] += timespan.count();

    // rotate blocks
    blocks_.push_back(std::array<uint8_t, kNumEvents>());
    for (size_t type_val = 0; type_val < kNumEvents; ++type_val) {
      blocks_.back()[type_val] =
          100ll * latestBlock_[type_val] / blockLength_.count();
    }
    timeOfLastCommittedEvent_ = latest_block_limit;
    latestBlockStartTime_ = latest_block_limit;
    latest_block_limit = latestBlockStartTime_ + blockLength_;
    latestBlock_.fill(0);
  }

  // now commit the remaining timestamp to the latest block
  auto timespan =
      std::chrono::duration_cast<TimeDuration>(now - timeOfLastCommittedEvent_);
  latestBlock_[event_type_val] += timespan.count();

  timeOfLastCommittedEvent_ = now;
}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
void EventTracker<EventType, Clock, mergeAction>::mergeTimeBreakdown(
    TimePoint now,
    TimeBreakdown<EventType>& breakdown,
    TimeDuration timespan) {
  // Step 0. Ongoing events must be committed and forgotten if they exist.
  if (ongoingEventType_.hasValue()) {
    simpleCommit(now, ongoingEventType_.value());
    ongoingEventType_.clear();
    return;
  }

  // Step 1. Obtain actual time durations
  std::array<TimeDuration, kNumEvents> durations;
  TimeDuration duration_events_to_discard{0};
  TimeDuration total_duration{0};
  for (size_t event_type_val = 0; event_type_val < kNumEvents;
       event_type_val++) {
    auto event_type = static_cast<EventType>(event_type_val);
    auto percentage = breakdown.getPercentage(event_type);
    auto duration = timespan * percentage / 100;
    durations[event_type_val] = duration;
    total_duration += duration;
    if (mergeAction(event_type) == EventTrackerMergeAction::DISCARD) {
      duration_events_to_discard += duration;
    }
  }

  // Step 2. Scale everything to fit timespan
  auto timespan_to_fit =
      std::chrono::duration_cast<TimeDuration>(now - timeOfLastCommittedEvent_);

  ld_spew("Duration of events to discard = %ld, total duration = %ld, timespan "
          "to fit = %ld\n",
          duration_events_to_discard.count(),
          total_duration.count(),
          timespan_to_fit.count());

  if (total_duration <= timespan_to_fit) {
    // scale everything upwards
    for (auto& duration : durations) {
      duration = duration * timespan_to_fit.count() / total_duration.count();
    }
  } else if (total_duration - duration_events_to_discard <= timespan_to_fit) {
    // scale down just the discardable events
    auto leftover_space =
        timespan_to_fit - (total_duration - duration_events_to_discard);
    for (size_t event_type_val = 0; event_type_val < kNumEvents;
         event_type_val++) {
      auto event_type = static_cast<EventType>(event_type_val);
      if (mergeAction(event_type) == EventTrackerMergeAction::DISCARD) {
        durations[event_type_val] *= leftover_space.count();
        durations[event_type_val] /= duration_events_to_discard.count();
      }
    }
  } else {
    // all discardable event should be dropped at this point. The remainder
    // should be scaled down to fit.
    total_duration -= duration_events_to_discard;

    for (size_t event_type_val = 0; event_type_val < kNumEvents;
         event_type_val++) {
      auto event_type = static_cast<EventType>(event_type_val);
      if (mergeAction(event_type) != EventTrackerMergeAction::DISCARD) {
        durations[event_type_val] *= timespan_to_fit.count();
        durations[event_type_val] /= total_duration.count();
      } else {
        durations[event_type_val] = TimeDuration{0};
      }
    }
  }

  // Step 3. Commit it all
  for (size_t type_val = 0; type_val < kNumEvents; type_val++) {
    auto type = static_cast<EventType>(type_val);
    auto duration = durations[type_val];
    if (duration.count() > 0) {
      simpleCommit(timeOfLastCommittedEvent_ + duration, type);
    }
  }
}

template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
TimeBreakdown<EventType>
EventTracker<EventType, Clock, mergeAction>::getTimeBreakdown(
    folly::Optional<TimePoint> now) {
  if (now.hasValue() && ongoingEventType_.hasValue()) {
    simpleCommit(now.value(), ongoingEventType_.value());
  }

  std::array<uint64_t, kNumEvents> result_large;
  result_large.fill(0);

  for (auto& block : blocks_) {
    for (size_t type = 0; type < kNumEvents; ++type) {
      auto percentage = block[type];
      result_large[type] += percentage * blockLength_.count() / 100;
    }
  }

  for (size_t type = 0; type < kNumEvents; ++type) {
    result_large[type] += latestBlock_[type];
  }

  auto time_of_oldest_block =
      latestBlockStartTime_ - blockLength_ * blocks_.size();
  auto timespan = std::chrono::duration_cast<TimeDuration>(
      timeOfLastCommittedEvent_ - time_of_oldest_block);

  TimeBreakdown<EventType> result;

  if (timespan.count() > 0) {
    // We now compute the percentage rounded to the nearest integer (for
    // simplicity).
    for (size_t type_val = 0; type_val < kNumEvents; ++type_val) {
      auto type = static_cast<EventType>(type_val);
      auto value = result_large[type_val];
      auto percentage = (100 * value + timespan.count() / 2) / timespan.count();
      result.setPercentage(type, percentage);
    }
  }

  return result;
}

} // namespace facebook::logdevice
