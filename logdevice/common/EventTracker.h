/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <boost/circular_buffer.hpp>

#include "folly/Optional.h"
#include "logdevice/include/types.h"

namespace facebook::logdevice {

/**
 * TimeBreakdown maps each event type to the fraction of time (in percentage) it
 * occupied in the time span tracked by a EventTracker.
 */
template <typename EventType>
class TimeBreakdown {
 public:
  static constexpr size_t kNumEvents =
      static_cast<size_t>(EventType::MAX_EVENT_VALUE) + 1;

  using ContainerT = std::array<uint8_t, kNumEvents>;

  uint8_t getPercentage(EventType type) const;
  void setPercentage(EventType type, uint8_t percentage);

 private:
  ContainerT eventPercentage_{};

  friend class EventTrackerTest;
};

/**
 * Establish policy for handling overflowing events when merging breakdown with
 * ongoing time line. If overflowing events of type X should be discarded before
 * events with different merge actions, then its merge action should be discard.
 * Otherwise, it is SCALE.
 */
enum class EventTrackerMergeAction {
  DISCARD,
  SCALE,
};

/**
 * EventTracker keeps track of time spent by certain event types within a time
 * window (time window length varies from `window_length  - window_length /
 * num_blocks` to `window_length`).
 *
 * Note: EventType should preferably follow a linear order that corresponds to
 * the order you expect to see those events in practice (for accurate results
 * when running `mergeTimeBreakdown()`).
 */
template <typename EventType,
          typename Clock,
          EventTrackerMergeAction mergeAction(EventType)>
class EventTracker {
 public:
  using TimeDuration = std::chrono::milliseconds;
  using TimePoint = std::chrono::time_point<Clock, TimeDuration>;
  template <typename T>
  using CircularBuffer = boost::circular_buffer_space_optimized<T>;

  constexpr EventTracker(TimeDuration window_length,
                         size_t num_blocks,
                         TimePoint start_time);

  /**
   * Commits to the timeline an event of type `event_type` from
   * `time_of_last_commited_event_` until `now`. If there is an ongoing event,
   * that event is committed instead and then forgotten (i.e. there will be no
   * ongoing event after that point). Any event that lies within (-infinity,
   * `now` - window_length + blockLength_] might be forgotten.
   *
   * @param now         Point until which event applies;
   * @param event_type  Type of event;
   */
  void commit(TimePoint now, EventType event_type);

  /**
   * Commits to the timeline an event of type `event_type` from
   * `time_of_last_commited_event_` until `now` and assume that the event is
   * still ongoing, see mergeTimeBreakdown() and commit().
   *
   * @param now         Point until which event applies;
   * @param event_type  Type of event;
   */
  void commitOngoingEvent(TimePoint now, EventType event_type);

  /**
   * Populates time from `time_of_last_commited_event_` until `now` with
   * events listed in `breakdown` according to the merge policy of each
   * event. All events with merge policy `DISCARD` are
   * proportionally reduced until all events fit the time window or until they
   * sum to 0 units of time, then the resulting breakdown is proportionally
   * reduced to fully occupy the time window.
   *
   * If there is an ongoing event, that event is committed instead and then
   * forgotten (same as commit()), and nothing else is done (`breakdown` and
   * `timespan` are ignored).
   *
   * @param now   Point until which we say that `breakdown` was merged.
   * @param breakdown    TimeBreakdown to merge;
   * @param timespan     Timespan that corresponds to provided breakdown;
   */
  void mergeTimeBreakdown(TimePoint now,
                          TimeBreakdown<EventType>& breakdown,
                          TimeDuration timespan);

  /**
   * Returns breakdown of time spent on each event accounted for. If `now`
   * parameter is passed and there is an ongoing event, we commit() the ongoing
   * event to `now` and proceed normally. Please note that breakdowns not
   * necessarily add up to exactly 100 due to rounding when storing data.
   */
  TimeBreakdown<EventType>
  getTimeBreakdown(folly::Optional<TimePoint> now = folly::none);

 private:
  void simpleCommit(
      TimePoint now,
      EventType event_type); // same as commit() but ignores ongoing events

  static constexpr size_t kNumEvents =
      static_cast<size_t>(EventType::MAX_EVENT_VALUE) + 1;

  const TimeDuration blockLength_;
  // Time breakdown in completed blocks. Values in percentages.
  CircularBuffer<std::array<uint8_t, kNumEvents>> blocks_;
  // Milliseconds spent by committed events of each type in current block.
  std::array<uint64_t, kNumEvents> latestBlock_{};
  TimePoint latestBlockStartTime_;
  TimePoint timeOfLastCommittedEvent_;
  folly::Optional<EventType> ongoingEventType_;
};

} // namespace facebook::logdevice

#include "logdevice/common/EventTracker-inl.h"
