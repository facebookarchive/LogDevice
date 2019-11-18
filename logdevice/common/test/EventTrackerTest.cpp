/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EventTracker.h"

#include <chrono>
#include <unordered_map>

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

namespace facebook::logdevice {

enum class TestEventType {
  DISCARDABLE_EVENT_TYPE_A = 0,
  DISCARDABLE_EVENT_TYPE_B = 1,
  DISCARDABLE_EVENT_TYPE_C = 2,
  LAST_DISCARDABLE_EVENT = 2,
  A = 3,
  B = 4,
  C = 5,
  MAX_EVENT_VALUE = 5,
};

inline EventTrackerMergeAction testMergeAction(TestEventType type) {
  if (type <= TestEventType::LAST_DISCARDABLE_EVENT) {
    return EventTrackerMergeAction::DISCARD;
  } else {
    return EventTrackerMergeAction::SCALE;
  }
}

std::string toString(TestEventType type) {
  switch (type) {
    case TestEventType::DISCARDABLE_EVENT_TYPE_A:
      return "DISCARDABLE_EVENT_TYPE_A";
    case TestEventType::DISCARDABLE_EVENT_TYPE_B:
      return "DISCARDABLE_EVENT_TYPE_B";
    case TestEventType::DISCARDABLE_EVENT_TYPE_C:
      return "DISCARDABLE_EVENT_TYPE_C";
    case TestEventType::A:
      return "A";
    case TestEventType::B:
      return "B";
    case TestEventType::C:
      return "C";
    default:
      return "INVALID_EVENT_TYPE";
  }
}

using SteadyClock = std::chrono::steady_clock;

using TestEventTracker =
    EventTracker<TestEventType, SteadyClock, testMergeAction>;

const auto reference_time_point =
    std::chrono::time_point_cast<TestEventTracker::TimeDuration>(
        SteadyClock::now());
constexpr auto default_window = std::chrono::milliseconds{30000};
constexpr auto default_num_blocks = 4;

class EventTrackerTest : public ::testing::Test {
 public:
  TestEventTracker buildOne() {
    return TestEventTracker(
        default_window, default_num_blocks, reference_time_point);
  }

  void compareTimeBreakdown(
      const TimeBreakdown<TestEventType>& breakdown,
      std::unordered_map<TestEventType, uint8_t> expected_values) {
    for (auto [type, value] : expected_values) {
      EXPECT_EQ(breakdown.getPercentage(type), value)
          << "Time breakdown has unexpected value for event " << toString(type);
    }
  }

  void compareTimeBreakdown(const TimeBreakdown<TestEventType>& breakdown,
                            const TimeBreakdown<TestEventType>& breakdown2) {
    for (size_t type_val = 0;
         type_val < TimeBreakdown<TestEventType>::kNumEvents;
         ++type_val) {
      auto type = static_cast<TestEventType>(type_val);
      EXPECT_EQ(breakdown.getPercentage(type), breakdown2.getPercentage(type))
          << "Time breakdown has unexpected value for event "
          << toString(static_cast<TestEventType>(type));
    }
  }

  TimeBreakdown<TestEventType>
  buildBreakdown(std::unordered_map<TestEventType, uint8_t> values) {
    TimeBreakdown<TestEventType> result;
    for (auto [type, value] : values) {
      result.setPercentage(type, value);
    }
    return result;
  }
};

TEST_F(EventTrackerTest, ShouldProvideEmptyBreakdown) {
  auto tracker = buildOne();
  auto breakdown = tracker.getTimeBreakdown();
  for (size_t type_val = 0; type_val < TimeBreakdown<TestEventType>::kNumEvents;
       ++type_val) {
    auto type = static_cast<TestEventType>(type_val);
    EXPECT_EQ(0, breakdown.getPercentage(type))
        << "Event " << toString(type) << " was non-zero!";
  }
}

TEST_F(EventTrackerTest, ShouldCommitOneEvent) {
  for (auto event_length : {default_window / default_num_blocks / 2,
                            default_window / default_num_blocks,
                            default_window / default_num_blocks * 2,
                            default_window,
                            default_window * 2}) {
    auto tracker = buildOne();
    tracker.commit(
        reference_time_point + event_length, TestEventType{TestEventType::A});

    const auto time_breakdown = tracker.getTimeBreakdown();

    /* note: there might be some rounding error here */
    ASSERT_EQ(100, time_breakdown.getPercentage(TestEventType::A))
        << "Test event A should be taking up the whole time. Event_length = "
        << event_length.count();
    ASSERT_EQ(0, time_breakdown.getPercentage(TestEventType::B))
        << "Test event B should not be listed in time breakdown. Event_length "
           "= "
        << event_length.count();
    ASSERT_EQ(0, time_breakdown.getPercentage(TestEventType::C))
        << "Test event C should not be listed in time breakdown. Event_length "
           "= "
        << event_length.count();
  }
}

TEST_F(EventTrackerTest, ShouldCommitTwoEvents) {
  for (auto total_event_length : {default_window / default_num_blocks / 2,
                                  default_window / default_num_blocks,
                                  default_window / default_num_blocks * 2,
                                  default_window}) {
    auto eventA_length = total_event_length / 2;
    auto eventB_length = total_event_length / 2;
    auto tracker = buildOne();

    tracker.commit(
        reference_time_point + eventA_length, TestEventType{TestEventType::A});
    tracker.commit(reference_time_point + eventA_length + eventB_length,
                   TestEventType{TestEventType::B});

    const auto time_breakdown = tracker.getTimeBreakdown();
    /* note: there might be some rounding error here */
    ASSERT_EQ(50, time_breakdown.getPercentage(TestEventType::A))
        << "Test event A should be taking up half the time. "
           "total_event_length = "
        << total_event_length.count();
    ASSERT_EQ(50, time_breakdown.getPercentage(TestEventType::B))
        << "Test event B should be taking up half the time. "
           "total_event_length "
           "= "
        << total_event_length.count();
    ASSERT_EQ(0, time_breakdown.getPercentage(TestEventType::C))
        << "Test event C should not be listed in time breakdown. "
           "total_event_length "
           "= "
        << total_event_length.count();
  }
}

TEST_F(EventTrackerTest, ShouldForgetOldEvents) {
  auto tracker = buildOne();

  /* create old event */
  tracker.commit(reference_time_point + default_window, TestEventType::A);
  auto breakdown1 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown1,
      {{TestEventType::A, 100}, {TestEventType::B, 0}, {TestEventType::C, 0}});
  /* add new event */
  tracker.commit(reference_time_point + 2 * default_window, TestEventType::B);
  auto breakdown2 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown2,
      {{TestEventType::A, 0}, {TestEventType::B, 100}, {TestEventType::C, 0}});
  /* and again */
  tracker.commit(reference_time_point + 3 * default_window, TestEventType::C);
  auto breakdown3 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown3,
      {{TestEventType::A, 0}, {TestEventType::B, 0}, {TestEventType::C, 100}});
}

TEST_F(EventTrackerTest, ShouldBeAbleToMergeBreakdown) {
  auto breakdown = buildBreakdown(
      {{TestEventType::A, 33}, {TestEventType::B, 34}, {TestEventType::C, 33}});

  auto tracker = buildOne();
  tracker.mergeTimeBreakdown(
      reference_time_point + default_window, breakdown, default_window);

  auto resulting_breakdown = tracker.getTimeBreakdown();
  compareTimeBreakdown(breakdown, resulting_breakdown);
}

/**
 * In case we merge a breakdown for a time window that is larger than the space
 * it's meant to fit, we should scale down timespans.
 */
TEST_F(EventTrackerTest, ShouldScaleDownBreakdown) {
  auto reference_breakdown = buildBreakdown(
      {{TestEventType::A, 50}, {TestEventType::B, 50}, {TestEventType::C, 0}});

  auto tracker = buildOne();
  // We scale the breakdown by 2x when the time window is 2x smaller.
  tracker.mergeTimeBreakdown(reference_time_point + default_window / 2,
                             reference_breakdown,
                             default_window);

  auto breakdown1 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown1,
      {{TestEventType::A, 50}, {TestEventType::B, 50}, {TestEventType::C, 0}});

  // Now, we add event C to the last half.
  tracker.commit(reference_time_point + default_window, TestEventType::C);
  auto breakdown2 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown2,
      {{TestEventType::A, 25}, {TestEventType::B, 25}, {TestEventType::C, 50}});
}

/**
 * In case we merge a breakdown for a time window that is smaller than the space
 * it's meant to fit, we should scale up timespans.
 */
TEST_F(EventTrackerTest, ShouldScaleUpBreakdown) {
  auto reference_breakdown =
      buildBreakdown({{TestEventType::A, 50}, {TestEventType::B, 50}});

  auto tracker = buildOne();
  // We scale the breakdown by 2x when the time window is 2x smaller.
  tracker.mergeTimeBreakdown(reference_time_point + default_window,
                             reference_breakdown,
                             default_window / 2);

  auto breakdown1 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown1,
      {{TestEventType::A, 50}, {TestEventType::B, 50}, {TestEventType::C, 0}});

  // There should be an inherent ordering between the events, so if we now add
  // event of type C for default_window/2 we should have 50% of event B and 50%
  // of event type C.
  tracker.commit(reference_time_point + default_window + default_window / 2,
                 TestEventType::C);
  auto breakdown2 = tracker.getTimeBreakdown();
  compareTimeBreakdown(
      breakdown2,
      {{TestEventType::A, 0}, {TestEventType::B, 50}, {TestEventType::C, 50}});
}

TEST_F(EventTrackerTest, MergeShouldDiscardDiscardableEventsThatDontFit) {
  auto reference_breakdown =
      buildBreakdown({{TestEventType::DISCARDABLE_EVENT_TYPE_A, 50},
                      {TestEventType::B, 50},
                      {TestEventType::C, 0}});

  auto tracker = buildOne();
  tracker.mergeTimeBreakdown(reference_time_point + default_window / 2,
                             reference_breakdown,
                             default_window);
  auto breakdown = tracker.getTimeBreakdown();
  compareTimeBreakdown(breakdown,
                       {{TestEventType::DISCARDABLE_EVENT_TYPE_A, 0},
                        {TestEventType::B, 100},
                        {TestEventType::C, 0}});
}

TEST_F(EventTrackerTest, OngoingEventsShouldBeAccountedInTimeBreakdown) {
  auto tracker = buildOne();

  tracker.commit(reference_time_point + default_window / 2, TestEventType::A);
  tracker.commitOngoingEvent(
      reference_time_point + default_window / 2, TestEventType::B);

  auto breakdown =
      tracker.getTimeBreakdown(reference_time_point + default_window);
  compareTimeBreakdown(
      breakdown,
      {{TestEventType::A, 50}, {TestEventType::B, 50}, {TestEventType::C, 0}});
}

TEST_F(EventTrackerTest, OngoingEventsMakesCommitNoop) {
  auto tracker = buildOne();
  tracker.commit(reference_time_point + default_window / 2, TestEventType::A);
  tracker.commitOngoingEvent(
      reference_time_point + default_window / 2, TestEventType::B);
  tracker.commit(reference_time_point + default_window, TestEventType::C);

  auto breakdown =
      tracker.getTimeBreakdown(reference_time_point + default_window);
  compareTimeBreakdown(
      breakdown,
      {{TestEventType::A, 50}, {TestEventType::B, 50}, {TestEventType::C, 0}});
}

TEST_F(EventTrackerTest, OngoingEventsMakesMergeNoop) {
  auto tracker = buildOne();
  tracker.commitOngoingEvent(reference_time_point, TestEventType::A);
  auto reference_breakdown = buildBreakdown({{TestEventType::B, 100}});

  tracker.mergeTimeBreakdown(reference_time_point + default_window,
                             reference_breakdown,
                             default_window);

  auto breakdown = tracker.getTimeBreakdown();
  compareTimeBreakdown(breakdown, {{TestEventType::A, 100}});
}

TEST_F(EventTrackerTest, CommitCausesOngoingEventToBeForgotten) {
  auto tracker = buildOne();
  tracker.commitOngoingEvent(reference_time_point, TestEventType::A);
  tracker.commit(reference_time_point + default_window / 2, TestEventType::B);
  tracker.commit(reference_time_point + default_window, TestEventType::B);

  auto breakdown = tracker.getTimeBreakdown();

  compareTimeBreakdown(
      breakdown, {{TestEventType::A, 50}, {TestEventType::B, 50}});
}

TEST_F(EventTrackerTest, MergeCausesOngoingEventToBeForgotten) {
  auto tracker = buildOne();
  tracker.commitOngoingEvent(reference_time_point, TestEventType::A);
  auto reference_breakdown = buildBreakdown({{TestEventType::B, 100}});
  tracker.mergeTimeBreakdown(reference_time_point + default_window / 2,
                             reference_breakdown,
                             default_window / 2);
  tracker.mergeTimeBreakdown(reference_time_point + default_window,
                             reference_breakdown,
                             default_window / 2);

  auto breakdown = tracker.getTimeBreakdown();

  compareTimeBreakdown(
      breakdown, {{TestEventType::A, 50}, {TestEventType::B, 50}});
}

TEST_F(EventTrackerTest, ShouldHandleMultipleSmallEventsGracefully) {
  auto tracker = buildOne();
  auto block_size = default_window / default_num_blocks;

  for (int v = 1; v < block_size.count(); ++v) {
    tracker.commit(
        reference_time_point + decltype(block_size){v}, TestEventType::A);
  }
  auto breakdown = tracker.getTimeBreakdown();
  compareTimeBreakdown(breakdown, {{TestEventType::A, 100}});
}

} // namespace facebook::logdevice
