/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/FlowGroupTest.h"
using namespace facebook::logdevice;

namespace {

TEST_F(FlowGroupTest, DisableTest) {
  update.policy.setEnabled(true);
  flow_group->applyUpdate(update);

  auto send_ops = [this](bool expect_success) {
    size_t op_cost = 10;
    for (Priority p = Priority::MAX; p < Priority::NUM_PRIORITIES;
         p = priorityBelow(p)) {
      auto op = std::make_unique<FlowOperation>(sent, "Test Op", op_cost);
      if (expect_success) {
        ASSERT_TRUE(flow_group->drain(op->cost(), p));
        // Messages unimpeeded by traffic shaping aren't interesting to us.
        // Just discard them so we don't consume memory.
      } else {
        ASSERT_FALSE(flow_group->drain(op->cost(), p));
        // Queue up for credits.  Op is now owned by the FlowGroup. The callback
        // is responsible for deletion.
        flow_group->push(*op.release(), p);
      }
    }
  };

  // At this point, we should have enough bandwidth to send a message at each
  // priority level.
  send_ops(/*expect_success*/ true);

  // Reset all meters to 0. Attempts to drain should fail.
  resetMeter(0);
  send_ops(/*expect_success*/ false);
  // Nothing should have been sent yet. We have to run the FlowGroup first.
  CHECK_SENDQ();

  // Running an enabled FlowGroup without available bandwidth does nothing.
  run();
  CHECK_SENDQ();

  // Applying a disabled policy should allow the messages to be released.
  update.policy.setEnabled(false);
  flow_group->applyUpdate(update);
  run();
  for (Priority p = Priority::MAX; p < Priority::NUM_PRIORITIES;
       p = priorityBelow(p)) {
    CHECK_SENT("Test Op", 10);
  }
  // Nothing should be left to send.
  ASSERT_TRUE(flow_group->empty());

  update.policy.setEnabled(true);
  flow_group->applyUpdate(update);
  resetMeter(-1);
  // Reset the meter to -1 so all priority levels have debt. Attempts to drain
  // should fail.
  send_ops(/*expect_success*/ false);
  // Nothing should have been sent yet. We have to run the FlowGroup first.
  CHECK_SENDQ();

  // Running without bandwidth budget does nothing.
  run();
  CHECK_SENDQ();

  // Applying a disabled policy should allow the messages to be released.
  update.policy.setEnabled(false);
  flow_group->applyUpdate(update);
  run();
  for (Priority p = Priority::MAX; p < Priority::NUM_PRIORITIES;
       p = priorityBelow(p)) {
    CHECK_SENT("Test Op", 10);
  }
  // Nothing should be left to send.
  ASSERT_TRUE(flow_group->empty());
}

TEST_F(FlowGroupTest, FlowGroupRunLimits) {
  class SlowCallback : public BWAvailableCallback {
   public:
    explicit SlowCallback(std::chrono::microseconds delay) : delay_(delay) {}

    void operator()(FlowGroup&, std::mutex&) override {
      /* sleep override */
      std::this_thread::sleep_for(delay_);
    }

   private:
    std::chrono::microseconds delay_;
  };

  // Test 1
  update.policy.setEnabled(true);
  flow_group->applyUpdate(update);

  // Increase deadline to reduce the chance of false positives if this
  // test is run in parallel with other, CPU intensive, tests.
  flow_groups_run_yield_interval = std::chrono::milliseconds(100);

  // Running without any pending work should return false since there
  // is no additional work pending.
  EXPECT_FALSE(run());

  SlowCallback callback1(flow_groups_run_yield_interval * 2);
  SlowCallback callback2(flow_groups_run_yield_interval / 2);
  flow_group->push(callback1, Priority::MAX);
  flow_group->push(callback2, Priority::MAX);

  // The first run should terminate after the first callback is processed
  // due to its excessive runtime, and signal more work is required by
  // returning true.
  EXPECT_TRUE(run());
  EXPECT_FALSE(callback1.active());
  EXPECT_TRUE(callback2.active());

  // Running again should release the second callback and return false since
  // the run deadline was not exceeded.
  EXPECT_FALSE(run());
  EXPECT_FALSE(callback2.active());

  // Test Case 2
  // Push the callbacks again. Disable policy and make sure after disabling we
  // should still be able to drain all the callback

  update.policy.setEnabled(false);
  flow_group->applyUpdate(update);
  flow_group->push(callback1, Priority::MAX);
  flow_group->push(callback2, Priority::MAX);
  EXPECT_TRUE(run());
  EXPECT_FALSE(callback1.active());
  EXPECT_TRUE(callback2.active());
  // Running again should release the second callback and return false since
  // the run deadline was not exceeded.
  EXPECT_FALSE(run());
  EXPECT_FALSE(callback2.active());

  // Test Case 3
  // Transition from enabled to disabled when flow_group already has pending
  // callbacks. Same expectations as Test Case 2.
  flow_group->push(callback1, Priority::MAX);
  flow_group->push(callback2, Priority::MAX);
  update.policy.setEnabled(false);
  flow_group->applyUpdate(update);
  EXPECT_TRUE(run());
  EXPECT_FALSE(callback1.active());
  EXPECT_TRUE(callback2.active());
  // Running again should release the second callback and return false since
  // the run deadline was not exceeded.
  EXPECT_FALSE(run());
  EXPECT_FALSE(callback2.active());
}

TEST_F(FlowGroupTest, FlowGroupBandwidthCaps) {
  auto send_op = [this](size_t op_cost, bool expect_success) {
    auto op = std::make_unique<FlowOperation>(sent, "Test Op", op_cost);
    if (expect_success) {
      ASSERT_TRUE(flow_group->drain(op->cost(), Priority::CLIENT_HIGH));
    } else {
      ASSERT_FALSE(flow_group->drain(op->cost(), Priority::CLIENT_HIGH));
      // Queue up for credits.  Op is now owned by the FlowGroup. The callback
      // is responsible for deletion.
      flow_group->push(*op.release(), Priority::CLIENT_HIGH);
    }
  };

  // Limit the append traffic to a max of 11KB.
  const int64_t CLIENT_HIGH_BUCKET_SIZE = 10000;
  const int64_t CLIENT_HIGH_GUARANTEED_BPS = CLIENT_HIGH_BUCKET_SIZE / 2;
  const int64_t CLIENT_HIGH_MAX_BPS = CLIENT_HIGH_GUARANTEED_BPS + 1000;
  update.policy.set(Priority::CLIENT_HIGH,
                    CLIENT_HIGH_BUCKET_SIZE,
                    CLIENT_HIGH_GUARANTEED_BPS,
                    CLIENT_HIGH_MAX_BPS);
  // Ensure the PQ bucket has plenty to give.
  update.policy.set(FlowGroup::PRIORITYQ_PRIORITY,
                    /*burst*/ 3 * CLIENT_HIGH_BUCKET_SIZE,
                    /*guaranteed_bps*/ 3 * CLIENT_HIGH_BUCKET_SIZE,
                    /*max_bps*/ 3 * CLIENT_HIGH_BUCKET_SIZE);
  resetMeter(0);
  flow_group->applyUpdate(update);
  ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 0);

  // After a single quantum, the guaranteed_bps deposit will half fill
  // the bucket. Since the priority queue bucket was filled at the end
  // of the update (after filling the CLIENT_HIGH bucket), no transfer of
  // priority queue credit occured.  The fill for this quantum is below the
  // max_bps limit, so there should be 1000 bytes of unused cap that can be
  // used by other Workers/Senders.
  auto& client_high_overflow =
      update.overflow_entries[asInt(Priority::CLIENT_HIGH)];
  ASSERT_EQ(
      flow_group->level(Priority::CLIENT_HIGH), CLIENT_HIGH_BUCKET_SIZE / 2);
  ASSERT_EQ(client_high_overflow.cur_deposit_budget_overflow, 1000);

  // Sending using available credit shouldn't incur any debt.
  size_t bytes_remaining = flow_group->level(Priority::CLIENT_HIGH);
  size_t typical_op_size = bytes_remaining / 10;
  while (bytes_remaining) {
    auto op_size = std::min(typical_op_size, bytes_remaining);
    send_op(op_size, /*expect_success*/ true);
    bytes_remaining -= op_size;
    ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 0);
  }

  // The next message can only be sent once BW becomes available in the
  // next quantum.
  ASSERT_EQ(flow_group->level(Priority::CLIENT_HIGH), 0);
  send_op(/*op_cost*/ 500, /*expect_success*/ false);
  ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 0);

  // Simulate the deposit budget accounting performed by the traffic
  // shaper.
  client_high_overflow.last_deposit_budget_overflow =
      client_high_overflow.cur_deposit_budget_overflow;
  client_high_overflow.cur_deposit_budget_overflow = 0;

  // Since the priority queue bucket has excess credits (from the end of the
  // last update) we can consume them during the next update.  Create a fake
  // update that will not deposit anything new to the CLIENT_HIGH bucket or
  // priority queue bucket, but will lead to a transfer of tokens from the
  // priority queue bucket to the CLIENT_HIGH bucket.
  FlowGroupsUpdate::GroupEntry force_transfer_update(update);
  force_transfer_update.policy.set(Priority::CLIENT_HIGH,
                                   CLIENT_HIGH_BUCKET_SIZE,
                                   /*guaranteed_bps*/ 0,
                                   /*max_bps*/ CLIENT_HIGH_MAX_BPS);
  force_transfer_update.policy.set(FlowGroup::PRIORITYQ_PRIORITY,
                                   /*burst*/ 3 * CLIENT_HIGH_BUCKET_SIZE,
                                   /*guaranteed_bps*/ 0,
                                   /*max_bps*/ 0);
  ASSERT_EQ(force_transfer_update.overflow_entries[asInt(Priority::CLIENT_HIGH)]
                .last_deposit_budget_overflow,
            client_high_overflow.last_deposit_budget_overflow);

  // The transfer is limited by the maximum bandwidth limit. This is
  // the limit for the current quantum plus any unused budget from the
  // last quantum.
  auto last_overflow = client_high_overflow.last_deposit_budget_overflow;
  flow_group->applyUpdate(force_transfer_update);
  ASSERT_EQ(flow_group->level(Priority::CLIENT_HIGH),
            CLIENT_HIGH_MAX_BPS + last_overflow);

  run();
  CHECK_SENT("Test Op", 500);
  ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 0);
  ASSERT_EQ(flow_group->level(Priority::CLIENT_HIGH),
            CLIENT_HIGH_MAX_BPS + last_overflow - 500);

  // This next message will consume more credit than the current balance in
  // the bucket. We allow it to be sent, but we'll be left with a debt.
  send_op(
      /*op_cost*/ CLIENT_HIGH_MAX_BPS + last_overflow, /*expect_success*/ true);
  ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 500);

  // A message queued now will not be sent, even after a flow group run because
  // there are insufficient credits in the bucket.
  send_op(/*op_cost*/ 50, /*expect_success*/ false);
  run();
  // Nothing sent.
  CHECK_SENDQ();

  // Another update should reset the max cap and allow the message to be
  // dispatched.
  flow_group->applyUpdate(update);
  run();
  CHECK_SENT("Test Op", 50);
  ASSERT_EQ(flow_group->debt(Priority::CLIENT_HIGH), 0);
}

TEST_F(FlowGroupTest, AllowCompleteBandwidthUsageForBurstyTraffic) {
  constexpr auto BURST_SIZE = 20;
  constexpr auto OP_COST = 1000;
  auto send_op = [&]() {
    // Pretend to send an message of OP_COST.
    auto saved_level = flow_group->level(Priority::MAX);
    // No messages will be deferred by traffic shaping in this test, make sure
    // that is the case.
    ASSERT_TRUE(flow_group->drain(OP_COST, Priority::MAX));
    ASSERT_EQ(flow_group->level(Priority::MAX), saved_level - OP_COST);
  };

  // Set the deposit budget (max bandwidth) to be total size of 10 messages. Set
  // burst capacity twice of deposit budget. The bucket config parameters for
  // traffic class MAX have following relationship:
  // burst capacity ( 20 * OP_COST ) > max_bps (10 * OP_COST) >
  // guaranteed_bps ( 5 * OP_COST ).
  constexpr auto MAXP_BUCKET_SIZE = 10 * OP_COST;
  update.policy.set(Priority::MAX,
                    /*burst*/ 2 * MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE / 2,
                    /*max_bps*/ MAXP_BUCKET_SIZE);
  // Ensure the PQ bucket has plenty to provide to traffic class MAX.
  update.policy.set(FlowGroup::PRIORITYQ_PRIORITY,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);

  // Rest of the classes are initialized. The values for their bucket config is
  // selected such that they don't borrow from priority queue traffic class.
  update.policy.set(Priority::CLIENT_HIGH,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);
  update.policy.set(Priority::CLIENT_NORMAL,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);
  update.policy.set(Priority::CLIENT_NORMAL,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);
  update.policy.set(Priority::BACKGROUND,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);
  update.policy.set(Priority::IDLE,
                    /*burst*/ MAXP_BUCKET_SIZE,
                    /*guaranteed_bps*/ MAXP_BUCKET_SIZE,
                    /*max_bps*/ MAXP_BUCKET_SIZE);

  // Reset bucket levels for all traffic classes to zero.
  resetMeter(0);

  // Prefill the priority queue bucket so it has sufficient credit to fill the
  // MAX bucket to capacity, when applyUpdate() distributes priority queue
  // bandwidth to other buckets.
  flow_group->resetMeter(FlowGroup::PRIORITYQ_PRIORITY,
                         /*level*/ MAXP_BUCKET_SIZE / 2);

  flow_group->applyUpdate(update);
  ASSERT_EQ(flow_group->debt(Priority::MAX), 0);
  ASSERT_EQ(flow_group->level(Priority::MAX), MAXP_BUCKET_SIZE);

  // If in the current interval none of the credits in Priority MAX class got
  // used, in the next interval we should allow it to use credits accrued from
  // both the intervals. This is done only if the burst capacity allows us to do
  // so. Generally speaking, new credits will be deposited into the bucket by
  // the traffic shaper from guaranteed allocation and the pq class. If there
  // are unused credits from previous iteration, this will lead to accrual of
  // credits in the bucket. This deposit will be limited by deposit budget
  // and bucket capacity.
  // Thus, if a traffic class is not using the credits entirely
  // in an iteration then credits keep on adding up in it's bucket. This way
  // credits accumulated from multiple previous iteration can be used and
  // requested bandwidth can be achieved.
  flow_group->applyUpdate(update);
  ASSERT_EQ(flow_group->debt(Priority::MAX), 0);
  ASSERT_EQ(flow_group->level(Priority::MAX), 2 * MAXP_BUCKET_SIZE);

  // If we try to add some more we won't be able to, because the burst capacity
  // will limit the number of credits in the bucket.
  flow_group->applyUpdate(update);
  ASSERT_EQ(flow_group->debt(Priority::MAX), 0);
  ASSERT_EQ(flow_group->level(Priority::MAX), 2 * MAXP_BUCKET_SIZE);

  // Now if a burst of 20 messages comes in, it should go through without any
  // debt thus maintaining the requested bandwidth.
  for (auto bytes_remaining = BURST_SIZE * OP_COST; bytes_remaining > 0;
       bytes_remaining -= OP_COST) {
    send_op();
    ASSERT_EQ(flow_group->debt(Priority::MAX), 0);
  }
}

} // anonymous namespace
