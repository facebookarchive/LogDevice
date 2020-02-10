/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <array>

#include <folly/ConstexprMath.h>

#include "logdevice/common/Priority.h"
#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * @file  A FlowMeter holds the currently available bandwidth credit for
 *        each priority level in a FlowGroup.
 *
 *        Messages are released so long as a FlowMeter::Entry has a positive
 *        level (credit is available). This allows us to support message sizes
 *        that exceed the capacity of the FlowMeter::Entry (i.e. its configured
 *        maximum burst value), but also means that FlowMeters can go negative.
 *        This debt must be repaid before messages will again be released.
 *
 *        Debt is paid off by per-priority level bandwidth allocations that
 *        arrive via FlowGroupUpdates. During FlowGroupUpdates, credits are also
 *        transferred from priority queue's FlowMeter::Entry to per-priority
 *        FlowMeter::Entries in the defined priority order.
 *
 *        Releasing messages larger than the configured burst size will
 *        mean a transient violation of the burst size policy. We rely on
 *        buffering in the switches to absorb and process the excess during
 *        the period bandwidth credits are applied to paying down the debt.
 *        The only way to avoid this and head of queue blocking by large,
 *        low priority, messages, would be to have priority specific
 *        connections. This would allow us to safely perform mid-message
 *        flow-control. Currently, that approach is considered to be too
 *        resource intensive.
 */

class FlowMeter {
 public:
  class Entry {
   public:
    static constexpr size_t UNRESTRICTED_BUDGET{INT64_MAX};

    int64_t level() const {
      return level_;
    }

    void setCapacity(int64_t capacity) {
      ld_check(capacity >= 0);
      bucket_capacity_ = capacity;
    }

    /**
     * Withdraw all returned credit received in the previous quantum
     * that could not be stored in the meter's bucket. These credits
     * will be deposited into FlowGroupsUpdate's 'last_overflow'
     * during FlowGroup::applyUpdate().
     */
    size_t consumeReturnedCreditOverflow() {
      return std::exchange(returned_credits_, 0);
    }

    /*
     * Put unconsumed credit back into the meter if e.g. consumer debited
     * more than required initially.
     *
     * @return credits that couldn't be accepted(for stats purpose)
     */
    size_t returnCredits(size_t amount) {
      int64_t clamped_amount = static_cast<int64_t>(folly::constexpr_clamp(
          amount, size_t(0), static_cast<size_t>(INT64_MAX)));
      size_t overflow = amount - clamped_amount;

      int64_t max_level =
          folly::constexpr_add_overflow_clamped(level_, clamped_amount);
      int64_t new_level = std::min(max_level, bucket_capacity_);

      if (max_level == INT64_MAX) {
        // Possible integer overflow detected. Retain this credit in the
        // overflow accounting used for bucket spills.
        ld_check(level_ >= 0);
        overflow = folly::constexpr_add_overflow_clamped(
            overflow,
            static_cast<size_t>(level_) - (max_level - clamped_amount));
      }

      // Catch the bucket spill
      overflow = folly::constexpr_add_overflow_clamped(
          overflow, static_cast<size_t>(max_level - new_level));

      level_ = new_level;
      returned_credits_ =
          folly::constexpr_add_overflow_clamped(returned_credits_, overflow);

      return overflow; // for stats purpose only
    }

    /**
     * Add bandwidth credit to this bucket.
     *
     * @param credit   Bytes of credit to deposit into this meter.
     * @param budget   External budget limiting the deposit amount.
     *                 This budget is updated based on the credit flow
     *                 into (typical) or out of (bucket size config change)i
     *                 the bucket.
     *
     * @return     0: Bandwidth addition fit within the capacity of this bucket.
     *         non-0: Amount of bandwidth credit left over after filling the
     *                bucket.
     */
    size_t fill(size_t credit, size_t& budget) {
      ld_check(bucket_capacity_ >= 0);

      // Enforce budget and burst limits.
      ssize_t consumed_credit = std::min(credit, budget);
      ssize_t new_level = level_ + consumed_credit;
      new_level = std::min(new_level, bucket_capacity_);
      consumed_credit = new_level - level_;

      level_ = new_level;

      // Calculate budget consumption and bandwidth bucket overflow.
      // NOTE: If the bucket capacity has been reduced from historic levels,
      //       this can result in both a budget credit and an overflow that is
      //       larger than the requested fill amount.
      if (budget != UNRESTRICTED_BUDGET) {
        budget -= consumed_credit;
      }
      return credit - consumed_credit;
    }

    /**
     * Remove bandwidth credit from this bucket.
     *
     * @return true  Bandwidth level was positive, and the requested credit
     *               was taken from the bucket. The level in the bucket may
     *               now be negative, indicating the cost of this debit must
     *               be "paid off" before future drain() calls will succeed.
     *         false The level in this bucket was already zero or negative(when
     *               drain at negative level is not allowed)
     */
    bool drain(size_t amount, bool drain_on_negative_level = false) {
      if (!drain_on_negative_level && (level_ <= 0)) {
        return false;
      }

      unconditionalDrain(amount);
      return true;
    }

    int64_t unconditionalDrain(size_t amount) {
      int64_t new_level = level_ - amount;
      ld_check(new_level < level_);
      level_ = new_level;
      return level_;
    }

    /**
     * Transfer credit from source to sink, up to requested_amount, without
     * causing source to go into debt.
     * Return value indicates whether entire requested amount was transferred.
     */
    bool transferCredit(Entry& bwSink,
                        size_t requested_amount,
                        size_t& bwSink_budget) {
      int64_t transfer_amount = std::min(requested_amount, bwSink_budget);
      transfer_amount = std::min(transfer_amount, level_);
      transfer_amount =
          std::min(bwSink.bucket_capacity_ - bwSink.level_, transfer_amount);
      if (transfer_amount < 0) {
        return false;
      }

      level_ -= transfer_amount;
      bwSink.level_ += transfer_amount;
      bwSink_budget -= transfer_amount;
      return static_cast<size_t>(transfer_amount) == requested_amount;
    }

    /** @return  true  iff a call to drain() on this Entry will succeed. */
    bool canDrain() const {
      return level_ > 0;
    }

    /** @return  true  iff there is debt to cancel on this meter. */
    size_t debt() const {
      return level_ >= 0 ? 0 : -level_;
    }

    /**
     * Discard all accumulated capacity.
     */
    void reset(int32_t level) {
      level_ = level;
    }

   private:
    // Current bucket capacity.
    int64_t level_ = 0;

    // Bucket size as calculated from config
    int64_t bucket_capacity_{0};

    // Credits that get returned to the meter via returnCredits()
    // but that could not be accepted into the meter's bucket without
    // causing an overflow. These credits are distributed by
    // FlowGroup::applyUpdates() via consumeReturnedCreditOverflow().
    size_t returned_credits_{0};
  };

  /**
   * Convenience functions for accessing the meter entry for the
   * shared priority queue.
   */
  Entry& priorityQEntry() {
    return entries.back();
  }
  const Entry& priorityQEntry() const {
    return entries.back();
  }

  // The flow meter entries for all message priorities for a flow.
  //
  // The entry at Priority::NUM_PRIORITIES contains the bandwidth
  // state for bandwidth shared between priorities that is allocated
  // in priority order after each run of the EventLoop.
  std::array<Entry, asInt(Priority::NUM_PRIORITIES) + 1> entries;
};

}} // namespace facebook::logdevice
