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
    int64_t level() const {
      return level_;
    }
    int64_t depositBudget() const {
      return deposit_budget_;
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
      auto add =
          [](int64_t level, int64_t credits, size_t& overflow_out) -> int64_t {
        ld_check(credits >= 0);
        int64_t max_level;
        if ((credits > 0) && (level > (INT64_MAX - credits))) {
          // overflow detected
          max_level = INT64_MAX;
          overflow_out += level - (INT64_MAX - credits);
        } else {
          max_level = level + credits;
        }
        return max_level;
      };

      ld_check(bucket_capacity_ >= 0);
      int64_t allowed_amount = std::min(amount, static_cast<size_t>(INT64_MAX));
      size_t overflow = 0;
      // We don't need to consult deposit_budget_ since this is bandwidth
      // that was already approved in the past
      int64_t max_level = add(level_, allowed_amount, overflow);
      int64_t new_level = std::min(max_level, bucket_capacity_);
      // additional overflow (can fit in size_t)
      overflow += (max_level - new_level);
      level_ = new_level;

      returned_credits_ += overflow;
      return overflow; // for stats purpose only
    }

    /**
     * Add bandwidth credit to this bucket.
     *
     * @param amount   Bytes of credit to deposit into this meter.
     * @param capacity The maximum accumulation of credit in bytes allowed
     *                 for this meter.
     *
     * @return     0: Bandwidth addition fit within the capacity of this bucket.
     *         non-0: Amount of bandwidth credit left over after filling the
     *                bucket.
     */
    size_t fill(size_t amount, size_t capacity) {
      ld_check(capacity <= INT64_MAX);
      capacity = std::min(capacity, static_cast<size_t>(INT64_MAX));

      ld_check(deposit_budget_ >= 0);
      deposit_budget_ = std::max(deposit_budget_, static_cast<int64_t>(0));

      // Enforce bandwidth cap.
      size_t allowed_amount =
          std::min(amount, static_cast<size_t>(deposit_budget_));

      // Enforce burst limits.
      ssize_t new_level = level_ + allowed_amount;
      new_level = std::min(new_level, static_cast<ssize_t>(capacity));

      // Calculate bandwidth bucket overflow.
      // Note: If the capacity has been reduced from historic levels, this
      //       will result in an overflow that is larger than the requested
      //       fill amount.
      ssize_t unlimited_level = level_ + amount;
      ld_check((unlimited_level - new_level) >= 0);
      size_t overflow = unlimited_level - new_level;

      // Update the deposit budget
      // Note: If "overflow" is greater than "amount" (capacity has been
      //       reduced), this will add credit to the deposit_budget_.
      if (deposit_budget_ != INT64_MAX) {
        deposit_budget_ -= new_level - level_;
      }

      level_ = new_level;

      return overflow;
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
    bool transferCredit(Entry& bwSink, int64_t requested_amount) {
      auto transfer_amount = requested_amount;
      transfer_amount = std::min(transfer_amount, level_);
      transfer_amount = std::min(transfer_amount, bwSink.deposit_budget_);

      if (transfer_amount > 0) {
        level_ -= transfer_amount;
        bwSink.deposit_budget_ -= transfer_amount;
        bwSink.level_ += transfer_amount;
      }

      return transfer_amount == requested_amount;
    }

    /**
     * Set the maximum amount of bandwidth that can be added to this
     * meter until the next call to resetDepositBudget().
     */
    void resetDepositBudget(size_t amount) {
      amount = std::min(amount, static_cast<size_t>(INT64_MAX));
      deposit_budget_ = amount;
    }

    /** @return  true  iff a call to drain() on this Entry will succeed. */
    bool canDrain() const {
      return level_ > 0;
    }

    /** @return  true iff this meter can accept any bandwidth. */
    bool canFill() const {
      return deposit_budget_ > 0;
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

    // Max number of bytes that can be added to the bucket until the next
    // bandwidth deposit by the traffic shaper.
    int64_t deposit_budget_ = INT64_MAX;

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
