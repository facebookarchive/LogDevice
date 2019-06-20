/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <limits>

#include <boost/noncopyable.hpp>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * This class represents a limited resource (such as memory or available
 * file descriptors) and provides methods to acquire and release it, making
 * sure that the limit is not exceeded.  All methods are thread-safe.
 */
class ResourceBudget {
 public:
  explicit ResourceBudget(uint64_t limit) : limit_(limit) {}

  // Class responsible for releasing an acquired resource in the RAII fashion.
  class Token {
   public:
    Token(const Token&) = delete;
    Token& operator=(const Token&) = delete;
    Token() : budget_(nullptr), count_(0) {}
    Token(Token&& rhs) noexcept : budget_(rhs.budget_), count_(rhs.count_) {
      rhs.budget_ = nullptr;
      rhs.count_ = 0;
    }
    ~Token() {
      release();
    }
    Token const& operator=(Token&& rhs) {
      if (this != &rhs) {
        budget_ = rhs.budget_;
        count_ = rhs.count_;
        rhs.budget_ = nullptr;
        rhs.count_ = 0;
      }
      return *this;
    }
    // Release a portion of the resource acquired.
    void release(uint64_t count) {
      ld_check(valid());
      ld_check(count <= count_);
      const uint64_t to_release = std::min(count_, count);
      budget_->release(to_release);
      count_ -= to_release;
    }
    // Release all the remaining resource acquired.
    void release() {
      if (budget_) {
        budget_->release(count_);
        budget_ = nullptr;
        count_ = 0;
      }
    }
    // Returns true if the resource has been successfully acquired.
    bool valid() const {
      return budget_ != nullptr;
    }
    operator bool() const {
      return valid();
    }

   private:
    Token(ResourceBudget* budget, uint64_t count)
        : budget_(budget), count_(count) {}

    ResourceBudget* budget_;
    uint64_t count_;

    friend class ResourceBudget;
  };

  /**
   * Acquire @param count units of the resource.
   * @return true on success, false if there's not enough of the resource
   *         available
   */
  bool acquire(uint64_t count = 1) {
    uint64_t prev = used_.load();
    do {
      // Allow at least one allocation of the resource even if that allocation
      // would exceed the limit specified.
      if (prev > std::numeric_limits<uint64_t>::max() - count ||
          (prev + count > limit_ && prev > 0)) {
        return false;
      }
    } while (!used_.compare_exchange_weak(prev, prev + count));
    return true;
  }

  /**
   * Like acquire(), but returns a special Token object which will release the
   * resource upon destruction.  The resulting Token is invalid if resource
   * acquisition failed.  Caller should ensure that the token doesn't outlive
   * the ResourceBudget instance.
   */
  Token acquireToken(uint64_t count = 1) {
    if (!acquire(count)) {
      return Token(); // invalid Token
    }
    return Token(this, count);
  }

  void release(uint64_t count = 1) {
    uint64_t prev = used_.load();
    do {
      // how can one release that which was not acquired?
      ld_check_le(count, prev);
    } while (!used_.compare_exchange_weak(prev, prev - count));
  }

  // Returning signed because used_ can be more than limit_ if setLimit() is
  // called with a lower limit
  int64_t available() const {
    return int64_t(limit_.load()) - used_.load();
  }

  // How much of the budget is currently used.
  // Normally between 0 and 1, but may be greater than 1 if limit was decreased
  // or if a single acquire() call was bigger than the limit.
  double fractionUsed() const {
    uint64_t limit = limit_.load();
    uint64_t used = used_.load();
    return 1. * used / limit;
  }

  void setLimit(uint64_t limit) {
    limit_.store(limit);
  }

  uint64_t getLimit() {
    return limit_.load();
  }

 private:
  std::atomic<uint64_t> limit_, used_{0};
};

}} // namespace facebook::logdevice
