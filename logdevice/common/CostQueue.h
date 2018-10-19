/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

#include <folly/IntrusiveList.h>

#include "logdevice/common/checks.h"

/**
 * @file CostQueue: an object-intrusive deque container for objects
 *       supporting a cost() method, that sums the cost of its contents.
 * There are a few landmines to avoid when using this class:
 *  1. Don't let the folly::IntrusiveListHook auto-unlink! It won't update the
 *     cost in CostQueue. Removing from CostQueue should only be done with
 *     CostQueue methods, like erase() and pop_back(). An exception to this rule
 *     is cost() == 0 - no cost update is needed, and auto-unlinking is fine.
 *  2. Item's cost() method must return the same value when removing the item
 *     as when adding it. Sounds easy? See next caveat.
 *  3. Don't make cost() method virtual, or be very careful if you do.
 *     Suppose base class A has a virtual method cost(), and ~A() calls
 *     CostQueue::erase(*this); subclass B overrides cost() to return a
 *     different value. This won't work - when ~A() calls erase(), B is already
 *     destroyed, so the call to cost() will be dispatched to A::cost(), not
 *     B::cost(). This can be solved by calling erase() in ~B().
 */

namespace facebook { namespace logdevice {

template <typename T, folly::IntrusiveListHook T::*ListHook>
using CostQueueBase = folly::IntrusiveList<T, ListHook>;

/**
 * An intrusive-list that sums the cost of all items in the
 * list.
 */
template <typename T, folly::IntrusiveListHook T::*ListHook>
class CostQueue : private CostQueueBase<T, ListHook> {
 public:
  CostQueue() : cost_(0) {}
  CostQueue(CostQueue&&) noexcept;
  CostQueue& operator=(CostQueue&&) noexcept;

  /** The cost to service all elements in this queue. */
  uint64_t cost() const {
    return cost_;
  }

  /* STL idiom conforming methods. */
  using CostQueueBase<T, ListHook>::begin;
  using CostQueueBase<T, ListHook>::end;
  using CostQueueBase<T, ListHook>::front;
  using CostQueueBase<T, ListHook>::back;
  using CostQueueBase<T, ListHook>::empty;
  using CostQueueBase<T, ListHook>::size;
  void push_back(T&);
  void push_front(T&);
  void erase(T&);
  void pop_back() {
    erase(back());
  }
  void pop_front() {
    erase(front());
  }

 private:
  // Sum of calling T::cost() on all members of this Queue.
  uint64_t cost_;
};

template <typename T, folly::IntrusiveListHook T::*ListHook>
CostQueue<T, ListHook>::CostQueue(CostQueue&& rhs) noexcept
    : CostQueueBase<T, ListHook>(std::move(rhs)), cost_(rhs.cost_) {
  rhs.cost_ = 0;
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
CostQueue<T, ListHook>& CostQueue<T, ListHook>::
operator=(CostQueue&& rhs) noexcept {
  CostQueueBase<T, ListHook>::operator=(std::move(rhs));
  cost_ = rhs.cost_;
  rhs.cost_ = 0;
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void CostQueue<T, ListHook>::push_back(T& item) {
  cost_ += item.cost();
  ld_check(cost_ >= item.cost());
  ld_check(!(item.*ListHook).is_linked());
  CostQueueBase<T, ListHook>::push_back(item);
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void CostQueue<T, ListHook>::push_front(T& item) {
  cost_ += item.cost();
  ld_check(cost_ >= item.cost());
  ld_check(!(item.*ListHook).is_linked());
  CostQueueBase<T, ListHook>::push_front(item);
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void CostQueue<T, ListHook>::erase(T& item) {
  ld_check(cost_ >= item.cost());
  cost_ -= item.cost();
  ld_check((item.*ListHook).is_linked());
  (item.*ListHook).unlink();
}

}} // namespace facebook::logdevice
