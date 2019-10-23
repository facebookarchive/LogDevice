/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>
#include <climits>
#include <cstdint>

#include <boost/noncopyable.hpp>
#include <folly/lang/Bits.h>

#include "logdevice/common/CostQueue.h"
#include "logdevice/common/Priority.h"
#include "logdevice/common/checks.h"

/**
 * @file PriorityQueue: an object-intrusive, finite priority bucket,
 *       priority queue for objects supporting both a cost() and
 *       priority() method, that sums the cost of its contents.
 *
 *       Popping an object from PriorityQueue will return the
 *       highest (lowest numbered) priority object, with objects
 *       of the same priority returned in FIFO order.
 *
 *       PriorityQueue also supports trimming objects at or below
 *       a specified priority level until a cost reduction target
 *       is reached.
 *
 * This class is relatively easy to misuse. See caveats in CostQueue.h
 */

namespace facebook { namespace logdevice {

template <typename T, folly::IntrusiveListHook T::*ListHook>
class PriorityQueue : boost::noncopyable {
 public:
  PriorityQueue() {}
  PriorityQueue(PriorityQueue&&) noexcept;
  PriorityQueue& operator=(PriorityQueue&&) noexcept;

  bool empty() const {
    return active_queues_.none();
  }
  bool empty(Priority p) const {
    ld_check(p < Priority::NUM_PRIORITIES);
    return (!active_queues_.test(asInt(p)));
  }

  uint64_t size(Priority& p) const {
    ld_check(p < Priority::NUM_PRIORITIES);
    if (empty(p)) {
      return 0;
    }

    return queues_[asInt(p)].size();
  }

  /** The cost to service all elements in this queue. */
  uint64_t cost() const {
    return total_cost_;
  }

  /** The cost to service all queued elements in a particular priority class. */
  uint64_t cost(Priority p) const {
    return queues_[asInt(p)].cost();
  }

  /* STL idiom conforming methods. */
  T& front();
  T& front(Priority& p);
  void push(T&);
  void pop();
  void pop(Priority& p);
  void erase(T&);

  /**
   * Remove and apply function callback on items enqueued with a priority
   * at or below max_trimable_priority until at least 'to_cut' amount of cost
   * has been removed from the queue.
   *
   * @returns true  if to_cut is 0, or sufficent low priority elements were
   *                found and trimmed to remove to_cut cost from the queue.
   *          false Trimming to_cut cost was not possible. No items were
   *                trimmed.
   */
  bool trim(Priority max_trimable_priority,
            uint64_t to_cut,
            std::function<void(T&)> = nullptr);

 private:
  using Queue = CostQueueBase<T, ListHook>;
  using CostQueues =
      std::array<CostQueue<T, ListHook>, asInt(Priority::NUM_PRIORITIES)>;

  // Sum of calling T::cost() on all members of all queues.
  uint64_t total_cost_ = 0;

  // Set bits indicate CostQueues that contain elements.
  std::bitset<asInt(Priority::NUM_PRIORITIES)> active_queues_;

  // One CostQueue "bucket" per-priority level.
  CostQueues queues_;

  static_assert(asInt(Priority::NUM_PRIORITIES) <
                    sizeof(unsigned long) * CHAR_BIT,
                "PriorityQueue's use of find(First/Last)Set can't handle "
                "the defined number of priority levels.");
};

template <typename T, folly::IntrusiveListHook T::*ListHook>
PriorityQueue<T, ListHook>::PriorityQueue(PriorityQueue&& rhs) noexcept
    : total_cost_(rhs.total_cost_),
      active_queues_(std::move(rhs.active_queues_)),
      queues_(std::move(rhs.queues_)) {
  rhs.total_cost_ = 0;
  rhs.active_queues_.reset();
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
PriorityQueue<T, ListHook>& PriorityQueue<T, ListHook>::
operator=(PriorityQueue&& rhs) noexcept {
  total_cost_ = rhs.total_cost_;
  active_queues_ = std::move(rhs.active_queues_);
  queues_ = std::move(rhs.queues_);
  rhs.total_cost_ = 0;
  rhs.active_queues_.reset();
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
T& PriorityQueue<T, ListHook>::front() {
  int p_index = folly::findFirstSet(active_queues_.to_ulong());
  ld_check(p_index != 0);
  return queues_[p_index - 1].front();
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
T& PriorityQueue<T, ListHook>::front(Priority& p) {
  int p_index = asInt(p);
  return queues_[p_index].front();
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void PriorityQueue<T, ListHook>::push(T& item) {
  int p_index = asInt(item.priority());
  auto& queue = queues_[p_index];

  total_cost_ += item.cost();
  ld_check(total_cost_ >= item.cost());

  queue.push_back(item);
  active_queues_.set(p_index);
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void PriorityQueue<T, ListHook>::erase(T& item) {
  if ((item.*ListHook).is_linked()) {
    int p_index = asInt(item.priority());
    auto& queue = queues_[p_index];

    ld_check(total_cost_ >= item.cost());
    total_cost_ -= item.cost();

    queue.erase(item);
    if (queue.empty()) {
      active_queues_.reset(p_index);
    }
  }
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void PriorityQueue<T, ListHook>::pop() {
  if (!empty()) {
    erase(front());
  }
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
void PriorityQueue<T, ListHook>::pop(Priority& p) {
  int p_index = asInt(p);
  auto& queue = queues_[p_index];
  if (!queue.empty()) {
    erase(queue.front());
  }
}

template <typename T, folly::IntrusiveListHook T::*ListHook>
bool PriorityQueue<T, ListHook>::trim(Priority max_trimable_priority,
                                      uint64_t to_cut,
                                      std::function<void(T&)> cb) {
  if (to_cut == 0) {
    return true;
  }

  // If the number of priorities ever gets large, perhaps
  // use folly::BitSetIterator and extend it for efficient
  // reverse iteration of set bits.
  int fls = folly::findLastSet(active_queues_.to_ulong());
  if (fls == 0) {
    return false;
  }
  int p_index = fls - 1;
  ld_check(p_index < queues_.size());

  uint64_t trimmed_cost = 0;
  while (p_index >= asInt(max_trimable_priority) && trimmed_cost < to_cut) {
    auto& queue = queues_[p_index];
    trimmed_cost += queue.cost();
    p_index--;
  }
  if (trimmed_cost < to_cut) {
    return false;
  }

  // To ease recovery in state machines, batch trimmed notifications via a
  // local queue and guarantee callbacks are delivered in FIFO order.
  Queue trimmed_items;

  p_index = fls - 1;
  auto cost_target = total_cost_ - to_cut;
  while (p_index >= asInt(max_trimable_priority) && total_cost_ > cost_target) {
    auto& queue = queues_[p_index];
    if (!queue.empty()) {
      // Trim newest entries first since we've invested the least
      // amount of time attempting to process them.
      auto& item = queue.back();
      erase(item);
      if (cb) {
        trimmed_items.push_front(item);
      }
    } else {
      p_index--;
    }
  }
  assert(total_cost_ <= cost_target);
  while (!trimmed_items.empty()) {
    auto& item = trimmed_items.front();
    trimmed_items.pop_front();
    cb(item);
  }
  return true;
}

}} // namespace facebook::logdevice
