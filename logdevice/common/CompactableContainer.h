/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <iterator>
#include <type_traits>
#include <utility>

/**
 * @file An adapter around std containers which provides a `compact()' method
 * that reclaims memory, at the cost of some bookkeeping overhead, and
 * requiring T to be movable or copyable.  This is good for long-lived
 * containers that grow and shrink in size.
 *
 * The issue is that std::deque/vector::shrink_to_fit() is a "non-binding
 * request" according to the standard and current implementations (like GCC
 * 4.9 libstdc++) choose to make it a no-op (at least for std::deque) under
 * certain, unclear, conditions.
 *
 * Use the arrow operator to access the underlying container, and call
 * `observe()' and `compact()' at your convenience.
 *   CompactableContainer<std::deque<int> > q;
 *   q->push_back(1);
 *   q.observe();
 *   q->pop_front();
 *   q.compact();  // Reclaims memory!  Invalidates iterators and references!
 */

namespace facebook { namespace logdevice {

template <typename Container>
class CompactableContainer {
 public:
  using container_type = Container;
  using value_type = typename container_type::value_type;

  static_assert(std::is_move_constructible<value_type>::value,
                "T must be move-constructible");

  CompactableContainer() {}
  explicit CompactableContainer(container_type c) : c_(std::move(c)) {}

  // Accesses the underlying container
  container_type* operator->() {
    return &c_;
  }
  const container_type* operator->() const {
    return &c_;
  }
  container_type& operator*() {
    return c_;
  }
  const container_type& operator*() const {
    return c_;
  }

  /**
   * Observes the size of the container.  It is recommended to call this after
   * every push/insert.
   */
  void observe() {
    if (c_.size() > max_size_since_compact_) {
      max_size_since_compact_ = c_.size();
    }
  }

  /**
   * Requests the container to be compacted.  This doesn't *always* reclaim
   * memory, only when more than half of the memory is unused.  The tradeoff
   * is the same as with the vector doubling trick: half of the memory may be
   * unused but the amortised complexity of draining the container one-by-one
   * and calling this method every time will be linear.
   *
   * NOTE: May invalidate iterators and references pointing into the
   * container.
   */
  void compact() {
    observe();
    if (c_.size() <= max_size_since_compact_ / 2 &&
        // Don't compact very small containers
        max_size_since_compact_ * sizeof(value_type) >= 512) {
      force_compact();
    }
  }

  /**
   * Forces a compaction, similar to what `shrink_to_fit()' is supposed to do.
   *
   * NOTE: Invalidates iterators and references pointing into the container.
   */
  void force_compact() {
    // NOTE: This explicitly moves every element to a new container one by
    // one.  tmp(std::move(c_)) would just steal a few pointers and have no
    // effect on memory usage.
    container_type tmp(std::make_move_iterator(c_.begin()),
                       std::make_move_iterator(c_.end()),
                       c_.get_allocator());
    c_.swap(tmp);
    max_size_since_compact_ = c_.size();
  }

 private:
  // Underlying container
  container_type c_;
  size_t max_size_since_compact_ = 0;
};

}} // namespace facebook::logdevice
