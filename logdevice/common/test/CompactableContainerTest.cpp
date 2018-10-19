/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CompactableContainer.h"

#include <atomic>
#include <deque>
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;

namespace {
// std::allocator adaptor that tracks the number of bytes outstanding
template <typename T>
struct TrackingAllocator : public std::allocator<T> {
  TrackingAllocator() : allocated_(std::make_shared<std::atomic<size_t>>(0)) {}
  TrackingAllocator(const TrackingAllocator& other)
      : std::allocator<T>(other), allocated_(other.allocated_) {}
  TrackingAllocator& operator=(const TrackingAllocator& other) {
    std::allocator<T>::operator=(other);
    allocated_ = other.allocated_;
    return *this;
  }
  template <class U>
  explicit TrackingAllocator(const TrackingAllocator<U>& other)
      : std::allocator<T>(other), allocated_(other.allocated_) {}
  template <typename U>
  struct rebind {
    using other = TrackingAllocator<U>;
  };

  T* allocate(size_t n, const void* hint = nullptr) {
    T* out = std::allocator<T>::allocate(n, hint);
    // fprintf(stderr, "allocate(%zu, %p) = %p\n", n * sizeof(T), hint, out);
    *allocated_ += n * sizeof(T);
    return out;
  }
  void deallocate(T* ptr, size_t n) {
    // fprintf(stderr, "deallocate(%p, %zu)\n", ptr, n * sizeof(T));
    *allocated_ -= n * sizeof(T);
    std::allocator<T>::deallocate(ptr, n);
  }
  size_t allocated() const {
    return *allocated_;
  }

  std::shared_ptr<std::atomic<size_t>> allocated_;
};
} // namespace

TEST(CompactableContainerTest, Deque) {
  struct T {
    char data[100];
  };
  TrackingAllocator<T> alloc;
  using DequeT = std::deque<T, TrackingAllocator<T>>;
  CompactableContainer<DequeT> q{DequeT(alloc)};

  const size_t usage_empty = alloc.allocated();
  ld_info("memory usage by empty queue = %zu bytes", usage_empty);

  {
    for (int i = 0; i < 1000; ++i) {
      q->push_back(T());
      q.observe();
    }
    size_t usage = alloc.allocated();
    ld_info("memory usage by full queue = %zu bytes", usage);
    ASSERT_GT(usage, usage_empty);
  }

  {
    while (!q->empty()) {
      q->pop_front();
    }
    size_t usage = alloc.allocated();
    ld_info("memory usage by queue after draining = %zu bytes", usage);
    // This may be less than `usage_empty' if the deque deallocated some
    // chunks but we expect `std::deque' to hold on to some memory.
    ASSERT_GT(usage, usage_empty);
  }

  {
    q.compact(); // For fun, try `q->shrink_to_fit()' instead here
    size_t usage = alloc.allocated();
    ld_info("memory usage by queue after compacting = %zu bytes", usage);
    ASSERT_EQ(usage, usage_empty);
  }
}
