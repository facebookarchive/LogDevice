/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <vector>

#include <folly/AtomicIntrusiveLinkedList.h>
#include <folly/Preprocessor.h>

#include "logdevice/common/CompactableContainer.h"
#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * @file An efficient unbounded multi-producer single-consumer queue, suitable
 * for use as a pipe for tasks incoming to an event loop from different
 * threads.
 *
 * Pushing consists of a single compare-and-swap loop to replace the head of
 * the singly-linked list; see folly::AtomicIntrusiveLinkedList::insertHead().
 *
 * Since the only way to pop off of folly::AtomicIntrusiveLinkedList is to
 * consume the entire contents of the list, we buffer items in a thread-local
 * stack (vector), from which our pop() gets items one by one.  This is very
 * efficient in terms of contention because pulling a batch of elements
 * requires a single XCHG; see folly::AtomicIntrusiveLinkedList::reverseSweep().
 *
 * Because this class is expected to be used to hand ownership of work items
 * between threads, the interface deals with std::unique_ptr<T> unlike most
 * other containers.
 */
template <typename T, folly::AtomicIntrusiveLinkedListHook<T> T::*HookMember>
class MPSCQueue {
 public:
  /**
   * Thread-safe
   */
  void push(std::unique_ptr<T> t) {
    ld_check(t != nullptr);
    pushed_.insertHead(t.release());
  }

  /**
   * Pops one element off the queue, or returns nullptr if the queue is empty.
   * Should always be called on the same (the one consumer) thread.
   */
  std::unique_ptr<T> pop() {
    if (pulled_->empty()) {
      // We have nothing buffered in the thread-local std::vector, see if
      // there is anything to pull from the atomic list.
      pull();
      if (pulled_->empty()) {
        return nullptr;
      }
    }
    std::unique_ptr<T> ret = std::move(pulled_->back());
    pulled_->pop_back();
    return ret;
  }

  /**
   * Requests that the queue reclaim memory allocated to excessive capacity.
   * Calling this occasionally (after a pop or batch of pops) ensures that
   * memory usage is O(current_size) instead of O(max_size).
   */
  void compact() {
    pulled_.compact();
  }

  ~MPSCQueue() {
    // AtomicIntrusiveLinkedList does not own elements so pull them into
    // unique_ptrs to avoid leaking
    pull();
  }

 private:
  // This is only touched on the consumer thread so does not need
  // synchronisation.
  CompactableContainer<std::vector<std::unique_ptr<T>>> pulled_;
  // Padding to avoid false sharing between the linked list that producers
  // concurrently write to and the std::vector that only the consumer reads
  // from.
  char FB_ANONYMOUS_VARIABLE(padding)[128];
  folly::AtomicIntrusiveLinkedList<T, HookMember> pushed_;
  // ... As well as any other consumer-only data that may be around the
  // MPSCQueue.
  char FB_ANONYMOUS_VARIABLE(padding)[128];

  void pull() {
    ld_check(pulled_->empty());
    // Calling reverseSweep() here to get elements in LIFO order; pop() will
    // consume back-to-front to get original insertion order (FIFO)
    pushed_.reverseSweep([this](T* t) { pulled_->emplace_back(t); });
    pulled_.observe();
  }
};

}} // namespace facebook::logdevice
