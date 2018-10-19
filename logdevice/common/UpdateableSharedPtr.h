/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include <boost/noncopyable.hpp>
#include <folly/Optional.h>
#include <folly/SpinLock.h>
#include <folly/ThreadLocal.h>

namespace facebook { namespace logdevice {

/**
 * @file UpdateableSharedPtr and FastUpdateableSharedPtr are thread-safe
 *       wrappers around std::shared_ptr.
 *       They allow a shared_ptr to be updated in the presence of concurrent
 *       reads and writes. They are primarily designed for
 *       read-often-write-rarely workloads. The implementation caches
 *       shared_ptr in thread-local storage.
 *
 *       FastUpdateableSharedPtr<T> doesn't clear or update thread-local cache
 *       when updating the pointer. This means that old instances of T can stay
 *       alive in thread-local cache indefinitely if get() is not called from
 *       some threads.
 *
 *       UpdateableSharedPtr<T> doesn't have this problem - it clears the cache
 *       when updating the pointer. This comes at a cost of slower get() and
 *       update().
 *
 *       Implementation note:
 *       For better performance, [Fast]UpdateableSharedPtr use thread-cached
 *       reference counters. The shared_ptr<T> that get() returns is actually a
 *       disguised shared_ptr<shared_ptr<T>>. The idea behind using shared_ptr
 *       to shared_ptr is to make the outer shared_ptr thread-local. This makes
 *       copying it fast because the reference counter is only accessed from one
 *       thread, avoiding cache line bouncing.
 *
 *       get() performance in a benchmark with high contention without updates:
 *         FastUpdateableSharedPtr                 29ns,
 *         UpdateableSharedPtr                     31ns,
 *         without shared_ptr<shared_ptr> trick:  410ns,
 *       without contention:
 *         FastUpdateableSharedPtr                 21ns,
 *         UpdateableSharedPtr                     28ns,
 *         without shared_ptr<shared_ptr> trick:   20ns,
 */

/**
 * Each FastUpdateableSharedPtr contains a version number and a shared_ptr
 * that points to the most recent object. The get() method uses
 * thread-local storage to efficiently return the current pointer
 * without locking when the pointer has not changed. The version of the
 * pointer in thread-local cache is compared to the master version. If
 * the master is found to be newer, it is copied into the thread-local
 * cache under a lock. The update() method grabs the lock, updates the
 * master pointer and bumps the version number.
 */
template <typename T>
class FastUpdateableSharedPtr : boost::noncopyable {
 public:
  explicit FastUpdateableSharedPtr(std::shared_ptr<T> ptr = nullptr) {
    masterPtr_ = std::move(ptr);
    masterVersion_.store(1);
  }

  /**
   * Replaces the managed object.
   */
  void update(std::shared_ptr<T> desired) {
    updateImpl(std::move(desired), nullptr);
  }

  /**
   * Replaces the managed object only if current value of the pointer is equal
   * to expected. Returns true on success, false if current pointer is not equal
   * to desired. If false is returned, places the current value of the pointer
   * into expected
   */
  bool compare_and_swap(std::shared_ptr<T>& expected,
                        std::shared_ptr<T> desired) {
    return updateImpl(std::move(desired), &expected);
  }

  /**
   * Returns a shared_ptr to the managed object.
   *
   * NOTE: This pattern is not safe:
   *         Foo& foo = my_updateable_shared_ptr->get()->getFoo();
   *         ...
   *         // Use `foo` :( Possibly see a crash.  The snippet does not pin the
   *         // shared_ptr.  If the UpdateableSharedPtr is updated between where
   *         // `foo` is assigned and used, the object that was returned by
   *         // get() may be destroyed and `foo` becomes a dangling reference.
   *
   *       The correct way to write this is:
   *         std::shared_ptr<T> ptr = my_updateable_shared_ptr->get();
   *         Foo& foo = ptr->getFoo();
   *         ...
   *         // Use `foo` :) It is guaranteed that the instance containing `foo`
   *         // exists while we have `ptr`.
   */
  std::shared_ptr<T> get() const {
    auto& local = *threadLocalCache_;
    if (local.version != masterVersion_.load()) {
      std::lock_guard<std::mutex> guard(mutex_);

      if (!masterPtr_) {
        local.ptr = nullptr;
      } else {
        // The following expression is tricky.
        //
        // It creates a shared_ptr<shared_ptr<T>> that points to a copy of
        // masterPtr_. The reference counter of this shared_ptr<shared_ptr<T>>
        // will normally only be modified from this thread, which avoids
        // cache line bouncing. (Though the caller is free to pass the pointer
        // to other threads and bump reference counter from there)
        //
        // Then this shared_ptr<shared_ptr<T>> is turned into shared_ptr<T>.
        // This means that the returned shared_ptr<T> will internally point to
        // control block of the shared_ptr<shared_ptr<T>>, but will dereference
        // to T, not shared_ptr<T>.
        local.ptr = std::shared_ptr<T>(
            std::make_shared<std::shared_ptr<T>>(masterPtr_), masterPtr_.get());
      }

      local.version = masterVersion_.load();
    }
    return local.ptr;
  }

 private:
  bool updateImpl(std::shared_ptr<T> ptr, std::shared_ptr<T>* cmp) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (cmp && cmp->get() != masterPtr_.get()) {
        *cmp = masterPtr_;
        return false;
      }
      // Swap to avoid calling ~T() under the lock
      std::swap(masterPtr_, ptr);
      masterVersion_.fetch_add(1);
      return true;
    }
  }

  struct VersionedPointer : boost::noncopyable {
    VersionedPointer() {}
    std::shared_ptr<T> ptr;
    uint64_t version = 0;
  };

  folly::ThreadLocal<VersionedPointer> threadLocalCache_;

  std::shared_ptr<T> masterPtr_;
  std::atomic<uint64_t> masterVersion_;

  // Ensures safety between concurrent update() and get() calls
  mutable std::mutex mutex_;
};

/**
 * Similar to FastUpdateableSharedPtr, but doesn't use a version number.
 * Instead, update() just iterates over all thread-local caches and clears their
 * pointers. update() will briefly block all UpdateableSharedPtrs with the same
 * <T, Tag> pair, so consider using different tags everywhere. update() only
 * blocks other update() calls, first get() call for each thread and destruction
 * of threads that have ever called get().
 */
template <typename T, typename Tag = void>
class UpdateableSharedPtr : boost::noncopyable {
 public:
  explicit UpdateableSharedPtr(std::shared_ptr<T> ptr = nullptr) {
    masterPtr_ = std::move(ptr);
  }

  void update(std::shared_ptr<T> desired) {
    updateImpl(std::move(desired), nullptr);
  }

  bool compare_and_swap(std::shared_ptr<T>& expected,
                        std::shared_ptr<T> desired) {
    return updateImpl(std::move(desired), &expected);
  }

  std::shared_ptr<T> get() const {
    auto& local = *threadLocalCache_;

    // This lock makes UpdateableSharedPtr 25% slower
    // than FastUpdateableSharedPtr.
    std::lock_guard<folly::SpinLock> local_lock(local.lock);

    if (!local.ptr.hasValue()) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!masterPtr_) {
        local.ptr.emplace(nullptr);
      } else {
        // See comment in FastUpdateableSharedPtr::get() for what this is for.
        local.ptr = std::shared_ptr<T>(
            std::make_shared<std::shared_ptr<T>>(masterPtr_), masterPtr_.get());
      }
    }

    return local.ptr.value();
  }

 private:
  bool updateImpl(std::shared_ptr<T> ptr, std::shared_ptr<T>* cmp = nullptr) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (cmp && cmp->get() != masterPtr_.get()) {
        *cmp = masterPtr_;
        return false;
      }
      std::swap(masterPtr_, ptr);
    }

    {
      // This also holds a lock that prevents thread cache entry creation and
      // destruction.
      auto accessor = threadLocalCache_.accessAllThreads();

      for (CachedPointer& local : accessor) {
        std::lock_guard<folly::SpinLock> local_lock(local.lock);
        // We could instead just assign masterPtr_ to local.ptr, but it's better
        // if the thread allocates the Ptr for itself - the allocator is more
        // likely to place its reference counter in a region optimal for access
        // from that thread.
        local.ptr.clear();
      }
    }
    return true;
  }

  struct CachedPointer : boost::noncopyable {
    folly::Optional<std::shared_ptr<T>> ptr;
    folly::SpinLock lock;
  };

  std::shared_ptr<T> masterPtr_;

  // Instead of using Tag as tag for ThreadLocal, effectively use pair (T, Tag),
  // which is more granular.
  struct ThreadLocalTag {};

  folly::ThreadLocal<CachedPointer, ThreadLocalTag> threadLocalCache_;

  mutable std::mutex mutex_;
};

}} // namespace facebook::logdevice
