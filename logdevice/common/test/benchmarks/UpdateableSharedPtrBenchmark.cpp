/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <iostream>
#include <thread>

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "logdevice/common/UpdateableSharedPtr.h"

using namespace facebook::logdevice;

/**
 * @file Benchmark comparing UpdateableSharedPtr, FastUpdateableSharedPtr and
 *       an implementation similar to FastUpdateableSharedPtr, but without
 *       shared_ptr<shared_ptr> trick.
 *
 *       Run with --bm_min_usec=1000000.
 */

namespace slow {

// An implementation with thread local cache of shared_ptrs.
template <typename T>
class UpdateableSharedPtr : boost::noncopyable {
 public:
  explicit UpdateableSharedPtr(std::shared_ptr<T> ptr = nullptr) {
    master_.ptr = std::move(ptr);
    master_.version.store(1);
  }

  std::shared_ptr<T> update(std::shared_ptr<T> ptr) {
    std::lock_guard<std::mutex> guard(mutex_);
    std::swap(master_.ptr, ptr);
    master_.version.fetch_add(1);
    return ptr;
  }

  std::shared_ptr<T> get() const {
    // We are the only thread accessing threadLocalCache_->version so it is
    // fine to use memory_order_relaxed
    auto local_version =
        threadLocalCache_->version.load(std::memory_order_relaxed);
    if (local_version != master_.version.load()) {
      std::lock_guard<std::mutex> guard(mutex_);
      threadLocalCache_->ptr = master_.ptr;
      threadLocalCache_->version.store(
          master_.version.load(), std::memory_order_relaxed);
    }
    return threadLocalCache_->ptr;
  }

 private:
  struct VersionedPointer : boost::noncopyable {
    VersionedPointer() : version(0) {}
    std::shared_ptr<T> ptr;
    std::atomic<uint64_t> version;
  };

  folly::ThreadLocal<VersionedPointer> threadLocalCache_;
  VersionedPointer master_;

  // Ensures safety between concurrent update() and get() calls
  mutable std::mutex mutex_;
};

} // namespace slow

template <class PtrInt>
void benchReads(int n) {
  PtrInt ptr(std::make_shared<int>(42));
  for (int i = 0; i < n; ++i) {
    auto val = ptr.get();
    folly::doNotOptimizeAway(val.get());
  }
}

template <class PtrInt>
void benchWrites(int n) {
  PtrInt ptr;
  std::shared_ptr<int> values[2]{
      std::make_shared<int>(3), std::make_shared<int>(14)};
  for (int i = 0; i < n; ++i) {
    ptr.update(values[i & 1]);
  }
}

template <class PtrInt>
void benchReadsWhenWriting(int n) {
  PtrInt ptr;
  std::atomic<bool> shutdown{false};
  std::thread writing_thread;

  BENCHMARK_SUSPEND {
    writing_thread = std::thread([&] {
      std::shared_ptr<int> values[2]{
          std::make_shared<int>(3), std::make_shared<int>(14)};
      for (uint64_t i = 0; !shutdown.load(); ++i) {
        ptr.update(values[i & 1]);
      }
    });
  }

  for (uint64_t i = 0; i < n; ++i) {
    auto val = ptr.get();
    folly::doNotOptimizeAway(val.get());
  }

  BENCHMARK_SUSPEND {
    shutdown.store(true);
    writing_thread.join();
  }
}

template <class PtrInt>
void benchWritesWhenReading(int n) {
  PtrInt ptr;
  std::atomic<bool> shutdown{false};
  std::thread reading_thread;

  BENCHMARK_SUSPEND {
    reading_thread = std::thread([&] {
      for (uint64_t i = 0; !shutdown.load(); ++i) {
        auto val = ptr.get();
        folly::doNotOptimizeAway(val.get());
      }
    });
  }

  std::shared_ptr<int> values[2]{
      std::make_shared<int>(3), std::make_shared<int>(14)};
  for (uint64_t i = 0; i < n; ++i) {
    ptr.update(values[i & 1]);
  }

  BENCHMARK_SUSPEND {
    shutdown.store(true);
    reading_thread.join();
  }
}

template <class PtrInt>
void benchReadsIn10Threads(int n) {
  PtrInt ptr(std::make_shared<int>(27));
  std::vector<std::thread> threads(10);
  int n_per_thread = n;

  for (std::thread& t : threads) {
    t = std::thread([&] {
      for (int i = 0; i < n; ++i) {
        auto val = ptr.get();
        folly::doNotOptimizeAway(val.get());
      }
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }
}

#define BENCH(name)                                 \
  BENCHMARK(name##_Slow, n) {                       \
    bench##name<slow::UpdateableSharedPtr<int>>(n); \
  }                                                 \
  BENCHMARK(name##_UpdateableSharedPtr, n) {        \
    bench##name<UpdateableSharedPtr<int, int>>(n);  \
  }                                                 \
  BENCHMARK(name##_FastUpdateableSharedPtr, n) {    \
    bench##name<FastUpdateableSharedPtr<int>>(n);   \
  }                                                 \
  BENCHMARK_DRAW_LINE();

BENCH(Reads)
BENCH(Writes)
BENCH(ReadsWhenWriting)
BENCH(WritesWhenReading)
BENCH(ReadsIn10Threads)

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
#endif
