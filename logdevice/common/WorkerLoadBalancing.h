/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <mutex>
#include <random>

#include <folly/Preprocessor.h>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Keeps track of load across workers and offers to select workers for
 * new work based on load.
 *
 * See WorkerLoadBalancing.cpp for the selection strategy.
 */

class WorkerLoadBalancing {
 public:
  /**
   * Main constructor.  Pass an initialized RNG; tests can use a fixed seed,
   * production can properly initialize.
   */
  explicit WorkerLoadBalancing(size_t nworkers) : loads_(nworkers) {
    static_assert(sizeof(PaddedLoad) == 128, "");
  }

  /**
   * Called by workers to report their load.  Load should be reported per some
   * unit of time.
   *
   * Thread-safe.
   */
  void reportLoad(worker_id_t idx, int64_t load) {
    ld_check(idx.val_ >= 0);
    ld_check(idx.val_ < loads_.size());
    loads_[idx.val_].val.store(load);
  }

  /**
   * Selects a worker based on load (less loaded workers are more likely to be
   * chosen).
   *
   * Thread-safe.
   */
  worker_id_t selectWorker();

 private:
  struct PaddedLoad {
    std::atomic<int64_t> val{0};
    char FB_ANONYMOUS_VARIABLE(padding)[128 - sizeof(std::atomic<int64_t>)];
  };

  std::vector<PaddedLoad> loads_;
};

}} // namespace facebook::logdevice
