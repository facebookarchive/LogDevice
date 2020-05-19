/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/IOTracing.h"

#include "logdevice/common/chrono_util.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

IOTracing::IOTracing(shard_index_t shard_idx, StatsHolder* stats)
    : shardIdx_(shard_idx), stats_(stats) {}

IOTracing::~IOTracing() {
  if (stallDetectionThread_.enabled) {
    // Stop the thread.
    {
      std::unique_lock lock(stallDetectionThread_.mutex);
      stallDetectionThread_.enabled = false;
    }
    stallDetectionThread_.cv.notify_all();
    stallDetectionThread_.thread.join();
  }
}

void IOTracing::reportCompletedOp(std::chrono::steady_clock::duration duration,
                                  LockedThreadState& locked_state) {
  auto threshold = options_->threshold.load(std::memory_order_relaxed);
  if (threshold.count() <= 0 || duration >= threshold) {
    PER_SHARD_STAT_INCR(stats_, slow_iops, shardIdx_);
    ld_info("[io:S%d] %s  %.3fms",
            static_cast<int>(shardIdx_),
            locked_state->context.c_str(),
            to_sec_double(duration) * 1e3);
  }
}

void IOTracing::updateOptions(bool tracing_enabled,
                              std::chrono::milliseconds threshold,
                              std::chrono::milliseconds stall_threshold) {
  options_->enabled.store(tracing_enabled);
  options_->threshold.store(threshold);
  auto prev_stall_threshold =
      options_->stall_threshold.exchange(stall_threshold);

  // Start or stop stall detection thread if needed.
  bool stall_detection_enabled = tracing_enabled && stall_threshold.count() > 0;
  if (stall_detection_enabled != stallDetectionThread_.enabled) {
    if (stall_detection_enabled) {
      // Start the thread.
      stallDetectionThread_.enabled = true;
      stallDetectionThread_.thread =
          std::thread([this] { stallDetectionThreadMain(); });
    } else {
      // Stop the thread.
      {
        std::unique_lock lock(stallDetectionThread_.mutex);
        stallDetectionThread_.enabled = false;
      }
      stallDetectionThread_.cv.notify_all();
      stallDetectionThread_.thread.join();
    }
  } else if (stall_threshold != prev_stall_threshold) {
    // Stall detection period changed. Wake up the thread to apply the update.
    // Useful when threshold is decreased from a very large value.
    {
      // This is needed to avoid a race condition where the cv notification
      // may get lost if it happens at the wrong time.
      std::unique_lock lock(stallDetectionThread_.mutex);
    }
    stallDetectionThread_.cv.notify_all();
  }
}

void IOTracing::stallDetectionThreadMain() {
  ThreadID::set(
      ThreadID::Type::UTILITY, folly::sformat("io-stall:s{}", shardIdx_));
  ld_info("Started IO stall detection thread for shard %d",
          static_cast<int>(shardIdx_));

  std::unique_lock lock(stallDetectionThread_.mutex);
  while (stallDetectionThread_.enabled) {
    std::chrono::milliseconds stall_threshold =
        options_->stall_threshold.load();
    std::chrono::milliseconds longest_ongoing_op{0};
    size_t num_stalled_threads = 0;

    // Check all threads.
    for (auto& state : threadStates_.accessAllThreads()) {
      auto locked_state = state.lock();
      if (locked_state->currentOpStartTime ==
          std::chrono::steady_clock::time_point::max()) {
        continue;
      }
      std::chrono::milliseconds duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() -
              locked_state->currentOpStartTime);
      longest_ongoing_op = std::max(longest_ongoing_op, duration);
      if (duration < stall_threshold) {
        continue;
      }

      ++num_stalled_threads;
      ld_warning("An IO operation in shard %d appears to be stuck for %.3fs "
                 "(so far): %s",
                 static_cast<int>(shardIdx_),
                 to_sec_double(duration),
                 locked_state->context.c_str());
    }

    PER_SHARD_STAT_SET(
        stats_, longest_ongoing_io_ms, shardIdx_, longest_ongoing_op.count());
    PER_SHARD_STAT_SET(
        stats_, num_threads_stalled_on_io, shardIdx_, num_stalled_threads);

    // Wake every 1/3 of the stall threshold, rounded up. The 1/3 is arbitrary.
    std::chrono::milliseconds wait_duration{(stall_threshold.count() + 2) / 3};
    // Avoid waiting too little. Especially avoid zero if the setting was
    // changed to zero.
    wait_duration = std::max(wait_duration, std::chrono::milliseconds(10));
    stallDetectionThread_.cv.wait_for(lock, wait_duration);
  }

  ld_info("Stopped IO stall detection thread for shard %d",
          static_cast<int>(shardIdx_));
}

}} // namespace facebook::logdevice
