/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WatchDogThread.h"

#include <signal.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/BacktraceRunner.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

WatchDogThread::WatchDogThread(Processor* p,
                               std::chrono::milliseconds poll_interval,
                               rate_limit_t bt_ratelimit)
    : processor_(p),
      poll_interval_ms_(poll_interval),
      events_called_(processor_->settings()->num_workers, 0),
      events_completed_(processor_->settings()->num_workers, 0),
      bt_ratelimiter_(bt_ratelimit),
      total_stalled_time_ms_(processor_->settings()->num_workers,
                             std::chrono::milliseconds::zero()) {
  ld_check(processor_->getWorkerCount(WorkerType::GENERAL) != 0);
  thread_ = std::thread(&WatchDogThread::run, this);
}

void WatchDogThread::detectStalls() {
  std::vector<int> stalled_worker_pids;
  std::vector<std::string> stalled_worker_names;
  processor_->applyToWorkerPool(
      [&](Worker& w) {
        if (!w.isAcceptingWork()) {
          return;
        }
        int idx = w.idx_.val_;
        ld_check(idx < events_called_.size());

        auto event_loop = checked_downcast<EventLoop*>(w.getExecutor());
        size_t events_called_new = event_loop->event_handlers_called_.load();
        size_t events_completed_new =
            event_loop->event_handlers_completed_.load();
        STAT_ADD(w.processor_->stats_,
                 event_loop_sched_delay,
                 event_loop->delay_us_.exchange(0, std::memory_order_relaxed));

        // Watchdog considers a worker as stalled if the worker hasn't
        // completed any outstanding events since the time Watchdog last
        // checked on it.
        //
        // e.g. 2,1 2,1 OR
        //      2,1 3,1 OR
        //      0,uint64_max 0,uint64_max OR
        //      0,uint64_max 1,uint64_max
        if (events_called_[idx] != events_completed_[idx] &&  // requests were
                                                              // outstanding
            events_completed_[idx] == events_completed_new) { // they are still
                                                              // outstanding
          stalled_worker_pids.push_back(w.getThreadId());
          stalled_worker_names.push_back(w.getName());
          total_stalled_time_ms_[idx] += poll_interval_ms_;

          ld_info("WatchDog found %s(tid:%d) stalled for %ld ms, old("
                  "posted:%zu, completed:%zu), new(posted:%zu, completed:%zu)",
                  w.getName().c_str(),
                  w.getThreadId(),
                  total_stalled_time_ms_[idx].count(),
                  events_called_[idx],
                  events_completed_[idx],
                  events_called_new,
                  events_completed_new);
        } else {
          // Clear accumulated time for idx which is not stalled any more
          total_stalled_time_ms_[idx] = std::chrono::milliseconds::zero();
        }

        events_called_[idx] = events_called_new;
        events_completed_[idx] = events_completed_new;
      },
      Processor::Order::FORWARD,
      WorkerType::GENERAL);

  if (stalled_worker_pids.size()) {
    ld_warning("Found %zu stalled workers", stalled_worker_pids.size());
    STAT_ADD(
        processor_->stats_, num_stalled_workers, stalled_worker_pids.size());

    if (processor_->settings()->watchdog_print_bt_on_stall) {
      auto plugin =
          processor_->getPluginRegistry()->getSinglePlugin<BacktraceRunner>(
              PluginType::BACKTRACE_RUNNER);
      if (plugin != nullptr) {
        for (size_t i = 0; (i < stalled_worker_pids.size()) &&
             (bt_ratelimiter_.isAllowed()) && !processor_->isShuttingDown();
             ++i) {
          ld_info("bt of %s(pid=%d)",
                  stalled_worker_names[i].c_str(),
                  stalled_worker_pids[i]);
          plugin->printBacktraceOnStall(stalled_worker_pids[i]);
        }
        if (!processor_->isShuttingDown()) {
          plugin->printKernelStacktrace();
        }
      }
    }

    if (processor_->settings()->watchdog_abort_on_stall) {
      pthread_kill(stalled_worker_pids[0], SIGUSR1);
    }
  }
}

void WatchDogThread::run() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:watchdog");

  std::unique_lock<std::mutex> cv_lock(mutex_);
  std::chrono::steady_clock::time_point last_entry_time =
      std::chrono::steady_clock::now();
  while (!shutdown_) {
    int64_t loop_entry_delay = msec_since(last_entry_time);
    if (loop_entry_delay - poll_interval_ms_.count() > 100) {
      STAT_INCR(processor_->stats_, watchdog_num_delays);
      RATELIMIT_INFO(std::chrono::minutes(1),
                     1,
                     "Entry into watchdog loop took %lums",
                     loop_entry_delay);
    }
    detectStalls();
    last_entry_time = std::chrono::steady_clock::now();

    cv_.wait_for(cv_lock, poll_interval_ms_);
  }
  ld_info("Exiting the watchdog thread");
}

void WatchDogThread::shutdown() {
  std::unique_lock<std::mutex> cv_lock(mutex_);
  shutdown_ = true;
  cv_.notify_all();
  cv_lock.unlock();

  thread_.join();
}

}} // namespace facebook::logdevice
