/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <queue>
#include <thread>
#include <unordered_set>

#include <folly/Random.h>
#include <folly/ThreadLocal.h>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"
#include "logdevice/test/ldbench/worker/Worker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

static constexpr const char* BENCH_NAME = "read";

/**
 * Read benchmark worker.
 *
 * Tail a set of log ids until told to stop or until duration has passed.
 */
class ReadWorker final : public Worker {
 public:
  using Worker::Worker;
  int run() override;

 private:
  struct LogTailerState {
    logid_t log_id;
    size_t tailer_idx;

    RandomEventSequence::State restarts_state;
    LibeventTimer restart_timer;

    // How far in the backlog to start reading. If none, start at tail.
    folly::Optional<std::chrono::milliseconds> backlog_depth;

    // LSN from which we should start reading. If none, we're in the process of
    // finding that LSN (using getTailLSN or findTime).
    folly::Optional<lsn_t> start_lsn;

    // Index in readers_.
    // -1 if the tailer is not running (e.g. waiting for a findTime).
    int reader_idx = -1;

    // Used for retrying failed findTime(), getTailLSN() and startReading().
    ExponentialBackoffTimer retry_timer;

    // Used for resuming reading after it's paused by rate limiter.
    LibeventTimer resume_timer;

    // Some information that might be useful for debugging.
    // Updated from record delivery callback.
    std::atomic<lsn_t> last_seen_lsn{LSN_INVALID};
    AtomicRecordTimestamp last_seen_record_timestamp;
    AtomicRecordTimestamp last_record_delivery_time;

    // True if we're told by rate_limiter_ to sleep for a little. We do the
    // sleeping using LogTailerState::resume_timer, and after the delay we
    // tell AsyncReader to redeliver the last record.
    // In record callback we use this set to detect that it's a redelivery,
    // in which case we don't need to report the record to rate_limiter_.
    // This is best-effort: it's not updated when the tailer is restarted,
    // potentially on a different reader and worker thread. But that's ok, worst
    // that can happen is a bit too much or too little throttling.
    std::atomic<bool> waiting_for_rate_limiter{false};

    std::atomic<bool> is_backlog_reading{false};

    LogTailerState(logid_t log, size_t idx) : log_id(log), tailer_idx(idx) {}
  };

  struct LogState {
    std::vector<std::unique_ptr<LogTailerState>> tailers;

    LogState() = default;
    LogState(LogState&& s) = default;
    LogState& operator=(LogState&& s) = default;
  };

  struct ReaderState {
    std::unique_ptr<LogStoreReader> reader;
    // Logs that this reader is reading. Modified from ev_ thread, with
    // logs_mutex locked exclusively.
    std::unordered_map<logid_t, LogTailerState*> logs;
    folly::SharedMutex logs_mutex;
  };

  // Called from LogTailerState::retry_timer.
  // If start_lsn is not known, issues a getTailLSN/findTime which will assign
  // start_lsn and call tryStartTailer() again upon completion.
  // If start_lsn is known, picks one of readers_ and starts reading.
  // If anything fails, schedules a retry using retry_timer.
  void tryStartTailer(LogTailerState* tailer);

  void activateRestartTimer(LogTailerState* tailer);
  void maybeRestartTailer(LogTailerState* tailer);

  void stopTailer(LogTailerState* tailer);

  void printProgress(double seconds_since_start,
                     double seconds_since_last_call) override;
  void dumpDebugInfo() override;

  void maybeSetBacklogDepth(LogTailerState* tailer);

  std::atomic<uint64_t> nrecords_{0};
  std::atomic<uint64_t> ngaps_{0};
  std::atomic<uint64_t> nrestarts_{0};
  std::atomic<uint64_t> nrestarts_skipped_{0};
  std::atomic<uint64_t> nbytes_{0};

  // Number of LogTailerState-s that are not waiting for
  // getTailLSN()/findTime()/startReading().
  std::atomic<uint64_t> num_active_tailers_{0};

  // Values of the above atomic at the time of previous call to printProgress().
  uint64_t prev_nrecords_ = 0;
  uint64_t prev_nbytes_ = 0;

  std::vector<ReaderState> readers_;
  std::unordered_map<logid_t, LogState, logid_t::Hash> logs_;
  size_t num_tailers_;
  RoughProbabilityDistribution restart_backlog_depth_distribution_;
  RandomEventSequence restarts_sequence_;

  // Used for limiting total read rate (in bytes/s) across all tailers.
  RateLimiter rate_limiter_;
  // How soon AsyncReader redelivers a rejected record by default.
  // Used for throttling.
  std::chrono::milliseconds default_redelivery_delay_;
};

static double steadyTime() {
  return std::chrono::duration_cast<std::chrono::duration<double>>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void ReadWorker::tryStartTailer(LogTailerState* tailer) {
  ld_check(tailer->reader_idx == -1);
  ld_check(!tailer->retry_timer.isActive());

  // Get tail LSN if needed.
  if (!tailer->start_lsn.hasValue()) {
    if (options.read_all) {
      // Start at the beginning of the log.
      tailer->start_lsn = LSN_OLDEST;
    } else {
      // Do either getTailLSN() or findTime() to determine where to start.
      bool backlog = tailer->backlog_depth.hasValue();
      tailer->is_backlog_reading = backlog;

      // It's safe to capture `tailer` by raw pointer because `LogTailerState`s
      // are never deleted while isStopped() is false.
      auto cb = [tailer, backlog](bool, lsn_t lsn) {
        if (lsn == LSN_INVALID) {
          RATELIMIT_INFO(std::chrono::seconds(10),
                         2,
                         "%s() failed for log %lu",
                         backlog ? "findTime" : "getTailLSN",
                         tailer->log_id.val_);
          tailer->retry_timer.activate();
        } else {
          // Add 1 to getTailLSN() result.
          // In kafka, getting tails will return OFFSET_END
          // However, OFFSET_END + 1 == 0;
          lsn = lsn + !backlog > lsn ? lsn + !backlog : lsn;
          tailer->start_lsn = lsn;
          tailer->retry_timer.fire();
        }
      };

      if (backlog) {
        auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
            (std::chrono::system_clock::now() - tailer->backlog_depth.value())
                .time_since_epoch());
        bool rv = findTime(tailer->log_id, ts, cb);
        if (rv != true) {
          RATELIMIT_INFO(std::chrono::seconds(10),
                         2,
                         "Failed to post findTime() for log %lu: %s",
                         tailer->log_id.val_,
                         error_name(err));
          tailer->retry_timer.activate();
        }
      } else {
        bool rv = getTailLSN(tailer->log_id, cb);
        if (rv != true) {
          RATELIMIT_INFO(std::chrono::seconds(10),
                         2,
                         "Failed to post getTailLSN() for log %lu: %s",
                         tailer->log_id.val_,
                         error_name(err));
          tailer->retry_timer.activate();
        }
      }

      return;
    }
  }

  ld_check(tailer->start_lsn.hasValue());

  // Pick a random reader that isn't reading this log.
  int ri = -1;
  size_t offset = folly::Random::rand32() % readers_.size();
  for (size_t i = 0; i < readers_.size(); ++i) {
    size_t candidate = (offset + i) % readers_.size();
    if (!readers_[candidate].logs.count(tailer->log_id)) {
      ri = (int)candidate;
      break;
    }
  }
  ld_check(ri != -1);

  // Start reading.
  auto& r = readers_[ri];
  bool rv = r.reader->startReading(
      tailer->log_id.val(), tailer->start_lsn.value(), LSN_MAX);
  if (rv != true) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Failed to start reading log %lu: %s",
                   tailer->log_id.val_,
                   error_name(err));
    tailer->retry_timer.activate();
    return;
  }

  {
    folly::SharedMutex::WriteHolder lock(r.logs_mutex);
    auto ins = r.logs.emplace(tailer->log_id, tailer);
    ld_check(ins.second);
  }
  tailer->reader_idx = ri;
  ++num_active_tailers_;
  STAT_INCR(stats_.get(), ldbench->readers_active);
}

void ReadWorker::stopTailer(LogTailerState* tailer) {
  if (tailer->reader_idx == -1) {
    tailer->retry_timer.reset();
  } else {
    auto& r = readers_[tailer->reader_idx];
    auto it = r.logs.find(tailer->log_id);
    ld_check(it != r.logs.end());
    r.logs.erase(it);
    bool rv = r.reader->stopReading(tailer->log_id.val());
    ld_check(rv == true);
    tailer->resume_timer.cancel();
  }

  tailer->backlog_depth.reset();
  tailer->start_lsn.reset();
  tailer->reader_idx = -1;
  STAT_DECR(stats_.get(), ldbench->readers_active);
  --num_active_tailers_;
}

void ReadWorker::activateRestartTimer(LogTailerState* tailer) {
  double now = steadyTime();
  double t;
  while (true) {
    t = restarts_sequence_.nextEvent(tailer->restarts_state);
    if (t >= now) {
      break;
    }
    STAT_INCR(stats_.get(), ldbench->reader_restarts_skipped);
    ++nrestarts_skipped_;
  }

  auto duration = std::chrono::duration<double>(t - now);
  tailer->restart_timer.activate(
      std::chrono::duration_cast<std::chrono::microseconds>(duration));
}

void ReadWorker::maybeRestartTailer(LogTailerState* tailer) {
  if (tailer->reader_idx == -1) {
    STAT_INCR(stats_.get(), ldbench->reader_restarts_skipped);
    ++nrestarts_skipped_;
    return;
  }

  STAT_INCR(stats_.get(), ldbench->reader_restarts_done);
  ++nrestarts_;
  stopTailer(tailer);
  maybeSetBacklogDepth(tailer);
  tailer->retry_timer.fire();
}

int ReadWorker::run() {
  // Get the log set from config.
  std::vector<logid_t> logs;
  if (getLogs(logs)) {
    return 1;
  }
  if (options.partition_by == PartitioningMode::LOG) {
    logs = getLogsPartition(logs);
  }

  // Push stats to ODS to indicate some of the settings used.
  bool no_restarts = options.reader_restart_period.count() < 0;
  STAT_SET(stats_.get(),
           ldbench->reader_restart_period_target_ms,
           no_restarts ? 0 : options.reader_restart_period.count());
  STAT_SET(stats_.get(),
           ldbench->reader_avg_backlog_depth_target_ms,
           options.restart_backlog_depth.count());
  // 1000x because floating point stats don't seem supported
  STAT_SET(stats_.get(),
           ldbench->reader_avg_fanout_target_1000x,
           1000. * options.fanout);
  // 1000x because floating point stats don't seem supported
  STAT_SET(stats_.get(),
           ldbench->reader_target_restart_backlog_probability_1000x,
           1000. * options.restart_backlog_probability);

  rate_limit_t rate_limit = options.rate_limit_bytes;
  rate_limit.first = (rate_limit.first + options.worker_id_count - 1) /
      options.worker_id_count;
  rate_limiter_ = RateLimiter(rate_limit);
  STAT_SET(stats_.get(),
           ldbench->reader_rate_limit_bytes_per_sec_per_worker,
           rate_limit.second.count() == 0
               ? 0
               : 1000.0 * rate_limit.first / rate_limit.second.count());

  if (options.sys_name == "logdevice") {
    auto client_settings =
        dynamic_cast<ClientSettingsImpl*>(&client_->settings());
    ld_check(client_settings != nullptr);
    default_redelivery_delay_ =
        client_settings->getSettings()->client_initial_redelivery_delay;
  }

  restart_backlog_depth_distribution_ = RoughProbabilityDistribution(
      std::chrono::duration_cast<std::chrono::duration<double>>(
          options.restart_backlog_depth)
          .count(),
      options.restart_backlog_depth_distribution);
  restarts_sequence_ = RandomEventSequence(options.reader_restart_spikiness);

  // Determine number of tailers for each log and populate `LogState`s.
  RoughProbabilityDistribution fanout_distribution(
      options.fanout, options.fanout_distribution);
  // We want the average frequency of restarts to be equal to
  // 1/restart_period, as opposed to average period being equal to
  // restart_period.
  RoughProbabilityDistribution restart_rate_distribution(
      no_restarts ? 1
                  : 1. /
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  options.reader_restart_period)
                  .count(),
      options.reader_restart_period_distribution.inverse());
  size_t max_tailers_per_log = 0;
  size_t total_my_tailers = 0;
  std::map<logid_t, size_t> tailers_per_log;

  // Wait until start time
  waitUntilStartTime();

  // Run most of the initialization in event thread to be able to manipulate
  // timers and such.
  executeOnEventLoopSync([&] {
    for (logid_t log : logs) {
      // Figure out how many tailers for this log we're going to run.
      double x = folly::hash::hash_128_to_64(log.val_, 262171) /
          (std::numeric_limits<uint64_t>::max() + 1.);
      double y = folly::hash::hash_128_to_64(log.val_, 833811) /
          (std::numeric_limits<uint64_t>::max() + 1.);
      size_t total_tailers = fanout_distribution.sampleInteger(x, y);
      size_t my_tailers = 0;
      if (options.partition_by == PartitioningMode::LOG) {
        my_tailers = total_tailers;
      } else {
        ld_check(options.partition_by == PartitioningMode::LOG_AND_IDX);
        // Figure out how many of the tailers are assigned to this worker.
        for (uint64_t i = 0; i < total_tailers; ++i) {
          if (folly::hash::hash_128_to_64(
                  folly::hash::hash_128_to_64(log.val_, 877211), i) %
                  options.worker_id_count ==
              options.worker_id_index) {
            ++my_tailers;
          }
        }
      }

      if (my_tailers == 0) {
        continue;
      }

      auto ins = logs_.emplace(log, LogState());
      ld_check(ins.second);

      // Create the tailers but don't start them yet.
      for (size_t idx = 0; idx < my_tailers; ++idx) {
        auto tailer = std::make_unique<LogTailerState>(log, idx);
        auto tailer_ptr = tailer.get();

        if (folly::Random::randDouble01() <
            options.pct_readers_consider_backlog_on_start) {
          // Consider whether this tailer should start in the backlog.
          maybeSetBacklogDepth(tailer_ptr);
        }

        tailer->retry_timer.assign(
            [this, tailer_ptr] { tryStartTailer(tailer_ptr); },
            std::chrono::milliseconds(100),
            std::chrono::minutes(5));

        if (!rate_limiter_.isUnlimited()) {
          tailer->resume_timer.assign(&ev_->getEvBase(), [this, tailer_ptr] {
            int reader_idx = tailer_ptr->reader_idx;
            ld_check(reader_idx != -1);
            readers_[reader_idx].reader->resumeReading(
                tailer_ptr->log_id.val());
          });
        }

        if (!no_restarts) {
          double z = folly::hash::hash_128_to_64(
                         folly::hash::hash_128_to_64(log.val_, 15563), idx) /
              (std::numeric_limits<uint64_t>::max() + 1.);
          tailer->restarts_state = restarts_sequence_.newState(
              restart_rate_distribution.sampleFloat(z), steadyTime());
          tailer->restart_timer.assign(&ev_->getEvBase(), [this, tailer_ptr] {
            activateRestartTimer(tailer_ptr);
            maybeRestartTailer(tailer_ptr);
          });
        }

        ins.first->second.tailers.push_back(std::move(tailer));
      }

      tailers_per_log[log] = my_tailers;
      max_tailers_per_log = std::max(max_tailers_per_log, my_tailers);
      total_my_tailers += my_tailers;
    }

    // Use 2x more AsyncReader objects than necessary. This leaves more room
    // for random assignment of logs to readers.
    readers_ = std::vector<ReaderState>(max_tailers_per_log * 2);
    num_tailers_ = total_my_tailers;

    // Create the readers.
    for (size_t i = 0; i < readers_.size(); ++i) {
      // Create record and gap callbacks.
      auto record_cb = [this, reader_idx = i](
                           LogIDType logid,
                           LogPositionType lsn,
                           std::chrono::milliseconds timestamp,
                           std::string payload) {
        auto delivery_time = std::chrono::system_clock::now();
        logid_t log(logid);
        ReaderState& reader_state = readers_.at(reader_idx);
        LogTailerState* tailer_state;
        {
          folly::SharedMutex::ReadHolder lock(reader_state.logs_mutex);
          auto it = reader_state.logs.find(log);
          if (it != reader_state.logs.end()) {
            tailer_state = it->second;
          } else {
            // Probably the tailer was restarted on a different reader.
            tailer_state = nullptr;
          }
        }

        // Bump some per-log stats.
        if (tailer_state != nullptr) {
          tailer_state->last_seen_lsn.store(lsn);
          tailer_state->last_seen_record_timestamp.store(
              RecordTimestamp(timestamp));
          tailer_state->last_record_delivery_time.store(RecordTimestamp::now());
        }

        auto update_read_bytes_stats = [&]() {
          ++nrecords_;
          STAT_INCR(stats_.get(), ldbench->reader_records);
          nbytes_ += payload.size();
          STAT_ADD(stats_.get(), ldbench->reader_bytes, payload.size());

          RecordWriterInfo info;
          bool have_writer_info = false;
          if (options.record_writer_info) {
            Payload tmp(payload.data(), payload.size());
            int rv = info.deserialize(tmp);
            have_writer_info = rv == 0;
          }

          if (!have_writer_info) {
            STAT_INCR(
                stats_.get(), ldbench->reader_records_without_writer_info);
          }

          if (tailer_state) {
            if (tailer_state->is_backlog_reading) {
              STAT_ADD(
                  stats_.get(), ldbench->reader_backlog_bytes, payload.size());
            } else {
              if (have_writer_info && stats_) {
                int64_t latency_usec =
                    (to_usec(delivery_time.time_since_epoch()) -
                     info.client_timestamp)
                        .count();
                stats_->get().ldbench->delivery_latency->add(latency_usec);
              }
            }
          }
        };

        // If this record is a redelivery caused by throttling, we don't
        // need to report it to the rate limiter
        // (see RateLimiter::isAllowed()).
        // Also, if we failed to look up LogTailerState due to a recent restart,
        // just let the record through.
        if (tailer_state == nullptr ||
            tailer_state->waiting_for_rate_limiter.exchange(false)) {
          update_read_bytes_stats();
          return true;
        }

        // Ask rate limiter how long we need to wait before processing the
        // record.
        RateLimiter::Duration to_wait;
        bool allowed = rate_limiter_.isAllowed(
            payload.size(), &to_wait, default_redelivery_delay_);
        if (!allowed) {
          // Rate limiter won't let us proceed any time soon.
          // Let AsyncReader redeliver the record after its default timeout.
          return false;
        }
        if (to_wait.count() == 0) {
          // No delay needed. The normal case.
          update_read_bytes_stats();
          return true;
        }

        // Need to wait for a bit due to throttling. Start a timer.
        tailer_state->waiting_for_rate_limiter.store(true);
        ev_->add([this, tailer_state, to_wait] {
          if (!isStopped() && tailer_state->reader_idx != -1) {
            tailer_state->resume_timer.activate(
                std::chrono::duration_cast<std::chrono::microseconds>(to_wait));
          }
        });

        return false;
      };
      auto gap_cb = [this, reader_idx = i](LogStoreGapType type,
                                           LogIDType logid,
                                           LogPositionType lo_lsn,
                                           LogPositionType hi_lsn) {
        if (type == LogStoreGapType::ACCESS) {
          STAT_INCR(stats_.get(), ldbench->reader_gap_ACCESS);
        } else if (type == LogStoreGapType::DATALOSS) {
          STAT_INCR(stats_.get(), ldbench->reader_gap_DATALOSS);
        } else {
          STAT_INCR(stats_.get(), ldbench->reader_gap_OTHER);
        }

        if (type == LogStoreGapType::DATALOSS ||
            type == LogStoreGapType::ACCESS || type == LogStoreGapType::OTHER ||
            hi_lsn >= LSN_MAX) {
          RATELIMIT_INFO(std::chrono::seconds(2),
                         2,
                         "Gap for log %lu lsn [%lu, %lu]",
                         logid,
                         lo_lsn,
                         hi_lsn);
        }

        logid_t log(logid);
        ReaderState& reader_state = readers_.at(reader_idx);
        LogTailerState* tailer_state;
        {
          folly::SharedMutex::ReadHolder lock(reader_state.logs_mutex);
          auto it = reader_state.logs.find(log);
          if (it != reader_state.logs.end()) {
            tailer_state = it->second;
          } else {
            tailer_state = nullptr;
          }
        }

        if (tailer_state != nullptr) {
          tailer_state->last_seen_lsn.store(hi_lsn);
        }

        return true;
      };

      if (!options.pretend) {
        auto r = client_holder_->createReader();
        r->setWorkerRecordCallback(std::move(record_cb));
        r->setWorkerGapCallback(std::move(gap_cb));
        readers_[i].reader = std::move(r);
      }
    }

    // Post requests to start reading.
    if (!options.pretend) {
      ld_info(
          "Starting %lu tailers in %lu logs", total_my_tailers, logs_.size());

      for (auto& kv : logs_) {
        for (auto& tailer : kv.second.tailers) {
          if (!no_restarts) {
            activateRestartTimer(tailer.get());
          }
          tailer->retry_timer.fire();
        }
      }

      STAT_ADD(stats_.get(), ldbench->readers_assigned, total_my_tailers);
      STAT_ADD(stats_.get(), ldbench->reader_num_distinct_logs, logs_.size());
      STAT_SET(stats_.get(),
               ldbench->reader_max_readers_per_log,
               max_tailers_per_log);
    }
  });

  // Start counting benchmark time. It's a bit too early: we still haven't
  // gotten start LSNs and haven't started reading, but hopefully it doesn't
  // take too long.
  std::chrono::milliseconds actual_duration_ms = sleepForDurationOfTheBench();
  ld_check(isStopped());

  // Gracefully stop reading.
  if (!options.pretend) {
    ld_info("Stopping reads");
    executeOnEventLoopSync([&] {
      for (auto& kv : logs_) {
        for (auto& tailer : kv.second.tailers) {
          tailer->restart_timer.cancel();
          stopTailer(tailer.get());
        }
      }
    });
    // No new readers could start while we're here because before calling
    // startReading() we always check isStopped().
  }

  ld_info("Destroying readers");
  for (auto& r : readers_) {
    r.reader.reset();
  }

  // Make sure no callbacks will be called after ReadWorker is destroyed.
  destroyClient();

  ld_info("All done");

  std::cout << actual_duration_ms.count() << ' ' << nrecords_ << ' ' << ngaps_
            << ' ' << nrestarts_ << ' ' << nrestarts_skipped_ << ' ' << nbytes_
            << '\n';

  return 0;
}

void ReadWorker::printProgress(double seconds_since_start,
                               double seconds_since_last_call) {
  uint64_t nrecords = nrecords_.load();
  uint64_t nbytes = nbytes_.load();
  double records_per_sec =
      (nrecords - prev_nrecords_) / seconds_since_last_call;
  double bytes_per_sec = (nbytes - prev_nbytes_) / seconds_since_last_call;

  prev_nrecords_ = nrecords;
  prev_nbytes_ = nbytes;

  std::array<std::array<char, 32>, 6> bufs; // for commaprint_r()
  ld_info("ran for: %.3fs, records: %s, bytes: %s, gaps: %lu, restarts: %lu, "
          "restarts skipped: %lu, logs initializing: %lu/%lu, records/s: %s, "
          "bytes/s: %s",
          seconds_since_start,
          commaprint_r(nrecords, &bufs[0][0], 32),
          commaprint_r(nbytes, &bufs[1][0], 32),
          ngaps_.load(),
          nrestarts_.load(),
          nrestarts_skipped_.load(),
          num_tailers_ - num_active_tailers_.load(),
          num_tailers_,
          commaprint_r((uint64_t)records_per_sec, &bufs[2][0], 32),
          commaprint_r((uint64_t)bytes_per_sec, &bufs[3][0], 32));
}

void ReadWorker::dumpDebugInfo() {
  std::array<std::array<char, 32>, 6> bufs; // for commaprint_r()
  ld_error("Debug info:\nlogs: %s, records: %s, gaps: %s, restarts: "
           "%s, restarts skipped: %s, bytes: %s",
           commaprint_r(logs_.size(), &bufs[0][0], 32),
           commaprint_r(nrecords_.load(), &bufs[1][0], 32),
           commaprint_r(ngaps_.load(), &bufs[2][0], 32),
           commaprint_r(nrestarts_.load(), &bufs[3][0], 32),
           commaprint_r(nrestarts_skipped_.load(), &bufs[4][0], 32),
           commaprint_r(nbytes_.load(), &bufs[5][0], 32));

  RecordTimestamp now = RecordTimestamp::now();

  // Print states of all tailers into a string. There can be tens of thousands
  // of them, so use very compact format and break it up into pieces
  // (ld_error() truncates the message to something around 1900 characters).

  std::vector<std::string> pieces;
  std::stringstream ss;
  auto maybe_start_new_piece = [&] {
    if (ss.tellp() > 1500) {
      pieces.push_back(ss.str());
      ss = std::stringstream();
    }
  };

  bool first_log = true;
  for (auto& p : logs_) {
    if (!first_log) {
      ss << ",";
      maybe_start_new_piece();
    }
    first_log = false;

    ss << "L" << p.first.val() << ":[";
    LogState& log_state = p.second;
    for (size_t tailer_idx = 0; tailer_idx < log_state.tailers.size();
         ++tailer_idx) {
      if (tailer_idx > 0) {
        ss << ';';
        maybe_start_new_piece();
      }

      ss << tailer_idx << ":";
      LogTailerState& tailer = *log_state.tailers[tailer_idx];

      char tailer_state;
      if (!tailer.start_lsn.hasValue()) {
        // Waiting for getTailLSN()/findTime().
        tailer_state = 'F';
      } else if (tailer.reader_idx == -1) {
        // startReading() failed, pending retry.
        tailer_state = 'S';
      } else if (tailer.resume_timer.isActive()) {
        // Throttled, waiting for rate limiter.
        // Note that this doesn't capture all the throttling, see
        // rate_limiter_.isAllowed() = false case in record callback.
        tailer_state = 'T';
      } else {
        // Reading.
        tailer_state = 'R';
      }
      ss << tailer_state;

      if (tailer.reader_idx != -1) {
        ss << tailer.reader_idx;
      }

      lsn_t lsn = tailer.last_seen_lsn.load();
      if (lsn == LSN_INVALID) {
        // This is a frequent case, so shorten "LSN_INVALID" to "0".
        ss << ":0";
      } else {
        ss << ":" << lsn_to_string(lsn);

        RecordTimestamp delivery_time = tailer.last_record_delivery_time;
        if (delivery_time != RecordTimestamp::min()) {
          RecordTimestamp record_timestamp = tailer.last_seen_record_timestamp;
          double delivered_seconds_ago =
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  now - delivery_time)
                  .count();
          double lag_seconds =
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  delivery_time - record_timestamp)
                  .count();
          ss.setf(std::ios::fixed, std::ios::floatfield);
          ss.precision(3);
          ss << "," << delivered_seconds_ago << "s ago," << lag_seconds
             << "s lag";
        }
      }
    }
    ss << "]";
  }

  // Write out the dump.
  pieces.push_back(ss.str());
  for (size_t i = 0; i < pieces.size(); ++i) {
    if (pieces.size() == 1) {
      ld_error("Tailers: %s", pieces[i].c_str());
    } else {
      ld_error(
          "Tailers [%lu/%lu]: %s", i + 1, pieces.size(), pieces[i].c_str());
    }
  }
}

void ReadWorker::maybeSetBacklogDepth(LogTailerState* tailer) {
  if (folly::Random::randDouble01() < options.restart_backlog_probability) {
    double x = restart_backlog_depth_distribution_.sampleFloat();
    tailer->backlog_depth =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::duration<double>(x));
    STAT_INCR(stats_.get(), ldbench->reader_restarting_to_backlog);
    STAT_ADD(stats_.get(),
             ldbench->reader_restart_backlog_depth_ms_sum,
             tailer->backlog_depth->count());
  } else {
    STAT_INCR(stats_.get(), ldbench->reader_restarting_to_tail);
  }
}

} // namespace

void registerReadWorker() {
  registerWorkerImpl(
      BENCH_NAME,
      []() -> std::unique_ptr<Worker> {
        return std::make_unique<ReadWorker>();
      },
      OptionsRestrictions(
          {
              "pretend",
              "read-all",
              "duration",
              "filter-type",
              "fanout",
              "fanout-distribution",
              "reader-restart-period",
              "reader-restart-period-distribution",
              "reader-restart-spikiness",
              "restart-backlog-probability",
              "restart-backlog-depth",
              "restart-backlog-depth-distribution",
              "rate-limit-bytes",
              "pct-readers-consider-backlog-on-start",
              "record-writer-info",
              "start-time",
          },
          {PartitioningMode::LOG, PartitioningMode::LOG_AND_IDX}));
}

}}} // namespace facebook::logdevice::ldbench
