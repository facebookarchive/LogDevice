/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/Worker.h"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <random>
#include <thread>
#include <time.h>

#include <folly/Hash.h>
#include <folly/Random.h>
#include <folly/Range.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/ldbench/worker/BenchStats.h"
#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice { namespace ldbench {

Worker::Worker() : ev_(new EventLoop("ldbench:ev")) {
  if (options.pretend) {
    return;
  }

  // Use the generic client interfaces
  client_holder_ = std::make_unique<LogStoreClientHolder>();
  client_holder_->setWorkerCallBack(std::bind(&Worker::onAppendDone,
                                              this,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3,
                                              std::placeholders::_4,
                                              std::placeholders::_5));
  ld_check(options.sys_name == "logdevice" || options.sys_name == "kafka");
  if (options.sys_name == "logdevice") {
    if (client_ == nullptr) {
      // Take client from client_holder_ if we test logdevice
      // This client will be used for the workloads only supporting logdevice
      // Finally, those workloads, as well as this client_, may be removed.
      client_ =
          std::static_pointer_cast<Client>(client_holder_->getRawClient());
      ld_check(client_ != nullptr);
    }
    // Old stats can still be used in logdevice
    stats_ = std::make_unique<StatsHolder>(
        StatsParams().setIsServer(false).setStatsSet(
            StatsParams::StatsSet::LDBENCH_WORKER));
    STAT_INCR(stats_.get(), ldbench->workers_started);
    // Register our custom stats.
    static_cast<ClientImpl*>(client_.get())->registerCustomStats(stats_.get());
  }
}

Worker::~Worker() = default;

bool Worker::getLogs(std::vector<logid_t>& logs_out) const {
  if (options.pretend) {
    // We could just leave logs_out empty, but instead we add an arbitrary log
    // so that more of the code is hit in dry run mode. The log is randomly
    // generated to make it very unlikely to be a real log, in case a check for
    // options.pretend is missing somewhere.
    logs_out.emplace_back(4483415206162507972ul);
    return false;
  }
  std::vector<uint64_t> log_out_int;
  ld_check(client_holder_ != nullptr);
  client_holder_->getLogs(&log_out_int);
  for (uint64_t id : log_out_int) {
    double x = folly::hash::hash_128_to_64(id, options.log_hash_salt) /
        (std::numeric_limits<uint64_t>::max() + 1.);
    if (x >= options.log_hash_range.first &&
        x < options.log_hash_range.second) {
      logs_out.emplace_back(id);
    }
  }
  ld_debug("%lu logs in config, %lu in hash range",
           log_out_int.size(),
           logs_out.size());
  return false;
}

std::vector<logid_t>
Worker::getLogsPartition(const std::vector<logid_t>& logs) {
  ld_check(options.partition_by == PartitioningMode::LOG);
  std::vector<logid_t> res;
  for (logid_t log : logs) {
    // Use some random salt to make sure we don't accidentally use the same hash
    // function for multiple purposes (e.g. partitioning and selecting log
    // throughput).
    if (folly::hash::hash_128_to_64(log.val_, 757071) %
            options.worker_id_count ==
        options.worker_id_index) {
      res.push_back(log);
    }
  }
  return res;
}

Worker::LogIdDist Worker::getLogIdDist(std::vector<logid_t> logs) {
  if (logs.empty()) {
    return nullptr;
  }
  return [logs = std::move(logs)] {
    return logs[folly::Random::rand32() % logs.size()];
  };
}

std::string Worker::generatePayload(size_t size) {
  // Generate a random payload. Payload buffer is allocated lazily because
  // options.payload_size may not be parsed yet when Worker is created. Also,
  // this avoids allocating a buffer in case a benchmark does not perform any
  // appends.
  folly::ThreadLocalPRNG prng;
  payload_buf_.resize((size + sizeof(uint32_t) - 1) / sizeof(uint32_t));
  std::generate(payload_buf_.begin(), payload_buf_.end(), [&] {
    return folly::Random::rand32(prng);
  });
  return std::string(reinterpret_cast<char*>(payload_buf_.data()), size);
}

bool Worker::tryAppend(logid_t log_id,
                       std::string payload,
                       const append_callback_t& cb,
                       const AppendAttributes* attrs) {
  if (!options.pretend) {
    // Actual append. May fail.
    return client_->append(log_id,
                           std::move(payload),
                           cb,
                           attrs ? *attrs : AppendAttributes{}) != 0;
  } else {
    // Pretend only. Just call the callback from a detached thread. It is the
    // callback's responsibility to be safe regardless of when the callback is
    // called eventually.
    ev_->add([log_id, payload2 = std::move(payload), cb] {
      static std::atomic<lsn_t> lsn{LSN_OLDEST};
      const DataRecord record(
          log_id, Payload(payload2.c_str(), payload2.size()), lsn++);
      cb(E::OK, record);
    });
    return false;
  }
}

bool Worker::getTailLSNs(LogToLsnMap& tail_lsns,
                         const std::vector<logid_t>& logs) const {
  // Clear output map.
  tail_lsns.clear();

  if (options.pretend) {
    // Pretend only. Just use LSN_INVALID.
    for (logid_t log : logs) {
      tail_lsns.emplace(log.val(), LSN_INVALID);
    }
    return false;
  }

  // First error code we encountered, if any.
  Status e = E::OK;

  const size_t max_in_flight = 10000; // maybe this should be an option
  size_t pend = 0;
  std::mutex mutex;
  std::condition_variable pend_cv;
  std::unique_lock<std::mutex> lock(mutex);

  for (logid_t log : logs) {
    auto tail_lsn_cb = [&, log](Status status, lsn_t lsn) {
      std::lock_guard<std::mutex> lock2(mutex);
      if (status != E::OK) {
        ld_warning("getTailLSN for log %" PRIu64 " failed: %s",
                   log.val_,
                   error_name(status));
        lsn = LSN_INVALID;
        e = status;
      }
      tail_lsns.emplace(log.val(), lsn);
      --pend;
      if (pend == 0 || pend + 1 == max_in_flight) {
        pend_cv.notify_one();
      }
    };
    pend_cv.wait(lock, [&] { return pend < max_in_flight; });
    ++pend;
    if (client_->getTailLSN(log, std::move(tail_lsn_cb)) != 0) {
      ld_warning("getTailLSN for log %" PRIu64 " failed: %s",
                 log.val_,
                 error_name(err));
      if (e == E::OK) {
        e = err;
      }
      tail_lsns.emplace(log.val(), LSN_INVALID);
      --pend;
    }
  }

  // Wait for async calls to finish.
  pend_cv.wait(lock, [&pend] { return pend == 0; });

  // Restore and log first error code, if any.
  err = e;
  if (err != E::OK) {
    ld_error("getTailLSN failed for at least one log, first error: %s (%s)",
             error_name(err),
             error_description(err));
  }

  return !options.ignore_errors && err != E::OK;
}

ReadStreamAttributes Worker::getReadAttrs() const {
  ReadStreamAttributes attrs{options.filter_type, FILTER_KEY, FILTER_KEY};
  return attrs;
}

bool Worker::startReading(const LogToLsnMap& from_lsns,
                          RecordCallback record_cb,
                          GapCallback gap_cb,
                          const LogToLsnMap* until_lsns) {
  if (options.pretend) {
    // Pretend mode. Do not actually read.
    return false;
  }
  ld_check(client_ != nullptr);
  ld_check(reader_ == nullptr);
  ld_check(tailed_logs_.empty());

  // Record callback is mandatory. Gap callback is optional.
  ld_check(record_cb != nullptr);
  // First error code we encountered, if any.
  Status e = E::OK;

  // Start reading all logs from their tail LSNs.
  std::vector<logid_t::raw_type> started;
  reader_ = client_->createAsyncReader();
  reader_->setRecordCallback(std::move(record_cb));
  reader_->setGapCallback(std::move(gap_cb));
  for (const auto& entry : from_lsns) {
    lsn_t until_lsn = LSN_MAX;
    if (until_lsns != nullptr) {
      auto until_lsn_it = until_lsns->find(entry.first);
      if (until_lsn_it != until_lsns->end()) {
        until_lsn = until_lsn_it->second;
      }
    }
    auto attrs = getReadAttrs();
    if (reader_->startReading(
            logid_t(entry.first), entry.second, until_lsn, &attrs) == 0) {
      // Success.
      tailed_logs_.push_back(logid_t(entry.first));
    } else {
      // Error occurred.
      if (e == E::OK) {
        e = err;
      }
      if (!options.ignore_errors) {
        break;
      }
    }
  }

  if (!options.ignore_errors && e != E::OK) {
    // Abort reads we just started and destroy reader.
    for (logid_t log_id : tailed_logs_) {
      reader_->stopReading(log_id, nullptr);
    }
    tailed_logs_.clear();
    reader_.reset();
  }

  // Restore and log first error code, if any.
  err = e;
  if (err != E::OK) {
    ld_error("Failed to start reading: %s (%s)",
             error_name(err),
             error_description(err));
  }

  return !options.ignore_errors && err != E::OK;
}

bool Worker::stopReading() {
  if (options.pretend) {
    // Pretend mode. We did not actually read.
    ld_check(reader_ == nullptr);
    ld_check(tailed_logs_.empty());
    return false;
  }
  ld_check(reader_ != nullptr);

  // First error code we encountered, if any.
  Status e = E::OK;

  logid_t::raw_type pend = 0;
  std::mutex mutex;
  std::condition_variable pend_cv;
  std::unique_lock<std::mutex> lock(mutex);

  // Try to stop reading all logs.
  decltype(tailed_logs_) failed_logs;
  for (logid_t log_id : tailed_logs_) {
    auto stop_cb = [&mutex, &pend, &pend_cv, log_id]() {
      std::lock_guard<std::mutex> lock2(mutex);
      --pend;
      if (pend == 0) {
        pend_cv.notify_all();
      }
    };
    ++pend;
    if (reader_->stopReading(log_id, std::move(stop_cb)) != 0) {
      failed_logs.push_back(log_id);
      if (e == E::OK) {
        e = err;
      }
      --pend;
    }
  }

  // Wait for all reads to complete.
  pend_cv.wait(lock, [&pend] { return pend == 0; });

  if (options.ignore_errors || e == E::OK) {
    // Destroy reader and forget any failed logs.
    reader_.reset();
    decltype(tailed_logs_)().swap(tailed_logs_);
  } else {
    // Keep reader and remember failed logs. Allows caller to try again.
    tailed_logs_.swap(failed_logs);
  }

  // Restore and log first error code, if any.
  err = e;
  if (err != E::OK) {
    ld_error("Failed to stop reading: %s (%s)",
             error_name(err),
             error_description(err));
  }

  return !options.ignore_errors && err != E::OK;
}

void Worker::waitUntilStartTime() {
  std::mutex mutex;
  std::condition_variable cv;
  auto now = std::chrono::system_clock::now();
  auto start_timepoint = options.start_time.timePoint();
  if (start_timepoint > now) {
    std::unique_lock<std::mutex> lock(mutex);
    // use condition wait to avoid link error
    cv.wait_until(lock, start_timepoint);
  }
  auto now_ts = SystemTimestamp::now();
  std::cout << options.bench_name << " worker started at " << now_ts.toString()
            << std::endl;
}

void Worker::destroyClient() {
  reader_.reset();
  if (client_) {
    client_.reset();
  }
  if (client_holder_) {
    client_holder_.reset();
  }
}

int Worker::getTailLSN(logid_t log, get_tail_lsn_callback_t cb) {
  ld_check(!options.pretend);
  return client_->getTailLSN(
      log, [this, cb = std::move(cb)](Status status, lsn_t tail_lsn) mutable {
        ev_->add([this, cb = std::move(cb), status, tail_lsn] {
          // Safe to use `this` here because we're running on event loop
          // owned by `this`.
          if (isStopped()) {
            return;
          }
          cb(status, tail_lsn);
        });
      });
}

int Worker::getTailLSN(logid_t log, std::function<void(bool, uint64_t)> cb) {
  ld_check(!options.pretend);
  return client_holder_->getTail(
      log.val(),
      [this, cb = std::move(cb)](bool successful, lsn_t tail_lsn) mutable {
        ev_->add([this, cb = std::move(cb), successful, tail_lsn] {
          // Safe to use `this` here because we're running on event loop
          // owned by `this`.
          if (isStopped()) {
            return;
          }
          cb(successful, tail_lsn);
        });
      });
}

int Worker::findTime(logid_t log,
                     std::chrono::milliseconds timestamp,
                     find_time_callback_t cb) {
  ld_check(!options.pretend);
  return client_->findTime(
      log,
      timestamp,
      [this, cb = std::move(cb)](Status status, lsn_t lsn) mutable {
        ev_->add([this, cb = std::move(cb), status, lsn]() {
          // Safe to use `this` here because we're running on event loop
          // owned by `this`.
          if (isStopped()) {
            return;
          }
          cb(status, lsn);
        });
      });
}

int Worker::findTime(logid_t log,
                     std::chrono::milliseconds timestamp,
                     std::function<void(bool, uint64_t)> cb) {
  ld_check(!options.pretend);
  return client_holder_->findTime(
      log.val(),
      timestamp,
      [this, cb = std::move(cb)](bool successful, lsn_t lsn) mutable {
        ev_->add([this, cb = std::move(cb), successful, lsn]() {
          // Safe to use `this` here because we're running on event loop
          // owned by `this`.
          if (isStopped()) {
            return;
          }
          cb(successful, lsn);
        });
      });
}

int Worker::isLogEmpty(logid_t log, is_empty_callback_t cb) {
  ld_check(!options.pretend);
  return client_->isLogEmpty(
      log, [this, cb = std::move(cb)](Status status, bool empty) mutable {
        ev_->add([this, cb = std::move(cb), status, empty]() mutable {
          // Safe to use `this` here because we're running on event loop
          // owned by `this`.
          if (isStopped()) {
            return;
          }
          cb(status, empty);
        });
      });
}

std::chrono::milliseconds Worker::sleepForDurationOfTheBench() {
  auto start_time = std::chrono::steady_clock::now();
  auto end_time = std::chrono::steady_clock::time_point::max();
  if (options.duration > 0) {
    end_time = start_time + std::chrono::seconds(options.duration);
  }

  const auto print_progress_every = std::chrono::seconds(1);
  auto last_progress_time = std::chrono::steady_clock::now();

  // Check periodically for the stop notification.
  while (!isStopped()) {
    auto now = std::chrono::steady_clock::now();
    if (now - last_progress_time >= print_progress_every) {
      printProgress(std::chrono::duration_cast<std::chrono::duration<double>>(
                        now - start_time)
                        .count(),
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        now - last_progress_time)
                        .count());
      last_progress_time = now;
    }

    if (std::chrono::steady_clock::now() > end_time) {
      stop();
      break;
    }

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ld_check(isStopped());

  auto finish_time = std::chrono::steady_clock::now();

  return std::chrono::duration_cast<std::chrono::milliseconds>(finish_time -
                                                               start_time);
}

void Worker::requestDebugInfoDump() {
  ev_->add([this] { dumpDebugInfo(); });
}

void Worker::executeOnEventLoopSync(folly::Func f) {
  folly::Baton<> baton;
  ev_->add([&baton, f = std::move(f)]() mutable {
    f();
    baton.post();
  });
  baton.wait();
}

}}} // namespace facebook::logdevice::ldbench
