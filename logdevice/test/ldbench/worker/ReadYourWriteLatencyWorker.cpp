/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <folly/Random.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/Histogram.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/Reservoir.h"
#include "logdevice/test/ldbench/worker/SteadyWriteRateWorker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

static constexpr const char* BENCH_NAME = "read_your_write_latency";

/**
 * Read-your-write latency benchmark worker.
 *
 * Performs async appends at a steady rate while tailing the logs it writes to.
 * Measures the end-to-end latency from the time of calling Client::append() to
 * the time of receiving a read callback of the respective record.
 *
 * Writes are uniformly randomly distributed across the set of logs. The
 * set of logs is partitioned among concurrent writers so that each worker
 * only reads its own writes. (Otherwise, one would quickly exceed the read
 * bandwidth of workers.)
 */
class ReadYourWriteLatencyWorker final : public SteadyWriteRateWorker {
 public:
  using SteadyWriteRateWorker::SteadyWriteRateWorker;
  ~ReadYourWriteLatencyWorker() override;
  int run() override;

 protected:
  std::string generatePayload(size_t size = options.payload_size) override;

 private:
  using Sample = std::chrono::microseconds;
  struct PayloadHeader;

  /// Maximum number of samples to collect per output histogram bucket.
  static constexpr size_t OVERSAMPLE_FACTOR = 100;
  // Key that will pass filtering test.
  static constexpr const char* FILTER_KEY = "abcdefg";

  void printResult();
  void appendCallback(Status status, const DataRecord& record);
  void recordCallback(const DataRecord& record);
  void handleAppendSuccess(Payload payload) override;
  void handleAppendError(Status st, Payload payload) override;
  size_t getNumPendingXacts() const noexcept override;
  bool error() const noexcept override;

  uint64_t worker_id = folly::Random::rand64() >> 8;
  uint64_t next_record_id_ = 0;
  uint64_t npending_appends_ = 0;
  bool collect_samples_ = false;
  bool error_ = false;
  std::unordered_set<uint64_t> pending_reads_;
  std::unique_ptr<Reservoir<Sample>> reservoir_;
};

struct ReadYourWriteLatencyWorker::PayloadHeader final {
  static constexpr uint32_t VERSION = 4; /// bump after structural changes
  static constexpr uint64_t MAGIC = 0x1DBE7C4000000000 + VERSION;
  PayloadHeader(uint64_t worker_id,
                uint64_t record_id,
                TimePoint append_time,
                bool filter_out)
      : magic(MAGIC),
        worker_id(worker_id),
        filter_out(filter_out),
        record_id(record_id),
        append_time(append_time) {}

  uint64_t magic;          /// to detect structural changes
  uint64_t worker_id : 56; /// to ensure we don't read others' records
  bool filter_out : 8;     /// expect this record to be filtered out
  uint64_t record_id;      /// to identify the record on callback
  TimePoint append_time;   /// time of append
};

ReadYourWriteLatencyWorker::~ReadYourWriteLatencyWorker() {
  // Make sure no callbacks are called after this subclass is destroyed.
  destroyClient();
}

std::string ReadYourWriteLatencyWorker::generatePayload(size_t size) {
  static_assert(
      sizeof(PayloadHeader) == 32, "Unexpected padding of PayloadHeader");
  auto record_id = next_record_id_++;
  PayloadHeader header(worker_id, record_id, Clock::now(), false);
  std::string payload(reinterpret_cast<char*>(&header), sizeof(header));
  if (size > sizeof(header)) {
    payload.append(generatePayload(size - sizeof(header)));
  }
  return payload;
}

int ReadYourWriteLatencyWorker::run() {
  // Create random log id distribution function. Each worker is assigned a
  // partition of the log set. The partition is determined by the worker
  // index passed via the worker_id option. Within its partition, each worker
  // picks a log uniformly at random.
  std::vector<logid_t> all_logs;
  if (getLogs(all_logs)) {
    return 1;
  }
  auto logs = getLogsPartition(all_logs);
  auto log_id_dist = getLogIdDist(logs);

  // Keep per-log throughput independent of number of workers.
  write_rate_ = (log_id_dist != nullptr)
      ? double(options.write_rate) * logs.size() / all_logs.size()
      : 0.0;

  // Allocate sample reservoir.
  reservoir_ = std::make_unique<Reservoir<Sample>>(
      options.histogram_bucket_count * OVERSAMPLE_FACTOR);

  // Clear append token bucket.
  token_bucket_.reset(token_bucket_.defaultClockNow());

  // Callback for use with Client::append.
  append_callback_t append_cb([this](Status status, const DataRecord& record) {
    appendCallback(status, record);
  });

  // Async reader callback.
  RecordCallback record_cb([this](std::unique_ptr<DataRecord>& record) {
    recordCallback(*record);
    record.reset();
    return true;
  });

  // Get tail LSN of each log in our log set partition.
  LogToLsnMap tail_lsns;
  if (getTailLSNs(tail_lsns, logs)) {
    return 1;
  }

  // Start tailing our log set partition ("read your own writes").
  ld_info("Starting reads");
  if (startReading(tail_lsns, std::move(record_cb))) {
    return 1;
  }

  // Warm up. Perform appends at steady write rate until
  // options.warmup_duration has passed. This is to make sure the servers are
  // in a steady state when we commence the actual benchmark.
  ld_info(
      "Performing warm-up for %" PRIu64 " seconds", options.warmup_duration);
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  TimePoint start_time = Clock::now();
  end_time_ = start_time + std::chrono::seconds(options.warmup_duration);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
  ld_info("Warm-up complete");

  // Actual benchmark. Perform appends at steady write rate until
  // options.duration has passed.
  ld_info("Performing read-your-write latency benchmark for %" PRIu64 " "
          "seconds",
          options.duration);
  collect_samples_ = true;
  end_time_ = Clock::now() + std::chrono::seconds(options.duration);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
  collect_samples_ = false;
  ld_info("Benchmark complete: nreads=%zu, nsamples=%zu",
          reservoir_->getNumberOfInsertions(),
          reservoir_->getNumberOfSamples());

  // Cool down. Perform appends at steady write rate until
  // options.cooldown_duration has passed. The purpose of this is to guarantee
  // a stable result if concurrent workers have started at slightly different
  // times.
  ld_info("Performing cool-down for %" PRIu64 " seconds",
          options.cooldown_duration);
  end_time_ = Clock::now() + std::chrono::seconds(options.cooldown_duration);
  while (!tryAppend(lock, log_id_dist, append_cb)) {
    /* keep going */
  }
  ld_info("Cool-down complete");

  // Wait for pending appends.
  ld_info("Waiting for pending appends: npending_appends=%" PRIu64,
          npending_appends_);
  cond_var_.wait(lock, [this] { return npending_appends_ == 0; });

  // Stop tailing.
  ld_info("Stopping reads");
  lock.unlock();
  if (stopReading()) {
    return 1;
  }
  ld_info("All done");

  if (!error()) {
    // All is good.
    printResult();
    return 0;
  }

  // Error occurred.
  return 1;
}

void ReadYourWriteLatencyWorker::printResult() {
  // Print number of reads i.e. number of insertions into sample reservoir.
  std::cout << reservoir_->getNumberOfInsertions() << '\n';

  // Compute and print equi-depth histogram unless there are no samples.
  size_t nsamples = reservoir_->getNumberOfSamples();
  if (nsamples > 0) {
    // Compute histogram.
    size_t nbuckets =
        std::min<size_t>(options.histogram_bucket_count, nsamples);
    std::vector<Sample> bucket_lower_bounds;
    std::vector<size_t> bucket_counts;
    bucket_lower_bounds.reserve(nbuckets + 1);
    bucket_counts.reserve(nbuckets);
    std::sort(reservoir_->begin(), reservoir_->end());
    equi_depth_histogram(reservoir_->begin(),
                         reservoir_->end(),
                         nbuckets,
                         std::back_inserter(bucket_lower_bounds),
                         std::back_inserter(bucket_counts));
    ld_check(bucket_lower_bounds.size() == nbuckets + 1);
    ld_check(bucket_counts.size() == nbuckets);

    // Print histogram.
    auto print_bucket = [](auto lb, auto c) {
      std::cout << lb << ' ' << c << '\n';
    };
    for (size_t bucket_num = 0; bucket_num < nbuckets; ++bucket_num) {
      auto lower_bound = bucket_lower_bounds[bucket_num].count();
      auto count = bucket_counts[bucket_num];
      print_bucket(lower_bound, count);
    }
    print_bucket(bucket_lower_bounds.back().count(), 0);
  }
}

void ReadYourWriteLatencyWorker::appendCallback(Status status,
                                                const DataRecord& record) {
  ++nwaiting_;
  std::unique_lock<std::recursive_mutex> lock(mutex_);
  --nwaiting_;

  // Decrease pending append counter, regardless of success or failure.
  --npending_appends_;
  const auto& header =
      *static_cast<const PayloadHeader*>(record.payload.data());
  // Handle append error, if any.
  if (status != E::OK) {
    handleAppendError(status, record.payload);
  }
  lock.unlock();

  if (status == E::OK && (options.pretend || header.filter_out)) {
    // Pretend mode. There is no reader. Just call record callback immediately.
    recordCallback(record);
  }

  // Notify main thread in run().
  cond_var_.notify_one();
}

void ReadYourWriteLatencyWorker::recordCallback(const DataRecord& record) {
  if (record.payload.size() >= sizeof(PayloadHeader)) {
    const auto& header =
        *static_cast<const PayloadHeader*>(record.payload.data());
    if ((header.magic == PayloadHeader::MAGIC) &&
        (header.worker_id == worker_id)) {
      // This is a record we have written. Read the time point we previously
      // wrote into the payload. The difference to the current time gives us
      // the read-your-write latency.
      auto now = Clock::now();
      auto latency = now - header.append_time;
      auto latency_sample = std::chrono::duration_cast<Sample>(latency);
      ld_debug(
          "Record read: latency=%" PRIu64, uint64_t(latency_sample.count()));

      {
        ++nwaiting_;
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        --nwaiting_;

        // Put sample into reservoir (or not).
        if (collect_samples_) {
          reservoir_->put(latency_sample);
        }

        // Remove record id from pending reads. May be a no-op in case the
        // append was considered failed but succeeded anyways. (That can happen
        // in a distributed system and is the reason for keeping a set of
        // record ids instead of a simple counter.)
        pending_reads_.erase(header.record_id);
      }

      // Notify main thread in tryAppend().
      cond_var_.notify_one();
    } else {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Read invalid record: "
                        "expected_magic=%" PRIu64 ", record_magic=%" PRIu64
                        ", expected_worker_id=%" PRIu64
                        ", record_worker_id=%" PRIu64,
                        PayloadHeader::MAGIC,
                        header.magic,
                        worker_id,
                        header.worker_id);
    }
  } else {
    RATELIMIT_WARNING(std::chrono::seconds(1), 1, "Read undersized record");
  }
}

void ReadYourWriteLatencyWorker::handleAppendSuccess(Payload payload) {
  ld_check(payload.size() >= sizeof(PayloadHeader));
  const auto& header = *reinterpret_cast<const PayloadHeader*>(payload.data());
  ld_check((header.magic == PayloadHeader::MAGIC) &&
           (header.worker_id == worker_id));
  auto record_id = header.record_id;

  ++npending_appends_;
  bool inserted = pending_reads_.insert(record_id).second;
  ld_check(inserted);
  (void)inserted;
}

void ReadYourWriteLatencyWorker::handleAppendError(Status st, Payload payload) {
  // Log error. Set error_ flag in case of serious error.
  switch (st) {
    case E::AGAIN:
    case E::NOBUFS:
    case E::OVERLOADED:
    case E::PENDING_FULL:
    case E::SEQNOBUFS:
    case E::SEQSYSLIMIT:
    case E::SYSLIMIT:
    case E::TEMPLIMIT:
    case E::TIMEDOUT:
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Append failed, probably too much load: %s (%s)",
                        error_name(st),
                        error_description(st));
      break;
    default:
      error_ = true;
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Append failed, serious error: %s (%s)",
                      error_name(st),
                      error_description(st));
  }

  // If given, remove record id from set of pending reads.
  if (payload.data() != nullptr) {
    ld_check(payload.size() >= sizeof(PayloadHeader));
    const auto& header =
        *reinterpret_cast<const PayloadHeader*>(payload.data());
    ld_check((header.magic == PayloadHeader::MAGIC) &&
             (header.worker_id == worker_id));
    auto record_id = header.record_id;
    pending_reads_.erase(record_id);
  }
}

size_t ReadYourWriteLatencyWorker::getNumPendingXacts() const noexcept {
  return std::max(npending_appends_, pending_reads_.size());
}

bool ReadYourWriteLatencyWorker::error() const noexcept {
  return !options.ignore_errors && error_;
}

} // namespace

void registerReadYourWriteLatencyWorker() {
  registerWorkerImpl(BENCH_NAME,
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<ReadYourWriteLatencyWorker>();
                     },
                     OptionsRestrictions({"pretend",
                                          "max-window",
                                          "warmup-duration",
                                          "duration",
                                          "cooldown-duration",
                                          "payload-size",
                                          "histogram-bucket-count",
                                          "filter-selectivity",
                                          "write-rate"},
                                         {PartitioningMode::LOG}));
}

}}} // namespace facebook::logdevice::ldbench
