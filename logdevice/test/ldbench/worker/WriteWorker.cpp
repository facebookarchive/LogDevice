/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <iostream>

#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/Random.h"
#include "logdevice/test/ldbench/worker/BenchStats.h"
#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"
#include "logdevice/test/ldbench/worker/Worker.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

static constexpr const char* BENCH_NAME = "write";

class WriteWorker final : public Worker {
 public:
  using Worker::Worker;
  int run() override;

 private:
  struct LogState {
    logid_t log_id;

    RandomEventSequence::State append_generator_state;
    LibeventTimer next_append_timer;
    double payload_size_multiplier;

    // See makePayload() for explanation.
    XorShift128PRNG initial_payload_prng;
    XorShift128PRNG payload_prng;

    explicit LogState(logid_t log) : log_id(log) {}
  };

  // Returns how many appends we need to do now. Typically 1, but can be more
  // if timer is ticking slower than our target rate of appends.
  size_t activateNextAppendTimer(LogState* state);

  void maybeAppend(LogState* state);

  void updateThroughput();

  void onAppendDone(LogIDType log_id,
                    bool successful,
                    bool buffered,
                    uint64_t num_records,
                    uint64_t payload_bytes) override;

  std::string makePayload(size_t size, LogState* state);

  void printProgress(double seconds_since_start,
                     double seconds_since_last_call) override;

  std::unordered_map<logid_t, std::unique_ptr<LogState>, logid_t::Hash> logs_;

  RandomEventSequence append_generator_;
  RoughProbabilityDistribution payload_size_distribution_;

  std::atomic<uint64_t> appends_in_flight_{0};
  std::atomic<uint64_t> append_bytes_in_flight_{0};

  std::atomic<uint64_t> appends_succeeded_{0};
  std::atomic<uint64_t> appends_failed_{0};
  std::atomic<uint64_t> appends_skipped_{0};
  std::atomic<uint64_t> batches_succeeded_{0};
  std::atomic<uint64_t> batches_failed_{0};

  std::atomic<uint64_t> largest_payload_size_{0};

  // Maintaining some numbers for printProgress().
  std::atomic<uint64_t> bytes_ok_since_last_call_{0};
  LibeventTimer next_increase_throughput_timer_;
  uint64_t increase_count_;
};

static double steadyTime() {
  return std::chrono::duration_cast<std::chrono::duration<double>>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void WriteWorker::onAppendDone(LogIDType log_id,
                               bool successful,
                               bool buffered,
                               uint64_t num_records,
                               uint64_t payload_bytes) {
  if (successful) {
    if (buffered) {
      ++batches_succeeded_;
    }
    appends_succeeded_ += num_records;
    bytes_ok_since_last_call_ += payload_bytes;
  } else {
    if (buffered) {
      ++batches_failed_;
    }
    appends_failed_ += num_records;
  }
  appends_in_flight_ -= num_records;
  STAT_SUB(stats_.get(), ldbench->writer_appends_in_flight, num_records);
  append_bytes_in_flight_ -= payload_bytes;
  STAT_SUB(stats_.get(), ldbench->writer_append_bytes_in_flight, payload_bytes);
  if (buffered) {
    ev_->add([this, log_id] {
      if (isStopped()) {
        return;
      }
      LogState* state = logs_.at(logid_t(log_id)).get();
      // Reset the generator of the deterministic part of payload.
      // See makePayload() for explanation.
      state->payload_prng = state->initial_payload_prng;
    });
  }
}

std::string WriteWorker::makePayload(size_t size, LogState* state) {
  // We want the payload to have a given compression ratio on the client,
  // followed by a given additional compression ratio in sequencer batching.
  // To do that, we'll generate payload consisting of 3 parts:
  //  x. random bytes,
  //  y. bytes generated with a PRNG seeded with log ID,
  //  z. repeats of x and y.
  // So, the payload size will be x+y+z. On client side it will compress to
  // x+y. On sequencer a batch of n such records will compress to x*n+y, which
  // is close to x*n for big n. So, we just need:
  // (x+y)/(x+y+z)=options.payload_entropy,
  // x/(x+y)=options.payload_entropy_sequencer
  //
  // There's one inconvenience: with BufferedWriter, the 'y' part needs to not
  // repeat within the batch. We need to restart the log-seeded PRNG every time
  // a new batch starts. But BufferedWriter doesn't tell us exactly when that
  // happens, so we use an approximation: instead of restarting the PRNG when
  // a block starts we restart it when an append completes, which is usually
  // soon after starting a new batch.

  // Let's do everything in blocks of 4 bytes for performance.
  size_t len = (size + 3) / 4;
  // Length of payload_buf_ prefix that's not randomly generated.
  size_t header;

  // Put non-random tracing info at the beginning.
  if (options.record_writer_info) {
    RecordWriterInfo info;
    info.client_timestamp =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    header = (info.serializedSize() + 3) / 4;
    len = std::max(len, header) - header;
    payload_buf_.resize(header + len);
    info.serialize(reinterpret_cast<char*>(payload_buf_.data()));
  } else {
    header = 0;
    payload_buf_.resize(len);
  }

  if (len == 0) {
    return std::string(
        reinterpret_cast<char*>(payload_buf_.data()), header * 4);
  }

  size_t random_len = (size_t)(
      options.payload_entropy * options.payload_entropy_sequencer * len + .5);
  size_t pseudorandom_len =
      (size_t)(options.payload_entropy * len + .5) - random_len;
  if (random_len + pseudorandom_len == 0) {
    random_len = 1;
  }

  ld_check_le(random_len, len);
  ld_check_le(pseudorandom_len, len);
  ld_check_le(random_len + pseudorandom_len, len);

  // x
  folly::ThreadLocalPRNG rng;
  for (size_t i = 0; i < random_len; ++i) {
    payload_buf_[header + i] = folly::Random::rand32(rng);
  }

  // y
  auto& prng = state->payload_prng;
  if (!options.use_buffered_writer) {
    // Without buffered writer, just reset the PRNG before every record.
    prng = state->initial_payload_prng;
  }
  for (size_t i = random_len; i < random_len + pseudorandom_len; ++i) {
    payload_buf_[header + i] = prng();
  }

  // z
  size_t block = random_len + pseudorandom_len;
  for (size_t pos = block; pos < len; pos += block) {
    size_t n = std::min(len - pos, block);
    std::copy(payload_buf_.begin() + header,
              payload_buf_.begin() + header + n,
              payload_buf_.begin() + header + pos);
  }

  return std::string(reinterpret_cast<char*>(payload_buf_.data()), size);
}

void WriteWorker::printProgress(double seconds_since_start,
                                double seconds_since_last_call) {
  uint64_t batches_succeeded = batches_succeeded_.load();
  uint64_t batches_failed = batches_failed_.load();
  uint64_t appends_succeeded = appends_succeeded_.load();
  uint64_t appends_failed = appends_failed_.load();

  double bytes_per_sec =
      bytes_ok_since_last_call_.exchange(0) / seconds_since_last_call;
  uint64_t num_batches = batches_succeeded + batches_failed;
  uint64_t num_appends = appends_succeeded + appends_failed;
  double batch_success_pct =
      num_batches == 0 ? 0 : 100. * batches_succeeded / num_batches;
  double append_success_pct =
      num_appends == 0 ? 0 : 100. * appends_succeeded / num_appends;

  std::array<std::array<char, 32>, 6> bufs; // for commaprint_r()
  ld_info("ran for: %.3fs, batches: %s (%.2f%% successful), "
          "appends: %s (%.2f%% successful), appends skipped: %s, "
          "appends in flight: %s, bytes in flight: %s, bytes/sec: %s",
          seconds_since_start,
          commaprint_r(num_batches, &bufs[0][0], 32),
          batch_success_pct,
          commaprint_r(num_appends, &bufs[1][0], 32),
          append_success_pct,
          commaprint_r(appends_skipped_.load(), &bufs[2][0], 32),
          commaprint_r(appends_in_flight_.load(), &bufs[3][0], 32),
          commaprint_r(append_bytes_in_flight_.load(), &bufs[4][0], 32),
          commaprint_r(uint64_t(bytes_per_sec), &bufs[5][0], 32));
}

size_t WriteWorker::activateNextAppendTimer(LogState* state) {
  double now = steadyTime();
  double t;
  size_t to_append = 1;
  while (true) {
    t = append_generator_.nextEvent(state->append_generator_state);
    if (t >= now) {
      break;
    }

    // Libevent timers are only ~1ms granularity (at least in our current
    // configuration), but we often want to append more than one record per
    // millisecond per log. So we may need to do multiple appends per timer
    // callback.
    if (t >= now - 0.010) { // 10 ms ago
      // If we missed a few events because the timer was a little late, let's
      // do as many extra appends as many events we missed.
      ++to_append;
      STAT_INCR(stats_.get(), ldbench->writer_append_timer_slightly_late);
    } else {
      // But if we're too far behind, it means we're probably out of CPU and
      // can't keep up with the append rate.
      // Skip some appends to avoid falling behind indefinitely far.
      ++appends_skipped_;
      STAT_INCR(stats_.get(), ldbench->writer_appends_skipped_latency);
      client_holder_->getBenchStatsHolder()->getOrCreateTLStats()->incStat(
          StatsType::SKIPPED, 1);
    }
  }
  state->next_append_timer.activate(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::duration<double>(t - now)));
  return to_append;
}

void WriteWorker::maybeAppend(LogState* state) {
  if (append_bytes_in_flight_.load() >= options.max_append_bytes_in_flight) {
    ++appends_skipped_;
    STAT_INCR(stats_.get(), ldbench->writer_appends_skipped_bytes_in_flight);
    client_holder_->getBenchStatsHolder()->getOrCreateTLStats()->incStat(
        StatsType::SKIPPED, 1);
    return;
  } else if (appends_in_flight_.load() >= options.max_appends_in_flight) {
    ++appends_skipped_;
    STAT_INCR(stats_.get(), ldbench->writer_appends_skipped_appends_in_flight);
    client_holder_->getBenchStatsHolder()->getOrCreateTLStats()->incStat(
        StatsType::SKIPPED, 1);
    return;
  }
  STAT_INCR(stats_.get(), ldbench->writer_appends_done);

  uint64_t payload_size = (uint64_t)(payload_size_distribution_.sampleFloat() *
                                         state->payload_size_multiplier +
                                     0.5);
  std::string payload = makePayload(payload_size, state);
  payload_size = payload.size(); // may be slightly different

  // Track the largest payload size we've constructed.
  uint64_t prev_largest_pl = largest_payload_size_.load();
  while (payload_size > prev_largest_pl) {
    if (largest_payload_size_.compare_exchange_weak(
            prev_largest_pl, payload_size)) {
      STAT_SET(
          stats_.get(), ldbench->writer_largest_payload_size, payload_size);
      break;
    }
  }

  bool failed = false;
  if (options.pretend) {
    // pretend
    ev_->add([this, log = state->log_id, payload_size] {
      DataRecordAttributes attrs;
      onAppendDone(log.val_, true, false, 1, payload_size);
    });
    failed = false;
  } else {
    failed = !(client_holder_->append(state->log_id.val(),
                                      std::move(payload),
                                      reinterpret_cast<void*>(payload_size)));
  }
  if (failed) {
    ++appends_failed_;
  } else {
    ++appends_in_flight_;
    STAT_INCR(stats_.get(), ldbench->writer_appends_in_flight);
    append_bytes_in_flight_ += payload_size;
    STAT_ADD(
        stats_.get(), ldbench->writer_append_bytes_in_flight, payload_size);
  }
}

// Produces probability distribution for per-log payload size multiplier,
// i.e. how big the payloads in this log should be on average, relative to
// global average payload size.
// This distribution is a bit tricky to get right because the resulting
// multiplier affects both payload sizes and rate of appends in a log, and
// for the latter it appears in denominator.
RoughProbabilityDistribution
transformLogPayloadMultiplierDistribution(const Log2Histogram& d) {
  // First compensate for the fact that the multiplier affects both payload size
  // and rate of appends. To do that, multiply probability of each bucket by
  // the average value in the bucket.
  //
  // Here's an attempted explanation. Suppose that we want all of the following:
  //  * Each log has the same throughput in bytes/s.
  //  * Payload size in each log is constant, but different logs can have
  //    different payload size.
  //  * When considering records from all the logs together, the distribution of
  //    payload sizes should be as given by `d`.
  // First thing that comes to mind is to pick payload size for each log
  // according to `d` and divide log's throughput in bytes/s by the payload size
  // to get log's throughput in records/s. But this is incorrect: logs with
  // smaller payloads will get more records, making bigger contribution to the
  // overall distribution of payload sizes. As an example, imagine that half the
  // logs have payload size 1, another half has payload size 1999. Then the
  // average will be ~2 instead of 1000.
  // This function compensates for this bias.
  // Note that this is also correct when logs have different (random)
  // throughputs and when payloads in the same log have
  // different (random) sizes.
  auto buckets = d.toBucketProbabilities();
  for (size_t i = 0; i < buckets.size(); ++i) {
    buckets[i] *= 1ull << i;
  }
  Log2Histogram h;
  h.fromBucketProbabilities(buckets);

  // Now compensate for the fact that the multiplier appears in denominator
  // when calculating rate of appends for a log.
  // For this reason we need the expected value of 1/multiplier to be 1
  // (as opposed to expected value of multiplier being 1).
  //
  // I don't have a good explanation for this, but consider:
  // E_record(payload_size) =
  //   E_log(bytes_per_sec)/E_log(bytes_per_sec/target_payload_size/multiplier),
  // where E_record is expected value when averaged over all records (of all
  // logs), E_log is expected value when averaged over all logs,
  // target_payload_size is Options::payload_size. We want this expected value
  // to be equal to target_payload_size, as if there were no multiplier.
  // For that we need E_log(1/multiplier) to be 1.
  double coef = h.getMean() * h.getMeanInverse();
  return RoughProbabilityDistribution(coef, h);
}

void WriteWorker::updateThroughput() {
  double avg_log_throughput = 1. * options.write_bytes_per_sec / logs_.size();
  if (options.partition_by == PartitioningMode::RECORD) {
    avg_log_throughput /= options.worker_id_count;
  }
  RoughProbabilityDistribution log_throughput_distribution(
      avg_log_throughput, options.log_write_bytes_per_sec_distribution);
  RoughProbabilityDistribution log_payload_multiplier_distribution =
      transformLogPayloadMultiplierDistribution(
          options.log_payload_size_distribution);
  payload_size_distribution_ = RoughProbabilityDistribution(
      (double)options.payload_size, options.payload_size_distribution);
  append_generator_ = RandomEventSequence(options.write_spikiness);

  double highest_target_log_throughput = 0;
  double largest_log_avg_payload_size = 0;
  for (auto& kv : logs_) {
    auto state = kv.second.get();
    double x = folly::hash::hash_128_to_64(state->log_id.val_, 386614) /
        (std::numeric_limits<uint64_t>::max() + 1.);
    state->payload_size_multiplier =
        log_payload_multiplier_distribution.sampleFloat(x);
    double avg_payload_size_in_this_log =
        state->payload_size_multiplier * options.payload_size;
    avg_payload_size_in_this_log = std::max(10., avg_payload_size_in_this_log);
    largest_log_avg_payload_size =
        std::max(largest_log_avg_payload_size, avg_payload_size_in_this_log);

    double y = folly::hash::hash_128_to_64(state->log_id.val_, 829613) /
        (std::numeric_limits<uint64_t>::max() + 1.);
    double throughput_of_this_log = log_throughput_distribution.sampleFloat(y);

    highest_target_log_throughput =
        std::max(highest_target_log_throughput, throughput_of_this_log);

    double z = folly::hash::hash_128_to_64(state->log_id.val_, 152004) /
        (std::numeric_limits<uint64_t>::max() + 1.);
    state->append_generator_state = append_generator_.newState(
        throughput_of_this_log / avg_payload_size_in_this_log, steadyTime(), z);

    // Seed the PRNG with hash of log ID so that all writers produce the same
    // random bytes, allowing sequencer batching to compress them.
    state->initial_payload_prng.seed(
        folly::hash::hash_128_to_64(state->log_id.val_, 155082),
        folly::hash::hash_128_to_64(state->log_id.val_, 509709));
    state->payload_prng = state->initial_payload_prng;
  }

  STAT_SET(stats_.get(),
           ldbench->writer_highest_target_log_throughput,
           highest_target_log_throughput);
  STAT_SET(stats_.get(),
           ldbench->writer_largest_log_avg_payload_size,
           largest_log_avg_payload_size);
}

int WriteWorker::run() {
  // Get the log set from config.
  std::vector<logid_t> all_logs;
  if (getLogs(all_logs)) {
    ld_error("No logs to append!");
    return 1;
  }

  auto logs = options.partition_by == PartitioningMode::LOG
      ? getLogsPartition(all_logs)
      : all_logs;
  if (logs.empty()) {
    return 0;
  }

  // Push stats to ODS to indicate some of the settings used.
  STAT_SET(stats_.get(),
           ldbench->writer_max_appends_in_flight,
           options.max_appends_in_flight);
  STAT_SET(stats_.get(),
           ldbench->writer_max_append_bytes_in_flight,
           options.max_append_bytes_in_flight);
  // 1000x because floating point stats don't seem supported
  STAT_SET(stats_.get(),
           ldbench->writer_target_client_compressibility_1000x,
           1000. / options.payload_entropy);
  // 1000x because floating point stats don't seem supported
  STAT_SET(stats_.get(),
           ldbench->writer_target_sequencer_compressibility_1000x,
           (1000. / options.payload_entropy) *
               (1. / options.payload_entropy_sequencer));
  STAT_SET(stats_.get(),
           ldbench->writer_throughput_total_target,
           options.write_bytes_per_sec);
  STAT_SET(stats_.get(),
           ldbench->writer_payload_target_avg_size,
           options.payload_size);

  double highest_target_log_throughput = 0;
  double largest_log_avg_payload_size = 0;
  ev_->add([&] {
    for (logid_t log : logs) {
      auto ins = logs_.emplace(log, std::make_unique<LogState>(log));
      ld_check(ins.second);
    }
    updateThroughput();
    for (auto& kv : logs_) {
      auto state = kv.second.get();
      state->next_append_timer.assign(&ev_->getEvBase(), [this, state] {
        size_t to_append = activateNextAppendTimer(state);
        for (size_t i = 0; i < to_append; ++i) {
          maybeAppend(state);
        }
      });
    }

    // wait until start time before triggering the timers
    waitUntilStartTime();

    // assign and activate the throughput increase timer
    // only assign when we configure the increase interval
    if (options.write_bytes_increase_interval > std::chrono::milliseconds(0)) {
      increase_count_ = 0;
      next_increase_throughput_timer_.assign(&ev_->getEvBase(), [this]() {
        // activate next timer
        next_increase_throughput_timer_.activate(
            options.write_bytes_increase_interval);
        // increase the throughput for each log
        if (options.write_bytes_increase_factor != 1.0) {
          options.write_bytes_per_sec *= options.write_bytes_increase_factor;
        } else if (options.write_bytes_increase_step != 0) {
          options.write_bytes_per_sec += options.write_bytes_increase_step;
        }
        STAT_SET(stats_.get(),
                 ldbench->writer_throughput_total_target,
                 options.write_bytes_per_sec);
        updateThroughput();
        increase_count_++;
      });
      next_increase_throughput_timer_.activate(
          options.write_bytes_increase_interval);
    }

    if (!options.pretend) {
      ld_info("Starting appends in %lu logs", logs_.size());
    }

    for (auto& kv : logs_) {
      activateNextAppendTimer(kv.second.get());
    }
  });

  std::chrono::milliseconds actual_duration_ms = sleepForDurationOfTheBench();

  ld_info("Stopping writes");
  executeOnEventLoopSync([&] {
    if (next_increase_throughput_timer_.isActive()) {
      next_increase_throughput_timer_.cancel();
    }
    for (auto& kv : logs_) {
      kv.second->next_append_timer.cancel();
    }
  });
  // wait until no inflight appends otherwise the system may crash
  while (appends_in_flight_) {
  };
  destroyClient();

  std::cout << actual_duration_ms.count() << ' ' << appends_succeeded_ << ' '
            << appends_failed_ << ' ' << appends_skipped_ << ' '
            << batches_succeeded_ << ' ' << batches_failed_ << std::endl;

  return 0;
}

} // namespace

void registerWriteWorker() {
  registerWorkerImpl(BENCH_NAME,
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<WriteWorker>();
                     },
                     OptionsRestrictions(
                         {
                             "pretend",
                             "duration",
                             "write-bytes-per-sec",
                             "write-bytes-increase-factor",
                             "write-bytes-increase-step",
                             "write-bytes-increase-interval",
                             "log-write-bytes-per-sec-distribution",
                             "write-spikiness",
                             "payload-size",
                             "payload-size-distribution",
                             "log-payload-size-distribution",
                             "max-appends-in-flight",
                             "max-append-bytes-in-flight",
                             "use-buffered-writer",
                             "payload-entropy",
                             "payload-entropy-sequencer",
                             "start-time",
                             "record-writer-info",
                         },
                         {PartitioningMode::LOG, PartitioningMode::RECORD},
                         OptionsRestrictions::AllowBufferedWriterOptions::YES));
}

}}} // namespace facebook::logdevice::ldbench
