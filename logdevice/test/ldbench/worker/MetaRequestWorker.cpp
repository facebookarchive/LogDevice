/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/MetaRequestWorker.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice { namespace ldbench {

MetaRequestWorker::~MetaRequestWorker() {
  // Make sure no callbacks are called after this subclass is destroyed.
  destroyClient();
}

void MetaRequestWorker::activateNextRequestTimer(LogState* state) {
  double now = steadyTime();
  double t;
  while (true) {
    t = request_generator_.nextEvent(state->request_generator_state);
    if (t >= now) {
      break;
    }
    ++requests_skipped_;
  }
  state->next_request_timer.activate(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::duration<double>(t - now)));
}

void MetaRequestWorker::runRequest(LogState* state) {
  if (requests_in_flight_.load() >= options.max_requests_in_flight) {
    ++requests_skipped_;
    return;
  }

  int rv = makeMetadataAPIRequest(state);

  if (rv == 0) {
    ++requests_in_flight_;
  } else {
    // Failed to even start request.
    ++requests_failed_;
  }
}

void MetaRequestWorker::onRequestDone(bool success) {
  if (success) {
    ++requests_succeeded_;
  } else {
    ++requests_failed_;
  }
  --requests_in_flight_;
}

int MetaRequestWorker::run() {
  // Get the log set from config.
  std::vector<logid_t> all_logs;
  if (getLogs(all_logs)) {
    return 1;
  }
  auto logs = options.partition_by == PartitioningMode::LOG
      ? getLogsPartition(all_logs)
      : all_logs;
  if (logs.empty()) {
    return 0;
  }

  request_generator_ = RandomEventSequence(options.meta_requests_spikiness);

  // Create a distribution for the number of requests/sec for different logs.
  double target_avg_requests_per_sec = static_cast<double>(
      options.meta_requests_per_sec / double(all_logs.size()));
  if (options.partition_by == PartitioningMode::RECORD) {
    target_avg_requests_per_sec /= options.worker_id_count;
  }
  RoughProbabilityDistribution log_requests_per_sec_distribution(
      target_avg_requests_per_sec, options.log_requests_per_sec_distribution);

  // Run most of the initialization in event thread to be able to manipulate
  // timers and such.
  executeOnEventLoopSync([&] {
    for (logid_t log : logs) {
      auto ins = logs_.emplace(log, std::make_unique<LogState>(log));
      ld_check(ins.second);
      LogState* state = ins.first->second.get();

      // Pick average requests/sec for each log with the given distribution.
      double reqs_per_sec_seed =
          folly::hash::hash_128_to_64(log.val_, 9136739) /
          (std::numeric_limits<uint64_t>::max() + 1.);
      double requests_per_second_of_this_log =
          log_requests_per_sec_distribution.sampleFloat(reqs_per_sec_seed);

      double spikes_randomness_seed =
          folly::hash::hash_128_to_64(log.val_, 362049) /
          (std::numeric_limits<uint64_t>::max() + 1.);
      state->request_generator_state =
          request_generator_.newState(requests_per_second_of_this_log,
                                      /*initial_time=*/steadyTime(),
                                      spikes_randomness_seed);

      state->next_request_timer.assign(&ev_->getEvBase(), [this, state] {
        // When timer fires, schedule next event before running request, to
        // make sure we stick to the requested rate.
        activateNextRequestTimer(state);
        runRequest(state);
      });
    }

    // Start the request timers
    for (auto& kv : logs_) {
      activateNextRequestTimer(kv.second.get());
    }
  });

  std::chrono::milliseconds actual_duration_ms = sleepForDurationOfTheBench();

  ld_info("Stopping requests");
  executeOnEventLoopSync([&] {
    for (auto& kv : logs_) {
      kv.second->next_request_timer.cancel();
    }
  });

  std::cout << actual_duration_ms.count() << ' ' << requests_succeeded_ << ' '
            << requests_failed_ << ' ' << requests_skipped_ << std::endl;

  return 0;
}

}}} // namespace facebook::logdevice::ldbench
