/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <chrono>

#include "logdevice/common/LibeventTimer.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/Worker.h"

namespace facebook { namespace logdevice { namespace ldbench {

/**
 * Intermediate class for workers that run metadata API requests.
 *
 * Runs a metadata API request as requested -- in terms of distribution,
 * spikiness, period -- until told to stop or until duration has passed.
 */
class MetaRequestWorker : public Worker {
 public:
  using Worker::Worker;
  ~MetaRequestWorker() override;
  int run() override;

 protected:
  struct LogState {
    logid_t log_id;
    RandomEventSequence::State request_generator_state;
    LibeventTimer next_request_timer;

    explicit LogState(logid_t log) : log_id(log) {}
  };

  // To return 0 if request was started properly, or -1 if it failed to start.
  // Must call onRequestDone() when the request finishes.
  virtual int makeMetadataAPIRequest(LogState* state) = 0;

  void activateNextRequestTimer(LogState* state);
  void onRequestDone(bool success);

  static double steadyTime() {
    return std::chrono::duration_cast<std::chrono::duration<double>>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

 private:
  void runRequest(LogState* state);

  std::unordered_map<logid_t, std::unique_ptr<LogState>> logs_;

  RandomEventSequence request_generator_;

  std::atomic<uint64_t> requests_in_flight_{0};

  std::atomic<uint64_t> requests_succeeded_{0};
  std::atomic<uint64_t> requests_failed_{0};
  std::atomic<uint64_t> requests_skipped_{0};
};

}}} // namespace facebook::logdevice::ldbench
