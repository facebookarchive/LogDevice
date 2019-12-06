/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/MetaRequestWorker.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

/**
 * findTime benchmark worker.
 *
 * Runs findTime as requested -- in terms of distribution, spikiness, period --
 * until told to stop or until duration has passed.
 *
 * The timestamp we run findTime for is based on the average that is provided,
 * the distance from which is according to the given distribution.
 */
class FindTimeWorker : public MetaRequestWorker {
 public:
  explicit FindTimeWorker()
      : MetaRequestWorker(),
        time_ago_distribution_(options.findtime_avg_time_ago.count(),
                               options.findtime_timestamp_distribution) {}

  int makeMetadataAPIRequest(LogState* state) override {
    uint64_t ms_ago = time_ago_distribution_.sampleFloat();
    std::chrono::milliseconds timestamp(
        (RecordTimestamp::now().toMilliseconds().count() - ms_ago));
    logid_t log_id = state->log_id;
    RATELIMIT_DEBUG(std::chrono::seconds(20),
                    20,
                    "Running findTime for log %lu %lums ago, i.e. a "
                    "timestamp of %s",
                    log_id.val(),
                    ms_ago,
                    RecordTimestamp(timestamp).toString().c_str());

    return client_->findTime(
        state->log_id, timestamp, [this](Status st, lsn_t /*result*/) mutable {
          if (isStopped()) {
            return;
          }
          onRequestDone(st == E::OK);
        });
  }

 private:
  RoughProbabilityDistribution time_ago_distribution_;
};

} // namespace

void registerFindTimeWorker() {
  registerWorkerImpl("findtime",
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<FindTimeWorker>();
                     },
                     OptionsRestrictions(
                         {
                             "duration",
                             "meta-requests-per-sec",
                             "meta-requests-spikiness",
                             "max-requests-in-flight",
                             "log-requests-per-sec-distribution",
                             "findtime-avg-time-ago",
                             "findtime-timestamp-distribution",
                         },
                         {PartitioningMode::LOG, PartitioningMode::RECORD},
                         OptionsRestrictions::AllowBufferedWriterOptions::NO));
}

}}} // namespace facebook::logdevice::ldbench
