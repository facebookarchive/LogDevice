/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/MetaRequestWorker.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/WorkerRegistry.h"

namespace facebook { namespace logdevice { namespace ldbench {
namespace {

/**
 * isLogEmpty benchmark worker.
 *
 * Runs isLogEmpty as requested -- in terms of distribution, spikiness, period
 * -- until told to stop or until duration has passed.
 */
class IsLogEmptyWorker : public MetaRequestWorker {
 public:
  // Inherit constructor
  using MetaRequestWorker::MetaRequestWorker;

  int makeMetadataAPIRequest(LogState* state) override {
    return client_->isLogEmpty(
        state->log_id, [this](Status st, bool empty) mutable {
          if (isStopped()) {
            return;
          }
          onRequestDone(st == E::OK);
        });
  }
};

} // namespace

void registerIsLogEmptyWorker() {
  registerWorkerImpl("islogempty",
                     []() -> std::unique_ptr<Worker> {
                       return std::make_unique<IsLogEmptyWorker>();
                     },
                     OptionsRestrictions(
                         {
                             "duration",
                             "meta-requests-per-sec",
                             "meta-requests-spikiness",
                             "max-requests-in-flight",
                             "log-requests-per-sec-distribution",
                         },
                         {PartitioningMode::LOG, PartitioningMode::RECORD},
                         OptionsRestrictions::AllowBufferedWriterOptions::NO));
}

}}} // namespace facebook::logdevice::ldbench
