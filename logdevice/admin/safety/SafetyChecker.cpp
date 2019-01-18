/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/SafetyChecker.h"

#include <boost/format.hpp>

#include "logdevice/admin/safety/CheckImpactForLogRequest.h"
#include "logdevice/admin/safety/CheckImpactRequest.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration;

SafetyChecker::SafetyChecker(Processor* processor,
                             size_t logs_in_flight,
                             bool abort_on_error,
                             std::chrono::milliseconds timeout,
                             size_t error_sample_size,
                             bool read_epoch_metadata_from_sequencer)
    : processor_(processor),
      timeout_(timeout),
      logs_in_flight_(logs_in_flight),
      abort_on_error_(abort_on_error),
      error_sample_size_(error_sample_size),
      read_epoch_metadata_from_sequencer_(read_epoch_metadata_from_sequencer) {}

Impact SafetyChecker::checkImpact(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& shards,
    StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_logs,
    bool check_internal_logs,
    folly::Optional<std::vector<logid_t>> logids_to_check) {
  // There is no point of checking this. It's always safe
  if (target_storage_state == StorageState::READ_WRITE) {
    return Impact();
  }

  ld_info("Shards to drain: %s", toString(shards).c_str());
  ld_info("Target storage state is: %s",
          storageStateToString(target_storage_state).c_str());

  std::chrono::steady_clock::time_point start_time =
      std::chrono::steady_clock::now();

  Semaphore sem;
  Impact impact_result;
  auto cb = [&](Impact impact) {
    SCOPE_EXIT {
      sem.post();
    };
    impact_result = std::move(impact);
  };

  WorkerType worker_type = CheckImpactRequest::workerType(processor_);
  std::unique_ptr<Request> request =
      std::make_unique<CheckImpactRequest>(shard_status,
                                           shards,
                                           target_storage_state,
                                           safety_margin,
                                           check_metadata_logs,
                                           check_internal_logs,
                                           std::move(logids_to_check),
                                           logs_in_flight_,
                                           abort_on_error_,
                                           timeout_,
                                           error_sample_size_,
                                           read_epoch_metadata_from_sequencer_,
                                           worker_type,
                                           cb);
  int rv = processor_->postRequest(request);
  if (rv != 0) {
    // We couldn't submit the request to the processor.
    ld_error("We couldn't submit the CheckImpactRequest to the logdevice "
             "processor: %s",
             error_description(err));
    ld_check(err != E::OK);
    return Impact(err);
  }

  sem.wait();

  double runtime = std::chrono::duration_cast<std::chrono::duration<double>>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
  ld_info("Done. Elapsed time: %.1fs", runtime);
  return impact_result;
}

}} // namespace facebook::logdevice
