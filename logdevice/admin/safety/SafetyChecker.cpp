/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "SafetyChecker.h"

#include <boost/format.hpp>

#include "logdevice/admin/safety/CheckImpactRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/stats/Stats.h"

#include "CheckMetaDataLogRequest.h"

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

Impact
SafetyChecker::checkImpact(const ShardAuthoritativeStatusMap& shard_status,
                           const ShardSet& shards,
                           int operations,
                           SafetyMargin safety_margin,
                           std::vector<logid_t> logids_to_check) {
  if ((operations & (Operation::DISABLE_WRITES | Operation::DISABLE_READS)) ==
      0) {
    return Impact();
  }

  ld_info("Shards to drain: %s", toString(shards).c_str());

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

  std::unique_ptr<Request> request =
      std::make_unique<CheckImpactRequest>(shard_status,
                                           shards,
                                           operations,
                                           safety_margin,
                                           logids_to_check,
                                           logs_in_flight_,
                                           abort_on_error_,
                                           timeout_,
                                           error_sample_size_,
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

std::string
storageSetWithStatus(const ShardSet& highlighted_shards,
                     const StorageSet& storage_set,
                     const ShardAuthoritativeStatusMap& status_map) {
  std::stringstream ss;
  ;
  ss << "[";
  for (auto it = storage_set.begin(); it != storage_set.end(); ++it) {
    AuthoritativeStatus status =
        status_map.getShardStatus(it->node(), it->shard());
    if (highlighted_shards.count(*it) > 0) {
      ss << "\033[1;1m";
    }
    if (status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
      ss << "\033[31;1m";
    }
    ss << it->toString() << "(" << toShortString(status) << ")";
    ss << "\033[0m";
    if (std::next(it) != storage_set.end()) {
      ss << ", ";
    }
  }
  ss << "]";
  return ss.str();
}

std::string
SafetyChecker::impactToString(const ShardSet& shards,
                              const ShardAuthoritativeStatusMap& shard_status,
                              const Impact& impact) {
  std::stringstream ss;
  if (impact.status != E::OK) {
    ss << "ERROR: Could NOT determine impact of all logs. "
          "Sample (log_id, epoch) pairs affected: ";
  } else if (impact.result != Impact::ImpactResult::NONE) {
    ss << folly::format("\033[31;1mUNSAFE\033[0m: Operation(s) on ({} shards) "
                        "would cause "
                        "{} for some log(s), as in "
                        "that storage set, as no enough domains will be "
                        "available. Sample logs/epochs affected: \n",
                        shards.size(),
                        Impact::toStringImpactResult(impact.result).c_str())
              .str();
  }
  for (auto& epoch_info : impact.logs_affected) {
    ss << folly::format(
        "({} -> Log ID: {}, Epoch: {}, Replication: {}, StorageSet: {})\n",
        Impact::toStringImpactResult(epoch_info.impact_result),
        epoch_info.log_id.val(),
        epoch_info.epoch.val(),
        toString(epoch_info.replication),
        storageSetWithStatus(shards, epoch_info.storage_set, shard_status));
  }
  return ss.str();
}
}} // namespace facebook::logdevice
