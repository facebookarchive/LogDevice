/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "SafetyChecker.h"

#include <boost/format.hpp>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/stats/Stats.h"

#include "CheckMetaDataLogRequest.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration;

Impact::Impact(int result,
               std::string details,
               std::vector<logid_t> logs_affected,
               bool ml_affected)
    : result(result),
      details(std::move(details)),
      logs_affected(std::move(logs_affected)),
      metadata_logs_affected(ml_affected) {}

std::string Impact::toStringImpactResult(int result) {
  std::string s;

#define TO_STR(f)     \
  if (result & f) {   \
    if (!s.empty()) { \
      s += ", ";      \
    }                 \
    s += #f;          \
  }

  TO_STR(ERROR)
  TO_STR(WRITE_AVAILABILITY_LOSS)
  TO_STR(READ_AVAILABILITY_LOSS)
  TO_STR(DATA_LOSS)
  TO_STR(REBUILDING_STALL)
  TO_STR(METADATA_LOSS)

#undef TO_STR

  if (s.empty()) {
    return "NONE";
  } else {
    return s;
  }
}

std::string Impact::toString() const {
  return toStringImpactResult(result) + ". " + details;
}

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
                           std::shared_ptr<ShardSet> shards,
                           int operations,
                           const SafetyMargin& safety_margin,
                           const std::vector<logid_t>& logids_to_check,
                           const bool check_metadata_logs) {
  if ((operations & (Operation::DISABLE_WRITES | Operation::DISABLE_READS)) ==
      0) {
    return Impact(Impact::ImpactResult::NONE);
  }

  ld_info("Shards to drain: %s", toString(*shards).c_str());

  std::chrono::steady_clock::time_point start_time =
      std::chrono::steady_clock::now();

  std::shared_ptr<UpdateableConfig> config = processor_->config_;
  auto cfg = config->get();

  std::shared_ptr<LocalLogsConfig> logs_config = config->getLocalLogsConfig();

  auto* cluster_state = processor_->cluster_state_.get();

  if (cluster_state) {
    ld_info("Refreshing cluster state ... ");
    cluster_state->refreshClusterStateAsync();
  } else {
    ld_warning("Cluster state is not available");
  }

  ld_info("Wait for ClasterState refresh... ");
  cluster_state->waitForRefresh();

  ld_info("Checking logs... ");

  std::atomic<size_t> logs_done{0};
  std::atomic<bool> all_logs_ok(true);
  std::atomic<bool> abort_processing(false);
  // protects access to shared data structures below.
  std::mutex mutex;
  int impact_result_all = 0;
  std::vector<logid_t> sampled_error_logs;
  std::vector<epoch_t> sampled_error_epochs;
  std::vector<logid_t> logs_affected;
  bool ml_affected = false;

  Semaphore sem;
  int in_flight = 0;

  auto cb = [&](Status st,
                int impact_result,
                logid_t completed_log_id,
                epoch_t error_epoch,
                StorageSet /* unused */,
                std::string message) {
    SCOPE_EXIT {
      sem.post();
    };

    // E::NOTINCONFIG - log not in config,
    // is ignored as it is possible due to config change
    // E::NOTFOUND - metadata not provisioned
    // is ignored as this means log is empty
    if (st == E::OK || st == E::NOTINCONFIG || st == E::NOTFOUND) {
      return;
    }

    if (completed_log_id == LOGID_INVALID) {
      ld_error("Error checking metadata nodeset, status: %s, impact: %s. %s",
               error_name(st),
               Impact::toStringImpactResult(impact_result).c_str(),
               message.c_str());
    } else {
      RATELIMIT_ERROR(std::chrono::seconds{5},
                      1,
                      "Error checking log %lu, status: %s, impact: %s. %s",
                      completed_log_id.val_,
                      error_name(st),
                      Impact::toStringImpactResult(impact_result).c_str(),
                      message.c_str());
    }
    {
      std::lock_guard<std::mutex> lock(mutex);
      all_logs_ok = false;
      impact_result_all |= impact_result;
      if (sampled_error_logs.size() < error_sample_size_) {
        sampled_error_logs.push_back(completed_log_id);
        sampled_error_epochs.push_back(error_epoch);
      } else if (abort_on_error_) {
        // We have enough samples and user allows us to stop early.
        abort_processing = true;
      }
      logs_affected.push_back(completed_log_id);
      if (completed_log_id == LOGID_INVALID) {
        ml_affected = true;
      }
    }
  };

  if (check_metadata_logs) {
    ld_debug("Checking metadata nodeset...");
    std::unique_ptr<Request> request =
        std::make_unique<CheckMetaDataLogRequest>(LOGID_INVALID,
                                                  timeout_,
                                                  cfg,
                                                  shard_status,
                                                  shards,
                                                  operations,
                                                  &safety_margin,
                                                  true,
                                                  cb);
    processor_->postImportant(request);
    ++in_flight;
  }

  auto process_log = [&](logid_t log_id) {
    while (in_flight >= logs_in_flight_) {
      sem.wait();
      --in_flight;
      ++logs_done;
      if (abort_processing) {
        break;
      }
    }
    ld_debug("Checking log %lu", log_id.val_);

    auto req = std::make_unique<CheckMetaDataLogRequest>(log_id,
                                                         timeout_,
                                                         cfg,
                                                         shard_status,
                                                         shards,
                                                         operations,
                                                         &safety_margin,
                                                         false,
                                                         cb);
    if (read_epoch_metadata_from_sequencer_) {
      req->readEpochMetaDataFromSequencer();
    }
    std::unique_ptr<Request> request = std::move(req);
    processor_->postImportant(request);

    ++in_flight;
  };

  std::vector<logid_t> scheduled_logs = logids_to_check;
  if (scheduled_logs.empty()) {
    for (auto it = logs_config->logsBegin(); it != logs_config->logsEnd();
         ++it) {
      scheduled_logs.push_back(logid_t(it->first));
    }
  }

  ld_info("Logs to check: %zu", scheduled_logs.size());
  for (auto it = scheduled_logs.begin(); it != scheduled_logs.end(); ++it) {
    RATELIMIT_INFO(std::chrono::seconds{1},
                   1,
                   "%lu/%lu complete (%i in flight)",
                   logs_done.load(),
                   scheduled_logs.size(),
                   in_flight);
    process_log(*it);
    if (abort_processing) {
      break;
    }
  }

  ld_debug("Logs in flight %i", in_flight);
  while (in_flight > 0) {
    sem.wait();
    --in_flight;
    ++logs_done;
  }

  double runtime = std::chrono::duration_cast<std::chrono::duration<double>>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();

  ld_info("Done (%lu logs). Elapsed time: %.1fs", logs_done.load(), runtime);
  if (all_logs_ok) {
    return Impact(Impact::ImpactResult::NONE);
  } else {
    std::lock_guard<std::mutex> lock(mutex);

    std::unordered_set<node_index_t> nodes_to_drain;
    for (const auto& shard_id : *shards) {
      nodes_to_drain.insert(shard_id.node());
    }

    std::stringstream ss;
    if (impact_result_all == Impact::ImpactResult::ERROR) {
      ss << "ERROR retrieving historical epoch metadata for some logs. "
            "Could NOT determine impact of all logs. "
            "Sample (log_id, epoch) pairs affected: ";
    } else {
      ss << boost::format(
                "ERROR: Operation(s) on (%u shards, %u nodes) would cause "
                "%s for some log(s), as in "
                "that storage set, as not enough domains would be "
                "available. Sample (log_id, epoch) pairs affected: ") %
              shards->size() % nodes_to_drain.size() %
              Impact::toStringImpactResult(impact_result_all).c_str();
    }

    ld_check(sampled_error_logs.size() == sampled_error_epochs.size());
    for (int i = 0; i < sampled_error_logs.size(); ++i) {
      ss << boost::format("(%lu, %u)") % sampled_error_logs[i].val() %
              sampled_error_epochs[i].val();
      if (i < sampled_error_logs.size() - 1) {
        ss << "; ";
      }
    }
    return Impact(impact_result_all, ss.str(), logs_affected, ml_affected);
  }
}

int parseSafetyMargin(const std::string& descriptor, SafetyMargin& out) {
  if (descriptor.empty()) {
    return 0;
  }
  std::vector<std::string> domains;
  folly::split(',', descriptor, domains);
  for (const std::string& domain : domains) {
    if (domain.empty()) {
      continue;
    }
    std::vector<std::string> tokens;
    folly::split(":", domain, tokens, /* ignoreEmpty */ false);
    if (tokens.size() != 2) {
      return -1;
    }
    int margin = folly::to<int>(tokens[1]);

    std::string scope_str = tokens[0];
    std::transform(
        scope_str.begin(), scope_str.end(), scope_str.begin(), ::toupper);

    NodeLocationScope scope =
        NodeLocation::scopeNames().reverseLookup(scope_str);
    static_assert(
        (int)NodeLocationScope::NODE == 0,
        "Did you add a location "
        "scope smaller than NODE? Update this validation code to allow it.");
    if (scope < NodeLocationScope::NODE || scope >= NodeLocationScope::ROOT) {
      ld_error("Invalid scope in safety-margin %s ", scope_str.c_str());
      return false;
    }
    out.emplace(scope, margin);
  }
  return 0;
}

int parseSafetyMargin(const std::vector<std::string>& descriptors,
                      SafetyMargin& out) {
  for (const std::string& descriptor : descriptors) {
    if (parseSafetyMargin(descriptor, out) != 0) {
      return -1;
    }
  }
  return 0;
}

}} // namespace facebook::logdevice
