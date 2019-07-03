/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/SafetyChecker.h"

#include <algorithm>
#include <cmath>

#include "logdevice/admin/safety/SafetyCheckerUtils.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

SafetyChecker::SafetyChecker(Processor* processor)
    : work_context_(getKeepAliveToken(folly::getCPUExecutor().get())),
      processor_(processor) {
  auto cb = [&]() { work_context_.add([&]() { this->onLogsConfigUpdate(); }); };

  config_update_handle_ =
      processor->config_->updateableLogsConfig()->subscribeToUpdates(
          std::move(cb));
  if (settings_->enable_safety_check_periodic_metadata_update) {
    // Initial fetch of metadata to speed up the first safety check request.
    ld_info("Periodic fetching of metadata for safety checker is enabled "
            "(enable-safety-check-periodic-metadata-update). Will perform an "
            "initial fetch.");
    refreshMetadata();
  }
}
SafetyChecker::~SafetyChecker() {
  next_refresh_.cancel();
}

folly::SemiFuture<folly::Expected<Impact, Status>> SafetyChecker::checkImpact(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& shards,
    folly::F14FastSet<node_index_t> /*sequencers*/,
    StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_logs,
    bool check_internal_logs,
    folly::Optional<std::vector<logid_t>> logids_to_check) {
  // If we have metadata use it (and trigger async refresh if needed), if not,
  // refreshMetadata will fetch.
  return refreshMetadata().via(&work_context_).thenValue([=](auto&&) {
    time_t t = std::chrono::system_clock::to_time_t(last_metadata_refresh_at_);
    ld_info("Using metadata cache fetched on %s", std::ctime(&t));
    return performSafetyCheck(shard_status,
                              metadata_,
                              shards,
                              {}, // TODO
                              target_storage_state,
                              safety_margin,
                              check_metadata_logs,
                              check_internal_logs,
                              std::move(logids_to_check));
  });
}

void SafetyChecker::useAdminSettings(
    UpdateableSettings<AdminServerSettings> admin_settings) {
  settings_ = std::move(admin_settings);
  auto cb = [&]() { work_context_.add([&]() { onSettingsUpdate(); }); };
  settings_handle_ = settings_.callAndSubscribeToUpdates(std::move(cb));
}

void SafetyChecker::onSettingsUpdate() {
  setMaxInFlight(settings_->safety_max_logs_in_flight);
  setAbortOnError(true);
  setMaxBatchSize(settings_->safety_check_max_batch_size);
  setErrorSampleSize(settings_->safety_check_failure_sample_size);
}

void SafetyChecker::onLogsConfigUpdate() {
  std::shared_ptr<Configuration> cfg = processor_->getConfig();
  // We only trigger this iff:
  //   - We don't have metadata yet (maybe first fetch)
  //   - We enabled perodic fetches
  //   - Config is fully loaded.
  if (metadata_ == nullptr &&
      settings_->enable_safety_check_periodic_metadata_update &&
      cfg->logsConfig()->isFullyLoaded()) {
    ld_info("Got a new fully loaded logs configuration");
    refreshMetadata();
  }
}

folly::SemiFuture<folly::Unit> SafetyChecker::refreshMetadata() {
  work_context_.add([&] {
    std::shared_ptr<Configuration> cfg = processor_->getConfig();
    if (!cfg->logsConfig()->isFullyLoaded()) {
      ld_debug("Refresh metadata requested while logs config is not ready yet, "
               "ignoring.");
      return;
    }
    if (refresh_in_flight_) {
      ld_debug("Refresh metadata requested while one is already in-flight, "
               "ignoring.");
      return;
    }
    if (std::chrono::system_clock::now() - last_metadata_refresh_at_ <
        settings_->safety_check_metadata_update_period) {
      ld_info("Skipping metadata update since it was refreshed recently.");
      return;
    }
    refresh_in_flight_ = true;
    //
    // Fetching logs from logsconfig.
    //
    const auto& local_logs_config = cfg->getLocalLogsConfig();
    const logsconfig::LogsConfigTree& log_tree =
        local_logs_config.getLogsConfigTree();
    const InternalLogs& internal_logs = local_logs_config.getInternalLogs();
    auto logids = std::vector<logid_t>();
    // Populate internal logs
    for (auto it = internal_logs.logsBegin(); it != internal_logs.logsEnd();
         ++it) {
      logids.push_back(logid_t(it->first));
    }
    // Populate data logs
    for (auto it = log_tree.logsBegin(); it != log_tree.logsEnd(); ++it) {
      logids.push_back(logid_t(it->first));
    }
    auto logids_count = logids.size();

    auto start_time = std::chrono::steady_clock::now();
    auto logsconfig_version = local_logs_config.getVersion();

    ld_info("Will refresh metadata for %zu logs. LogsConfig version=%lu.",
            logids_count,
            logsconfig_version);

    auto cb = [start_time, this, logids_count, logsconfig_version](
                  LogMetaDataFetcher::Results results) {
      this->onMetadataRefreshComplete(
          std::move(results), start_time, logids_count, logsconfig_version);
    };

    fetcher_ = std::make_unique<LogMetaDataFetcher>(
        nullptr,
        std::move(logids),
        std::move(cb),
        LogMetaDataFetcher::Type::HISTORICAL_METADATA_ONLY);

    fetcher_->setMaxInFlight(logs_in_flight_);
    fetcher_->start(processor_);
    ld_info("Started refreshing metadata cache of SafetyChecker");
  });

  return initial_fetch_promise_.getSemiFuture();
}

void SafetyChecker::onMetadataRefreshComplete(
    LogMetaDataFetcher::Results results,
    std::chrono::steady_clock::time_point start_time,
    size_t logids_count,
    uint64_t logsconfig_version) {
  std::chrono::seconds total_time =
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - start_time);
  ld_info("Metadata refresh completed in %lds for %zu logs. Contains "
          "metadata for %zu logs. LogsConfig version=%lu",
          total_time.count(),
          logids_count,
          results.size(),
          logsconfig_version);
  metadata_ = std::make_shared<LogMetaDataFetcher::Results>(std::move(results));
  // Deallocate the fetcher, we don't need it.
  fetcher_.reset();

  refresh_in_flight_ = false;
  last_metadata_refresh_at_ = std::chrono::system_clock::now();
  // Fulfill the fetch promise if not fulfilled so we unblock the
  // waiting requests on the first fetch.
  if (!initial_fetch_promise_.isFulfilled()) {
    initial_fetch_promise_.setValue(folly::unit);
  }
  if (settings_->enable_safety_check_periodic_metadata_update) {
    // Schedule the next run if period updates setting is set.
    auto next_run_at = last_metadata_refresh_at_ +
        settings_->safety_check_metadata_update_period;
    time_t next_run = std::chrono::system_clock::to_time_t(next_run_at);
    ld_info("Next refresh will happen at %s", std::ctime(&next_run));
    next_refresh_ =
        folly::futures::sleep(settings_->safety_check_metadata_update_period)
            .via(&work_context_)
            .thenValue([this](auto&&) { this->refreshMetadata(); });
  }
}

folly::SemiFuture<folly::Expected<Impact, Status>>
SafetyChecker::performSafetyCheck(
    ShardAuthoritativeStatusMap status_map,
    std::shared_ptr<LogMetaDataFetcher::Results> metadata,
    ShardSet shards,
    folly::F14FastSet<node_index_t> sequencers,
    configuration::StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_logs,
    bool check_internal_logs,
    folly::Optional<std::vector<logid_t>> logids_to_check) {
  auto start_time = std::chrono::steady_clock::now();
  // We are moving the values to ensure lifetime in this async operation. We
  // could have copied but there is no reason to.
  return folly::via(&work_context_)
      .thenValue([status_map = std::move(status_map),
                  metadata = std::move(metadata),
                  shards = std::move(shards),
                  sequencers = std::move(sequencers),
                  target_storage_state,
                  safety_margin = std::move(safety_margin),
                  check_metadata_logs,
                  check_internal_logs,
                  logids_to_check = std::move(logids_to_check),
                  start_time,
                  this](auto&&) mutable
                 -> folly::SemiFuture<folly::Expected<Impact, Status>> {
        std::shared_ptr<Configuration> cfg = processor_->getConfig();
        ld_info("Performing safety check shards: %s, "
                "sequencers: %s, authoritative status: %s",
                toString(shards).c_str(),
                toString(sequencers).c_str(),
                status_map.describe().c_str());

        const auto& local_logs_config = cfg->getLocalLogsConfig();
        const auto& internal_logs = local_logs_config.getInternalLogs();
        const logsconfig::LogsConfigTree& log_tree =
            local_logs_config.getLogsConfigTree();

        const auto nodes_config = processor_->getNodesConfiguration();
        ClusterState* cluster_state = processor_->cluster_state_.get();

        // CheckMetadataLog StorageSet
        Impact metadata_impact;
        if (check_metadata_logs) {
          metadata_impact =
              safety::checkMetadataStorageSet(status_map,
                                              shards,
                                              sequencers,
                                              target_storage_state,
                                              safety_margin,
                                              nodes_config,
                                              cluster_state,
                                              error_sample_size_);
          if (abort_on_error_ &&
              metadata_impact.result != Impact::ImpactResult::NONE) {
            // We will not proceed if internal logs are unhappy and
            // abort_on_error is set.
            std::chrono::seconds total_time =
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now() - start_time);
            metadata_impact.total_duration = total_time;
            return folly::makeSemiFuture(
                folly::Expected<Impact, Status>(metadata_impact));
          }
        }

        // Check Internal logs
        Impact internal_logs_impact;
        if (check_internal_logs) {
          std::vector<logid_t> internal_log_ids;
          for (auto it = internal_logs.logsBegin();
               it != internal_logs.logsEnd();
               ++it) {
            internal_log_ids.push_back(logid_t(it->first));
          }
          // We fail the safety check immediately if we cannot schedule internal
          // logs to be checked.
          auto impact = safety::checkImpactOnLogs(internal_log_ids,
                                                  metadata,
                                                  status_map,
                                                  shards,
                                                  sequencers,
                                                  target_storage_state,
                                                  safety_margin,
                                                  /* internal_logs = */ true,
                                                  abort_on_error_,
                                                  error_sample_size_,
                                                  nodes_config,
                                                  cluster_state);
          if (impact.hasError()) {
            // The operation failed. Possibly because we don't have metadata for
            // this log-id. This is critical.
            return folly::makeSemiFuture(impact);
          }
          internal_logs_impact = impact.value();
          if (abort_on_error_ &&
              internal_logs_impact.result != Impact::ImpactResult::NONE) {
            // We will not proceed if internal logs are unhappy and
            // abort_on_error is set.
            std::chrono::seconds total_time =
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now() - start_time);
            internal_logs_impact.total_duration = total_time;
            return folly::makeSemiFuture(
                folly::Expected<Impact, Status>(internal_logs_impact));
          }
        }

        // If no logids_to_check is none, we check all logs.
        Impact data_impact;
        std::deque<logid_t> logs;
        if (!logids_to_check) {
          logids_to_check = std::vector<logid_t>();
          for (auto it = log_tree.logsBegin(); it != log_tree.logsEnd(); ++it) {
            logs.push_back(logid_t(it->first));
          }
        } else {
          std::copy(logids_to_check->begin(),
                    logids_to_check->end(),
                    std::back_inserter(logs));
          ld_check(logs.size() == logids_to_check->size());
        }

        // Check logs. (max_batch_size_) at each run time.
        // We fail the safety check immediately if we cannot schedule internal
        // logs to be checked.
        //
        std::vector<folly::SemiFuture<folly::Expected<Impact, Status>>> futs;
        // We split the work into batches and schedule them accordingly.
        //
        // We need to figure out how many chunks by dividing the size over
        // max_batch_size_
        const size_t chunk_max_size = max_batch_size_;
        size_t chunks = std::ceil((double)logs.size() / chunk_max_size);
        // TODO: explore using folly::window to execute batches.
        while (chunks > 0) {
          std::vector<logid_t> batch;
          // allocate memory for the chunk
          size_t batch_size = std::min(logs.size(), chunk_max_size);
          batch.reserve(batch_size);
          while (batch.size() < chunk_max_size && logs.size() > 0) {
            batch.push_back(logs.front());
            logs.pop_front();
          }
          // Dispatch parallel processing of logs.
          // TODO: Remove me when we have a shared CPU thread pool executor in
          // processor
          auto executor = folly::getCPUExecutor();
          futs.push_back(folly::via(executor.get())
                             .thenValue([mbatch = std::move(batch),
                                         metadata,
                                         status_map,
                                         shards,
                                         sequencers,
                                         target_storage_state,
                                         safety_margin,
                                         this,
                                         cfg,
                                         cluster_state](auto&&) {
                               return safety::checkImpactOnLogs(
                                   mbatch,
                                   metadata,
                                   status_map,
                                   shards,
                                   sequencers,
                                   target_storage_state,
                                   safety_margin,
                                   /* internal_logs = */ false,
                                   abort_on_error_,
                                   error_sample_size_,
                                   processor_->getNodesConfiguration(),
                                   cluster_state);
                             }));
          --chunks;
        }

        // Initial impact is the combined result of metadata and internal logs.
        folly::Expected<Impact, Status> initial_value = Impact::merge(
            metadata_impact, internal_logs_impact, error_sample_size_);
        // Merges results as we get them.
        return folly::unorderedReduce(
                   std::move(futs),
                   initial_value,
                   [this](folly::Expected<Impact, Status> acc,
                          folly::Expected<Impact, Status> result) {
                     return Impact::merge(
                         std::move(acc), result, error_sample_size_);
                   })
            .thenValue([start_time](folly::Expected<Impact, Status> result) {
              std::chrono::seconds total_time =
                  std::chrono::duration_cast<std::chrono::seconds>(
                      std::chrono::steady_clock::now() - start_time);
              if (result.hasValue()) {
                ld_info(
                    "Sending %lu error samples", result->logs_affected.size());
                result->total_duration = total_time;
              }
              return result;
            });
      });
}
}} // namespace facebook::logdevice
