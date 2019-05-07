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
    StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_logs,
    bool check_internal_logs,
    folly::Optional<std::vector<logid_t>> logids_to_check) {
  // If we have metadata use it, if not, refreshMetadata will fetch.
  return refreshMetadata().via(&work_context_).thenValue([=](auto&&) {
    time_t t = std::chrono::system_clock::to_time_t(last_metadata_refresh_at_);
    ld_info("Using metadata cache fetched on %s", std::ctime(&t));

    folly::Promise<folly::Expected<Impact, Status>> promise;
    folly::SemiFuture<folly::Expected<Impact, Status>> future =
        promise.getSemiFuture();

    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();

    auto cb = [p = std::move(promise), start_time](
                  Status status, Impact impact) mutable {
      double runtime =
          std::chrono::duration_cast<std::chrono::duration<double>>(
              std::chrono::steady_clock::now() - start_time)
              .count();
      ld_info("Done. Elapsed time: %.1fs", runtime);
      if (status != E::OK) {
        p.setValue(folly::makeUnexpected(status));
      } else {
        p.setValue(std::move(impact));
      }
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
                                             true,
                                             worker_type,
                                             std::move(cb));
    int rv = processor_->postRequest(request);
    if (rv != 0) {
      folly::Promise<folly::Expected<Impact, Status>> p;
      folly::SemiFuture<folly::Expected<Impact, Status>> f = p.getSemiFuture();
      // We couldn't submit the request to the processor.
      ld_error("We couldn't submit the CheckImpactRequest to the logdevice "
               "processor: %s",
               error_description(err));
      ld_check(err != E::OK);
      p.setValue(folly::makeUnexpected(err));
      return f;
    }
    return future;
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
  setTimeout(settings_->safety_check_timeout);
  setErrorSampleSize(settings_->safety_check_failure_sample_size);
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
    // Fetching logs from logsconfig.
    //
    const auto& local_logs_config = cfg->getLocalLogsConfig();
    const logsconfig::LogsConfigTree& log_tree =
        local_logs_config.getLogsConfigTree();
    // If no logids_to_check is none, we check all logs.
    auto logids = std::vector<logid_t>();
    for (auto it = log_tree.logsBegin(); it != log_tree.logsEnd(); ++it) {
      logids.push_back(logid_t(it->first));
    }
    auto logids_count = logids.size();
    ld_info("Will refresh metadata for %zu logs.", logids_count);

    auto start_time = std::chrono::steady_clock::now();

    auto cb =
        [start_time, this, logids_count](LogMetaDataFetcher::Results results) {
          std::chrono::seconds total_time =
              std::chrono::duration_cast<std::chrono::seconds>(
                  std::chrono::steady_clock::now() - start_time);
          ld_info("Metadata refresh completed in %lds for %zu logs",
                  total_time.count(),
                  logids_count);
          metadata_ =
              std::make_shared<LogMetaDataFetcher::Results>(std::move(results));
          // Deallocate the fetcher, we don't need it.
          fetcher_.reset();

          refresh_in_flight_ = false;
          last_metadata_refresh_at_ = std::chrono::system_clock::now();
          // Fulfill the fetch promise if not fulfilled so we unblock the
          // waiting requests on the first fetch.
          if (!metadata_fetch_promise_.isFulfilled()) {
            metadata_fetch_promise_.setValue(folly::unit);
          }
          if (settings_->enable_safety_check_periodic_metadata_update) {
            // Schedule the next run if period updates setting is set.
            auto next_run_at = last_metadata_refresh_at_ +
                settings_->safety_check_metadata_update_period;
            time_t next_run = std::chrono::system_clock::to_time_t(next_run_at);
            ld_info("Next refresh will happen at %s", std::ctime(&next_run));
            next_refresh_ =
                folly::futures::sleep(
                    settings_->safety_check_metadata_update_period)
                    .via(&work_context_)
                    .thenValue([this](auto&&) { this->refreshMetadata(); });
          }
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

  return metadata_fetch_promise_.getSemiFuture();
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
}} // namespace facebook::logdevice
