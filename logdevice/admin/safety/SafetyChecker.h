/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Expected.h>
#include <folly/container/F14Set.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "logdevice/admin/safety/LogMetaDataFetcher.h"
#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/work_model/SerialWorkContext.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class Processor;
class ClusterState;

class SafetyChecker {
 public:
  explicit SafetyChecker(Processor* processor);

  ~SafetyChecker();

  /*
   * Find out what would be impact on 'logids_to_check'
   * (if 'logids_to_check' is empty then on all logs in the cluster)
   * if 'target_storage_state' is applied on specified shards
   */
  folly::SemiFuture<folly::Expected<Impact, Status>>
  checkImpact(const ShardAuthoritativeStatusMap& status_map,
              const ShardSet& shards,
              folly::F14FastSet<node_index_t> sequencers,
              configuration::StorageState target_storage_state,
              SafetyMargin safety_margin = SafetyMargin(),
              bool check_metadata_logs = true,
              bool check_internal_logs = true,
              folly::Optional<std::vector<logid_t>> logids_to_check = {});

  static std::string
  impactToString(const ShardSet& shards,
                 const ShardAuthoritativeStatusMap& shard_status,
                 const Impact& impact,
                 const ClusterState* cluster_state);

  /**
   * Uses the AdminSettings to update the configuration of safety checker. The
   * safety checker will subscribe to settings update as they happen.
   */
  void useAdminSettings(UpdateableSettings<AdminServerSettings> admin_settings);

  void setMaxBatchSize(size_t max_batch_size) {
    max_batch_size_ = max_batch_size;
  }

  void setMaxInFlight(size_t max_in_flight) {
    logs_in_flight_ = max_in_flight;
  }

  void setAbortOnError(bool abort_on_error) {
    abort_on_error_ = abort_on_error;
  }

  void setErrorSampleSize(size_t sample_size) {
    error_sample_size_ = sample_size;
  }

 private:
  SerialWorkContext work_context_;
  void onSettingsUpdate();
  // the future is fulfilled if we already have metadata. But will schedule
  // another metadata request asynchronously if elapsed time is higher than our
  // threshold.
  //
  folly::SemiFuture<folly::Unit> refreshMetadata();
  void onLogsConfigUpdate();

  // Callback when we finish refreshing the metadata.
  //
  // Must be called from within the work context.
  void
  onMetadataRefreshComplete(LogMetaDataFetcher::Results results,
                            std::chrono::steady_clock::time_point start_time,
                            size_t logids_count,
                            uint64_t logsconfig_version);

  /**
   * Performs a safety check against the cached metadata.
   */
  folly::SemiFuture<folly::Expected<Impact, Status>> performSafetyCheck(
      ShardAuthoritativeStatusMap status_map,
      // We capture metadata here to ensure we perform the entirety of the check
      // over the same set of metadata even if an async refresh changed the
      // pointer in metadata_
      std::shared_ptr<LogMetaDataFetcher::Results> metadata,
      /* Can be empty, means check given current state of shards*/
      ShardSet shards,
      folly::F14FastSet<node_index_t> sequencers,
      /* Can be READ_WRITE is shards is empty */
      configuration::StorageState target_storage_state,
      SafetyMargin safety_margin,
      /* Do we check the metadata logs too? */
      bool check_metadata_logs,
      /* Do we check the interal logs (see InternalLogs.h)? */
      bool check_internal_logs,
      /*
       * if folly::none we check all logs, if empty vector, we don't check any
       * logs, unless check_metadata_logs and/or check_internal_logs is/are set.
       */
      folly::Optional<std::vector<logid_t>> logids_to_check);

  bool appendImpactToSample(
      Impact::ImpactOnEpoch impact,
      std::vector<Impact::ImpactOnEpoch>& affected_logs_sample) const;

  bool refresh_in_flight_{false};
  // The future that will execute the next timed refresh of metadata. This is
  // cancelled on destruction to ensure we don't hold keep-alive tokens to this
  // work context in Timekeeper.
  folly::SemiFuture<folly::Unit> next_refresh_;
  // A promise that is fulfilled when we finish fetching the metadata
  folly::SharedPromise<folly::Unit> initial_fetch_promise_;

  std::shared_ptr<LogMetaDataFetcher::Results> metadata_;
  std::chrono::system_clock::time_point last_metadata_refresh_at_;

  ConfigSubscriptionHandle config_update_handle_;

  std::unique_ptr<LogMetaDataFetcher> fetcher_;

  UpdateableSettings<AdminServerSettings>::SubscriptionHandle settings_handle_;
  UpdateableSettings<AdminServerSettings> settings_;

  Processor* processor_;
  size_t max_batch_size_{10000};
  size_t logs_in_flight_{10000};
  bool abort_on_error_{true};
  size_t error_sample_size_{20};
};

}} // namespace facebook::logdevice
