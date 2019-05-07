/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Expected.h>
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

  void setTimeout(std::chrono::milliseconds millis) {
    timeout_ = millis;
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
  folly::SemiFuture<folly::Unit> refreshMetadata();
  void onLogsConfigUpdate();

  bool refresh_in_flight_{false};
  // The future that will execute the next timed refresh of metadata. This is
  // cancelled on destruction to ensure we don't hold keep-alive tokens to this
  // work context in Timekeeper.
  folly::SemiFuture<folly::Unit> next_refresh_;
  // A promise that is fulfilled when we finish fetching the metadata
  folly::SharedPromise<folly::Unit> metadata_fetch_promise_;

  std::shared_ptr<LogMetaDataFetcher::Results> metadata_;
  std::chrono::system_clock::time_point last_metadata_refresh_at_;

  ConfigSubscriptionHandle config_update_handle_;

  std::unique_ptr<LogMetaDataFetcher> fetcher_;

  UpdateableSettings<AdminServerSettings>::SubscriptionHandle settings_handle_;
  UpdateableSettings<AdminServerSettings> settings_;

  Processor* processor_;
  std::chrono::milliseconds timeout_{std::chrono::minutes(2)};
  size_t logs_in_flight_{10000};
  bool abort_on_error_{true};
  size_t error_sample_size_{20};
};

}} // namespace facebook::logdevice
