/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "common/fb303/cpp/FacebookBase2.h"
#include "logdevice/admin/CheckImpactHandler.h"
#include "logdevice/admin/ClusterMembershipAPIHandler.h"
#include "logdevice/admin/MaintenanceAPIHandler.h"
#include "logdevice/admin/NodesConfigAPIHandler.h"
#include "logdevice/admin/NodesStateAPIHandler.h"
#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {
class SafetyChecker;
/**
 * This is the Admin API Handler class, here we expect to see all callback
 * functions for the admin thrift interface to be implemented.
 *
 * You can use the synchronous version or the Future-based version based on
 * your preference and how long the operation might take. It's always
 * preferred to used the future_Fn flavor if you are performing an operation
 * on a background worker.
 */
class AdminAPIHandler : public facebook::fb303::FacebookBase2,
                        public NodesConfigAPIHandler,
                        public NodesStateAPIHandler,
                        public CheckImpactHandler,
                        public MaintenanceAPIHandler,
                        public ClusterMembershipAPIHandler {
 public:
  AdminAPIHandler(
      Processor* processor,
      std::shared_ptr<SettingsUpdater> settings_updater,
      UpdateableSettings<ServerSettings> updateable_server_settings,
      UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
      StatsHolder* stats_holder);

  /* FB303 exports */
  facebook::fb303::cpp2::fb_status getStatus() override;
  void getVersion(std::string& _return) override;
  int64_t aliveSince() override;

  // *** LogTree-related APIs
  void getLogTreeInfo(thrift::LogTreeInfo&) override;
  void getReplicationInfo(thrift::ReplicationInfo&) override;

  // Sync version since this response can be constructed quite fast
  void getSettings(thrift::SettingsResponse& response,
                   std::unique_ptr<thrift::SettingsRequest> request) override;

  // Take a snapshot of the LogTree running on this server.
  folly::SemiFuture<folly::Unit>
  semifuture_takeLogTreeSnapshot(thrift::unsigned64 min_version) override;

  void getLogGroupThroughput(
      thrift::LogGroupThroughputResponse& response,
      std::unique_ptr<thrift::LogGroupThroughputRequest> request) override;

  void getLogGroupCustomCounters(
      thrift::LogGroupCustomCountersResponse& response,
      std::unique_ptr<thrift::LogGroupCustomCountersRequest> request) override;

 private:
  // When was the admin server started.
  std::chrono::steady_clock::time_point start_time_;
};
}} // namespace facebook::logdevice
