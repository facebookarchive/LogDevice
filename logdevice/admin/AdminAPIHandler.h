/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>
#include "logdevice/admin/CheckImpactHandler.h"
#include "logdevice/admin/NodesConfigAPIHandler.h"
#include "logdevice/admin/NodesStateAPIHandler.h"
#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {
class ServerProcessor;
class Server;
namespace configuration {
class Node;
}
}} // namespace facebook::logdevice

namespace facebook { namespace logdevice {
/**
 * This is the Admin API Handler class, here we expect to see all callback
 * functions for the admin thrift interface to be implemented.
 *
 * You can use the synchronous version or the Future-based version based on
 * your preference and how long the operation might take. It's always
 * preferred to used the future_Fn flavor if you are performing an operation
 * on a background worker.
 */
class AdminAPIHandler : public NodesConfigAPIHandler,
                        public NodesStateAPIHandler,
                        public CheckImpactHandler {
 public:
  // *** LogTree-related APIs
  void getLogTreeInfo(thrift::LogTreeInfo&) override;
  void getReplicationInfo(thrift::ReplicationInfo&) override;

  // Sync version since this response can be constructed quite fast
  void getSettings(thrift::SettingsResponse& response,
                   std::unique_ptr<thrift::SettingsRequest> request) override;

  // Take a snapshot of the LogTree running on this server.
  folly::Future<folly::Unit>
  future_takeLogTreeSnapshot(thrift::unsigned64 min_version) override;

  void getLogGroupThroughput(
      thrift::LogGroupThroughputResponse& response,
      std::unique_ptr<thrift::LogGroupThroughputRequest> request) override;
};
}} // namespace facebook::logdevice
