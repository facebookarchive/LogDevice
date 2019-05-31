/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/types.h"

namespace facebook { namespace logdevice { namespace maintenance {

/**
 * A simple wrapper class that posts a MaintenanceLogWriteDeltaRequest
 */

class MaintenanceLogWriter {
 public:
  explicit MaintenanceLogWriter(Processor* processor);

  virtual ~MaintenanceLogWriter() {}

  virtual void writeDelta(
      const MaintenanceDelta& delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      ClusterMaintenanceStateMachine::WriteMode mode =
          ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  static std::string serializeDelta(const MaintenanceDelta& delta);
  virtual void writeDelta(std::unique_ptr<MaintenanceDelta> delta);

  static folly::SemiFuture<
      folly::Expected<ClusterMaintenanceState, MaintenanceError>>
  writeDelta(Processor* processor, const MaintenanceDelta&);

  /**
   * Returns RemoveMaintenancesRequest for removing an internal maintenance
   * for the given shard by setting appropriate fields in the request
   */
  static thrift::RemoveMaintenancesRequest
  buildRemoveMaintenancesRequest(ShardID shard, std::string reason);
  /**
   * Returns MaintenanceDefinition for rebuilding a given shard internally with
   * all appropirate fields set
   */
  static thrift::MaintenanceDefinition
  buildMaintenanceDefinitionForRebuilding(ShardID shard, std::string reason);

 private:
  Processor* processor_;
  /**
   * Queue of delta waiting to be appended to the maintenance log.  The front of
   * the queue is either an append in flight or an waiting to be appended after
   * the `appendRequestRetryTimer_` triggers.
   */
  std::queue<std::unique_ptr<MaintenanceDelta>> appendQueue_;

  /**
   * Timer with exponential backoff used for retrying appending a message to the
   * maintenance log.
   */
  std::unique_ptr<BackoffTimer> appendRequestRetryTimer_;

  /**
   * Used to get a callback on this worker thread when an AppendRequest to write
   * to the maintenance log completes.
   */
  std::unique_ptr<WorkerCallbackHelper<MaintenanceLogWriter>> callbackHelper_;

  /**
   * Write the next serialized event in `appendQueue_` to the maintenance log.
   */
  void writeNextDeltaInQueue();

  std::unique_ptr<BackoffTimer>
  createAppendRetryTimer(std::function<void()> callback);
};

}}} // namespace facebook::logdevice::maintenance
