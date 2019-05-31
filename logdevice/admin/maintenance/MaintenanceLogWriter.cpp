/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"

#include <folly/MoveWrapper.h>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/request_util.h"

namespace facebook { namespace logdevice { namespace maintenance {

MaintenanceLogWriter::MaintenanceLogWriter(Processor* processor)
    : processor_(processor),
      callbackHelper_(
          std::make_unique<WorkerCallbackHelper<MaintenanceLogWriter>>(this)) {}

std::unique_ptr<BackoffTimer>
MaintenanceLogWriter::createAppendRetryTimer(std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      callback, std::chrono::milliseconds(200), std::chrono::seconds(10));
  return std::move(timer);
}

void MaintenanceLogWriter::writeNextDeltaInQueue() {
  if (!appendRequestRetryTimer_) {
    appendRequestRetryTimer_ =
        createAppendRetryTimer([this]() { writeNextDeltaInQueue(); });
  }

  ld_check(!appendQueue_.empty());
  auto callback_ticket = callbackHelper_->ticket();

  auto cb = [=](Status st, lsn_t lsn, const std::string& /* unused */) {
    callback_ticket.postCallbackRequest([=](MaintenanceLogWriter* writer) {
      if (!writer) {
        // We shut down before the AppendRequest completed.
        return;
      }
      ld_check(!appendQueue_.empty());
      if (st != E::OK) {
        ld_error(
            "Could not write maintenance delta: %s. Will retry after %ldms",
            error_description(st),
            appendRequestRetryTimer_->getNextDelay().count());
        appendRequestRetryTimer_->activate();
      } else {
        ld_info("Wrote record with lsn %s", lsn_to_string(lsn).c_str());
        appendRequestRetryTimer_->reset();
        // pop the message from the queue and send the next one, if any.
        appendQueue_.pop();
        if (!appendQueue_.empty()) {
          writeNextDeltaInQueue();
        }
      }
    });
  };

  auto mode = ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPEND_ONLY;
  writeDelta(*appendQueue_.front(), cb, mode);
}

void MaintenanceLogWriter::writeDelta(std::unique_ptr<MaintenanceDelta> delta) {
  const bool was_empty = appendQueue_.empty();
  appendQueue_.push(std::move(delta));

  if (!was_empty) {
    return;
  }

  writeNextDeltaInQueue();
}

void MaintenanceLogWriter::writeDelta(
    const MaintenanceDelta& delta,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    ClusterMaintenanceStateMachine::WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  std::string serializedData = serializeDelta(delta);
  std::unique_ptr<Request> req =
      std::make_unique<MaintenanceLogWriteDeltaRequest>(
          ClusterMaintenanceStateMachine::workerType(processor_),
          std::move(serializedData),
          std::move(cb),
          std::move(mode),
          std::move(base_version));
  processor_->postWithRetrying(req);
}

std::string
MaintenanceLogWriter::serializeDelta(const MaintenanceDelta& delta) {
  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);
  return serializedData;
}

folly::SemiFuture<folly::Expected<ClusterMaintenanceState, MaintenanceError>>
MaintenanceLogWriter::writeDelta(Processor* processor,
                                 const MaintenanceDelta& delta) {
  using OutType = folly::Expected<ClusterMaintenanceState, MaintenanceError>;
  auto worker_type = ClusterMaintenanceStateMachine::workerType(processor);
  auto worker_index =
      worker_id_t(ClusterMaintenanceStateMachine::getWorkerIndex(
          processor->getWorkerCount(worker_type)));

  std::string payload = serializeDelta(delta);

  auto cb_on_worker = [payload = std::move(payload)](
                          folly::Promise<OutType> promise) mutable {
    auto sm = Worker::onThisThread()->cluster_maintenance_state_machine_;
    if (!sm) {
      ld_warning("No ClusterMaintenanceStateMachine available on this worker. "
                 "Returning E::NOTSUPPORTED.");
      promise.setValue(folly::makeUnexpected(MaintenanceError(
          E::NOTSUPPORTED,
          "No ClusterMaintenanceStateMachine running ont his machine!")));
    } else {
      // This is needed because we want to move this into a lambda, an
      // std::function<> does not allow capturing move-only objects, so here we
      // are!
      // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3610.html
      folly::MoveWrapper<folly::Promise<OutType>> mpromise(std::move(promise));
      auto cb_after_apply = [mpromise = std::move(mpromise)](
                                Status st,
                                lsn_t /* unused */,
                                const std::string& failure_reason) mutable {
        auto rsm = Worker::onThisThread()->cluster_maintenance_state_machine_;
        // This callback is executed on the RSM worker thread, it's safe to
        // access the state at this point.
        if (st == E::OK) {
          // We have a successfully applied delta.
          mpromise->setValue(rsm->getCurrentState());
        } else {
          // One of the input maintenances clash with existing ones.
          mpromise->setValue(
              folly::makeUnexpected(MaintenanceError(st, failure_reason)));
        }
      };

      sm->writeDelta(std::move(payload),
                     std::move(cb_after_apply),
                     ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPLIED,
                     folly::none);
    }
  };

  return fulfill_on_worker<OutType>(processor,
                                    worker_index,
                                    worker_type,
                                    std::move(cb_on_worker),
                                    RequestType::MAINTENANCE_LOG_REQUEST,
                                    /* with_retrying = */ true);
}

thrift::RemoveMaintenancesRequest
MaintenanceLogWriter::buildRemoveMaintenancesRequest(ShardID shard,
                                                     std::string reason) {
  thrift::MaintenancesFilter filter;
  filter.set_group_ids({toString(shard)});
  filter.set_user(INTERNAL_USER.str());

  thrift::RemoveMaintenancesRequest req;
  req.set_filter(std::move(filter));
  req.set_user(INTERNAL_USER.str());
  req.set_reason(reason);
  return req;
}

thrift::MaintenanceDefinition
MaintenanceLogWriter::buildMaintenanceDefinitionForRebuilding(
    ShardID shard,
    std::string reason) {
  thrift::MaintenanceDefinition def;
  auto nodeid = thrift::NodeID();
  nodeid.set_node_index(shard.node());
  auto shardid = thrift::ShardID();
  shardid.set_node(nodeid);
  shardid.set_shard_index(shard.shard());
  def.set_shards({shardid});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user(INTERNAL_USER.str());
  def.set_reason(std::move(reason));
  def.set_skip_safety_checks(true);
  def.set_force_restore_rebuilding(true);
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(false);
  def.set_group_id(toString(shard));
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());
  return def;
}

}}} // namespace facebook::logdevice::maintenance
