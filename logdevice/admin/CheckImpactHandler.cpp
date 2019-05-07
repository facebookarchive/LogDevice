/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/CheckImpactHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/Conv.h"
#include "logdevice/admin/safety/SafetyChecker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/server/ServerProcessor.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

folly::SemiFuture<std::unique_ptr<thrift::CheckImpactResponse>>
CheckImpactHandler::semifuture_checkImpact(
    std::unique_ptr<thrift::CheckImpactRequest> request) {
  // Fulfilling this promise means that we have a response.
  folly::Promise<std::unique_ptr<thrift::CheckImpactResponse>> promise;
  // We don't have a safety checker.
  if (safety_checker_ == nullptr) {
    // We don't have a safety checker.
    thrift::NotSupported err;
    err.set_message("SafetyChecker is not running on this admin server!");
    promise.setException(std::move(err));
    return promise.getSemiFuture();
  }
  std::shared_ptr<EventLogRebuildingSet> rebuilding_set =
      processor_->rebuilding_set_.get();
  // Rebuilding Set can be nullptr if rebuilding is disabled, or if event_log is
  // not ready yet.
  if (!rebuilding_set) {
    // We can't move on if we don't have the current AuthoritativeStatus. We
    // throw NotReady in this case.
    thrift::NodeNotReady err;
    err.set_message("Node does not have the shard states yet, try a "
                    "different node");
    promise.setException(std::move(err));
    return promise.getSemiFuture();
  }

  const auto& nodes_configuration = processor_->getNodesConfiguration();
  ShardAuthoritativeStatusMap status_map =
      rebuilding_set->toShardStatusMap(*nodes_configuration);

  // Resolve shards from thrift to logdevice
  ShardSet shards = expandShardSet(request->get_shards(), *nodes_configuration);

  ld_info("SHARDS: %s", toString(shards).c_str());

  StorageState target_storage_state = StorageState::READ_WRITE;
  if (request->get_target_storage_state()) {
    target_storage_state =
        toLogDevice<StorageState>(*request->get_target_storage_state());
  }

  // Convert thrift::ReplicationProperty into logdevice's SafetyMargin
  SafetyMargin safety_margin;
  if (request->get_safety_margin()) {
    safety_margin = safetyMarginFromReplication(
        toLogDevice<ReplicationProperty>(*request->get_safety_margin()));
  }
  // logids to check
  folly::Optional<std::vector<logid_t>> logs_to_check;
  if (request->get_log_ids_to_check()) {
    logs_to_check = std::vector<logid_t>();
    for (thrift::unsigned64 id : *request->get_log_ids_to_check()) {
      logs_to_check->push_back(logid_t(to_unsigned(id)));
    }
  }

  return safety_checker_
      ->checkImpact(std::move(status_map),
                    std::move(shards),
                    {}, // TODO: Support sequencers
                    target_storage_state,
                    safety_margin,
                    request->check_metadata_logs_ref().value_or(true),
                    request->check_internal_logs_ref().value_or(true),
                    logs_to_check)
      .via(this->getThreadManager())
      .thenValue([](const folly::Expected<Impact, Status>& impact) {
        folly::Promise<std::unique_ptr<thrift::CheckImpactResponse>> result;
        if (impact.hasError()) {
          // An error has happened.
          thrift::OperationError ex;
          ex.set_error_code(static_cast<uint16_t>(impact.error()));
          ex.set_message(error_description(impact.error()));
          result.setException(std::move(ex));
          return result.getSemiFuture();
        }

        std::vector<thrift::ImpactOnEpoch> logs_affected;
        for (const auto& epoch_info : impact->logs_affected) {
          logs_affected.push_back(toThrift<thrift::ImpactOnEpoch>(epoch_info));
        }

        auto response = std::make_unique<thrift::CheckImpactResponse>();

        response->set_impact(
            toThrift<std::vector<thrift::OperationImpact>>(impact->result));
        response->set_internal_logs_affected(impact->internal_logs_affected);
        response->set_logs_affected(std::move(logs_affected));
        response->set_total_duration(impact->total_duration.count());
        response->set_total_logs_checked(impact->total_logs_checked);
        result.setValue(std::move(response));
        return result.getSemiFuture();
      });
}
}} // namespace facebook::logdevice
