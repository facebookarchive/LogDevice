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
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

folly::SemiFuture<std::unique_ptr<thrift::CheckImpactResponse>>
CheckImpactHandler::semifuture_checkImpact(
    std::unique_ptr<thrift::CheckImpactRequest> request) {
  // We don't have a safety checker.
  if (safety_checker_ == nullptr) {
    // We don't have a safety checker.
    throw thrift::NotSupported(
        "SafetyChecker is not running on this admin server!");
  }
  std::shared_ptr<EventLogRebuildingSet> rebuilding_set =
      processor_->rebuilding_set_.get();
  // Rebuilding Set can be nullptr if rebuilding is disabled, or if event_log is
  // not ready yet.
  if (!rebuilding_set) {
    // We can't move on if we don't have the current AuthoritativeStatus. We
    // throw NotReady in this case.
    throw thrift::NodeNotReady("Node does not have the shard states yet, try a "
                               "different node");
  }

  const auto& nodes_configuration = processor_->getNodesConfiguration();
  ShardAuthoritativeStatusMap status_map =
      rebuilding_set->toShardStatusMap(*nodes_configuration);

  // Resolve shards from thrift to logdevice
  // TODO: Handle sequencer-only nodes once we have capacity checking in safety
  // checker. (T43766732)
  ShardSet shards = expandShardSet(request->get_shards(),
                                   *nodes_configuration,
                                   /* ignore_missing = */ false,
                                   /* ignore_non_storage_nodes = */ true);

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
                    {}, // TODO: Support sequencers (T43766732)
                    target_storage_state,
                    safety_margin,
                    request->check_metadata_logs_ref().value_or(true),
                    request->check_internal_logs_ref().value_or(true),
                    logs_to_check)
      .via(this->getThreadManager())
      .thenValue([](const folly::Expected<Impact, Status>& impact) {
        if (impact.hasError()) {
          // An error has happened.
          thrift::OperationError ex(error_description(impact.error()));
          ex.set_error_code(static_cast<uint16_t>(impact.error()));
          ex.set_message(error_description(impact.error()));
          throw ex;
        }

        return std::make_unique<thrift::CheckImpactResponse>(
            toThrift<thrift::CheckImpactResponse>(impact.value()));
      });
}
}} // namespace facebook::logdevice
