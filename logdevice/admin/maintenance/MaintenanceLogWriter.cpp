/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/ThriftCodec.h"

namespace facebook { namespace logdevice { namespace maintenance {

void MaintenanceLogWriter::writeDelta(
    std::unique_ptr<MaintenanceDelta> delta,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    ClusterMaintenanceStateMachine::WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  ld_check(delta);
  std::string serializedData =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(*delta);
  std::unique_ptr<Request> req =
      std::make_unique<MaintenanceLogWriteDeltaRequest>(
          ClusterMaintenanceStateMachine::workerType(processor_),
          std::move(serializedData),
          std::move(cb),
          std::move(mode),
          std::move(base_version));
  processor_->postWithRetrying(req);
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
