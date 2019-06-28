/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/cluster_membership/ClusterMembershipUtils.h"

#include "logdevice/admin/AdminAPIUtils.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::configuration::nodes;

namespace {
Sockaddr convert_thrift_address(const logdevice::thrift::SocketAddress& addr) {
  switch (addr.address_family) {
    case logdevice::thrift::SocketAddressFamily::INET:
      try {
        return Sockaddr(addr.address_ref().value(), addr.port_ref().value());
      } catch (const ConstructorFailed&) {
        return Sockaddr::INVALID;
      }
    case logdevice::thrift::SocketAddressFamily::UNIX:
      try {
        return Sockaddr(addr.address_ref().value());
      } catch (const ConstructorFailed&) {
        return Sockaddr::INVALID;
      }
    default:
      return Sockaddr::INVALID;
  }
}

logdevice::thrift::ClusterMembershipFailedNode
make_invalid_nodes_config_request(logdevice::thrift::NodeIndex node_idx,
                                  const std::string& reason) {
  return buildNodeFailure(node_idx,
                          logdevice::thrift::ClusterMembershipFailureReason::
                              INVALID_REQUEST_NODES_CONFIG,
                          reason);
}

} // namespace

folly::Expected<NodeUpdateBuilder,
                logdevice::thrift::ClusterMembershipFailedNode>
nodeUpdateBuilderFromNodeConfig(const logdevice::thrift::NodeConfig& cfg) {
  NodeUpdateBuilder update_builder;

  update_builder.setNodeIndex(cfg.node_index)
      .setName(cfg.name)
      .setDataAddress(convert_thrift_address(cfg.data_address));

  if (cfg.other_addresses_ref().has_value()) {
    const auto& other_addresses = cfg.other_addresses_ref().value();
    if (other_addresses.gossip_ref().has_value()) {
      update_builder.setGossipAddress(
          convert_thrift_address(other_addresses.gossip_ref().value()));
    }
    if (other_addresses.ssl_ref().has_value()) {
      update_builder.setSSLAddress(
          convert_thrift_address(other_addresses.ssl_ref().value()));
    }
  }

  if (cfg.location_ref().has_value()) {
    NodeLocation loc;
    loc.fromDomainString(cfg.location_ref().value());
    update_builder.setLocation(loc);
  }

  if (cfg.roles.count(logdevice::thrift::Role::SEQUENCER) > 0) {
    if (!cfg.sequencer_ref().has_value()) {
      return folly::makeUnexpected(make_invalid_nodes_config_request(
          cfg.node_index, "Missing sequencer config for a sequencer node."));
    }
    auto seq_cfg = cfg.sequencer_ref().value();
    update_builder.isSequencerNode().setSequencerWeight(seq_cfg.weight);
  }

  if (cfg.roles.count(logdevice::thrift::Role::STORAGE) > 0) {
    if (!cfg.storage_ref().has_value()) {
      return folly::makeUnexpected(make_invalid_nodes_config_request(
          cfg.node_index, "Missing storage config for a storage node."));
    }
    const auto& storage_cfg = cfg.storage_ref().value();
    update_builder.isStorageNode()
        .setStorageCapacity(storage_cfg.weight)
        .setNumShards(storage_cfg.num_shards);
  }

  if (auto validation_result = update_builder.validate();
      validation_result.status != Status::OK) {
    return folly::makeUnexpected(make_invalid_nodes_config_request(
        cfg.node_index, validation_result.message));
  }

  return std::move(update_builder);
}

logdevice::thrift::ClusterMembershipFailedNode
buildNodeFailure(logdevice::thrift::NodeID id,
                 logdevice::thrift::ClusterMembershipFailureReason reason,
                 const std::string& message) {
  logdevice::thrift::ClusterMembershipFailedNode failure;
  failure.set_node_id(id);
  failure.set_reason(reason);
  failure.set_message(message);
  return failure;
}

logdevice::thrift::ClusterMembershipFailedNode
buildNodeFailure(node_index_t idx,
                 logdevice::thrift::ClusterMembershipFailureReason reason,
                 const std::string& message) {
  return buildNodeFailure(mkNodeID(idx), reason, message);
}

}}}} // namespace facebook::logdevice::admin::cluster_membership
