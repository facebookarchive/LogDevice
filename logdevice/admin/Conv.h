/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/admin/if/gen-cpp2/admin_types.h"
#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/include/NodeLocationScope.h"

namespace facebook { namespace logdevice {

inline uint64_t to_unsigned(thrift::unsigned64 value) {
  return static_cast<uint64_t>(value);
}

// Templated thrift <-> LogDevice type converter. This interface can be used if
// there is 1:1 mapping between the types.
template <typename ThriftType, typename LDType>
ThriftType toThrift(const LDType& input);

/** Specializations **/

/// DEPRECATED
template <>
thrift::ShardStorageState toThrift(const configuration::StorageState& input);

template <>
thrift::ShardStorageState toThrift(const membership::StorageState& input);

template <>
membership::thrift::StorageState
toThrift(const membership::StorageState& input);

template <>
membership::thrift::MetaDataStorageState
toThrift(const membership::MetaDataStorageState& input);

template <>
thrift::Role toThrift(const configuration::NodeRole& role);

template <>
std::vector<thrift::OperationImpact>
toThrift(const Impact::ImpactResult& impact);

template <>
thrift::ShardID toThrift(const ShardID& shard);

template <>
thrift::LocationScope toThrift(const NodeLocationScope& input);

template <>
thrift::Location toThrift(const folly::Optional<NodeLocation>& input);

template <>
thrift::ServiceState toThrift(const ClusterStateNodeState& input);

template <>
thrift::ShardMetadata toThrift(const Impact::ShardMetadata& input);

template <>
thrift::ReplicationProperty toThrift(const ReplicationProperty& replication);

template <>
thrift::ImpactOnEpoch toThrift(const Impact::ImpactOnEpoch& epoch);

template <>
thrift::CheckImpactResponse toThrift(const Impact& impact);

template <typename LDType, typename ThriftType>
LDType toLogDevice(const ThriftType& input);

/** Specializations **/
template <>
configuration::nodes::NodeRole toLogDevice(const thrift::Role& role);

// DEPRECATED
template <>
configuration::StorageState toLogDevice(const thrift::ShardStorageState& input);

template <>
membership::StorageState
toLogDevice(const membership::thrift::StorageState& input);

template <>
membership::MetaDataStorageState
toLogDevice(const membership::thrift::MetaDataStorageState& input);

template <>
NodeLocationScope toLogDevice(const thrift::LocationScope& input);

template <>
ReplicationProperty toLogDevice(const thrift::ReplicationProperty& input);

thrift::ShardDataHealth toShardDataHealth(AuthoritativeStatus auth_status,
                                          bool has_dirty_ranges);

// If we can convert Type A => B then we should be able to convert
// std::vector<A> to std::vector<B>
template <typename ThriftType, typename LDType>
std::vector<ThriftType> toThrift(const std::vector<LDType>& input) {
  std::vector<ThriftType> output;
  for (const auto& it : input) {
    output.push_back(toThrift<ThriftType>(it));
  }
  return output;
}
}} // namespace facebook::logdevice
