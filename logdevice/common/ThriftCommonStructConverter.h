/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/if/gen-cpp2/common_types.h"
#include "logdevice/include/NodeLocationScope.h"

namespace facebook { namespace logdevice {

// Templated thrift <-> LogDevice type converter. This interface can be used if
// there is 1:1 mapping between the types.
template <typename ThriftType, typename LDType>
ThriftType toThrift(const LDType& input);

template <>
thrift::LocationScope toThrift(const NodeLocationScope& input);

template <>
thrift::Role toThrift(const configuration::NodeRole& role);

template <>
thrift::ShardID toThrift(const ShardID& shard);

template <>
thrift::Location toThrift(const folly::Optional<NodeLocation>& input);

template <>
thrift::ReplicationProperty toThrift(const ReplicationProperty& replication);

template <typename LDType, typename ThriftType>
LDType toLogDevice(const ThriftType& input);

template <>
NodeLocationScope toLogDevice(const thrift::LocationScope& input);

template <>
ReplicationProperty toLogDevice(const thrift::ReplicationProperty& input);

template <>
configuration::nodes::NodeRole toLogDevice(const thrift::Role& role);

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
