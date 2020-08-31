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
#include "logdevice/common/NodeHealthStatus.h"
#include "logdevice/common/ThriftCommonStructConverter.h"
#include "logdevice/common/membership/StorageState.h"

namespace facebook { namespace logdevice {

inline uint64_t to_unsigned(thrift::unsigned64 value) {
  return static_cast<uint64_t>(value);
}

/** Specializations **/

/// DEPRECATED
template <>
thrift::ShardStorageState toThrift(const configuration::StorageState& input);

template <>
membership::thrift::StorageState
toThrift(const membership::StorageState& input);

template <>
membership::thrift::MetaDataStorageState
toThrift(const membership::MetaDataStorageState& input);

template <>
thrift::ServiceState toThrift(const ClusterStateNodeState& input);

template <>
thrift::ServiceHealthStatus toThrift(const NodeHealthStatus& input);

/** Specializations **/

// DEPRECATED
template <>
configuration::StorageState toLogDevice(const thrift::ShardStorageState& input);

template <>
membership::StorageState
toLogDevice(const membership::thrift::StorageState& input);

template <>
membership::MetaDataStorageState
toLogDevice(const membership::thrift::MetaDataStorageState& input);

thrift::ShardDataHealth toShardDataHealth(AuthoritativeStatus auth_status,
                                          bool has_dirty_ranges);

}} // namespace facebook::logdevice
