/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/Conv.h"

using facebook::logdevice::membership::MetaDataStorageState;
using TMetaDataStorageState =
    facebook::logdevice::membership::thrift::MetaDataStorageState;
using facebook::logdevice::membership::StorageState;
using TStorageState = facebook::logdevice::membership::thrift::StorageState;

namespace facebook { namespace logdevice {


// DEPRECATED
template <>
thrift::ShardStorageState
toThrift(const configuration::StorageState& storage_state) {
  switch (storage_state) {
    case configuration::StorageState::DISABLED:
      return thrift::ShardStorageState::DISABLED;
    case configuration::StorageState::READ_ONLY:
      return thrift::ShardStorageState::READ_ONLY;
    case configuration::StorageState::READ_WRITE:
      return thrift::ShardStorageState::READ_WRITE;
  }
  ld_check(false);
  return thrift::ShardStorageState::DISABLED;
}

// DEPRECATED
template <>
configuration::StorageState
toLogDevice(const thrift::ShardStorageState& storage_state) {
  switch (storage_state) {
    case thrift::ShardStorageState::DISABLED:
      return configuration::StorageState::DISABLED;
    case thrift::ShardStorageState::DATA_MIGRATION:
    case thrift::ShardStorageState::READ_ONLY:
      return configuration::StorageState::READ_ONLY;
    case thrift::ShardStorageState::READ_WRITE:
      return configuration::StorageState::READ_WRITE;
  }
  ld_check(false);
  return configuration::StorageState::DISABLED;
}

// From Membership.thrift
template <>
TStorageState toThrift(const StorageState& storage_state) {
  return static_cast<TStorageState>(storage_state);
}

// From Membership.thrift
template <>
StorageState toLogDevice(const TStorageState& storage_state) {
  return static_cast<StorageState>(storage_state);
}

// From Membership.thrift
template <>
TMetaDataStorageState toThrift(const MetaDataStorageState& storage_state) {
  return static_cast<TMetaDataStorageState>(storage_state);
}

// From Membership.thrift
template <>
MetaDataStorageState toLogDevice(const TMetaDataStorageState& storage_state) {
  return static_cast<MetaDataStorageState>(storage_state);
}

template <>
thrift::ServiceState toThrift(const ClusterStateNodeState& input) {
  switch (input) {
    case ClusterStateNodeState::DEAD:
      return thrift::ServiceState::DEAD;
    case ClusterStateNodeState::FULLY_STARTED:
      return thrift::ServiceState::ALIVE;
    case ClusterStateNodeState::STARTING:
      return thrift::ServiceState::STARTING_UP;
    case ClusterStateNodeState::FAILING_OVER:
      return thrift::ServiceState::SHUTTING_DOWN;
  }
  return thrift::ServiceState::UNKNOWN;
}

template <>
thrift::ServiceHealthStatus toThrift(const NodeHealthStatus& input) {
  switch (input) {
    case NodeHealthStatus::UNHEALTHY:
      return thrift::ServiceHealthStatus::UNHEALTHY;
    case NodeHealthStatus::HEALTHY:
      return thrift::ServiceHealthStatus::HEALTHY;
    case NodeHealthStatus::OVERLOADED:
      return thrift::ServiceHealthStatus::OVERLOADED;
    case NodeHealthStatus::UNDEFINED:
      return thrift::ServiceHealthStatus::UNDEFINED;
  }
  return thrift::ServiceHealthStatus::UNKNOWN;
}

thrift::ShardDataHealth toShardDataHealth(AuthoritativeStatus auth_status,
                                          bool has_dirty_ranges) {
  switch (auth_status) {
    case AuthoritativeStatus::FULLY_AUTHORITATIVE:
      return has_dirty_ranges ? thrift::ShardDataHealth::LOST_REGIONS
                              : thrift::ShardDataHealth::HEALTHY;
    case AuthoritativeStatus::UNDERREPLICATION:
      return thrift::ShardDataHealth::LOST_ALL;
    case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
      return thrift::ShardDataHealth::EMPTY;
    case AuthoritativeStatus::UNAVAILABLE:
      return thrift::ShardDataHealth::UNAVAILABLE;
    default:
      return thrift::ShardDataHealth::UNKNOWN;
  }
}

}} // namespace facebook::logdevice
