/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/if/gen-cpp2/admin_types.h"
#include "logdevice/admin/maintenance/gen-cpp2/MaintenanceDelta_types.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace maintenance {
using ShardOperationalState = thrift::ShardOperationalState;
using SequencingState = thrift::SequencingState;
using MaintenanceDefinition = thrift::MaintenanceDefinition;
using GroupID = thrift::MaintenanceGroupID;
using MaintenanceStatus = thrift::MaintenanceStatus;
using ShardDataHealth = thrift::ShardDataHealth;
using ClusterMaintenanceState = thrift::ClusterMaintenanceState;

// The user string that identifies the maintenances
// triggered by logdevice internally
constexpr static folly::StringPiece INTERNAL_USER{"_internal_"};

/**
 * An exception type used to encapsulate the various thrift exceptions related
 * to maintenance operations. This is used in cases where we want to return a
 * folly::Expected<T, E> where E is one of the several expected error types
 * according to the thrift spec.
 */
class MaintenanceError : public std::exception {
 public:
  MaintenanceError(Status st) noexcept : st_(st), msg_(error_description(st)) {}
  MaintenanceError(Status st, std::string message) noexcept
      : st_(st), msg_(message) {}
  Status getStatus() const {
    return st_;
  }
  virtual const char* what() const noexcept {
    return msg_.c_str();
  }

  void throwThriftException() const {
    switch (st_) {
      case E::NOTREADY:
        throw thrift::NodeNotReady(msg_);
      case E::NOTSUPPORTED:
        throw thrift::NotSupported(msg_);
      case E::MAINTENANCE_CLASH:
        throw thrift::MaintenanceClash(msg_);
      case E::INVALID_PARAM:
        throw thrift::InvalidRequest(msg_);
      case E::INTERNAL:
        throw thrift::OperationError(msg_);
      case E::NOTFOUND:
        throw thrift::MaintenanceMatchError(msg_);
      default:
        throw *this;
    }
  }

  virtual ~MaintenanceError() {}

 private:
  Status st_;
  std::string msg_;
};

}}} // namespace facebook::logdevice::maintenance
