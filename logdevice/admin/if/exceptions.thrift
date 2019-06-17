/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.Exceptions

/**
 * The node you are communicating with is not ready to respond yet.
 */
exception NodeNotReady {
  1: string message,
// This will correctly set what() and give us an easy ctor in cpp2
} (message = "message")

/**
 * The server has an older version than expected
 */
exception StaleVersion {
  1: string message,
  2: common.unsigned64 server_version,
} (message = "message")

/**
 * The operation is not supported
 */
exception NotSupported {
  1: string message,
} (message = "message")

/**
 * An operation that failed for unexpected reasons
 */
exception OperationError {
  1: string message,
  2: optional i32 error_code, // maps to E
} (message = "message")

/**
 * The request contains invalid parameters
 */
exception InvalidRequest {
  1: string message,
} (message = "message")

/**
 * There is maintenance already set by the same user for different targets
 */
exception MaintenanceClash {
  1: string message,
} (message = "message")


/**
 * The system couldn't match the maintenance in the definition with the internal
 * state
 */
exception MaintenanceMatchError {
  1: string message,
} (message = "message")

/**
 * Nodes Configuration Manager rejected the proposed update.
 */
exception NodesConfigurationManagerError {
  1: string message,
  2: optional i32 error_code, // maps to E
} (message = "message")
