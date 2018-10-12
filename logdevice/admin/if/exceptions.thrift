/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "common.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice

// The node you are communicating with is not ready to respond yet.
exception NodeNotReady {
  1: string message,
}

// The server has an older version than expected
exception StaleVersion {
  1: string message,
  2: common.unsigned64 server_version,
}

// The operation is not supported
exception NotSupported {
  1: string message,
}

// An operation that failed for unexpected reasons
exception OperationError {
  1: string message,
  2: optional i32 error_code, // maps to E
}

// The request contains invalid parameters
exception InvalidRequest {
  1: string message,
}
