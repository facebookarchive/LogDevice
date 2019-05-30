/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RequestType.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

/**
 * @file A class for identifying the Request/Message type for which we are
 * executing methods/callbacks on a thread.
 */

namespace facebook { namespace logdevice {
// Identifies the type of Request/Message. Used for tracking what each worker
// thread is doing at any particular moment, and reporting when a piece of
// work takes longer than expected.
// This structure should be kept lightweight - it's copied multiple times per
// request execution - but contain enough information to locate the relevant
// piece of code with reasonable precision.
class RunContext {
 public:
  enum Type { NONE = 0, REQUEST, MESSAGE, STORAGE_TASK_RESPONSE };

  RunContext() : type_(NONE) {}
  explicit RunContext(MessageType t) {
    type_ = MESSAGE;
    subtype_.message = t;
  }

  explicit RunContext(RequestType t) {
    type_ = REQUEST;
    subtype_.request = t;
  }

  explicit RunContext(StorageTaskType t) {
    type_ = STORAGE_TASK_RESPONSE;
    subtype_.storage_task = t;
  }

  bool operator==(RunContext& b) {
    if (type_ != b.type_) {
      return false;
    }
    switch (type_) {
      case NONE:
        return true;
      case REQUEST:
        return subtype_.request == b.subtype_.request;
      case MESSAGE:
        return subtype_.message == b.subtype_.message;
      case STORAGE_TASK_RESPONSE:
        return subtype_.storage_task == b.subtype_.storage_task;
    }
    // We should never end up here,
    ld_check(false);
    return false;
  }

  bool operator!=(RunContext& b) {
    return !operator==(b);
  }

  std::string describe() {
    std::string res("[");
    switch (type_) {
      case NONE:
        res += "None";
        break;
      case REQUEST:
        res += "Request: ";
        res += requestTypeNames[subtype_.request];
        break;
      case MESSAGE:
        res += "Message sent/received: ";
        res += messageTypeNames()[subtype_.message].c_str();
        break;
      case STORAGE_TASK_RESPONSE:
        res += "Storage task response: ";
        res += storageTaskTypeNames[subtype_.storage_task].c_str();
        break;
    }
    res += "]";
    return res;
  }

  Type type_;
  union {
    RequestType request;
    MessageType message;
    StorageTaskType storage_task;
  } subtype_;
};
}} // namespace facebook::logdevice
