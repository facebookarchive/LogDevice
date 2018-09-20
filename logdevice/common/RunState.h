/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/checks.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

/**
 * @file A class for identifying the Request/Message type for which we are
 * executing methods/callbacks on a thread.
 */

namespace facebook { namespace logdevice {
// RunState stores the Request/Message type for which we are currently
// executing methods or timer callbacks on a particular thread.
class RunState {
 public:
  enum Type { NONE = 0, REQUEST, MESSAGE };

  RunState() : type_(NONE) {}
  explicit RunState(MessageType t) {
    type_ = MESSAGE;
    subtype_.message = t;
  }

  explicit RunState(RequestType t) {
    type_ = REQUEST;
    subtype_.request = t;
  }

  bool operator==(RunState& b) {
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
    }
    // We should never end up here,
    ld_check(false);
    return false;
  }

  bool operator!=(RunState& b) {
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
        res += "Message: ";
        res += messageTypeNames[subtype_.message].c_str();
        break;
    }
    res += "]";
    return res;
  }

  Type type_;
  union {
    RequestType request;
    MessageType message;
  } subtype_;
};
}} // namespace facebook::logdevice
