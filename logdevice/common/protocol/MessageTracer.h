/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

struct Message;
struct Address;

class MessageTracer {
  enum class Direction { RECEIVED = 0, SENT = 1 };

 public:
  void onReceived(Message* msg, const Address& from);
  void onSent(const Message& msg, Status st, const Address& to);

 private:
  bool shouldTrace(const Message& msg,
                   const Address& peer,
                   Direction direction,
                   Status st);
  void trace(const Message& msg,
             const Address& peer,
             Direction direction,
             Status st);
};

}} // namespace facebook::logdevice
