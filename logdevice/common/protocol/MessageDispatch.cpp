/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MessageDispatch.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageTracer.h"

namespace facebook { namespace logdevice {

MessageDispatch::MessageDispatch() : message_tracer_(new MessageTracer()) {}

Message::Disposition
MessageDispatch::onReceived(Message* msg,
                            const Address& from,
                            const PrincipalIdentity& principal) {
  message_tracer_->onReceived(msg, from);
  return onReceivedImpl(msg, from, principal);
}

void MessageDispatch::onSent(const Message& msg,
                             Status st,
                             const Address& to,
                             const SteadyTimestamp enqueue_time) {
  message_tracer_->onSent(msg, st, to);
  onSentImpl(msg, st, to, enqueue_time);
}

}} // namespace facebook::logdevice
