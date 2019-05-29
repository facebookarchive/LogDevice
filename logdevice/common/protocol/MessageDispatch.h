/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageTracer.h"

namespace facebook { namespace logdevice {

class MessageTracer;
struct Address;
struct PrincipalIdentity;

/**
 * Interface for dispatching received/sent notifications for messages to their
 * handlers.  Dispatching through virtual methods allows code for server- and
 * client-specific message handlers to live in server/ or lib/ while the
 * Message subclasses themselves are in common/, as is the messaging layer.
 *
 * ServerMessageDispatch and ClientMessageDispatch are the context-specific
 * subclasses.
 */
class MessageDispatch {
 public:
  MessageDispatch();
  /**
   * The handler may claim ownership of `msg' in which case it should return
   * Disposition::KEEP.
   */
  Message::Disposition onReceived(Message* msg,
                                  const Address& from,
                                  const PrincipalIdentity& principal);

  virtual Message::Disposition onReceivedImpl(Message* msg,
                                              const Address& from,
                                              const PrincipalIdentity&) {
    // By default, dispatch to the Message's onReceived() implementation
    return msg->onReceived(from);
  }

  void onSent(const Message& msg,
              Status st,
              const Address& to,
              const SteadyTimestamp enqueue_time);

  virtual void onSentImpl(const Message& msg,
                          Status st,
                          const Address& to,
                          const SteadyTimestamp enqueue_time) {
    // By default, dispatch to the Message's onSent() implementation
    return msg.onSent(st, to, enqueue_time);
  }

  virtual ~MessageDispatch() {}

 protected:
  std::unique_ptr<MessageTracer> message_tracer_;
};

}} // namespace facebook::logdevice
