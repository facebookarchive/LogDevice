/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ClientMessageDispatch.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/lib/NODE_STATS_REPLY_onReceived.h"
#include "logdevice/lib/NODE_STATS_onSent.h"

namespace facebook { namespace logdevice {

Message::Disposition
ClientMessageDispatch::onReceivedImpl(Message* msg,
                                      const Address& from,
                                      const PrincipalIdentity&) {
  switch (msg->type_) {
    case MessageType::NODE_STATS_REPLY:
      return NODE_STATS_REPLY_onReceived(
          checked_downcast<NODE_STATS_REPLY_Message*>(msg), from);
    case MessageType::STORED:
      return checked_downcast<STORED_Message*>(msg)->onReceivedCommon(from);
    case MessageType::CHECK_NODE_HEALTH:
    case MessageType::CHECK_SEAL:
    case MessageType::CLEAN:
    case MessageType::DELETE:
    case MessageType::FINDKEY:
    case MessageType::GET_EPOCH_RECOVERY_METADATA:
    case MessageType::GET_EPOCH_RECOVERY_METADATA_REPLY:
    case MessageType::GET_HEAD_ATTRIBUTES:
    case MessageType::NODE_STATS:
    case MessageType::NODE_STATS_AGGREGATE:
    case MessageType::NODE_STATS_AGGREGATE_REPLY:
    case MessageType::IS_LOG_EMPTY:
    case MessageType::RELEASE:
    case MessageType::SEAL:
    case MessageType::START:
    case MessageType::STOP:
    case MessageType::STORE:
    case MessageType::TRIM:
    case MessageType::WINDOW:
      RATELIMIT_ERROR(
          std::chrono::seconds(60),
          1,
          "ClientMessageDispatch::onReceived() called with %s message"
          "which is supposed to be server-only!",
          messageTypeNames()[msg->type_].c_str());
      err = E::PROTO;
      return Message::Disposition::ERROR;

    default:
      // By default, call the Message's onReceived() implementation (for
      // messages whose handler lives in common/ with the Message subclass)
      return msg->onReceived(from);
  }
}

void ClientMessageDispatch::onSentImpl(const Message& msg,
                                       Status st,
                                       const Address& to,
                                       const SteadyTimestamp /*enqueue_time*/) {
  switch (msg.type_) {
    case MessageType::NODE_STATS:
      NODE_STATS_onSent(
          checked_downcast<const NODE_STATS_Message&>(msg), st, to);
      return;

    case MessageType::STORE:
      checked_downcast<const STORE_Message&>(msg).onSentCommon(st, to);
      return;

    case MessageType::GET_EPOCH_RECOVERY_METADATA:
    case MessageType::NODE_STATS_AGGREGATE:
    case MessageType::NODE_STATS_AGGREGATE_REPLY:
    case MessageType::NODE_STATS_REPLY:
    case MessageType::RECORD:
    case MessageType::SHARD_STATUS_UPDATE:
      RATELIMIT_ERROR(std::chrono::seconds(60),
                      1,
                      "ClientMessageDispatch::onSent() called with %s message"
                      "which is supposed to be server-only!",
                      messageTypeNames()[msg.type_].c_str());
      ld_check(false);
      return;

    default:
      // By default, call the Message's onSent() implementation (for messages
      // whose handler lives in common/ with the Message subclass)
      return msg.onSent(st, to);
  }
}
}} // namespace facebook::logdevice
