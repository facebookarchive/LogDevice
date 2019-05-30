/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/MessageTracer.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {

bool MessageTracer::shouldTrace(const Message& msg,
                                const Address& peer,
                                Direction,
                                Status) {
  const auto& settings = Worker::settings();
  const auto& types = settings.message_tracing_types;
  const auto& peers = settings.message_tracing_peers;

  if (types.empty() && peers.empty()) {
    // Nothing is configured
    return false;
  }
  if (!types.empty() && (types.count(msg.type_) == 0)) {
    // doesn't pass the message type filter
    return false;
  }
  if (peers.empty()) {
    // no peer filter
    return true;
  }
  Sockaddr peer_address = Worker::onThisThread()->sender().getSockaddr(peer);
  if (peers.isPresent(peer_address)) {
    // passed the host filter
    return true;
  }

  // Doesn't pass the host filter
  return false;
}

void MessageTracer::trace(const Message& msg,
                          const Address& peer,
                          Direction direction,
                          Status st) {
  auto w = Worker::onThisThread();
  const char* direction_str =
      (direction == Direction::RECEIVED ? "Received" : "Sent");
  // there is no status for incoming messages, so only printing it for
  // outgoing ones
  std::string result_str(direction == Direction::SENT
                             ? std::string(", message send result: ") +
                                 error_name(st)
                             : "");

  auto message_dbg_info = msg.getDebugInfo();
  std::string debug_str;
  for (const auto& kv : message_dbg_info) {
    debug_str += ", " + kv.first + ": " + toString(kv.second);
  }

  ld_log(w->settings().message_tracing_log_level,
         "%s message of type %s, peer: %s, size in latest protocol "
         "version: %lu%s%s",
         direction_str,
         messageTypeNames()[msg.type_].c_str(),
         w->sender().describeConnection(peer).c_str(),
         msg.size(),
         result_str.c_str(),
         debug_str.c_str());
}

void MessageTracer::onReceived(Message* msg, const Address& from) {
  ld_check(msg != nullptr);
  if (shouldTrace(*msg, from, Direction::RECEIVED, E::OK)) {
    trace(*msg, from, Direction::RECEIVED, E::OK);
  }
}

void MessageTracer::onSent(const Message& msg, Status st, const Address& to) {
  if (shouldTrace(msg, to, Direction::SENT, st)) {
    trace(msg, to, Direction::SENT, st);
  }
}

}} // namespace facebook::logdevice
