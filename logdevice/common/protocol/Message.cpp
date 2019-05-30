/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/Message.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

void Message::onSent(Status st,
                     const Address& to,
                     const SteadyTimestamp enqueue_time) const {
  ld_log(st == E::OK ? dbg::Level::SPEW : dbg::Level::DEBUG,
         ": message=%s st=%s to=%s msg_size=%zu enqueue_time=%s",
         messageTypeNames()[type_].c_str(),
         error_name(st),
         to.toString().c_str(),
         // Sender::describeConnection(to).c_str(),
         size(),
         toString(enqueue_time).c_str());
  // Support legacy message handlers.
  onSent(st, to);
}

size_t Message::size(uint16_t proto) const {
  ProtocolWriter writer(type_, nullptr, proto);
  serialize(writer);
  ssize_t size = writer.result();
  ld_check(size >= 0);
  return ProtocolHeader::bytesNeeded(type_, proto) + size;
}

}} // namespace facebook::logdevice
