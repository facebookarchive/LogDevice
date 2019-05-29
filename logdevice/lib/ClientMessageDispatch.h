/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"

/**
 * @file Client-specific dispatcher for message events.  Because it lives in
 * server/ it can call handlers also in server/.
 */

namespace facebook { namespace logdevice {

class ClientMessageDispatch : public MessageDispatch {
 public:
  Message::Disposition
  onReceivedImpl(Message* msg,
                 const Address& from,
                 const PrincipalIdentity& principal) override;
  void onSentImpl(const Message& msg,
                  Status st,
                  const Address& to,
                  const SteadyTimestamp enqueue_time) override;
};
}} // namespace facebook::logdevice
