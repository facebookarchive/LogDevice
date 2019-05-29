/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"

/**
 * @file Server-specific dispatcher for message events.  Because it lives in
 * server/ it can call handlers also in server/.
 */

namespace facebook { namespace logdevice {

class Processor;

class ServerMessageDispatch : public MessageDispatch {
 public:
  explicit ServerMessageDispatch(Processor* processor)
      : MessageDispatch(), processor_(processor) {}
  Message::Disposition
  onReceivedImpl(Message* msg,
                 const Address& from,
                 const PrincipalIdentity& principal) override;
  void onSentImpl(const Message& msg,
                  Status st,
                  const Address& to,
                  const SteadyTimestamp enqueue_time) override;

 protected:
  Message::Disposition
  onReceivedHandler(Message* msg,
                    const Address& from,
                    PermissionCheckStatus permission_status) const;

 private:
  Processor* processor_;
};
}} // namespace facebook::logdevice
