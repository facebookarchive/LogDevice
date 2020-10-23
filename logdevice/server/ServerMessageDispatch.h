/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

/**
 * @file Server-specific dispatcher for message events.  Because it lives in
 * server/ it can call handlers also in server/.
 */

namespace facebook { namespace logdevice {

class Processor;

class ServerMessageDispatch : public MessageDispatch {
 public:
  explicit ServerMessageDispatch(UpdateableSecurityInfo* security_info,
                                 UpdateableSettings<Settings> settings,
                                 StatsHolder* stats)
      : MessageDispatch(),
        security_info_(security_info),
        settings_(std::move(settings)),
        stats_(stats) {}
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

  /**
   * Method to check if the message type is only authorised to be sent between
   * servers only but is being sent by a non-server (client) entity.
   */
  bool isInternalServerMessageFromNonServerNode(
      const PermissionParams& params,
      const PrincipalIdentity& principal) const;

 private:
  UpdateableSecurityInfo* security_info_;
  UpdateableSettings<Settings> settings_;
  StatsHolder* stats_;
};
}} // namespace facebook::logdevice
