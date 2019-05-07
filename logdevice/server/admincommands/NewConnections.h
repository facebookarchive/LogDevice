/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

/**
 * For tests: Any new ConnectionListener connections will be closed immediately.
 * Gossip and admin connections will be accepted though.
 *
 */
class NewConnections : public AdminCommand {
 private:
  bool accept_;

 public:
  explicit NewConnections(
      bool accept,
      RestrictionLevel restrictionLevel = RestrictionLevel::UNRESTRICTED)
      : AdminCommand(restrictionLevel), accept_(accept) {}

  std::string getUsage() override {
    return "newconnections accept|reject";
  }

  void run() override {
    server_->acceptNewConnections(accept_);
  }
};
}}} // namespace facebook::logdevice::commands
