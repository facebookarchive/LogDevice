/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/admincommands/CommandSelector.h"

namespace facebook { namespace logdevice {

/**
 * @file Knows all admin commands.
 */

class AdminCommandFactory {
 public:
  AdminCommandFactory();

  // Returns matching command and removes from inout_command the prefix that
  // was used to select command.
  // If there's no matching command returns nullptr and writes error to output.
  std::unique_ptr<AdminCommand> get(std::vector<std::string>& inout_args,
                                    struct evbuffer* output);

 protected:
  /**
   * Add a deprecated admin command. The command does nothing but notifying
   * that it is deprecated.
   *
   * @param old_prefix Prefix of the deprecated command
   * @param new_prefix New prefix that should be used instead.
   */
  void deprecated(const char* old_prefx, const char* new_prefix);

  CommandSelector selector_;
};

class TestAdminCommandFactory : public AdminCommandFactory {
 public:
  TestAdminCommandFactory();
};

}} // namespace facebook::logdevice
