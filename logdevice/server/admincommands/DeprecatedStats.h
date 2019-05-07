/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class DeprecatedStats : public AdminCommand {
 public:
  std::string old_;
  std::string new_;

  DeprecatedStats(const std::string& old, const std::string& n)
      : old_(old), new_(n) {}

  std::string getUsage() override {
    return "`" + old_ + "` is deprecated, use `" + new_ + "`";
  }

  void run() override {
    out_.printf("NOTE %s\r\n", getUsage().c_str());
  }
};

}}} // namespace facebook::logdevice::commands
