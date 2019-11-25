/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/admin/AdminCommandHandler.h"

namespace facebook { namespace logdevice {

class AdminCommandAPIHandler : public virtual AdminAPIHandlerBase {
 public:
  // check admin.thrift for documentation
  virtual folly::SemiFuture<std::unique_ptr<thrift::AdminCommandResponse>>
  semifuture_executeAdminCommand(
      std::unique_ptr<thrift::AdminCommandRequest> request) override;

  virtual void setAdminCommandHandler(AdminCommandHandler handler) {
    admin_command_handler_ = std::move(handler);
  }

 private:
  AdminCommandHandler admin_command_handler_{nullptr};
};
}} // namespace facebook::logdevice
