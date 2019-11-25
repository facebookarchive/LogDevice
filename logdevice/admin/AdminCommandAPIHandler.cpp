/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminCommandAPIHandler.h"

namespace facebook { namespace logdevice {

// check admin.thrift for documentation
folly::SemiFuture<std::unique_ptr<thrift::AdminCommandResponse>>
AdminCommandAPIHandler::semifuture_executeAdminCommand(
    std::unique_ptr<thrift::AdminCommandRequest> request) {
  if (admin_command_handler_ == nullptr) {
    throw thrift::NotSupported("AdminCommands are not supported on this host");
  }

  auto response = std::make_unique<thrift::AdminCommandResponse>();
  response->response = admin_command_handler_(
      request->request, *getConnectionContext()->getPeerAddress());
  return std::move(response);
}
}} // namespace facebook::logdevice
