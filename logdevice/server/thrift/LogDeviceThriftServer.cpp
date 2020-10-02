/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/thrift/LogDeviceThriftServer.h"

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "logdevice/common/debug.h"
#include "logdevice/server/thrift/LogDeviceThreadManager.h"

namespace facebook { namespace logdevice {

LogDeviceThriftServer::LogDeviceThriftServer(
    const std::string& name,
    const Sockaddr& listen_addr,
    std::shared_ptr<apache::thrift::ServerInterface> handler,
    RequestExecutor request_executor)
    : name_(name), handler_(handler) {
  ld_check(listen_addr.valid());
  server_ = std::make_shared<apache::thrift::ThriftServer>();
  server_->setInterface(handler_);

  auto thread_manager =
      std::make_shared<LogDeviceThreadManager>(request_executor);
  server_->setThreadManager(thread_manager);

  ld_check(listen_addr.valid());
  if (listen_addr.isUnixAddress()) {
    ld_info("Using unix socket for Thrift server %s: %s",
            name_.c_str(),
            listen_addr.toString().c_str());
    // We must unlink the previous socket if exists otherwise we will get
    // "Address already in use" error.
    unlink(listen_addr.toString().c_str());
    server_->setAddress(listen_addr.getSocketAddress());
  } else {
    ld_info("Thrift server %s will listen on port %i",
            name_.c_str(),
            listen_addr.port());
    server_->setPort(listen_addr.port());
  }
}

}} // namespace facebook::logdevice
