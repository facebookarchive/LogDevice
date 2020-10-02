/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/thrift/SimpleThriftServer.h"

#include "logdevice/common/configuration/UpdateableConfig.h"

using apache::thrift::util::ScopedServerThread;

namespace facebook { namespace logdevice {
SimpleThriftServer::SimpleThriftServer(
    const std::string& name,
    const Sockaddr& listen_addr,
    std::shared_ptr<apache::thrift::ServerInterface> handler,
    RequestExecutor request_executor)
    : LogDeviceThriftServer(name,
                            listen_addr,
                            std::move(handler),
                            request_executor) {}

bool SimpleThriftServer::start() {
  ld_check(!started_);
  ld_info("Starting a listener thread for Thrift server %s", name_.c_str());
  server_thread_ = std::make_unique<ScopedServerThread>();
  server_thread_->start(server_);
  started_.store(true);
  return started_;
}

void SimpleThriftServer::stop() {
  if (started_) {
    ld_info("Waiting for Thrift server %s to stop", name_.c_str());
    server_thread_->stop();
    ld_info("Thrift server %s stopped.", name_.c_str());
    started_.store(false);
  }
}

}} // namespace facebook::logdevice
