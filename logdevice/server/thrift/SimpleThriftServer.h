/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/server/thrift/LogDeviceThriftServer.h"

namespace apache { namespace thrift {
class ThriftServer;
class ServerInterface;

namespace util {
class ScopedServerThread;
}
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

/**
 * An basic implementation of Thrift server that uses cpp2's ThriftServer. This
 * class is designed to be a base for specific OSS-compatible Thrift servers.
 */
class SimpleThriftServer : public LogDeviceThriftServer {
 public:
  SimpleThriftServer(const std::string& name,
                     const Sockaddr& listen_addr,
                     std::shared_ptr<apache::thrift::ServerInterface> handler);

  bool start() override;

  void stop() override;

 private:
  std::unique_ptr<apache::thrift::util::ScopedServerThread> server_thread_;
  std::atomic_bool started_{false};
};

}} // namespace facebook::logdevice
