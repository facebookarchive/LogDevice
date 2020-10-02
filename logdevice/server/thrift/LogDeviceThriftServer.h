/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RequestExecutor.h"
#include "logdevice/common/Sockaddr.h"

namespace apache { namespace thrift {
class ThriftServer;
class ServerInterface;
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

/**
 * A base implementation for Thrift servers. The specific implementation is
 * expected to be defined as plug-ins.
 */
class LogDeviceThriftServer {
 public:
  /**
   * Creates a new Thrift server but returned instances will not accept
   * connections until start() is called.
   *
   * @param name             Used mostly for logging purposes to distinguish
   *                         multiple servers running in the same process.
   * @param listen_addr      Address on which this server will listen for
   *                         incoming request.
   * @param handler          Service-specific Thrift handler.
   * @param request_executor API to post requests to workers.
   */
  LogDeviceThriftServer(
      const std::string& name,
      const Sockaddr& listen_addr,
      std::shared_ptr<apache::thrift::ServerInterface> handler,
      RequestExecutor request_executor);
  /**
   * Starts listen and accept incoming requests.
   *
   * @return true iff server has successfully started
   */
  virtual bool start() = 0;

  /**
   * Stops server and prevent any new requests from being accepted.
   */
  virtual void stop() = 0;

  virtual ~LogDeviceThriftServer() {}

 protected:
  const std::string name_;
  std::shared_ptr<apache::thrift::ServerInterface> handler_;
  std::shared_ptr<apache::thrift::ThriftServer> server_;
};
}} // namespace facebook::logdevice
