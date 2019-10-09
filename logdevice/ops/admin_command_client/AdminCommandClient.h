/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <map>
#include <vector>

#include <folly/SocketAddress.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

/**
 * @file AdminCommandClient sends an admin command in parallel to several
 * logdeviced instances and waits for all response with a timeout.
 */

namespace facebook { namespace logdevice {

class AdminCommandClient {
 public:
  enum class ConnectionType { UNKNOWN, PLAIN, ENCRYPTED };

  AdminCommandClient(size_t num_threads = 4)
      : executor_(std::make_unique<folly::IOThreadPoolExecutor>(num_threads)) {}

  class Request {
   public:
    Request(folly::SocketAddress addr,
            std::string req,
            ConnectionType conntype = ConnectionType::UNKNOWN)
        : sockaddr(addr), request(req), conntype_(conntype) {}

    folly::SocketAddress sockaddr;
    std::string request;
    ConnectionType conntype_;
  };

  struct Response {
    Response() {}
    Response(std::string response, bool success, std::string failure_reason)
        : response(response),
          success(success),
          failure_reason(failure_reason) {}
    std::string response{""};
    bool success{false};
    std::string failure_reason{""};
  };

  std::vector<Response> send(const std::vector<Request>& r,
                             std::chrono::milliseconds command_timeout,
                             std::chrono::milliseconds connect_timeout =
                                 std::chrono::milliseconds(5000)) const;

  std::vector<folly::SemiFuture<Response>>
  asyncSend(const std::vector<Request>& rr,
            std::chrono::milliseconds command_timeout,
            std::chrono::milliseconds connect_timeout =
                std::chrono::milliseconds(5000)) const;

 private:
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
};

}} // namespace facebook::logdevice
