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
#include <folly/executors/CPUThreadPoolExecutor.h>
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
      : executor_(), num_threads_(num_threads) {}

  class RequestResponse {
   public:
    RequestResponse(folly::SocketAddress addr,
                    std::string req,
                    ConnectionType conntype = ConnectionType::UNKNOWN)
        : sockaddr(addr), request(req), conntype_(conntype) {}

    folly::SocketAddress sockaddr;
    std::string request;
    std::string response;
    bool success{false};
    std::string failure_reason;
    ConnectionType conntype_;
  };

  typedef std::vector<AdminCommandClient::RequestResponse> RequestResponses;

  void send(RequestResponses& rr,
            std::chrono::milliseconds command_timeout,
            std::chrono::milliseconds connect_timeout =
                std::chrono::milliseconds(5000));

  std::vector<folly::SemiFuture<RequestResponse*>>
  semifuture_send(std::vector<RequestResponse>& rr,
                  std::chrono::milliseconds command_timeout,
                  std::chrono::milliseconds connect_timeout =
                      std::chrono::milliseconds(5000));

  void terminate() {
    executor_.reset();
  }

 private:
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  size_t num_threads_{4};
};

}} // namespace facebook::logdevice
