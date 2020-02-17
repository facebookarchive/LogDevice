/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/py_extensions/admin_command_client/AdminCommandClient.h"

#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

std::vector<folly::SemiFuture<AdminCommandClient::Response>>
AdminCommandClient::asyncSend(
    const std::vector<AdminCommandClient::Request>& rr,
    std::chrono::milliseconds command_timeout,
    std::chrono::milliseconds connect_timeout) const {
  std::vector<folly::SemiFuture<AdminCommandClient::Response>> futures;
  futures.reserve(rr.size());

  for (auto& r : rr) {
    futures.push_back(
        folly::via(executor_.get())
            .then([executor = executor_.get(),
                   r,
                   connect_timeout,
                   command_timeout](auto&&) mutable {
              auto evb = executor->getEventBase();

              auto command = r.request;
              // Ldquery appends an extra new line at the end of the commands.
              // Strip it for now, and remove it in the next diff.
              if (folly::StringPiece(command).endsWith("\n")) {
                command.resize(command.size() - 1);
              }

              auto transport = apache::thrift::async::TAsyncSocket::newSocket(
                  evb, r.sockaddr);
              auto channel =
                  apache::thrift::HeaderClientChannel::newChannel(transport);
              channel->setTimeout(connect_timeout.count());
              auto client = std::make_unique<thrift::AdminAPIAsyncClient>(
                  std::move(channel));

              apache::thrift::RpcOptions rpc_options;
              rpc_options.setTimeout(command_timeout);

              thrift::AdminCommandRequest req;
              req.request = command;

              return client
                  ->semifuture_executeAdminCommand(rpc_options, std::move(req))
                  .via(evb)
                  .thenTry([](auto response) {
                    if (response.hasException()) {
                      return AdminCommandClient::Response{
                          "", false, response.exception().what().toStdString()};
                    }
                    std::string result = response->response;
                    // Strip the trailing END
                    if (folly::StringPiece(result).endsWith("END\r\n")) {
                      result.resize(result.size() - 5);
                    }
                    return AdminCommandClient::Response{
                        std::move(result), true, ""};
                  });
            }));
  }

  return futures;
}

std::vector<AdminCommandClient::Response>
AdminCommandClient::send(const std::vector<AdminCommandClient::Request>& rr,
                         std::chrono::milliseconds command_timeout,
                         std::chrono::milliseconds connect_timeout) const {
  return collectAllSemiFuture(asyncSend(rr, command_timeout, connect_timeout))
      .via(executor_.get())
      .thenValue(
          [](std::vector<folly::Try<AdminCommandClient::Response>> results) {
            std::vector<AdminCommandClient::Response> ret;
            ret.reserve(results.size());
            for (const auto& result : results) {
              if (result.hasValue()) {
                ret.emplace_back(result.value());
              } else {
                ret.emplace_back(std::string(),
                                 false,
                                 result.exception().what().toStdString());
              }
            }
            return ret;
          })
      .get();
}

}} // namespace facebook::logdevice
