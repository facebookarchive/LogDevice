/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/tools/ldadmin/LDAdmin.h"

#include <iostream>

#include <boost/program_options.hpp>
#include <folly/io/async/AsyncSocket.h>
#include <thrift/lib/cpp/async/TAsyncSSLSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"

using namespace facebook::logdevice;

/**
 * A tool to execute admin commands on hosts via the thrift based admin port.
 */

bool LDAdmin::parse_command_line(int argc, const char* argv[]) {
  try {
    boost::program_options::parsed_options parsed =
        boost::program_options::command_line_parser(argc, argv)
            .options(opt_dscr_)
            .positional(positional_)
            .run();
    boost::program_options::store(parsed, vm_);
    boost::program_options::notify(vm_);

    if (vm_.count("help")) {
      print_usage();
      exit(0);
    }
    if (vm_.count("command") == 0) {
      print_usage();
      return false;
    }

  } catch (const boost::program_options::error& ex) {
    print_usage();
    std::cerr << argv[0] << ": " << ex.what() << std::endl;
    return false;
  }
  return true;
}

bool LDAdmin::run() {
  folly::EventBase eb;

  folly::SocketAddress address("localhost", 6440, true);
  for (auto it : transport_options_) {
    if (vm_.count(it.first)) {
      try {
        address = it.second.get().getAddress();
      } catch (const std::exception& ex) {
        print_usage();
        std::cerr << ex.what() << std::endl;
        return false;
      }
      break;
    }
  }

  auto timeout_ms = timeout_sec_ * 1000;

  std::shared_ptr<folly::SSLContext> ssl_context{nullptr};
  std::shared_ptr<folly::AsyncSocket> transport;
  if (!ssl_) {
    transport = folly::AsyncSocket::newSocket(&eb);
  } else {
    ssl_context = std::make_shared<folly::SSLContext>();
    transport =
        apache::thrift::async::TAsyncSSLSocket::newSocket(ssl_context, &eb);
  }
  transport->connect(nullptr, address, timeout_ms);

  auto channel =
      apache::thrift::HeaderClientChannel::newChannel(std::move(transport));
  if (timeout_ms > 0) {
    channel->setTimeout(timeout_ms);
  }
  auto client =
      std::make_unique<thrift::AdminAPIAsyncClient>(std::move(channel));

  thrift::AdminCommandRequest req;
  *req.request_ref() = folly::join(" ", commands_);

  apache::thrift::RpcOptions rpc_options;
  rpc_options.setTimeout(std::chrono::seconds(timeout_sec_));

  thrift::AdminCommandResponse resp;
  try {
    client->sync_executeAdminCommand(rpc_options, resp, std::move(req));
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return false;
  }
  std::cout << *resp.response_ref() << std::endl;
  return true;
}

void LDAdmin::print_usage() {
  std::cerr << "[Description] A tool to execute admin commands on hosts "
               "via the thrift based admin port"
            << std::endl
            << "[Usage] ldadmin <options> command" << std::endl
            << opt_dscr_ << std::endl;
}
