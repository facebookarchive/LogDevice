/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminServer.h"

#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBaseManager.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "common/fb303/if/gen-cpp2/FacebookServiceAsyncClient.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Err.h"
#include "logdevice/test/utils/util.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

void AdminServer::start() {
  folly::Subprocess::Options options;
  options.parentDeathSignal(SIGKILL); // kill children if test process dies

  // Make any tcp port that we reserved available to the admin server binary.
  port_owners_.clear();

  // Without this, calling start() twice would causes a crash because
  // folly::Subprocess::~Subprocess asserts that the process is not running.
  if (isRunning()) {
    // The node is already started.
    return;
  }

  std::vector<std::string> argv = {admin_server_binary_};
  for (const auto& pair : cmd_args_) {
    argv.emplace_back(pair.first);
    if (pair.second.has_value()) {
      argv.emplace_back(pair.second.value());
    }
  }

  ld_info("Admin Server Command Line: %s", folly::join(" ", argv).c_str());

  ld_info("Starting Admin Server on %s", address_.toString().c_str());
  admin_server_.reset(new folly::Subprocess(argv, options));
  ld_info("Started Admin Server");

  stopped_ = false;
}

bool AdminServer::isRunning() const {
  return admin_server_ && admin_server_->returnCode().running() &&
      admin_server_->poll().running();
}

void AdminServer::signal(int sig) {
  admin_server_->sendSignal(sig);
}

std::string AdminServer::getLogPath() const {
  return data_path_ + "/log";
}

void AdminServer::kill() {
  if (isRunning()) {
    ld_info(
        "Killing the standalone admin server %s", address_.toString().c_str());
    admin_server_->kill();
    admin_server_->wait();
    ld_info("Killed the standalone admin server on %s",
            address_.toString().c_str());
    stopped_ = true;
  }
  admin_server_.reset();
}

folly::SocketAddress AdminServer::getAdminAddress() const {
  return address_.getSocketAddress();
}

std::unique_ptr<thrift::AdminAPIAsyncClient>
AdminServer::createAdminClient(uint32_t timeout_ms) const {
  auto transport = folly::AsyncSocket::newSocket(
      folly::EventBaseManager::get()->getEventBase(),
      address_.getSocketAddress());
  auto channel =
      apache::thrift::HeaderClientChannel::newChannel(std::move(transport));
  channel->setTimeout(timeout_ms);
  if (!channel->good()) {
    ld_debug("Couldn't create a thrift client for the Admin server at "
             "%s. It might mean that the server is down.",
             address_.toString().c_str());
    err = E::FAILED;
    return nullptr;
  } else {
    return std::make_unique<thrift::AdminAPIAsyncClient>(std::move(channel));
  }
}

int AdminServer::waitUntilStarted(
    std::chrono::steady_clock::time_point deadline) {
  ld_info("Waiting for Admin Server to start");
  bool died = false;
  // If we wait for a long time, dump the server's error log file to stderr to
  // help debug.
  auto t1 = std::chrono::steady_clock::now();
  bool should_dump_log = dbg::currentLevel >= dbg::Level::WARNING;
  auto started = [this, &died, &should_dump_log, t1]() {
    // No need to wait if the process is not even running
    died = !isRunning();
    if (died) {
      return true;
    }
    auto admin_client = createAdminClient(500);
    if (admin_client != nullptr) {
      try {
        auto status = admin_client->sync_getStatus();
        return (status == facebook::fb303::cpp2::fb_status::ALIVE ||
                status == facebook::fb303::cpp2::fb_status::STARTING);
      } catch (const std::exception& ex) {
        ld_info("Admin Server didn't start yet, will continue waiting...");
      }
    }
    auto t2 = std::chrono::steady_clock::now();
    if (should_dump_log && t2 - t1 > DEFAULT_TEST_TIMEOUT / 3) {
      ld_warning(
          "Admin Server process is taking a long time to start responding to "
          "the 'FB303' API.  Dumping its error log to help debug issues:");
      int rv = dump_file_to_stderr(getLogPath().c_str());
      if (rv == 0) {
        should_dump_log = false;
      }
    }
    return false;
  };
  int rv =
      wait_until(("admin server at " + address_.toString() + " starts").c_str(),
                 started,
                 deadline);
  if (died) {
    rv = -1;
  }
  if (rv != 0) {
    ld_info("Admin Server failed to start. Dumping its error log");
    dump_file_to_stderr(getLogPath().c_str());
  } else {
    ld_info("Admin Server started");
  }
  return rv;
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
