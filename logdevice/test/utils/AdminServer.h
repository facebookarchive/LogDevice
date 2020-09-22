/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <vector>

#include <folly/Subprocess.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPIAsyncClient.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/test/utils/ParamMaps.h"
#include "logdevice/test/utils/port_selection.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

/**
 * RAII-style container for the standalone admin server.
 */
class AdminServer {
 public:
  std::unique_ptr<folly::Subprocess> admin_server_;
  std::string data_path_;
  std::string config_path_;
  std::string admin_server_binary_;
  // override cluster params for this particular server
  ParamMap cmd_args_;
  Sockaddr address_;
  // Stopped until start() is called
  bool stopped_ = true;
  // Used to lock the port(s) before starting
  std::vector<detail::PortOwner> port_owners_;

  AdminServer() {}
  ~AdminServer() {
    kill();
  }

  void signal(int sig);
  std::string getLogPath() const;
  /**
   * @return true if the server is running.
   */
  bool isRunning() const;
  void kill();
  /**
   * Starts the admin server if not started already (without waiting for it to
   * become ready).
   */
  void start();
  /**
   * Returns the admin API address for this node
   */
  folly::SocketAddress getAdminAddress() const;
  /**
   * Creates a thrift client for the Admin API.
   */
  std::unique_ptr<thrift::AdminAPIAsyncClient>
  createAdminClient(uint32_t timeout_ms = 5000) const;

  AdminServer& setParam(const std::string& key, const std::string& value) {
    cmd_args_[key] = value;
    return *this;
  }

  /**
   * Waits for the server to start accepting connections and can respond to
   * FB303 requests. Note that this doesn't mean that the server has been fully
   * initialised.
   * @return 0 if started, -1 if the call timed out.
   */
  int waitUntilStarted(std::chrono::steady_clock::time_point deadline =
                           std::chrono::steady_clock::time_point::max());

  /**
   * Waits until the ClusterMaintenanceStateMachine is fully loaded on that
   * machine.
   */
  int waitUntilFullyLoaded();

  std::vector<std::string> commandLine() const;
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
