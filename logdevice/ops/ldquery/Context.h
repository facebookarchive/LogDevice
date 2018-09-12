/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <map>
#include <folly/Memory.h>
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace ldquery {

struct FailedNodeDetails {
  std::string address;
  std::string failure_reason;
  std::string toString() {
    return "Host: " + address + ", Reason: " + failure_reason;
  }
};

struct ActiveQueryMetadata {
  std::map<int, FailedNodeDetails> failures;
  uint64_t contacted_nodes;
  uint64_t latency;
  bool success() {
    return failures.empty();
  }
};

struct Context {
 public:
  /**
   * Creates a client that has LogsConfig fully loaded
   */
  std::shared_ptr<Client> getFullClient() {
    if (!logdeviceFullClient_) {
      logdeviceFullClient_ = createClient(/* withLogsConfig=*/true);
    }
    return logdeviceFullClient_;
  }
  /**
   * Creates a _thin_ client that uses On Demand LogsConfig
   */
  std::shared_ptr<Client> getClient() {
    if (!logdeviceClient_) {
      logdeviceClient_ = createClient(/* withLogsConfig=*/false);
    }
    return logdeviceClient_;
  }
  std::chrono::milliseconds commandTimeout;
  std::string config_path;
  bool use_ssl{false};

  // If true, convert values of some types to human-readable strings,
  // e.g. LSNs like "e5n42" and timestamps like "2017-02-23 12:20:34.137".
  bool pretty_output{false};

  std::unique_ptr<ActiveQueryMetadata> activeQueryMetadata =
      std::make_unique<ActiveQueryMetadata>();

  void resetActiveQuery() {
    activeQueryMetadata.reset(new ActiveQueryMetadata());
  }

 private:
  std::shared_ptr<logdevice::Client> logdeviceClient_;
  std::shared_ptr<logdevice::Client> logdeviceFullClient_;

  std::shared_ptr<logdevice::Client> createClient(bool withLogsConfig) {
    std::shared_ptr<logdevice::Client> client;
    std::unique_ptr<ClientSettings> settings(ClientSettings::create());
    int rv = settings->set(
        "on-demand-logs-config", withLogsConfig ? "false" : "true");
    if (rv != 0) {
      ld_error("Could not create client: %s", error_description(err));
      throw ConstructorFailed();
    }
    client = Client::create(
        "foo", config_path, "none", commandTimeout, std::move(settings));

    if (!client) {
      ld_error("Could not create client: %s", error_description(err));
      throw ConstructorFailed();
    }
    return client;
  }
};
}}} // namespace facebook::logdevice::ldquery
