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

#include <folly/Memory.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice { namespace ldquery {

struct FailedNodeDetails {
  std::string address;
  std::string failure_reason;
  std::string toString() {
    return folly::to<std::string>(
        "Host: ", address, ", Reason: ", failure_reason);
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
  folly::Optional<std::chrono::milliseconds> commandTimeout;
  std::string config_path;
  bool use_ssl{false};

  // If true, convert values of some types to human-readable strings,
  // e.g. LSNs like "e5n42" and timestamps like "2017-02-23 12:20:34.137".
  bool pretty_output{false};

  ActiveQueryMetadata activeQueryMetadata;

  void resetActiveQuery() {
    activeQueryMetadata = ActiveQueryMetadata();
  }

 private:
  std::shared_ptr<logdevice::Client> logdeviceClient_;
  std::shared_ptr<logdevice::Client> logdeviceFullClient_;

  std::shared_ptr<logdevice::Client> createClient(bool withLogsConfig) {
    ClientFactory factory;

    if (commandTimeout.hasValue()) {
      factory.setTimeout(commandTimeout.value());
    }
    std::shared_ptr<logdevice::Client> client =
        factory
            .setSetting(
                "on-demand-logs-config", withLogsConfig ? "false" : "true")
            .create(config_path);

    if (!client) {
      ld_error("Could not create client: %s", error_description(err));
      throw ConstructorFailed();
    }
    return client;
  }
};
}}} // namespace facebook::logdevice::ldquery
