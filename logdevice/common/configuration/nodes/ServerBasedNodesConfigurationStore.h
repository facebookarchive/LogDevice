/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/NodesConfigurationPoller.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

namespace facebook { namespace logdevice {

class NodesConfigurationPoller;
class Processor;
struct Settings;

namespace configuration { namespace nodes {

// An implementation of a NodesConfigurationStore where the NodesConfiguration
// is fetched from random servers in the cluster.
class ServerBasedNodesConfigurationStore : public NodesConfigurationStore {
 public:
  ~ServerBasedNodesConfigurationStore() override = default;

  // Enqueues a ConfigurationFetchRequest request of type NodesConfiguration on
  // the same worker, with a request callback invoking the passed `cb`. The
  // config fetch request is sent to a random serer in the cluster.
  //
  // Must be called from a worker thread.
  void getConfig(value_callback_t cb,
                 folly::Optional<version_t> base_version = {}) const override;

  // We can't support a sync config fetch easily. The `getConfig` methods posts
  // a ConfigurationFetchRequest which may end up scheduled on this worker
  // causing a deadlock.
  Status
  getConfigSync(std::string* value_out,
                folly::Optional<version_t> base_version = {}) const override;

  // This NodesConfigurationStore doesn't support linearizable read.
  // It will unconditionally throw a runtime_error.
  void getLatestConfig(value_callback_t cb) const override;

  // This is a read only NodesConfigStore. Updates are not supported.
  // It will unconditionally throw a runtime_error.
  void updateConfig(std::string value,
                    folly::Optional<version_t> base_version,
                    write_callback_t cb = {}) override;

  // This is a read-only NodesConfigStore. Updates are not supported.
  // It will unconditionally throw a runtime_error.
  Status updateConfigSync(std::string value,
                          folly::Optional<version_t> base_version,
                          version_t* version_out = nullptr,
                          std::string* value_out = nullptr) override;

  void shutdown() override;

 private:
  std::atomic<bool> shutdown_signaled_{false};

  // helper utility to generate a polling option based on processor
  // settings and existing nodes configuration
  static NodesConfigurationPoller::Poller::Options genPollerOptions(
      NodesConfigurationPoller::Poller::Mode mode,
      const Settings& settings,
      const configuration::nodes::NodesConfiguration& nodes_configuration);
};
}} // namespace configuration::nodes
}} // namespace facebook::logdevice
