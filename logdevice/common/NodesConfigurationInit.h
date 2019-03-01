/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/futures/Future.h>

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

class Processor;

/**
 * NodesConfigurationInit is the class responsible for fetching the very first
 * NodesConfiguration that the client will use to get bootstrapped.
 * Given that this process happens during the bootstrapping phase of the
 * client, this is not called in a context of a processor, that's why
 * it initializes its own dummy processor to fetch the configuration.
 */
class NodesConfigurationInit {
  struct HostListFetchCallback : public ConfigSource::AsyncCallback {
    using hostlist_cb_t = folly::Function<void(Status, ConfigSource::Output)>;

    virtual void onAsyncGet(ConfigSource* /* source */,
                            const std::string& /* path */,
                            Status status,
                            ConfigSource::Output output) override {
      cb_(status, std::move(output));
    }

    hostlist_cb_t cb_;
  };

 public:
  explicit NodesConfigurationInit(
      std::shared_ptr<configuration::nodes::NodesConfigurationStore> store,
      UpdateableSettings<Settings> settings)
      : store_(std::move(store)), settings_(std::move(settings)) {}

  virtual ~NodesConfigurationInit() = default;

  /**
   * Updates the passed updatable `nodes_configuration_config` with the
   * NodesConfiguration fetched using the following process:
   * 1. The server seed string is parsed and a list of initial servers are
   * fetched.
   * 2. We build a dummy ServerConfig with this list of servers, as simple as
   * it takes to satisfy the parser.
   * 3. Using the dummy config, we start a dummy processor.
   * 4. On one of the workers of the processor, we invoke a getConfig on the
   * NodesConfigurationStore of the class.
   * 5. We synchronously wait for the config to be fetched and parsed.
   * 6. When the config is successfully parsed, we update the
   * UpdateableNodesConfiguration.
   *
   * @return true on success, false otherwise.
   */
  bool
  init(std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config,
       std::shared_ptr<PluginRegistry> plugin_registry,
       const std::string& server_seed_str);

  /**
   * Updates the passed updatable `nodes_configuration_config` with the
   * NodesConfiguration fetched using the following process:
   * 1. We invoke getConfig on the passed store.
   * 2. We synchronously wait for the config to be fetched and parsed.
   * 3. When the config is successfully parsed, we update the
   * UpdateableNodesConfiguration.
   *
   * NOTE: This requires a NodesConfigurationStore that doesn't depend on a
   * processor.
   *
   * @return true on success, false otherwise.
   */
  bool initWithoutProcessor(
      std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config);

 protected:
  std::shared_ptr<UpdateableConfig> buildBootstrappingServerConfig(
      const std::vector<std::string>& host_list) const;

  // Used by the unit tests to inject extra settings to the created processor
  virtual void injectExtraSettings(Settings&) const {}

  bool parseAndFetchHostList(std::shared_ptr<PluginRegistry> plugin_registry,
                             const std::string& seed,
                             std::vector<std::string>* addrs) const;

 private:
  std::shared_ptr<Processor>
  buildBootstrappingProcessor(std::shared_ptr<UpdateableConfig> config) const;

  static std::shared_ptr<const configuration::nodes::NodesConfiguration>
  parseNodesConfiguration(const std::string& config);

  // @param processor    if not nullptr, execute the config fetch workflow on
  //                     the given Processor context, otherwise, execute the
  //                     fetch on the current context
  folly::SemiFuture<bool> executeGetConfig(
      std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config,
      Processor* processor);

  folly::SemiFuture<bool> getConfigImpl(
      std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config);

  // @return      true on success, false failed to get a valid config within the
  //              given timeout period
  folly::Future<bool> getConfigWithRetryingAndTimeout(
      std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config,
      Processor* processor,
      std::chrono::milliseconds timeout);

 private:
  std::shared_ptr<configuration::nodes::NodesConfigurationStore> store_;
  UpdateableSettings<Settings> settings_;
};

}} // namespace facebook::logdevice
