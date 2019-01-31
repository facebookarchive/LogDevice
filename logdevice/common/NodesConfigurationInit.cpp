/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationInit.h"

#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/util.h"

namespace facebook { namespace logdevice {

bool NodesConfigurationInit::init(
    std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config,
    const std::string& server_seed_str) {
  ld_info("Trying to fetch the NodesConfiguration using the server seed: %s",
          server_seed_str.c_str());
  auto host_list = parseAndFetchHostList(server_seed_str);
  if (host_list.empty()) {
    ld_error(
        "There are no seed servers to bootstrap the nodes configuration from");
    return false;
  }
  auto dummy_config = buildDummyServerConfig(host_list);
  if (dummy_config == nullptr) {
    ld_error("Failed to build a dummy server config for the processor");
    return false;
  }
  auto processor = buildDummyProcessor(std::move(dummy_config));

  folly::Promise<bool> promise;
  auto config_cb = [&](Status status, std::string config) {
    if (status == Status::OK) {
      auto nc = parseNodesConfiguration(config);
      if (nc == nullptr) {
        ld_error("Failed to parse the nodes configuration");
        promise.setValue(false);
        return;
      }
      ld_info(
          "Got a NodesConfiguration of version: %lu", nc->getVersion().val());
      nodes_configuration_config->update(std::move(nc));
      promise.setValue(true);
    } else {
      ld_error("Failed to get the NodesConfiguration with Status: %s",
               error_description(status));
      promise.setValue(false);
    }
  };
  auto& store = store_;
  run_on_worker(processor.get(), 0, [&config_cb, &store]() {
    // TODO Override the timeout of the store with the time we have left to
    // do the config fetch.
    store->getConfig(std::move(config_cb));
    return 0;
  });
  return promise.getSemiFuture().get();
}

std::vector<std::string> NodesConfigurationInit::parseAndFetchHostList(
    const std::string& server_seed) const {
  std::vector<std::string> hosts;
  folly::split(",", server_seed, hosts);
  return hosts;
}

std::shared_ptr<UpdateableConfig>
NodesConfigurationInit::buildDummyServerConfig(
    const std::vector<std::string>& host_list) const {
  // clang-format off
  folly::dynamic json = folly::dynamic::object
    ("cluster", "config_source")
    ("nodes", folly::dynamic::array())
    ("metadata_logs", folly::dynamic::object
      ("nodeset", folly::dynamic::array(0))
      ("replication_factor", 1));

  for (size_t index = 0; index < host_list.size(); index++) {
    json["nodes"].push_back(folly::dynamic::object
      ("node_id", index)
      ("host", host_list[index])
      ("roles", folly::dynamic::array("sequencer"))
      ("generation", 1));
  }
  // clang-format on

  auto server_config = ServerConfig::fromJson(std::move(json));
  ld_check(server_config);
  auto config = std::make_shared<UpdateableConfig>();
  config->updateableServerConfig()->update(std::move(server_config));
  return config;
}

std::shared_ptr<Processor> NodesConfigurationInit::buildDummyProcessor(
    std::shared_ptr<UpdateableConfig> config) const {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;

  // Given that we don't have the cluster name at this point.
  settings.include_cluster_name_on_handshake = false;
  settings.include_destination_on_handshake = false;
  settings.enable_config_synchronization = false;
  injectExtraSettings(settings);

  auto plugin_registry = std::make_shared<PluginRegistry>(
      createAugmentedCommonBuiltinPluginVector<>());

  auto trace_logger = std::make_shared<NoopTraceLogger>(config);
  return Processor::create(std::move(config),
                           trace_logger,
                           UpdateableSettings<Settings>(settings),
                           /*stats*/ nullptr,
                           plugin_registry,
                           /*credentials*/ "",
                           "ld:cfg-src");
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
NodesConfigurationInit::parseNodesConfiguration(
    const std::string& config) const {
  return configuration::nodes::NodesConfigurationCodecFlatBuffers::deserialize(
      config);
}

}} // namespace facebook::logdevice
