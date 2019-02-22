/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationInit.h"

#include "logdevice/common/ConfigSourceLocationParser.h"
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
    std::shared_ptr<PluginRegistry> plugin_registry,
    const std::string& server_seed_str) {
  ld_info("Trying to fetch the NodesConfiguration using the server seed: %s",
          server_seed_str.c_str());
  std::vector<std::string> host_list;
  auto success =
      parseAndFetchHostList(plugin_registry, server_seed_str, &host_list);
  if (!success) {
    return false;
  }
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

  return run_on_worker(
             processor.get(),
             0,
             [&]() { return executeGetConfig(nodes_configuration_config); })
      .get();
}

bool NodesConfigurationInit::initWithoutProcessor(
    std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config) {
  return executeGetConfig(std::move(nodes_configuration_config)).get();
}

bool NodesConfigurationInit::parseAndFetchHostList(
    std::shared_ptr<PluginRegistry> plugin_registry,
    const std::string& server_seed,
    std::vector<std::string>* addrs) const {
  // Get all the config sources
  // TODO(T40741918): Only create the sources matching the seed's scheme.
  std::vector<std::unique_ptr<ConfigSource>> sources;
  auto factories = plugin_registry->getMultiPlugin<ConfigSourceFactory>(
      PluginType::CONFIG_SOURCE_FACTORY);
  for (const auto& f : factories) {
    std::vector<std::unique_ptr<ConfigSource>> srcs = (*f)(plugin_registry);
    for (auto& src : srcs) {
      sources.push_back(std::move(src));
    }
  }

  // Determine which ConfigSource to use.
  auto src = ConfigSourceLocationParser::parse(sources, server_seed);
  if (src.first == nullptr) {
    return false;
  }
  auto& source = src.first;
  auto& path = src.second;

  // Prepare the async callback in case the config is not ready immediately.
  folly::Promise<std::pair<Status, ConfigSource::Output>> promise;
  HostListFetchCallback::hostlist_cb_t cb = [&](Status status,
                                                ConfigSource::Output out) {
    promise.setValue(std::make_pair(status, out));
  };
  HostListFetchCallback host_cb;
  host_cb.cb_ = std::move(cb);
  source->setAsyncCallback(&host_cb);

  // Fetch the host list
  std::string config_str;
  ConfigSource::Output out;
  auto status = source->getConfig(path, &out);
  if (status == Status::NOTREADY) {
    // Block waiting for it.
    std::tie(status, out) = promise.getSemiFuture().get();
  }

  if (status == Status::OK) {
    config_str = out.contents;
  } else {
    ld_error("Failed to fetch the host list with Status: %s",
             error_description(status));
    return false;
  }

  ld_info(
      "Using %s as the NodesConfiguration servers seed", config_str.c_str());

  folly::split(",", config_str, *addrs);
  return true;
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
NodesConfigurationInit::parseNodesConfiguration(const std::string& config) {
  return configuration::nodes::NodesConfigurationCodecFlatBuffers::deserialize(
      config);
}

folly::SemiFuture<bool> NodesConfigurationInit::executeGetConfig(
    std::shared_ptr<UpdateableNodesConfiguration> nodes_configuration_config) {
  folly::Promise<bool> promise;
  auto future = promise.getSemiFuture();
  auto config_cb = [nodes_configuration_config =
                        std::move(nodes_configuration_config),
                    promise = std::move(promise)](
                       Status status, std::string config) mutable {
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

  // TODO Override the timeout of the store with the time we have left to
  // do the config fetch.
  // TODO Make this config fetch more robust by adding retries and timeouts.
  // TODO For servers this should be `getLatestConfig`.
  store_->getConfig(std::move(config_cb));
  return future;
}

}} // namespace facebook::logdevice
