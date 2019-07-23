/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ServerConfigSource.h"

#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/NodesConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/util.h"

namespace facebook { namespace logdevice {

Status ServerConfigSource::getConfig(const std::string& path,
                                     Output* /* out */) {
  // TODO (#12733971): Add a retry mechanism in case all seed hosts don't
  //                   respond
  std::vector<std::string> hosts;
  size_t pos = 0;
  size_t next_delim_pos = 0;
  while (next_delim_pos != std::string::npos) {
    next_delim_pos = path.find(',', pos);
    hosts.push_back(path.substr(pos, next_delim_pos - pos));
    pos = next_delim_pos + 1;
  }

  if (!processor_) {
    ld_info("Initializating processor to fetch config");
    init(path, hosts);
  }
  bool fetch_succeeded = false;
  for (const std::string& host : hosts) {
    fetch_succeeded |= fetch(host);
  }
  if (!fetch_succeeded) {
    return err;
  }
  return E::NOTREADY;
}

static std::shared_ptr<ServerConfig>
constructConfig(const std::vector<std::string>& hosts) {
  folly::dynamic nodes = folly::dynamic::array;
  for (size_t index = 0; index < hosts.size(); index++) {
    folly::dynamic node_dict = folly::dynamic::object
        // "node_id" only needs to be unique per node here
        ("node_id", index)("host", hosts[index])(
            "roles", folly::dynamic::array("sequencer"))("generation", 1);
    nodes.push_back(node_dict);
  }

  // These fields are needed to satisfy the config parser and ensure that no
  // GetClusterStateRequests are posted to the processor
  folly::dynamic nodeset = folly::dynamic::array;
  nodeset.push_back(0);
  folly::dynamic metadata_logs =
      folly::dynamic::object("nodeset", nodeset)("replication_factor", 1);

  folly::dynamic json_all = folly::dynamic::object("cluster", "config_source")(
      "nodes", std::move(nodes))("metadata_logs", std::move(metadata_logs));

  return ServerConfig::fromJson(json_all);
}

void ServerConfigSource::init(const std::string& path,
                              const std::vector<std::string>& hosts) {
  config_ = std::make_shared<UpdateableConfig>();
  auto trace_logger = std::make_shared<NoopTraceLogger>(config_);

  // Make a placeholder config with the seed hosts
  auto updateable_server_config = config_->updateableServerConfig();
  auto serv_config = constructConfig(hosts);
  ld_check(serv_config != nullptr);
  updateable_server_config->update(std::move(serv_config));

  // Set a callback to pass the received config to the AsyncCallback
  server_config_subscription_ =
      updateable_server_config->subscribeToUpdates([this, path]() {
        auto server_config = config_->getServerConfig();
        Output out;
        auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
        out.contents = server_config->toString(logs_config.get());
        ServerConfig::ConfigMetadata metadata =
            server_config->getMainConfigMetadata();
        out.mtime = metadata.modified_time;
        out.hash = metadata.hash;
        async_cb_->onAsyncGet(this, path, E::OK, std::move(out));
      });

  // This is a hack to construct a Settings object with default settings
  // Cluster name verification is disabled since it is not know at the time of
  // creating the initial connection without a config file
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  settings.include_cluster_name_on_handshake = false;
  settings.include_destination_on_handshake = false;

  // ServerConfigSource doesn't really need any plugins. Loading BuildInfo since
  // it's used in socket communication though
  auto plugin_registry = std::make_shared<PluginRegistry>(
      createAugmentedCommonBuiltinPluginVector<>());

  // Construct a Processor to send the first CONFIG_FETCH message
  processor_ = Processor::create(config_,
                                 trace_logger,
                                 UpdateableSettings<Settings>(settings),
                                 /*stats*/ nullptr,
                                 plugin_registry,
                                 /*credentials*/ "",
                                 "ld:cfg-src");
}

bool ServerConfigSource::fetch(const std::string& host) {
  Sockaddr sock_addr;
  if (!configuration::parser::parseHostString(host, sock_addr, "server")) {
    ld_error("Failed to parse host: %s", host.c_str());
    return false;
  }
  NodeID node_id;

  for (const auto& [idx, node] :
       *config_->getNodesConfiguration()->getServiceDiscovery()) {
    if (node.address == sock_addr) {
      node_id = config_->getNodesConfiguration()->getNodeID(idx);
      break;
    }
  }
  if (!node_id.isNodeID()) {
    ld_error("Could not find node %s in config", sock_addr.toString().c_str());
    return false;
  }

  std::unique_ptr<Request> fetch_request =
      std::make_unique<ConfigurationFetchRequest>(
          node_id, ConfigurationFetchRequest::ConfigType::MAIN_CONFIG);
  int rv = processor_->postRequest(fetch_request);
  if (rv != 0) {
    ld_error("Unable to request config from server");
    return false;
  }
  return true;
}

}} // namespace facebook::logdevice
