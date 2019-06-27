/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/ClientFactory.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/NodesConfigurationInit.h"
#include "logdevice/common/NodesConfigurationPublisher.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/protocol/HELLO_Message.h"
#include "logdevice/common/settings/SSLSettingValidation.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/RemoteLogsConfig.h"

namespace facebook { namespace logdevice {

namespace {

bool validateSSLSettings(
    std::shared_ptr<const NodesConfiguration> nodes_configuration,
    std::shared_ptr<const Settings> settings) {
  size_t ssl_nodes = 0;
  for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
    if (kv.second.ssl_address.hasValue()) {
      ++ssl_nodes;
    }
  }
  if (ssl_nodes == 0) {
    // There are no nodes configured with SSL, no need to validate SSL certs
    return true;
  }
  // If not configured to load the client cert, we only need the CA cert.
  // If loading client cert, we need to check all of them
  bool ca_only = !settings->ssl_load_client_cert;
  return validateSSLCertificatesExist(settings, ca_only);
}

bool applySettingOverrides(SettingsUpdater& updater) {
  const char* env = getenv("LOGDEVICE_OVERRIDE_CLIENT_SETTINGS");
  if (env) {
    auto parsed = configuration::parser::parseJson(env);
    if (!parsed.isObject()) {
      ld_error("Failed to parse environment variable "
               "LOGDEVICE_OVERRIDE_CLIENT_SETTINGS, it should be a JSON map");
      return false;
    }

    // the below will throw an exception that won't be caught by anything within
    // LD if it can't represent the value as string or set the setting.
    try {
      for (auto& kv : parsed.items()) {
        std::string key = kv.first.asString();
        std::string val = kv.second.asString();
        ld_info("Overriding client setting from environment variable "
                "LOGDEVICE_OVERRIDE_CLIENT_SETTINGS: %s=\"%s\"",
                key.c_str(),
                val.c_str());
        updater.setFromClient(key, val);
      }
    } catch (std::exception& e) {
      ld_error("Error while setting settings from environment variable "
               "LOGDEVICE_OVERRIDE_CLIENT_SETTINGS: %s",
               e.what());
      return false;
    }
  }
  return true;
}

} // namespace

ClientFactory&
ClientFactory::setClientSettings(std::unique_ptr<ClientSettings> v) {
  ld_check(!client_settings_);
  client_settings_ = std::move(v);
  return *this;
}

std::shared_ptr<Client> ClientFactory::create(std::string config_url) noexcept {
  auto start_time = std::chrono::steady_clock::now();
  ld_info("Creating Client with config: %s", config_url.c_str());

  // Only one of these is allowed
  ld_check(string_settings_.empty() || client_settings_ == nullptr);

  // If a ClientSettings instance was not provided, create
  // one with default settings and override from string_settings_
  if (!client_settings_) {
    client_settings_.reset(ClientSettings::create());
    for (auto& kv : string_settings_) {
      int rv = client_settings_->set(kv.first.c_str(), kv.second.c_str());
      if (rv != 0) {
        ld_error(
            "Error setting %s to %s.", kv.first.c_str(), kv.second.c_str());
        return nullptr;
      }
    }
  }
  ld_check(client_settings_);
  std::unique_ptr<ClientSettingsImpl> impl_settings(
      static_cast<ClientSettingsImpl*>(client_settings_.release()));

  auto settings_updater = impl_settings->getSettingsUpdater();

  auto plugin_registry = impl_settings->getPluginRegistry();
  if (!plugin_registry) {
    plugin_registry =
        std::make_shared<PluginRegistry>(getClientPluginProviders());
    plugin_registry->addOptions(settings_updater.get());
  }

  ld_info(
      "Plugins loaded: %s", plugin_registry->getStateDescriptionStr().c_str());

  if (!applySettingOverrides(*settings_updater)) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  auto update_settings = [settings_updater](ServerConfig& config) -> bool {
    auto settings = config.getClientSettingsConfig();

    try {
      settings_updater->setFromConfig(settings);
    } catch (const boost::program_options::error&) {
      return false;
    }
    return true;
  };

  auto config = std::make_shared<UpdateableConfig>();
  auto handle =
      config->updateableServerConfig()->addHook(std::move(update_settings));

  std::unique_ptr<LogsConfig> logs_cfg;

  bool enable_remote_logsconfig =
      impl_settings->getSettings()->on_demand_logs_config ||
      impl_settings->getSettings()->force_on_demand_logs_config;

  std::shared_ptr<std::weak_ptr<Processor>> logs_cfg_processor_ptr_ptr;
  if (enable_remote_logsconfig) {
    // We don't want internal logsconfig management if we have
    // on_demand_logs_config enabled
    ld_info("Remote (on-demand) LogsConfig is ENABLED");
    auto cache_ttl = impl_settings->getSettings()->remote_logs_config_cache_ttl;
    RemoteLogsConfig* raw_logs_cfg = new RemoteLogsConfig(timeout_, cache_ttl);
    logs_cfg_processor_ptr_ptr = raw_logs_cfg->getProcessorPtrPtr();
    logs_cfg.reset(raw_logs_cfg);
  }

  ConfigParserOptions options;
  ConfigInit config_init(timeout_);

  int rv = config_init.attach(config_url,
                              plugin_registry,
                              config,
                              std::move(logs_cfg),
                              impl_settings->getSettings(),
                              options);
  if (rv != 0) {
    return nullptr;
  }

  auto ncm_enabled =
      impl_settings->getSettings()->enable_nodes_configuration_manager;
  // Init Nodes Configuration
  auto nodes_configuration_seed =
      impl_settings->getSettings()->nodes_configuration_seed_servers;
  bool use_server_ncs = !nodes_configuration_seed.empty();
  bool use_zk_ncs = impl_settings->getSettings()->admin_client_capabilities;
  if (ncm_enabled && (use_server_ncs || use_zk_ncs)) {
    std::shared_ptr<ZookeeperClientFactory> zk_client_factory =
        plugin_registry->getSinglePlugin<ZookeeperClientFactory>(
            PluginType::ZOOKEEPER_CLIENT_FACTORY);
    // For regular clients, we will construct a (LD)ServerBased
    // NodesConfigurationStore; for admin clients (e.g., emergency tooling), we
    // will construct a Zookeeper NodesConfigurationStore.
    auto ncs = configuration::nodes::NodesConfigurationStoreFactory::create(
        *config->get(),
        *impl_settings->getSettings().get(),
        std::move(zk_client_factory));

    NodesConfigurationInit nodes_cfg_init(
        std::move(ncs), impl_settings->getSettings());
    bool success = false;
    if (use_server_ncs) {
      ld_info("Trying to obtain initial NodesConfiguration from a LogDevice "
              "server...");
      success = nodes_cfg_init.init(config->updateableNCMNodesConfiguration(),
                                    plugin_registry,
                                    nodes_configuration_seed);
    } else if (use_zk_ncs) {
      ld_info("Trying to obtain initial NodesConfiguration from Zookeeper...");
      success = nodes_cfg_init.initWithoutProcessor(
          config->updateableNCMNodesConfiguration());
    }
    if (!success) {
      return nullptr;
    }
    ld_check(config->getNodesConfigurationFromNCMSource() != nullptr);
  }

  // publish the NodesConfiguration for the first time. Later a
  // long-living subscribing NodesConfigurationPublisher will be created again
  // in Processor
  // TODO(T43023435): use an actual TraceLogger to log this initial update.
  NodesConfigurationPublisher publisher(
      config,
      impl_settings->getSettings(),
      std::make_shared<NoopTraceLogger>(config),
      /*subscribe*/ false);
  ld_check(config->getNodesConfiguration() != nullptr);

  if (!validateSSLSettings(config->getNodesConfiguration(),
                           impl_settings->getSettings().get())) {
    // validateSSLSettings() should output the error
    return nullptr;
  }

  if (credentials_.size() > HELLO_Header::CREDS_SIZE_V1) {
    // credentials is too large to fit in HELLO_Message credential buffer
    err = E::INVALID_PARAM;
    return nullptr;
  }

  if (csid_.size() > MAX_CSID_SIZE) {
    // csid is too large
    err = E::INVALID_PARAM;
    return nullptr;
  }

  if (csid_.empty()) {
    boost::uuids::uuid gen_csid = boost::uuids::random_generator()();
    csid_ = boost::uuids::to_string(gen_csid);
  }

  std::shared_ptr<ClientImpl> impl = nullptr;
  try {
    impl = std::make_shared<ClientImpl>(cluster_name_,
                                        std::move(config),
                                        credentials_,
                                        csid_,
                                        timeout_,
                                        std::move(impl_settings),
                                        plugin_registry);
  } catch (const ConstructorFailed&) {
    // err set by the constructor
    ld_error("Constructing ClientImpl failed with %s.", error_description(err));
    return nullptr;
  }

  ld_check(impl != nullptr);
  ld_check(impl->getProcessorPtr());

  impl->addServerConfigHookHandle(std::move(handle));

  // Setting the logs config's shared processor pointer to the actual
  // processor
  if (logs_cfg_processor_ptr_ptr) {
    *logs_cfg_processor_ptr_ptr =
        std::weak_ptr<Processor>(impl->getProcessorPtr());
  }

  auto end_time = std::chrono::steady_clock::now();
  ld_info("Created Client in %.3f seconds. Cluster name: %s, Config: %s",
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                    start_time)
              .count(),
          cluster_name_.empty()
              ? impl->getConfig()->getServerConfig()->getClusterName().c_str()
              : cluster_name_.c_str(),
          config_url.c_str());

  return std::shared_ptr<Client>(impl);
}

}} // namespace facebook::logdevice
