/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"

#include <folly/Conv.h>

#include "logdevice/common/ZookeeperClientFactoryProd.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/FileBasedNodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerDependencies.h"
#include "logdevice/common/configuration/nodes/ServerBasedNodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

bool NodesConfigurationStoreFactory::Params::isValid() const {
  switch (type) {
    case NCSType::Zookeeper:
      return zk_config != nullptr;
    case NCSType::File:
      return !file_store_path.empty();
    case NCSType::Server:
      return true;
  }
  ld_check(false);
  return false;
}

/*static*/
std::unique_ptr<NodesConfigurationStore>
NodesConfigurationStoreFactory::create(Params params) noexcept {
  std::unique_ptr<NodesConfigurationStore> ncs;
  if (!params.isValid()) {
    ld_critical(
        "Unable to create nodes configuration store (type %d) due to invalid "
        "parameters!",
        (int)params.type);
    err = E::INTERNAL;
    // the NCM/NCS creation is not part of public API, so this is likely an
    // internal error
    ld_check(false);
    return nullptr;
  }

  switch (params.type) {
    case NCSType::Zookeeper: {
      std::shared_ptr<ZookeeperClientBase> zkclient =
          zkFactoryProd(*params.zk_config);
      if (zkclient == nullptr) {
        ld_error("Unable to create the zookeeper client for NCS!");
        return nullptr;
      }
      return std::make_unique<ZookeeperNodesConfigurationStore>(
          NodesConfigurationCodecFlatBuffers::extractConfigVersion,
          std::move(zkclient));
    }
    case NCSType::File:
      return std::make_unique<FileBasedNodesConfigurationStore>(
          params.file_store_path,
          NodesConfigurationCodecFlatBuffers::extractConfigVersion);
    case NCSType::Server:
      return std::make_unique<ServerBasedNodesConfigurationStore>();
  }

  ld_check(false);
  return nullptr;
}

/*static*/
std::unique_ptr<NodesConfigurationStore>
NodesConfigurationStoreFactory::create(const Configuration& config,
                                       const Settings& settings) noexcept {
  configuration::nodes::NodesConfigurationStoreFactory::Params ncs_params;
  const bool is_server = settings.server;

  if (!settings.nodes_configuration_store_file_path.empty()) {
    // use file based store if its path is given
    ncs_params.type = NCSType::File;
    ncs_params.file_store_path = settings.nodes_configuration_store_file_path;
  } else {
    if (!is_server && !settings.admin_client_capabilities) {
      // for clients without admin capabilities (observer only client),
      // use the server based store
      ncs_params.type = NCSType::Server;
    } else {
      ncs_params.type = NCSType::Zookeeper;
      ncs_params.zk_config = config.zookeeperConfig();
    }
  }
  ld_assert(ncs_params.isValid());
  return create(std::move(ncs_params));
}

/*static*/
std::shared_ptr<NodesConfigurationManager>
NodesConfigurationManagerFactory::create(
    NodesConfigurationManager::OperationMode mode,
    Processor* processor,
    std::unique_ptr<NodesConfigurationStore> store) noexcept {
  ld_check(processor != nullptr);
  ld_check(store != nullptr);

  if (!mode.isClientOnly() &&
      dynamic_cast<ServerBasedNodesConfigurationStore*>(store.get()) !=
          nullptr) {
    ld_error("NodesConfiguratonManager is requested to run in non-client-only "
             "mode but with NCSType::Server!");
    return nullptr;
  }

  auto dep = std::make_unique<ncm::Dependencies>(processor, std::move(store));
  // dep construction shouldn't fail or throw
  ld_check(dep != nullptr);

  return NodesConfigurationManager::create(mode, std::move(dep));
}

/*static*/
std::shared_ptr<NodesConfigurationManager>
NodesConfigurationManagerFactory::create(
    NodesConfigurationManager::OperationMode mode,
    Processor* processor,
    NodesConfigurationStoreFactory::Params params) noexcept {
  ld_check(processor != nullptr);
  return NodesConfigurationManagerFactory::create(
      mode,
      processor,
      NodesConfigurationStoreFactory::create(std::move(params)));
}

/*static*/
std::shared_ptr<NodesConfigurationManager>
NodesConfigurationManagerFactory::create(
    Processor* processor,
    std::unique_ptr<configuration::nodes::NodesConfigurationStore> store,
    folly::Optional<NodeServiceDiscovery::RoleSet> roles) noexcept {
  ld_check(processor != nullptr);
  const auto& settings = *processor->settings();
  const bool is_server = settings.server;

  if (is_server && !roles.hasValue()) {
    ld_critical("Unable to create NodesConfigurationStore for creating "
                "NodesConfiguratonManager!");
    err = E::INTERNAL;
    ld_check(false);
    return nullptr;
  }

  if (store == nullptr) {
    store = NodesConfigurationStoreFactory::create(
        *processor->config_->get(), settings);
    if (store == nullptr) {
      ld_error("Unable to create NodesConfigurationStore for creating "
               "NodesConfiguratonManager!");
      return nullptr;
    }
  }

  const auto mode = is_server
      ? NodesConfigurationManager::OperationMode::forNodeRoles(roles.value())
      : (settings.admin_client_capabilities
             ? NodesConfigurationManager::OperationMode::forTooling()
             : NodesConfigurationManager::OperationMode::forClient());

  return configuration::nodes::NodesConfigurationManagerFactory::create(
      mode, processor, std::move(store));
}

}}}} // namespace facebook::logdevice::configuration::nodes
