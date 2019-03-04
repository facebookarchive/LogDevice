/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"

namespace facebook { namespace logdevice {

class Configuration;
class Processor;
class ZookeeperClientFactory;
struct Settings;

namespace configuration { namespace nodes {

class NodesConfigurationStoreFactory {
 public:
  enum class NCSType { Zookeeper = 0, File, Server, Invalid };

  struct Params {
    NCSType type{NCSType::Invalid};

    // used when type == NCSType::Zookeeper, NCS will be created as
    // ZookeeperNodesConfigurationStore with the specified zookeeper config
    std::shared_ptr<configuration::ZookeeperConfig> zk_config{nullptr};
    std::shared_ptr<ZookeeperClientFactory> zk_client_factory{nullptr};

    // used when type == NCSType::File
    std::string file_store_root_dir{};

    // The path under-which the config itself will be stored. This is used by
    // all the NCSes as the key to the fetches/updates. The NCS itself can
    // prepend a root_path to this path if needed.
    std::string path{};

    bool isValid() const;
  };

  static std::unique_ptr<NodesConfigurationStore>
  create(Params params) noexcept;

  /**
   * utility function that creates the store with params deducted from
   * config and settings
   */
  static std::unique_ptr<NodesConfigurationStore>
  create(const Configuration& config,
         const Settings& settings,
         std::shared_ptr<ZookeeperClientFactory> zk_client_facotory) noexcept;

  static std::string getDefaultConfigStorePath(NCSType type,
                                               const std::string& cluster_name);
};

class NodesConfigurationManagerFactory {
 public:
  /**
   * Create a NodesConfigurationManager with given NodesConfigurationStore and
   * other given parameters.
   */
  static std::shared_ptr<NodesConfigurationManager>
  create(NodesConfigurationManager::OperationMode mode,
         Processor* processor,
         std::unique_ptr<NodesConfigurationStore> store) noexcept;

  /**
   * Convenient utility that create NodesConfigurationManager as well as its
   * underlying NodesConfigurationStore together.
   */
  static std::shared_ptr<NodesConfigurationManager>
  create(NodesConfigurationManager::OperationMode mode,
         Processor* processor,
         NodesConfigurationStoreFactory::Params params) noexcept;

  /**
   * utility function that creates the NodesConfigurationManager with params
   * deducted from Processor config and settings.
   *
   * @param roles    if the NCM is created for a server, _roles_ must be
   *                 provided to infer the NCM operational modes
   */
  static std::shared_ptr<NodesConfigurationManager>
  create(Processor* processor,
         std::unique_ptr<configuration::nodes::NodesConfigurationStore> store,
         folly::Optional<NodeServiceDiscovery::RoleSet> roles,
         std::shared_ptr<ZookeeperClientFactory> zk_client_factory) noexcept;
};

}} // namespace configuration::nodes
}} // namespace facebook::logdevice
