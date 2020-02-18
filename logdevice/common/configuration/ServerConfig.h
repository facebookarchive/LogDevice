/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include <boost/concept/assert.hpp>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/dynamic.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/NodesConfig.h"
#include "logdevice/common/configuration/PrincipalsConfig.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/configuration/SequencersConfig.h"
#include "logdevice/common/configuration/TrafficShapingConfig.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

/**
 * @file A container for a parsed representation of the configuration file.
 */

namespace facebook { namespace logdevice {

/**
 * Represents a parsed config file.  A config file contains configuration for
 * one LogDevice cluster.  A cluster is configured with a set of logs, a list
 * of participating nodes, and other infrequently changing metadata.
 */
class ServerConfig {
 public:
  using MetaDataLogsConfig =
      facebook::logdevice::configuration::MetaDataLogsConfig;
  using Node = facebook::logdevice::configuration::Node;
  using Nodes = facebook::logdevice::configuration::Nodes;
  using NodesConfig = facebook::logdevice::configuration::NodesConfig;
  using NodesConfiguration = configuration::nodes::NodesConfiguration;
  using PrincipalsConfig = facebook::logdevice::configuration::PrincipalsConfig;
  using SecurityConfig = facebook::logdevice::configuration::SecurityConfig;
  using SequencersConfig = facebook::logdevice::configuration::SequencersConfig;
  using ShapingConfig = facebook::logdevice::configuration::ShapingConfig;
  using TrafficShapingConfig =
      facebook::logdevice::configuration::TrafficShapingConfig;
  using SettingsConfig = std::unordered_map<std::string, std::string>;
  using OptionalTimestamp = folly::Optional<std::chrono::seconds>;
  using InternalLogs = facebook::logdevice::configuration::InternalLogs;
  using ZookeeperConfig = facebook::logdevice::configuration::ZookeeperConfig;

  /**
   * Local overrides of cluster global configuration data. Typically
   * set via the admin interface.
   */
  class Overrides {
   private:
    template <typename T>
    std::unique_ptr<T> copy_unique(const std::unique_ptr<T>& source) {
      return source ? std::make_unique<T>(*source) : nullptr;
    }

   public:
    Overrides() {}
    Overrides(const Overrides& src)
        : trafficShapingConfig(copy_unique(src.trafficShapingConfig)) {}
    Overrides(Overrides&& src) noexcept
        : trafficShapingConfig(std::move(src.trafficShapingConfig)) {}

    Overrides& operator=(const Overrides& rhs) noexcept {
      if (this != &rhs) {
        trafficShapingConfig = copy_unique(rhs.trafficShapingConfig);
      }
      return *this;
    }
    Overrides& operator=(Overrides&& rhs) noexcept {
      if (this != &rhs) {
        trafficShapingConfig = std::move(rhs.trafficShapingConfig);
      }
      return *this;
    }

    /**
     * Generate a new configuration by combining the base configuration
     * and any overrides contained in this class.
     */
    std::shared_ptr<ServerConfig>
    apply(const std::shared_ptr<ServerConfig>& base_cfg) {
      std::shared_ptr<ServerConfig> overridden_config = base_cfg->copy();
      if (trafficShapingConfig) {
        overridden_config->trafficShapingConfig_ = *trafficShapingConfig;
      }
      return overridden_config;
    }

    std::unique_ptr<TrafficShapingConfig> trafficShapingConfig;
  };

  /**
   * Creates a ServerConfig object from a JSON string.
   *
   * @param jsonPiece               string containing JSON-formatted
   *                                configuration
   * @return On success, returns a new ServerConfig instance.  On
   * failure, returns nullptr and sets err to:
   *           INVALID_CONFIG  various errors in parsing the config
   *           NOTREADY        config refers to external resource that is not
   *                           yet ready
   */
  static std::unique_ptr<ServerConfig> fromJson(const std::string& jsonPiece);

  static std::unique_ptr<ServerConfig> fromJson(const folly::dynamic& parsed);

  std::string getClusterName() const {
    return clusterName_;
  }

  void setVersion(config_version_t version) {
    version_ = version;
  }

  void setNodesConfigurationVersion(config_version_t version) {
    nodesConfig_.setNodesConfigurationVersion(version);
  }

  /**
   * Returns the version of this config
   */
  config_version_t getVersion() const {
    return version_;
  }

  /**
   * Gets all nodes.
   */
  const Nodes& getNodes() const {
    return nodesConfig_.getNodes();
  }

  /**
   * Returns a description of sequencer nodes in the cluster.
   * NOTE: being DEPRECATED. Use NodesConfiguration::getSequencersConfig()
   * instead.
   */
  const SequencersConfig& getSequencers_DEPRECATED() const {
    return sequencersConfig_;
  }

  /**
   * Looks up a node by index.
   *
   * @return On success, returns a pointer to a Node object contained in
   *         this config.  On failure, returns nullptr and sets err to:
   *           NOTFOUND       no node with given index appears in config
   */
  const Node* getNode(node_index_t index) const;

  /**
   * Looks up a node by NodeID. If id.generation() == 0, ignores generation in
   * config, i.e. equivalent to getNode(id.index()).
   *
   * @return On success, returns a pointer to a Node object contained in
   *         this config.  On failure, returns nullptr and sets err to:
   *           INVALID_PARAM  node ID was invalid
   *           NOTFOUND       no node with given ID appears in config
   */
  const Node* getNode(const NodeID& id) const;

  /**
   * Returns the NodeID of the server that this config was received from.
   * Returns an invalid NodeID if the config did not originate from a server.
   */
  NodeID getServerOrigin() const {
    return server_origin_;
  }

  void setServerOrigin(const NodeID& id) {
    server_origin_ = id;
  }

  /**
   * @return if unauthenticated connections are allowed
   */
  bool allowUnauthenticated() const {
    return securityConfig_.allowUnauthenticated;
  }

  /**
   * @return if servers are authenticated by IP addresses
   */
  bool authenticateServersByIP() const {
    return securityConfig_.enableServerIpAuthentication;
  }

  struct ConfigMetadata {
    std::string uri;
    std::string hash;
    std::chrono::milliseconds modified_time;
    std::chrono::milliseconds loaded_time;

    // This is a hacky fix for a race condition in config synchronization.
    //
    // If this flag is true, it means we're updated the main config to the
    // version corresponding to `hash`, but left logs config at a potentially
    // older version (even if `hash` is a hash of a string that contains the
    // new logs config!). This happens when config synchronization updates
    // main config using data+hash received from a peer; such data doesn't
    // include logs config, even if the `hash` bundled with it does.
    //
    // When we get a config update from the main config source (as opposed to
    // configu synchronization messages), this flag will tell us that we need
    // to update logs config even if hash matches.
    //
    // This is a hack because this flag is not really a property of the server
    // config, but rather tells whether logs config (which is not part of server
    // config) has been updated. The source of this abstraction leak (at least
    // in part) is the fact that `hash` also doesn't describe just server config
    // but both server and logs config. This is all a mess.
    bool logs_config_may_be_outdated = false;
  };

  void setMainConfigMetadata(const ConfigMetadata& metadata) {
    main_config_metadata_ = metadata;
  }

  /**
   * Expose the metadata of the main config (URI, hash, last modified, last
   * loaded).
   */
  const ConfigMetadata& getMainConfigMetadata() const {
    return main_config_metadata_;
  }

  /**
   * Note: deprecated. Use StorageMembership in NodesConfiguration instead.
   *
   * Get the indices of metadata log nodes
   *
   * @return  a const reference to a vector of indices of nodes
   *          that store metadata logs
   */
  const std::vector<node_index_t>& getMetaDataNodeIndices() const {
    return metaDataLogsConfig_.metadata_nodes;
  }

  /**
   * Get the metadata Log configuration
   *
   * @return   a const reference to the Log object describing
   *           properties of all metadata logs
   */
  const std::shared_ptr<LogsConfig::LogGroupNode>& getMetaDataLogGroup() const {
    return metaDataLogsConfig_.metadata_log_group;
  }

  /**
   * Get the metadata Log and directory configuration
   *
   * @return   a const reference to the LogGroupInDirectory object describing
   *           properties of all metadata logs and their directory
   */
  const LogsConfig::LogGroupInDirectory& getMetaDataLogGroupInDir() const {
    return metaDataLogsConfig_.metadata_log_group_in_dir;
  }

  /**
   * @return  true if the responsibility of provisioning logs to epoch store is
   *          on sequencers.
   */
  bool sequencersProvisionEpochStore() const {
    return metaDataLogsConfig_.sequencers_provision_epoch_store;
  }

  const MetaDataLogsConfig& getMetaDataLogsConfig() const {
    return metaDataLogsConfig_;
  }

  const NodesConfig& getNodesConfig() const {
    return nodesConfig_;
  }

  /**
   * Get the new representation of cluster nodes (i.e. NodesConfiguration
   * class). reiterate the _FromServerConfigSource_ part to avoid confusion
   * during NodesConfiguration migration.
   */
  const std::shared_ptr<const NodesConfiguration>&
  getNodesConfigurationFromServerConfigSource() const {
    return nodesConfig_.getNodesConfiguration();
  }

  /**
   * Creates a ServerConfig object from existing cluster name,
   * NodesConfig, LogsConfig, SecurityConfig instances.
   *
   * Note that it regenerates the new NodesConfiguration format from the
   * existing NodesConfig and MetaDataLogsConfig. returns nullptr if the
   * conversion failed.
   *
   * Public for testing.
   */
  static std::unique_ptr<ServerConfig> fromDataTest(
      std::string cluster_name,
      NodesConfig nodes,
      MetaDataLogsConfig metadata_logs = MetaDataLogsConfig(),
      PrincipalsConfig = PrincipalsConfig(),
      SecurityConfig securityConfig = SecurityConfig(),
      TrafficShapingConfig = TrafficShapingConfig(),
      ShapingConfig =
          ShapingConfig(std::set<NodeLocationScope>{NodeLocationScope::NODE},
                        std::set<NodeLocationScope>{NodeLocationScope::NODE}),
      SettingsConfig server_settings_config = SettingsConfig(),
      SettingsConfig client_settings_config = SettingsConfig(),
      InternalLogs internal_logs = InternalLogs(),
      OptionalTimestamp clusterCreationTime = OptionalTimestamp(),
      folly::dynamic customFields = folly::dynamic::object,
      const std::string& ns_delimiter =
          LogsConfig::default_namespace_delimiter_);

  /**
   * Returns a duplicate of the configuration.
   */
  std::unique_ptr<ServerConfig> copy() const;

  /**
   * Returns a clone of the ServerConfig object with the nodes section
   * replaced by the parameter.
   */
  std::shared_ptr<ServerConfig> withNodes(NodesConfig) const;

  /**
   * Returns a clone of the ServerConfig object with the MetadataLogsConfig
   * section replaced by the parameter.
   */
  std::shared_ptr<ServerConfig>
      withMetaDataLogsConfig(MetaDataLogsConfig) const;

  /**
   * Returns a clone of the ServerConfig object with version replaced by the
   * given value.
   */
  std::shared_ptr<ServerConfig> withVersion(config_version_t) const;

  /**
   * Returns a clone of the ServerConfig object with version increased by one.
   */
  std::shared_ptr<ServerConfig> withIncrementedVersion() const {
    return withVersion(config_version_t(getVersion().val_ + 1));
  }

  /**
   * Returns the maximum finite backlog duration of a log.
   */
  std::chrono::seconds getMaxBacklogDuration() const;

  /**
   * Creates an empty ServerConfig object.  For testing.
   */
  static std::shared_ptr<ServerConfig> createEmpty();

  /**
   * @return the authentication type defined in the configuration file
   */
  AuthenticationType getAuthenticationType() const {
    return securityConfig_.authenticationType;
  }

  /**
   * @return the permission checker type defined in the configuration file
   */
  PermissionCheckerType getPermissionCheckerType() const {
    return securityConfig_.permissionCheckerType;
  }

  /**
   * @return the PrincipalConfig, if any, matching the provided principal name.
   */
  std::shared_ptr<const Principal> getPrincipalByName(const std::string*) const;

  /**
   * Exposes the entire SecurityConfig structure.
   */
  const SecurityConfig& getSecurityConfig() const {
    return securityConfig_;
  }

  /**
   * Exposes the entire TrafficShapingConfig structure.
   */
  const TrafficShapingConfig& getTrafficShapingConfig() const {
    return trafficShapingConfig_;
  }

  const ShapingConfig& getReadIOShapingConfig() const {
    return readIOShapingConfig_;
  }

  /**
   * Exposes the settings configured via the config file in "server_settings"
   */
  const SettingsConfig& getServerSettingsConfig() const {
    return serverSettingsConfig_;
  }

  /**
   * Exposes the settings configured via the config file in "client_settings"
   */
  const SettingsConfig& getClientSettingsConfig() const {
    return clientSettingsConfig_;
  }

  /**
   * Exposes custom fields that logdevice ignores from the config
   */
  const folly::dynamic& getCustomFields() const {
    return customFields_;
  }

  /**
   * Exposes the cluster_creation_time attribute from the config
   */
  OptionalTimestamp getClusterCreationTime() const {
    return clusterCreationTime_;
  }

  /**
   * Returns the namespace delimiter as defined in config
   */
  inline const std::string& getNamespaceDelimiter() const {
    return ns_delimiter_;
  }

  const InternalLogs& getInternalLogsConfig() const {
    return internalLogs_;
  }

  const std::string toString(const LogsConfig* with_logs = nullptr,
                             const ZookeeperConfig* with_zk = nullptr,
                             bool compress = false) const;
  folly::dynamic toJson(const LogsConfig* with_logs = nullptr,
                        const ZookeeperConfig* with_zk = nullptr) const;

 private:
  //
  // Allow only one way of constructing that the factories use.  Delete copy
  // and move facilities.
  //
  ServerConfig(std::string cluster_name,
               NodesConfig nodesConfig,
               MetaDataLogsConfig metaDataLogsConfig,
               PrincipalsConfig principalConfig,
               SecurityConfig securityConfig,
               TrafficShapingConfig trafficShapingConfig,
               ShapingConfig readIOShapingConfig,
               SettingsConfig serverSettingsConfig,
               SettingsConfig clientSettingsConfig,
               InternalLogs internalLogs,
               OptionalTimestamp clusterCreationTime,
               folly::dynamic customFields,
               const std::string& ns_delimiter);
  ServerConfig(const ServerConfig&) = delete;
  ServerConfig(ServerConfig&&) = delete;
  ServerConfig& operator=(const ServerConfig&) = delete;
  ServerConfig& operator=(ServerConfig&&) = delete;

  // Creates a ServerConfig object from existing cluster name,
  // NodesConfig, LogsConfig, SecurityConfig and an optional ZookeeperConfig
  // instances.
  static std::unique_ptr<ServerConfig> fromData(
      std::string cluster_name,
      NodesConfig nodes,
      MetaDataLogsConfig metadata_logs = MetaDataLogsConfig(),
      PrincipalsConfig = PrincipalsConfig(),
      SecurityConfig securityConfig = SecurityConfig(),
      TrafficShapingConfig = TrafficShapingConfig(),
      ShapingConfig =
          ShapingConfig(std::set<NodeLocationScope>{NodeLocationScope::NODE},
                        std::set<NodeLocationScope>{NodeLocationScope::NODE}),
      SettingsConfig server_settings_config = SettingsConfig(),
      SettingsConfig client_settings_config = SettingsConfig(),
      InternalLogs internal_logs = InternalLogs(),
      OptionalTimestamp clusterCreationTime = OptionalTimestamp(),
      folly::dynamic customFields = folly::dynamic::object,
      const std::string& ns_delimiter =
          LogsConfig::default_namespace_delimiter_);

  std::string clusterName_;
  OptionalTimestamp clusterCreationTime_;

  NodesConfig nodesConfig_;
  MetaDataLogsConfig metaDataLogsConfig_;
  PrincipalsConfig principalsConfig_;
  SecurityConfig securityConfig_;
  SequencersConfig sequencersConfig_;
  TrafficShapingConfig trafficShapingConfig_;
  ShapingConfig readIOShapingConfig_;
  SettingsConfig serverSettingsConfig_;
  SettingsConfig clientSettingsConfig_;
  configuration::InternalLogs internalLogs_;

  std::string ns_delimiter_;

  // version of this config
  config_version_t version_{1};

  NodeID my_node_id_;

  // The server this config was received from. This will be an invalid NodeID if
  // the config did not originate at a server.
  NodeID server_origin_;

  /**
   * Arbitrary fields that logdevice does not recognize
   */
  folly::dynamic customFields_;

  std::string toStringImpl(const LogsConfig* with_logs,
                           const ZookeeperConfig* with_zk) const;
  mutable std::mutex to_string_cache_mutex_;
  mutable std::string all_to_string_cache_; // includes the logs config
  mutable std::string compressed_all_to_string_cache_;
  mutable std::string main_to_string_cache_; // excludes the logs config
  mutable std::string compressed_main_to_string_cache_;
  // The LogsConfig version at the last time toString() was called
  mutable uint64_t last_to_string_logs_config_version_{0};

  // Metadata for the main config
  ConfigMetadata main_config_metadata_;
};

}} // namespace facebook::logdevice
