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

#include <folly/dynamic.h>
#include <folly/Memory.h>
#include <folly/Optional.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/Log.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/NodesConfig.h"
#include "logdevice/common/configuration/PrincipalsConfig.h"
#include "logdevice/common/configuration/SequencersConfig.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/configuration/TraceLoggerConfig.h"
#include "logdevice/common/configuration/TrafficShapingConfig.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
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
  using PrincipalsConfig = facebook::logdevice::configuration::PrincipalsConfig;
  using SecurityConfig = facebook::logdevice::configuration::SecurityConfig;
  using SequencersConfig = facebook::logdevice::configuration::SequencersConfig;
  using TraceLoggerConfig =
      facebook::logdevice::configuration::TraceLoggerConfig;
  using TrafficShapingConfig =
      facebook::logdevice::configuration::TrafficShapingConfig;
  using ZookeeperConfig = facebook::logdevice::configuration::ZookeeperConfig;
  using SettingsConfig = std::unordered_map<std::string, std::string>;
  using OptionalTimestamp = folly::Optional<std::chrono::seconds>;
  using InternalLogs = facebook::logdevice::configuration::InternalLogs;

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

  // Return the number of shards in storage nodes of that cluster or zero if
  // there are no storage nodes.
  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  shard_size_t getNumShards() const {
    return nodesConfig_.getNumShards();
  }

  /**
   * Gets nodes config hash.
   */
  uint64_t getStorageNodesConfigHash() const {
    return nodesConfig_.getStorageNodeHash();
  }

  /**
   * Returns the maximum key in getNodes().
   */
  size_t getMaxNodeIdx() const {
    size_t r = 0;
    for (const auto& it : nodesConfig_.getNodes()) {
      r = std::max(r, (size_t)it.first);
    }
    return r;
  }

  /**
   * Returns a description of sequencer nodes in the cluster.
   */
  const SequencersConfig& getSequencers() const {
    return sequencersConfig_;
  }

  /**
   * Check if nodeset is valid with the given replication property and
   * nodes config:
   *    1) there are enough non-zero weight nodes in the nodeset to satisfy
   *       replication property,
   *    2) (if strict == true) all nodes in nodeset are present in config and
   *       are storage nodes.
   */
  static bool validStorageSet(const Nodes& cluster_nodes,
                              const StorageSet& storage_set,
                              ReplicationProperty replication,
                              bool strict = false);

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
   * Gets the Zookeeper configuration
   */
  const ZookeeperConfig& getZookeeperConfig() const {
    return zookeeperConfig_;
  }

  /**
   * @return if config has a zookeeper section, returns a comma-separated list
   *         of ip:ports of all ZK servers listed in it. Otherwise returns an
   *         empty string.
   */
  std::string getZookeeperQuorumString() const;

  /**
   * @return if config has a zookeeper section, returns the session timeout
   *         specified there, guaranteed to be positive. Otherwise returns 0.
   */
  std::chrono::milliseconds getZookeeperTimeout() const {
    return zookeeperConfig_.session_timeout;
  }

  /**
   * Returns the NodeID of the server that we are running.  (The NodeID needs
   * to have been previously set via a call to setMyNodeID().  This is
   * typically done right after the config is created.)
   *
   * Must not be called in client code.
   */
  NodeID getMyNodeID() const {
    ld_check(Address(my_node_id_).valid());
    return my_node_id_;
  }

  /**
   * True if a valid NodeID was set using setMyNodeID().
   */
  bool hasMyNodeID() const {
    return Address(my_node_id_).valid();
  }

  /**
   * Sets the node ID to be returned by getMyNodeID().
   */
  void setMyNodeID(const NodeID& id) {
    ld_check(Address(id).valid());
    my_node_id_ = id;
  }

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
   * Looks up the NodeID by its address.
   *
   * @param address   address of the node
   * @param node_id   NodeID to update if address is found in the config
   *
   * @return Returns 0 on success. Otherwise, returns -1 with err set to
   *         NOTFOUND.
   */
  int getNodeID(const Sockaddr& address, NodeID* node_id) const;

  /**
   * Looks up the sampling percentage for a certain tracer in the config
   *
   * @return Returns sampling percentage when an override has been found,
   *         folly::none otherwise
   */
  folly::Optional<double>
  getTracerSamplePercentage(const std::string& key) const;

  /**
   * @return the global default sampling percentage
   */
  double getDefaultSamplePercentage() const;

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
  };

  void setMainConfigMetadata(const ConfigMetadata& metadata) {
    main_config_metadata_ = metadata;
  }
  void setIncludedConfigMetadata(const ConfigMetadata& metadata) {
    included_config_metadata_ = metadata;
  }

  /**
   * Expose the metadata of the main config and included config (URI, hash,
   * last modified, last loaded).
   *
   * If there is no included config, all fields in the included config metadata
   * will be uninitialized.
   */
  const ConfigMetadata& getMainConfigMetadata() const {
    return main_config_metadata_;
  }
  const ConfigMetadata& getIncludedConfigMetadata() const {
    return included_config_metadata_;
  }

  /**
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

  /**
   * Creates a ServerConfig object from existing cluster name,
   * NodesConfig, LogsConfig, SecurityConfig and an optional ZookeeperConfig
   * instances. Public for testing.
   */
  static std::unique_ptr<ServerConfig>
  fromData(std::string cluster_name,
           NodesConfig nodes,
           MetaDataLogsConfig metadata_logs = MetaDataLogsConfig(),
           PrincipalsConfig = PrincipalsConfig(),
           SecurityConfig securityConfig = SecurityConfig(),
           TraceLoggerConfig trace_config = TraceLoggerConfig(),
           TrafficShapingConfig = TrafficShapingConfig(),
           ZookeeperConfig zookeeper = ZookeeperConfig(),
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
   * Returns a clone of the ServerConfig object with the Zookeeper section
   * replaced by the parameter.
   */
  std::shared_ptr<ServerConfig> withZookeeperConfig(ZookeeperConfig zk) const;

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
   * Helper method for determining whether we need to use SSL for connection
   * to a node. The diff_level specifies the level in the location hierarchy,
   * where, if a difference is encountered, we should use SSL. For instance,
   * if diff_level == NodeLocationScope::RACK, the method will return true
   * for any node that is in a rack different to my_location's, and return
   * false otherwise.
   *
   * @param my_location   local NodeLocation
   * @param node          ID of the node we are connecting to
   * @param diff_level    The scope of NodeLocation to compare
   */
  bool getNodeSSL(folly::Optional<NodeLocation> my_location,
                  NodeID node,
                  NodeLocationScope diff_level) const;

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
                             bool compress = false) const;
  folly::dynamic toJson(const LogsConfig* with_logs = nullptr) const;

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
               TraceLoggerConfig traceLoggerConfig,
               TrafficShapingConfig trafficShapingConfig,
               ZookeeperConfig zookeeperConfig,
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

  std::string clusterName_;
  OptionalTimestamp clusterCreationTime_;
  NodesConfig nodesConfig_;
  MetaDataLogsConfig metaDataLogsConfig_;
  PrincipalsConfig principalsConfig_;
  SecurityConfig securityConfig_;
  SequencersConfig sequencersConfig_;
  TrafficShapingConfig trafficShapingConfig_;
  TraceLoggerConfig traceLoggerConfig_;
  ZookeeperConfig zookeeperConfig_; // .quorum is empty if "zookeeper" section
                                    // is not present in the config
  SettingsConfig serverSettingsConfig_;
  SettingsConfig clientSettingsConfig_;
  configuration::InternalLogs internalLogs_;

  std::string ns_delimiter_;
  // mapping from address to the index in the nodes vector
  std::unordered_map<Sockaddr, node_index_t, Sockaddr::Hash> addrToIndex_;

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

  std::string toStringImpl(const LogsConfig* with_logs) const;
  mutable std::mutex to_string_cache_mutex_;
  mutable std::string all_to_string_cache_; // includes the logs config
  mutable std::string compressed_all_to_string_cache_;
  mutable std::string main_to_string_cache_; // excludes the logs config
  mutable std::string compressed_main_to_string_cache_;
  // The LogsConfig version at the last time toString() was called
  mutable uint64_t last_to_string_logs_config_version_{0};

  // Metadata for the main config and included config
  ConfigMetadata main_config_metadata_;
  ConfigMetadata included_config_metadata_;
};

}} // namespace facebook::logdevice
