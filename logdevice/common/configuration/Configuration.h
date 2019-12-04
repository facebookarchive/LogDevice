/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <mutex>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfigTmpl.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice {

namespace configuration {
class LocalLogsConfig;
}

/**
 * structure that holds options altering the behavior of the config parser.
 */
struct ConfigParserOptions {};

/**
 * Represents a parsed config file.  A config file contains configuration for
 * one LogDevice cluster.  A cluster is configured with a set of logs, a list
 * of participating nodes, and other infrequently changing metadata.
 */

template <class Config>
class NoopOverrides {
 public:
  std::shared_ptr<Config> apply(const std::shared_ptr<Config>& base_cfg) {
    return base_cfg;
  }
};

using UpdateableServerConfig =
    configuration::UpdateableConfigTmpl<ServerConfig, ServerConfig::Overrides>;

using UpdateableLogsConfig =
    configuration::UpdateableConfigTmpl<LogsConfig, NoopOverrides<LogsConfig>>;

using facebook::logdevice::configuration::ZookeeperConfig;
using UpdateableZookeeperConfig =
    configuration::UpdateableConfigTmpl<ZookeeperConfig,
                                        NoopOverrides<ZookeeperConfig>>;

using configuration::nodes::NodesConfiguration;
using UpdateableNodesConfiguration = configuration::UpdateableConfigTmpl<
    const NodesConfiguration,
    NoopOverrides<const NodesConfiguration>>;

class Configuration {
 public:
  using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
  using Node = facebook::logdevice::configuration::Node;
  using NodeRole = facebook::logdevice::configuration::NodeRole;
  using Nodes = facebook::logdevice::configuration::Nodes;
  using NodesConfig = facebook::logdevice::configuration::NodesConfig;
  using SecurityConfig = facebook::logdevice::configuration::SecurityConfig;
  using SequencersConfig = facebook::logdevice::configuration::SequencersConfig;
  using TraceLoggerConfig =
      facebook::logdevice::configuration::TraceLoggerConfig;
  using TrafficShapingConfig =
      facebook::logdevice::configuration::TrafficShapingConfig;
  using MetaDataLogsConfig =
      facebook::logdevice::configuration::MetaDataLogsConfig;

  Configuration(std::shared_ptr<ServerConfig> server_config,
                std::shared_ptr<LogsConfig> logs_config,
                std::shared_ptr<ZookeeperConfig> zookeeper_config = nullptr);

  /**
   * NOTE: This returns a *reference* to the shared_ptr, but the shared_ptr and
   * the underlying ServerConfig are guaranteed to to last at least as long as
   * the Configuration object, even if the user doesn't make a copy of the
   * shared_ptr.
   */
  const std::shared_ptr<ServerConfig>& serverConfig() const {
    return server_config_;
  }

  const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
  getNodesConfigurationFromServerConfigSource() const {
    return server_config_->getNodesConfigurationFromServerConfigSource();
  }

  /**
   * NOTE: This returns a *reference* to the shared_ptr, but the shared_ptr and
   * the underlying LogsConfig are guaranteed to to last at least as long as
   * the Configuration object, even if the user doesn't make a copy of the
   * shared_ptr.
   */
  const std::shared_ptr<LogsConfig>& logsConfig() const {
    return logs_config_;
  }

  /**
   * NOTE: This returns a *reference* to the shared_ptr, but the shared_ptr and
   * the underlying ZookeeperConfig are guaranteed to to last at least as
   * long as the Configuration object, even if the user doesn't make a
   * copy of the shared_ptr. This pointer might be nullptr in some cases.
   */
  const std::shared_ptr<ZookeeperConfig>& zookeeperConfig() const {
    return zookeeper_config_;
  }

  // Helper to convert logs config into LocalLogsConfig
  const std::shared_ptr<facebook::logdevice::configuration::LocalLogsConfig>
  localLogsConfig() const;

  /**
   * [DEPRECATED] Please use localLogsConfig() instead.
   * Exposes the LogsConfig structure cast into LocalLogsConfig, for testing
   * and server-side access.
   */
  const facebook::logdevice::configuration::LocalLogsConfig&
  getLocalLogsConfig() const;

  /**
   * Returns a full path of a LogGroup where the supplied logid belongs to.
   * This returns folly::none if logid was not found.
   * This method should only be used when we can ensure the LogsConfig is local
   * i.e. on the server and in tests.
   */
  folly::Optional<std::string> getLogGroupPath(logid_t id) const;

  /**
   * Looks up a log by ID and returns a shared pointer to the LogGroupNode
   * object. Use this on the client when you don't care about blocking the
   * current thread.  Note that this method should not be called on a worker
   * thread on a client.
   *
   * @return On success, returns a pointer to a Log object contained in
   *         this config.  On failure, returns nullptr and sets err to:
   *           NOTFOUND       no log with given ID appears in config
   */
  std::shared_ptr<LogsConfig::LogGroupNode>
  getLogGroupByIDShared(logid_t id) const;

  /**
   * Looks up a log by ID and returns a raw pointer to the LogGroupInDirectory
   * object. This can be used to derive both the LogGroup properties and its
   * (absolute) path within the directory.
   * This method should only be used when we can ensure the LogsConfig is local
   * i.e. on the server and in tests.
   *
   * @return On success, returns a pointer to a LogGroupInDirectory object
   *         contained in this config.  On failure, returns nullptr and sets
   *         err to:
   *           NOTFOUND       no log with given ID appears in config
   */
  const LogsConfig::LogGroupInDirectory*
  getLogGroupInDirectoryByIDRaw(logid_t id) const;

  /**
   * Looks up a log by ID and calls the supplied callback. This should be the
   * method used for getting log config data on the client in most cases. If
   * called on a Worker thread, the callback will be invoked on the same
   * Worker. The callback can be invoked synchronously if the logs config is
   * immediately available. If the requested log ID is not in the config, the
   * callback will be invoked with a nullptr
   *
   * @param id ID of the log
   * @param cb callback to pass the LogGroupNode struct to
   */
  void getLogGroupByIDAsync(
      logid_t id,
      std::function<void(std::shared_ptr<LogsConfig::LogGroupNode>)> cb) const;

  /**
   * Creates a ServerConfig object from the given file.
   *
   * NOTE: Only used in tests.  Production config loading flows go through
   * TextConfigUpdater.
   *
   * @param path                      path to file containing JSON-formatted
   *                                  config
   * @param alternative_logs_config   an alternative log configuration fetcher,
   *                                  in case log data isn't included in the
   *                                  main config file. If null, log config
   *                                  will be read from the file specified in
   *                                  "include_log_config".
   * @return On success, returns a new ServerConfig instance.  On
   * failure, returns nullptr and sets err to:
   *           FILE_OPEN       file could not be opened
   *           FILE_READ       error reading the file
   *           INVALID_CONFIG  various errors in parsing the config
   */
  static std::unique_ptr<Configuration>
  fromJsonFile(const char* path,
               std::unique_ptr<LogsConfig> alternative_logs_config = nullptr,
               const ConfigParserOptions& options = ConfigParserOptions());

  static std::unique_ptr<Configuration>
  fromJson(const std::string& jsonPiece,
           std::shared_ptr<LogsConfig> alternative_logs_config,
           std::function<Status(const char*, std::string*)> loadFileCallback,
           const ConfigParserOptions& options = ConfigParserOptions());

  static std::unique_ptr<Configuration>
  fromJson(const folly::dynamic& jsonPiece,
           std::shared_ptr<LogsConfig> alternative_logs_config,
           std::function<Status(const char*, std::string*)> loadFileCallback,
           const ConfigParserOptions& options = ConfigParserOptions());

  /**
   * Attempts to parse config contents from a string containing JSON.
   *
   * @param contents            contents of the main config.
   * @param log_config_contents if the main config contains an
   *                            "include_config_log" statement, this param
   *                            should contain the log config contents.
   * @return On success, returns 0 (a ServerConfig object can be created
   * from the config).  If the config is invalid, returns -1.
   */
  static int validateJson(const char* server_config_contents,
                          const char* logs_config_contents);

  /**
   * Parses config contents from a string containing JSON, and formats it
   * back into JSON in a deterministic way. The output JSON is easier to
   * interpret because it doesn't have "defaults" section,
   * and has all default values explicitly given for missing attributes.
   *
   * @return On success, returns a string with JSON config.
   * If the config is invalid, returns empty string.
   * (note: empty string is not a valid config)
   */
  static std::string normalizeJson(const char* contents,
                                   const char* log_config_contents);

  /**
   * This method should only be used when we can ensure the LogsConfig is local
   * i.e. on the server and in tests.
   */
  std::chrono::seconds getMaxBacklogDuration() const;

  std::string toString() const;
  std::unique_ptr<Configuration> copy() const {
    return std::make_unique<Configuration>(
        server_config_->copy(), logs_config_->copy());
  }

 private:
  static std::unique_ptr<Configuration>
  loadFromString(const std::string& server, const std::string& logs);
  const std::shared_ptr<ServerConfig> server_config_;
  const std::shared_ptr<LogsConfig> logs_config_;
  const std::shared_ptr<ZookeeperConfig> zookeeper_config_;
};
}} // namespace facebook::logdevice
