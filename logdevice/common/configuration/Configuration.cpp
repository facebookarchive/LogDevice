/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/Configuration.h"

#include <boost/filesystem.hpp>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ServerConfig.h"

using namespace facebook::logdevice::configuration::parser;
using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

const std::shared_ptr<facebook::logdevice::configuration::LocalLogsConfig>
Configuration::localLogsConfig() const {
  return std::dynamic_pointer_cast<
      facebook::logdevice::configuration::LocalLogsConfig>(logs_config_);
}

const facebook::logdevice::configuration::LocalLogsConfig&
Configuration::getLocalLogsConfig() const {
  return *localLogsConfig().get();
}

Configuration::Configuration(
    std::shared_ptr<ServerConfig> server_config,
    std::shared_ptr<LogsConfig> logs_config,
    std::shared_ptr<facebook::logdevice::configuration::ZookeeperConfig>
        zookeeper_config)
    : server_config_(std::move(server_config)),
      logs_config_(std::move(logs_config)),
      zookeeper_config_(std::move(zookeeper_config)) {}

std::shared_ptr<LogsConfig::LogGroupNode>
Configuration::getLogGroupByIDShared(logid_t id) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    return server_config_->getMetaDataLogGroup();
  } else if (configuration::InternalLogs::isInternal(id)) {
    const auto raw_directory =
        server_config_->getInternalLogsConfig().getLogGroupByID(id);
    return raw_directory != nullptr ? raw_directory->log_group : nullptr;
  } else {
    return logs_config_->getLogGroupByIDShared(id);
  }
}

const LogsConfig::LogGroupInDirectory*
Configuration::getLogGroupInDirectoryByIDRaw(logid_t id) const {
  // raw access is only supported by the local config.
  ld_check(logs_config_->isLocal());
  if (MetaDataLog::isMetaDataLog(id)) {
    return &server_config_->getMetaDataLogGroupInDir();
  } else if (configuration::InternalLogs::isInternal(id)) {
    return server_config_->getInternalLogsConfig().getLogGroupByID(id);
  } else {
    return localLogsConfig()->getLogGroupInDirectoryByIDRaw(id);
  }
}

void Configuration::getLogGroupByIDAsync(
    logid_t id,
    std::function<void(std::shared_ptr<LogsConfig::LogGroupNode>)> cb) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    cb(server_config_->getMetaDataLogGroup());
    return;
  } else if (configuration::InternalLogs::isInternal(id)) {
    const auto raw_directory =
        server_config_->getInternalLogsConfig().getLogGroupByID(id);
    cb(raw_directory != nullptr ? raw_directory->log_group : nullptr);
    return;
  } else {
    logs_config_->getLogGroupByIDAsync(id, cb);
  }
}

folly::Optional<std::string> Configuration::getLogGroupPath(logid_t id) const {
  ld_check(logs_config_->isLocal());
  return localLogsConfig()->getLogGroupPath(id);
}

std::chrono::seconds Configuration::getMaxBacklogDuration() const {
  ld_check(logs_config_->isLocal());
  return localLogsConfig()->getMaxBacklogDuration();
}

folly::dynamic stringToJsonObj(const std::string& json) {
  auto parsed = parseJson(json);
  // Make sure the parsed string is actually an object
  if (!parsed.isObject()) {
    ld_error("configuration must be a map");
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return parsed;
}

std::unique_ptr<Configuration> Configuration::fromJson(
    const std::string& jsonPiece,
    std::shared_ptr<LogsConfig> alternative_logs_config,
    std::function<Status(const char*, std::string*)> loadFileCallback,
    const ConfigParserOptions& options) {
  auto parsed = stringToJsonObj(jsonPiece);
  if (parsed == nullptr || jsonPiece.empty()) {
    return nullptr;
  }
  return fromJson(parsed, alternative_logs_config, loadFileCallback, options);
}

std::unique_ptr<Configuration> Configuration::fromJson(
    const folly::dynamic& parsed,
    std::shared_ptr<LogsConfig> alternative_logs_config,
    std::function<Status(const char*, std::string*)> loadFileCallback,
    const ConfigParserOptions& options) {
  auto server_config = ServerConfig::fromJson(parsed);
  if (!server_config) {
    // hopefully fromJson will correctly set err to INVALID_CONFIG
    return nullptr;
  }

  // Try to parse the Zookeeper section, but it is only required on servers
  std::unique_ptr<ZookeeperConfig> zookeeper_config;
  auto iter = parsed.find("zookeeper");
  if (iter != parsed.items().end()) {
    const folly::dynamic& zookeeperSection = iter->second;
    zookeeper_config = ZookeeperConfig::fromJson(zookeeperSection);
  }

  std::shared_ptr<LogsConfig> logs_config = nullptr;
  if (alternative_logs_config) {
    logs_config = alternative_logs_config;
  } else {
    auto cb = !alternative_logs_config ? loadFileCallback
                                       : LogsConfig::LoadFileCallback();
    auto local_logs_config =
        LocalLogsConfig::fromJson(parsed, *server_config, cb, options);
    if (!local_logs_config) {
      if (err != E::LOGS_SECTION_MISSING) {
        // we don't want to mangle the err if it's LOGS_SECTION_MISSING because
        // TextConfigUpdater uses this to decide whether to auto enable
        // logsconfig manager or not. Otherwise set the err to INVALID_CONFIG
        err = E::INVALID_CONFIG;
      }
      // We couldn't not parse the logs/defaults section of the config, we will
      // return the nullptr logsconfig.
      return std::make_unique<Configuration>(
          std::move(server_config), nullptr, std::move(zookeeper_config));
    }
    local_logs_config->setInternalLogsConfig(
        server_config->getInternalLogsConfig());

    if (local_logs_config->isValid(*server_config)) {
      // This is a fully loaded config, so we mark it as one.
      local_logs_config->markAsFullyLoaded();
      logs_config = std::move(local_logs_config);
    } else {
      return std::make_unique<Configuration>(
          std::move(server_config), nullptr, std::move(zookeeper_config));
    }
  }
  // Ensure the logs config knows about the namespace delimiter (which is
  // specified in the server config).
  logs_config->setNamespaceDelimiter(server_config->getNamespaceDelimiter());

  return std::make_unique<Configuration>(
      std::move(server_config), logs_config, std::move(zookeeper_config));
}

std::unique_ptr<Configuration>
Configuration::fromJsonFile(const char* path,
                            std::unique_ptr<LogsConfig> alternative_logs_config,
                            const ConfigParserOptions& options) {
  // Extracting prefix from the path for referencing any files that would be
  // includable from the main config
  boost::filesystem::path path_prefix(path);
  path_prefix.remove_filename();

  std::string json_blob = readFileIntoString(path);
  if (json_blob.size() == 0) {
    return nullptr;
  }
  auto parsed = stringToJsonObj(json_blob);
  return fromJson(parsed,
                  std::move(alternative_logs_config),
                  [path_prefix](const char* filename, std::string* out) {
                    std::string full_path = filename[0] == '/'
                        ? filename
                        : (path_prefix / filename).string();
                    out->assign(readFileIntoString(full_path.c_str()));
                    return out->empty() ? err : E::OK;
                  },
                  options);
}

std::unique_ptr<Configuration>
Configuration::loadFromString(const std::string& server,
                              const std::string& logs) {
  std::shared_ptr<ServerConfig> server_config;
  std::shared_ptr<LocalLogsConfig> logs_config;
  std::shared_ptr<ZookeeperConfig> zookeeper_config;

  auto parsed = parseJson(server);
  if (!parsed.isObject()) {
    return nullptr;
  }

  server_config = ServerConfig::fromJson(parsed);
  if (server_config) {
    auto zookeeper = parsed.find("zookeeper");
    if (zookeeper != parsed.items().end()) {
      zookeeper_config = ZookeeperConfig::fromJson(zookeeper->second);
      if (!zookeeper_config) {
        return nullptr;
      }
    }

    logs_config = LocalLogsConfig::fromJson(server,
                                            *server_config,
                                            [&](const char*, std::string* out) {
                                              out->assign(logs);
                                              return E::OK;
                                            },
                                            ConfigParserOptions());
    if (logs_config && logs_config->isValid(*server_config)) {
      return std::make_unique<Configuration>(
          server_config, logs_config, zookeeper_config);
    }
  }
  return nullptr;
}

int Configuration::validateJson(const char* server_config_contents,
                                const char* logs_config_contents) {
  auto config = loadFromString(server_config_contents, logs_config_contents);
  return (config && (bool)config->logsConfig()) ? 0 : -1;
}

std::string Configuration::normalizeJson(const char* server_config_contents,
                                         const char* logs_config_contents) {
  auto config = loadFromString(server_config_contents, logs_config_contents);
  if (config) {
    return config->toString();
  }
  return "";
}
std::string Configuration::toString() const {
  if (server_config_) {
    return server_config_->toString(
        logs_config_.get(), zookeeper_config_.get());
  }
  return "";
}
}} // namespace facebook::logdevice
