/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/LocalLogsConfig.h"

#include <folly/dynamic.h>

#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

using facebook::logdevice::logsconfig::LogGroupNode;

namespace facebook { namespace logdevice { namespace configuration {

/**
 * @param alternative_logs_config   an alternative log configuration fetcher,
 *                                  in case log data isn't included in the
 *                                  main config file. If null, log config
 *                                  will be read from the file specified in
 *                                  "include_log_config".
 */
std::shared_ptr<LocalLogsConfig>
LocalLogsConfig::fromJson(const std::string& jsonPiece,
                          const ServerConfig& server_config,
                          LoadFileCallback loadFileCallback,
                          const ConfigParserOptions& options) {
  auto parsed = parser::parseJson(jsonPiece);
  // Make sure the parsed string is actually an object
  if (!parsed.isObject()) {
    ld_error("configuration must be a map");
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return fromJson(parsed, server_config, loadFileCallback, options);
}

std::shared_ptr<LocalLogsConfig>
LocalLogsConfig::fromJson(const folly::dynamic& parsed,
                          const ServerConfig& server_config,
                          LoadFileCallback loadFileCallback,
                          const ConfigParserOptions& options) {
  auto local_logs_config = std::make_shared<LocalLogsConfig>();
  const auto& ns_delimiter = server_config.getNamespaceDelimiter();
  bool success = parser::parseLogs(parsed,
                                   local_logs_config,
                                   server_config.getSecurityConfig(),
                                   loadFileCallback,
                                   ns_delimiter,
                                   options);

  if (!success) {
    return nullptr;
  }
  local_logs_config->setNamespaceDelimiter(ns_delimiter);
  local_logs_config->markAsFullyLoaded();
  return local_logs_config;
}

folly::Optional<std::string>
LocalLogsConfig::getLogGroupPath(logid_t id) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    return folly::none;
  }

  const LogGroupInDirectory* res = getLogGroupInDirectoryByIDRaw(id);
  if (res) {
    return res->getFullyQualifiedName();
  }

  return folly::none;
}

const LocalLogsConfig::LogGroupInDirectory* FOLLY_NULLABLE
LocalLogsConfig::getLogGroupInDirectoryByIDRaw(logid_t id) const {
  const logsconfig::LogGroupInDirectory* res =
      config_tree_->getLogGroupByID(id);

  if (res) {
    return res;
  }

  res = internal_logs_.getLogGroupByID(id);
  if (res) {
    return res;
  }

  return nullptr;
}

bool LocalLogsConfig::logExists(logid_t id) const {
  // Use the masked logid to find the data log in the config because we don't
  // store the metadata logs in the tree
  const logid_t data_log_id = MetaDataLog::dataLogID(id);
  return config_tree_->logExists(data_log_id) ||
      internal_logs_.logExists(data_log_id);
}

std::shared_ptr<LogGroupNode>
LocalLogsConfig::getLogGroupByIDShared(logid_t id) const {
  // LogGroupNode can be both a normal or an internal log
  const logsconfig::LogGroupInDirectory* res =
      config_tree_->getLogGroupByID(id);
  if (!res) {
    res = internal_logs_.getLogGroupByID(id);
  }

  return res ? res->log_group : nullptr;
}

void LocalLogsConfig::getLogGroupByIDAsync(
    logid_t id,
    std::function<void(std::shared_ptr<LogGroupNode>)> cb) const {
  // since LocalLogsConfig does everything locally and without locking,
  // we can just call the callback immediately from here.
  cb(getLogGroupByIDShared(id));
}

void LocalLogsConfig::getLogRangeByNameAsync(
    std::string name,
    std::function<void(Status, logid_range_t)> cb) const {
  std::shared_ptr<logsconfig::LogGroupNode> result =
      config_tree_->findLogGroup(name);
  if (result == nullptr) {
    cb(E::NOTFOUND, std::make_pair(LOGID_INVALID, LOGID_INVALID));
    return;
  }
  cb(E::OK, result->range());
}

std::shared_ptr<logsconfig::LogGroupNode>
LocalLogsConfig::getLogGroupByName(std::string name) const {
  logid_range_t res;
  return config_tree_->findLogGroup(name);
}

std::shared_ptr<logsconfig::LogGroupNode>
LocalLogsConfig::getLogGroup(const std::string& path) const {
  return config_tree_->findLogGroup(path);
}

void LocalLogsConfig::getAllLogRangesfromDirectory(
    DirectoryNode* dir,
    NamespaceRangeLookupMap& res) const {
  if (dir != nullptr) {
    // add all ranges in the current namespace
    for (const auto& iter : dir->logs()) {
      res.insert(std::make_pair(
          iter.second->getFullyQualifiedName(dir), iter.second->range()));
    }
    // loop over all directories in this namespace
    for (const auto& iter : dir->children()) {
      getAllLogRangesfromDirectory(iter.second.get(), res);
    }
  }
}

void LocalLogsConfig::getLogRangesByNamespaceAsync(
    const std::string& ns,
    std::function<void(Status, NamespaceRangeLookupMap)> cb) const {
  ld_check(config_tree_ != nullptr);
  DirectoryNode* dir;
  // an empty string is a special case, we need all directories in the tree
  if (ns.size() == 0 || ns == config_tree_->delimiter()) {
    dir = config_tree_->root();
  } else {
    dir = config_tree_->findDirectory(ns);
  }

  LogsConfig::NamespaceRangeLookupMap res;
  getAllLogRangesfromDirectory(dir, res);

  cb((res.empty() ? E::NOTFOUND : E::OK), res);
}

std::shared_ptr<logsconfig::LogGroupNode>
LocalLogsConfig::insert(DirectoryNode* parent,
                        const logid_range_t& logid_interval,
                        const std::string& name,
                        LogAttributes attrs) {
  ld_check(config_tree_ != nullptr);

  was_modified_in_place_.store(true);

  // we need to figure out if the name has the _delimiter_ in it or not, if it
  // has, then we need to create the intermediate namespaces first or attach
  // the intermediate namespace (if exists).
  // That's why we set (addIntermediateDirectories = true)
  std::string failure_reason;
  auto ret = config_tree_->addLogGroup(parent,
                                       name,
                                       logid_interval,
                                       attrs,
                                       true /* addIntermediateDirectories */,
                                       failure_reason);
  if (!ret) {
    ld_info("Failed to insert log group with interval [%lu,%lu] %s",
            logid_interval.first.val_,
            logid_interval.second.val_,
            failure_reason.c_str());
  }
  return ret;
}

LocalLogsConfig::DirectoryNode*
LocalLogsConfig::insertNamespace(DirectoryNode* parent,
                                 const std::string& name,
                                 const LogAttributes& log_attrs) {
  ld_check(config_tree_ != nullptr);
  was_modified_in_place_.store(true);
  DirectoryNode* actual_parent = parent;
  // if no parent was passed we fallback to the root of the tree
  if (actual_parent == nullptr) {
    parent = config_tree_->root();
  }
  return config_tree_->addDirectory(parent, name, log_attrs);
}

bool LocalLogsConfig::replaceLogGroup(const std::string& path,
                                      const LogGroupNode& new_log_group) {
  ld_check(config_tree_ != nullptr);
  was_modified_in_place_.store(true);
  std::string failure_reason;
  bool ret = config_tree_->replaceLogGroup(path, new_log_group, failure_reason);
  if (!ret) {
    ld_info("Failed to replace LogGroup '%s' %s",
            path.c_str(),
            failure_reason.c_str());
  }
  return ret;
}

std::shared_ptr<logsconfig::LogGroupNode>
LocalLogsConfig::insert(logid_t::raw_type logid,
                        const std::string& name,
                        LogAttributes attrs) {
  ld_check(config_tree_ != nullptr);
  was_modified_in_place_.store(true);
  std::string failure_reason;
  auto ret =
      config_tree_->addLogGroup(name,
                                logid_range_t(logid_t(logid), logid_t(logid)),
                                std::move(attrs),
                                false /* overwrite */,
                                failure_reason);
  if (ret == nullptr) {
    ld_error("Failed to add a log-group: %s, %s",
             error_description(err),
             failure_reason.c_str());
  }
  return ret;
}

std::shared_ptr<logsconfig::LogGroupNode> LocalLogsConfig::insert(
    const boost::icl::right_open_interval<logid_t::raw_type>& logid_interval,
    const std::string& name,
    LogAttributes attrs) {
  ld_check(config_tree_ != nullptr);
  was_modified_in_place_.store(true);
  std::string failure_reason;
  auto added_group = config_tree_->addLogGroup(
      name,
      logid_range_t(
          logid_t(logid_interval.lower()), logid_t(logid_interval.upper() - 1)),
      std::move(attrs),
      true /* addIntermediateDirectories */,
      failure_reason);
  if (!added_group) {
    ld_warning("LogGroup %s was not added because %s, %s",
               name.c_str(),
               error_name(err),
               failure_reason.c_str());
  }
  return added_group;
}

bool LocalLogsConfig::isValid(const ServerConfig& server_config) const {
  return parser::validateNodeCount(server_config, this);
}

size_t LocalLogsConfig::size() const {
  return config_tree_->size() + internal_logs_.size();
}

ReplicationProperty LocalLogsConfig::getNarrowestReplication() {
  if (narrowest_replication_cache_.isEmpty()) {
    narrowest_replication_cache_ = config_tree_->getNarrowestReplication();
    for (auto it = internal_logs_.logsBegin(); it != internal_logs_.logsEnd();
         ++it) {
      narrowest_replication_cache_ = narrowest_replication_cache_.narrowest(
          it->second.log_group->getReplicationProperty());
    }
  }
  return narrowest_replication_cache_;
}

}}} // namespace facebook::logdevice::configuration
