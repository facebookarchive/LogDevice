/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include <boost/container/flat_map.hpp>

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfigIterator.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice {
struct ConfigParserOptions;
}} // namespace facebook::logdevice
/**
 * @file A container for logs config that is fully stored locally, i.e.
 *       fully available to this node. Loads the config from the file and
 *       serves it from memory.
 */

namespace facebook { namespace logdevice { namespace configuration {

class LocalLogsConfig : public LogsConfig {
 public:
  using LogsConfigTree = facebook::logdevice::logsconfig::LogsConfigTree;
  using DirectoryNode = facebook::logdevice::logsconfig::DirectoryNode;
  using LogGroupInDirectory =
      facebook::logdevice::logsconfig::LogGroupInDirectory;
  using LogGroupNode = facebook::logdevice::logsconfig::LogGroupNode;
  using DefaultLogAttributes =
      facebook::logdevice::logsconfig::DefaultLogAttributes;
  using LogMap = facebook::logdevice::logsconfig::LogMap;
  using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
  using Iterator = LocalLogsConfigIterator;
  using ReverseIterator = LocalLogsConfigReverseIterator;

  bool isLocal() const override {
    return true;
  }

  uint64_t getVersion() const override {
    return config_tree_->version();
  }

  static std::shared_ptr<LocalLogsConfig>
  fromJson(const std::string& jsonPiece,
           const ServerConfig& server_config,
           LoadFileCallback loadFileCallback,
           const ConfigParserOptions& options);

  static std::shared_ptr<LocalLogsConfig>
  fromJson(const folly::dynamic& parsed,
           const ServerConfig& server_config,
           LoadFileCallback loadFileCallback,
           const ConfigParserOptions& options);

  std::shared_ptr<LogGroupNode>
  getLogGroupByIDShared(logid_t id) const override;

  void getLogGroupByIDAsync(
      logid_t id,
      std::function<void(std::shared_ptr<LogGroupNode>)> cb) const override;

  bool logExists(logid_t id) const override;

  void getLogRangeByNameAsync(
      std::string name,
      std::function<void(Status, logid_range_t)> cb) const override;

  std::shared_ptr<LogGroupNode> getLogGroupByName(std::string name) const;

  std::shared_ptr<logsconfig::LogGroupNode>
  getLogGroup(const std::string& path) const override;

  void getLogRangesByNamespaceAsync(
      const std::string& ns,
      std::function<void(Status, NamespaceRangeLookupMap)> cb) const override;

  std::unique_ptr<LocalLogsConfig> copyLocal() const {
    return std::make_unique<LocalLogsConfig>(*this);
  }
  std::unique_ptr<LogsConfig> copy() const override {
    return copyLocal();
  }

  const LogGroupInDirectory* getLogGroupInDirectoryByIDRaw(logid_t id) const;

  folly::Optional<std::string> getLogGroupPath(logid_t id) const;

  std::chrono::seconds getMaxBacklogDuration() const {
    return config_tree_->getMaxBacklogDuration();
  }

  void setInternalLogsConfig(const InternalLogs& internal_logs) {
    internal_logs_ = internal_logs;
    // invalidate the narrowest replication cache
    narrowest_replication_cache_.clear();
  }

  const InternalLogs& getInternalLogs() const {
    return internal_logs_;
  }

  LocalLogsConfigIterator logsBegin() const {
    return LocalLogsConfigIterator(
        &internal_logs_, config_tree_.get(), config_tree_->logsBegin(), false);
  }

  LocalLogsConfigIterator logsEnd() const {
    return LocalLogsConfigIterator(
        &internal_logs_, config_tree_.get(), internal_logs_.logsEnd(), true);
  }

  LocalLogsConfigReverseIterator logsRBegin() const {
    return LocalLogsConfigReverseIterator(
        &internal_logs_, config_tree_.get(), config_tree_->logsRBegin(), false);
  }

  LocalLogsConfigReverseIterator logsREnd() const {
    return LocalLogsConfigReverseIterator(
        &internal_logs_, config_tree_.get(), internal_logs_.logsREnd(), true);
  }

  std::shared_ptr<logsconfig::LogGroupNode>
  insert(DirectoryNode* parent,
         const logid_range_t& logid_interval,
         const std::string& name,
         LogAttributes attrs = LogAttributes());

  // [DEPRECATED] used only in test cases
  std::shared_ptr<logsconfig::LogGroupNode> insert(
      const boost::icl::right_open_interval<logid_t::raw_type>& logid_interval,
      const std::string& name,
      LogAttributes attrs);

  // [DEPRECATED] exists only till Log is removed
  std::shared_ptr<logsconfig::LogGroupNode> insert(logid_t::raw_type logid,
                                                   const std::string& name,
                                                   LogAttributes attrs);
  // adds a new namespace in the tree
  DirectoryNode* insertNamespace(DirectoryNode* parent,
                                 const std::string& name,
                                 const LogAttributes& log_attrs);

  // [Used only for test cases]
  // replaces a log group node in the config tree
  bool replaceLogGroup(const std::string& path,
                       const LogGroupNode& new_log_group);

  bool isValid(const ServerConfig& server_config) const;

  const LogMap& getLogMap() const {
    return config_tree_->getLogMap();
  }

  size_t size() const;

  // returns true only if the underlying tree was initialized
  bool hasLogsConfigTree() const {
    return config_tree_ != nullptr;
  }
  // replaces the underlying logsconfig tree
  void setLogsConfigTree(std::unique_ptr<LogsConfigTree> tree) {
    config_tree_ = std::move(tree);
    // invalidate the narrowest replication cache
    narrowest_replication_cache_.clear();
  }
  const LogsConfigTree& getLogsConfigTree() const {
    return *config_tree_;
  }
  DirectoryNode* getRootNamespace() const {
    ld_check(config_tree_ != nullptr);
    return config_tree_->root();
  }

  // deletes the underlying tree
  void clearLogsConfigTree() {
    config_tree_ = nullptr;
    // invalidate the narrowest replication cache
    narrowest_replication_cache_.clear();
  }

  const std::string& getDelimiter() const {
    ld_check(config_tree_ != nullptr);
    return config_tree_->delimiter();
  }

  bool isFullyLoaded() const override {
    return is_full_config_;
  }

  void markAsFullyLoaded() {
    is_full_config_ = true;
  }

  /**
   * Returns the most restrictive replication policy in the underlying Log
   * Tree, this takes into account the replication policy for the internal logs
   * as well.
   */
  ReplicationProperty getNarrowestReplication();

  // a shortcut that returns the pretty string generated by the underlying tree.
  // This is mainly used for debugging to see a pretty representation of the
  // logs config tree.
  std::string print() const {
    std::stringstream stream;
    stream << config_tree_->print();
    stream << " ** INTERNAL LOGS ** " << std::endl;
    for (auto it = internal_logs_.logsBegin(); it != internal_logs_.logsEnd();
         ++it) {
      stream << it->first << "  " << it->second.log_group->name() << std::endl;
    }
    return stream.str();
  }

  /**
   * Can only be true in tests. Used by ServerConfig to prevent incorrect
   * caching.
   */
  bool wasModifiedInPlace() const {
    return was_modified_in_place_;
  }

  // by default if no delimiter is passed, we used "/" as the namespace
  // delimiter
  LocalLogsConfig() : config_tree_(LogsConfigTree::create()) {}
  LocalLogsConfig(const LocalLogsConfig& other)
      : LogsConfig(other),
        config_tree_(other.config_tree_->copy()),
        internal_logs_(other.internal_logs_),
        is_full_config_(other.is_full_config_),
        was_modified_in_place_(other.was_modified_in_place_.load()) {}
  LocalLogsConfig& operator=(const LocalLogsConfig& other) = delete;

 private:
  std::unique_ptr<LogsConfigTree> config_tree_;

  InternalLogs internal_logs_;
  ReplicationProperty narrowest_replication_cache_;
  bool is_full_config_ = false;

  // True if this LogsConfig was ever modified in place. Used in tests.
  // Modifying LogsConfig in place breaks the assumption that getVersion()
  // uniquely defines the config contents. ServerConfig::toString() makes that
  // assumption.
  // Note that bumping version on modificaion would be unsafe.
  std::atomic<bool> was_modified_in_place_{false};

  void getAllLogRangesfromDirectory(DirectoryNode* dir,
                                    NamespaceRangeLookupMap& res) const;
};
}}} // namespace facebook::logdevice::configuration
