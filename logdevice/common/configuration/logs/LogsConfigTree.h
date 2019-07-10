/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <boost/icl/interval_map.hpp>
#include <boost/icl/map.hpp>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/dynamic.h>

#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/logs/CodecType.h"
#include "logdevice/common/configuration/logs/DefaultLogAttributes.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace logsconfig {

class DirectoryNode;
class LogGroupNode;
struct LogGroupInDirectory;

class RenameDelta;

// the map is over logid_t's raw (integral) type so that we can
// iterate over individual logids rather than segments. logid_t doesn't
// have a ++.
//
// NOTE: by default interval_map swallows ("absorbs") default values
// (Log()).  You can try to understand why here:
// http://www.boost.org/doc/libs/1_55_0/libs/icl/doc/html/boost_icl/concepts/map_traits.html
// The partial_enricher behaviour allows to store default Log values, which
// is convenient for testing.
//
// This map is only an index and it gets updated when new log groups get
// added/deleted. It doesn't own the memory of the LogGroup (the ownership of
// the LogGroupNode is managed by shared_ptr in all of its parents (assuming
// that a given LogGroupNode will be immutably shared across multiple trees))
using LogMap = boost::icl::interval_map<
    logid_t::raw_type,
    // The value of this map is the LogGroup and it's parent directory pointer
    // This is encapsulated in the LogGroupInDirectory data structure
    LogGroupInDirectory,
    boost::icl::partial_enricher,
    std::less,
    boost::icl::inplace_plus,
    boost::icl::inter_section,
    boost::icl::right_open_interval<logid_t::raw_type, std::less>>;

enum class NodeType { DIRECTORY = 0, LOG_GROUP };

/*
 * An abstract parent for the configuration tree of logs
 */
class LogsConfigTreeNode {
 public:
  explicit LogsConfigTreeNode(const LogAttributes& attrs = LogAttributes())
      : attrs_(attrs) {}
  LogsConfigTreeNode(const std::string& name, const LogAttributes& attrs)
      : name_(name), attrs_(attrs) {}

  virtual NodeType type() const = 0;

  virtual const std::string& name() const {
    return name_;
  }
  // Returns a the attributes associated with this node
  virtual const LogAttributes& attrs() const {
    return attrs_;
  }

  virtual ReplicationProperty getReplicationProperty() const {
    return ReplicationProperty::fromLogAttributes(attrs_);
  }

  virtual ~LogsConfigTreeNode() {}

 protected:
  std::string name_;
  LogAttributes attrs_;
};

// This is the map that holds the children of a directory. A parent directory is
// the owner of the memory for his children directory.
using DirectoryMap =
    folly::F14FastMap<std::string, std::unique_ptr<DirectoryNode>>;

using LogGroupMap =
    folly::F14FastMap<std::string, std::shared_ptr<LogGroupNode>>;
/*
 * A node in the tree of logs config representing a directory (aka. Namespace)
 */
class DirectoryNode : public LogsConfigTreeNode {
 public:
  friend class LogsConfigTree;
  friend class RenameDelta;
  template <CodecType T>
  friend class LogsConfigCodec;

  explicit DirectoryNode(const std::string& delimiter)
      : delimiter_(delimiter) {}

  // The copy constructor, this ensures that the directory tree is deeply copied
  // by pointers for an effective snapshotting of the tree.
  DirectoryNode(const DirectoryNode& other, DirectoryNode* parent);

  // copy constructor (should generally be avoided)
  DirectoryNode(const DirectoryNode& other);

  DirectoryNode(const std::string& delimiter,
                const LogAttributes& attrs,
                DirectoryNode* parent = nullptr)
      : LogsConfigTreeNode(attrs), parent_(parent), delimiter_(delimiter) {}

  DirectoryNode(const std::string& name,
                DirectoryNode* parent,
                const LogAttributes& attrs,
                const std::string& delimiter)
      : LogsConfigTreeNode(name, attrs),
        parent_(parent),
        delimiter_(delimiter) {}

  DirectoryNode(const std::string& name,
                DirectoryNode* parent,
                const LogAttributes& attrs,
                DirectoryMap dirs,
                const LogGroupMap& logs,
                const std::string& delimiter)
      : LogsConfigTreeNode(name, attrs),
        parent_(parent),
        children_(std::move(dirs)),
        logs_(logs),
        delimiter_(delimiter) {}

  NodeType type() const override {
    return NodeType::DIRECTORY;
  }

  virtual const DirectoryMap& children() const {
    return children_;
  }

  virtual DirectoryNode* parent() const {
    return parent_;
  }

  // returns a FQN of the node (e.g, /dir1/group1)
  virtual std::string getFullyQualifiedName() const;

  // Checks whether that name (whether it's a LogGroup or a Directory) exists
  // under this directory or not.
  bool exists(const std::string& name) const {
    return (logs_.count(name) > 0 || children_.count(name) > 0);
  }

  /**
   * Sets the attributes of this node and updates all the children
   * accordingly by re-applying the inheritance tree
   */
  bool setAttributes(const LogAttributes& attrs, std::string& failure_reason) {
    attrs_ = attrs;
    return refreshAttributesInheritance(failure_reason);
  }

  virtual const LogGroupMap& logs() const {
    return logs_;
  }

  /*
   * Instantiates a new child in this directory, this automatically applies the
   * correct parent attributes if passed, if no attrs was passed, then all
   * attributes will be inherited from this directory
   */
  DirectoryNode* addChild(const std::string& name,
                          const LogAttributes& child_attrs = LogAttributes());

  // updates the attributes of the children directories and logs with the
  // current log attributes as a parent.
  // If refreshing attributes failed, we set err=E::INVALID_ATTRIBUTES and will
  // fill the failure_reason string with the reason message (unless
  // failure_reason is set to nullptr)
  bool refreshAttributesInheritance(std::string& failure_reason);

  /**
   * Overwrites the child (whether exists or not) with a new DirectoryNode
   * object.
   * Note that this does not set the parent for the supplied `DirectoryNode`.
   * The parent must be set before passing it through.
   */
  void setChild(const std::string& name, std::unique_ptr<DirectoryNode> child);
  /*
   * This doesn't update the tree interval map (LogID -> LogGroupNode)
   * This has to be done explicitly after calling this method
   * The name has to be without any _delimiter_
   */
  std::shared_ptr<LogGroupNode> addLogGroup(const std::string& name,
                                            const logid_range_t& range,
                                            const LogAttributes& log_attrs,
                                            bool overwrite,
                                            std::string& failure_reason);

  /*
   * This doesn't update the tree interval map (LogID -> LogGroupNode)
   * This has to be done after calling this method
   */
  std::shared_ptr<LogGroupNode> addLogGroup(const LogGroupNode& log_group,
                                            bool overwrite,
                                            std::string& failure_reason);

  std::shared_ptr<LogGroupNode>
  addLogGroup(std::shared_ptr<LogGroupNode> log_group,
              bool overwrite,
              std::string& failure_reason);

  ReplicationProperty getNarrowestReplication() const;
  /*
   * Deletes and returns a log group from the direct children of this directory
   * This doesn't update the tree interval map (LogID -> LogGroupNode)
   */
  std::shared_ptr<LogGroupNode> deleteLogGroup(const std::string& name);
  void deleteChild(const std::string& name);

  // sets the log groups map directly.
  void setLogGroups(const LogGroupMap& logs) {
    logs_ = logs;
  }

  void setName(const std::string& name) {
    name_ = name;
  }
  // changes the name for one of the child directories, this only changes
  // the map key but doesn't change anything in the Directory object for
  // that child
  bool replaceChild(const std::string& name, const std::string& new_name) {
    DirectoryMap::iterator search = children_.find(name);
    if (search != children_.end()) {
      auto value = std::move(search->second);
      // found, replace
      children_.erase(search);
      children_[new_name] = std::move(value);
      return true;
    }
    return false;
  }

  // sets the children map directly.
  void setChildren(DirectoryMap&& dirs) {
    children_ = std::move(dirs);
  }

 private:
  DirectoryNode* parent_;
  DirectoryMap children_;
  LogGroupMap logs_;
  std::string delimiter_;
};

/**
 * This represents a LogGroup node in the LogsConfigTree. This is a leaf and
 * _may_ represent a single log or a range of logs.
 */
class LogGroupNode : public LogsConfigTreeNode {
 public:
  LogGroupNode() {}
  LogGroupNode(const std::string& name,
               const LogAttributes& attrs,
               logid_range_t range)
      : LogsConfigTreeNode(name, attrs), range_(range) {}

  NodeType type() const override {
    return NodeType::LOG_GROUP;
  }

  virtual const logid_range_t& range() const {
    return range_;
  }

  /*
   * Returns a copy of the LogGroupNode with a different set of LogAttributes.
   */
  LogGroupNode withLogAttributes(const LogAttributes& attrs) const {
    return LogGroupNode(name_, attrs, range_);
  }

  /*
   * Returns a copy of the LogGroupNode with a different LogRange.
   */
  LogGroupNode withRange(const logid_range_t& range) const {
    return LogGroupNode(name_, attrs_, range);
  }

  /*
   * Returns a copy of LogGroupNode with a different name set.
   */
  LogGroupNode withName(const std::string& name) const {
    return LogGroupNode(name, attrs_, range_);
  }

  // returns a FQN of the node (e.g, /dir1/group1)
  virtual std::string getFullyQualifiedName(const DirectoryNode* parent) const;

  /*
   * Validates whether a LogGroupNode is compliant or not. For instance, it
   * validates that either replicationFactor or replicateAcross is set
   * (since at least one of the two is required) etc.
   *
   * If this LogGroup is invalid, we will set the failure_reason to a
   * user-readable why message (unless it's set to nullptr)
   */
  static bool isValid(const DirectoryNode* parent,
                      const LogGroupNode* lg,
                      std::string& failure_reason);

  static bool isRangeValid(const LogGroupNode* lg);

  bool operator==(const LogGroupNode& other) const {
    auto as_tuple = [](const LogGroupNode& l) {
      return std::tie(l.name_, l.attrs_, l.range_);
    };
    return as_tuple(*this) == as_tuple(other);
  }

  static std::unique_ptr<LogGroupNode>
  createFromJson(const std::string& json,
                 const std::string& namespace_delimiter);

  // Also returns the full path of the log,
  // since this information is lost in LogGroupNode (we keep only the log name)
  // Required for RemoteLogsConfig's getLogRangesByNamespaceAsync
  static std::pair<std::string, std::unique_ptr<LogGroupNode>>
  createFromFollyDynamic(const folly::dynamic& log_entry,
                         const std::string& namespace_delimiter);

 protected:
  logid_range_t range_;
};

// A structure that holds a log group with its parent directory
// mainly used when you query the tree for a log group by ID
struct LogGroupInDirectory {
  LogGroupInDirectory() : log_group(nullptr), parent(nullptr) {}
  LogGroupInDirectory(const std::shared_ptr<LogGroupNode> group,
                      const DirectoryNode* dir)
      : log_group(group), parent(dir) {}

  LogGroupInDirectory(const LogGroupInDirectory& lgind)
      : log_group(lgind.log_group), parent(lgind.parent) {}

  LogGroupInDirectory& operator=(const LogGroupInDirectory& in) {
    log_group = in.log_group;
    parent = in.parent;
    return *this;
  }

  std::shared_ptr<LogGroupNode> log_group;
  const DirectoryNode* parent;

  std::string getFullyQualifiedName() const {
    ld_check(log_group != nullptr);
    if (parent == nullptr) {
      // Useful for MetaDataLogConfig since it doesn't have a parent
      return log_group->name();
    } else {
      return log_group->getFullyQualifiedName(parent);
    }
  }

  std::string toJson(bool metadata_logs = false) const;
  folly::dynamic toFollyDynamic(bool metadata_logs = false) const;
};

// needed for LogGroupInDirectory to be used as a value in interval_map
bool operator==(const LogGroupInDirectory& left,
                const LogGroupInDirectory& right);

/**
 * LogsConfigTree is an immutable class representing a tree of directories and
 * LogGroups. Immutability here means that you should _never_ update a tree
 * unless you are in a test case or you know what you are doing quite well.
 *
 * LogsConfigTree is not thread-safe. All mutations happen in
 * LogsConfigStateMachine which runs in a Worker.
 */
class LogsConfigTree {
 public:
  friend class LogsConfigTreeTest;
  template <CodecType T>
  friend class LogsConfigCodec;
  using const_iterator = LogMap::element_const_iterator;
  using const_reverse_iterator = LogMap::element_const_reverse_iterator;

  LogsConfigTree() : version_(max_version++) {}
  LogsConfigTree(const LogsConfigTree& other) {
    copy(other);
  }

  LogsConfigTree(std::unique_ptr<DirectoryNode> root,
                 const std::string& delimiter)
      : root_(std::move(root)),
        version_(max_version++),
        delimiter_(delimiter) {}
  LogsConfigTree(std::unique_ptr<DirectoryNode> root,
                 const std::string& delimiter,
                 lsn_t version)
      : root_(std::move(root)), version_(version), delimiter_(delimiter) {}
  // returns the root directory for the tree
  virtual DirectoryNode* root() const {
    return root_.get();
  }

  LogsConfigTree& operator=(const LogsConfigTree& in) {
    return copy(in);
  }

  // returns the version of this config
  // The version of the config is the version of the LSN of the last applied
  // delta to this config. This gets updated even if the applied delta did not
  // result in any effective change in the config tree.
  const lsn_t& version() const {
    return version_;
  }

  void setVersion(lsn_t version) {
    version_ = version;
  }

  // returns the delimiter used to separate directory in FQN
  const std::string& delimiter() const {
    return delimiter_;
  }

  // searches the tree by _path_ and returns the corresponding DirectoryNode
  // instance or nullptr if not found.
  // For example, if delimiter is '/', a valid input path is "/important_logs"
  DirectoryNode* findDirectory(const std::string& path) const;

  // searches the tree by _path_ and returns the resulting node
  LogsConfigTreeNode* find(const std::string& path) const;

  // searches the tree by _path_ and returns the corresponding LogGroupNode
  // instance or nullptr if not found.
  // For example, if delimiter is '/', a valid input path is
  // "/important_logs/task_queue"
  std::shared_ptr<LogGroupNode> findLogGroup(const std::string& path) const;

  static std::unique_ptr<LogsConfigTree>
  create(const std::string& delimiter = "/",
         const LogAttributes& attrs = DefaultLogAttributes()) {
    auto root = std::make_unique<DirectoryNode>(delimiter, attrs);
    return std::make_unique<LogsConfigTree>(std::move(root), delimiter);
  }

  /*
   * Adding a new LogGroup to the tree given the parent path (string), name
   * (string)
   */
  std::shared_ptr<LogGroupNode> addLogGroup(const std::string& parent,
                                            const std::string& name,
                                            const logid_range_t& range,
                                            const LogAttributes& log_attrs,
                                            std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  std::shared_ptr<LogGroupNode>
  addLogGroup(const std::string& parent,
              const std::string& name,
              const logid_range_t& range,
              const LogAttributes& log_attrs = LogAttributes());

  /*
   * Adding a new LogGroup to the tree given the full path (string)
   */
  std::shared_ptr<LogGroupNode> addLogGroup(const std::string& path,
                                            const logid_range_t& range,
                                            const LogAttributes& log_attrs,
                                            bool addIntermediateDirectories,
                                            std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  std::shared_ptr<LogGroupNode>
  addLogGroup(const std::string& path,
              const logid_range_t& range,
              const LogAttributes& log_attrs = LogAttributes(),
              bool addIntermediateDirectories = false);

  /*
   * Adding a new LogGroup to the tree given the pointer to the parent and the
   * relative path (string)
   */
  std::shared_ptr<LogGroupNode> addLogGroup(DirectoryNode* parent,
                                            const std::string& relative_path,
                                            const logid_range_t& range,
                                            const LogAttributes& log_attrs,
                                            bool addIntermediateDirectories,
                                            std::string& failure_reason);
  // Same as above except that it logs the failure reason instead.
  std::shared_ptr<LogGroupNode>
  addLogGroup(DirectoryNode* parent,
              const std::string& relative_path,
              const logid_range_t& range,
              const LogAttributes& log_attrs = LogAttributes(),
              bool addIntermediateDirectories = false);

  /**
   * Removes a log group from the tree and updates the interval map accordingly.
   *
   * @return 0 if success, -1 otherwise and sets `err` to:
   * E::NOTFOUND the path does not exist or it's a path to directory
   */
  int deleteLogGroup(const std::string& path, std::string& failure_reason);
  // Same as above except that it logs the failure reason instead.
  int deleteLogGroup(const std::string& path);
  int deleteLogGroup(DirectoryNode* parent,
                     std::shared_ptr<LogGroupNode> group,
                     std::string& failure_reason);

  /**
   * Removes a directory node and all its children recursively (if recursive is
   * set).
   * @return 0 if success, -1 otherwise and sets `err` to:
   *    E::NOTFOUND the path does not exist or it's a log-group
   */
  int deleteDirectory(const std::string& path,
                      bool recursive,
                      std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  int deleteDirectory(const std::string& path, bool recursive);

  int deleteDirectory(DirectoryNode* dir,
                      bool recursive,
                      std::string& failure_reason);

  /*
   * Replaces an existing LogGroupNode (defined at path) with a new
   * LogGroupNode. This also updates the lookup index (interval_map)
   */
  bool replaceLogGroup(const std::string& path,
                       const LogGroupNode& new_log_group,
                       std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  bool replaceLogGroup(const std::string& path,
                       const LogGroupNode& new_log_group);
  /**
   * rename a node name in the tree
   * @return 0 on success, -1 otherwise and `err` is set to one of the following
   *    E::NOTFOUND if source doesn't exist
   *    E::EXISTS if to_path exists
   */
  int rename(const std::string& from_path,
             const std::string& to_path,
             std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  int rename(const std::string& from_path, const std::string& to_path);
  /**
   * Sets the attributes of a node in the tree and updates the children if
   * applicable
   * @return 0 on success, -1 otherwise and sets `err` to one of the following:
   *    E::INVALID_ATTRIBUTES the supplied attributes are not valid or caused
   *    one or more of the LogGroups in the children tree to become invalid. The
   *    supplied argument &failure_reaason will be populated with the reason.
   *    E::NOTFOUND The path does not exist.
   *
   */
  int setAttributes(const std::string& path,
                    const LogAttributes& attrs,
                    std::string& failure_reason);

  // Same as above except that it logs the failure reason instead.
  int setAttributes(const std::string& path, const LogAttributes& attrs);

  // The following methods provide direct access to the log map.
  LogsConfigTree::const_iterator logsBegin() const {
    return boost::icl::elements_begin(logs_index_);
  }

  LogsConfigTree::const_iterator logsEnd() const {
    return boost::icl::elements_end(logs_index_);
  }

  LogsConfigTree::const_reverse_iterator logsRBegin() const {
    return boost::icl::elements_rbegin(logs_index_);
  }

  LogsConfigTree::const_reverse_iterator logsREnd() const {
    return boost::icl::elements_rend(logs_index_);
  }

  // returns the LogGroupNode that has this logid in its range.
  // The returned structure `LogGroupInDirectory` contains pointer to the log
  // group and its parent directory
  // this returns nullptr if the logid was not found.
  //
  const LogGroupInDirectory* getLogGroupByID(const logid_t& logid) const {
    auto iter = logs_index_.find(logid.val());
    if (iter == logs_index_.end()) {
      err = E::NOTFOUND;
      return nullptr;
    }
    return &iter->second;
  }

  std::pair<DirectoryNode*, std::shared_ptr<LogGroupNode>>
  getLogGroupAndParent(const std::string& path) const;

  // returns true if the logid exists in the tree
  bool logExists(logid_t logid) const {
    auto iter = logs_index_.find(logid.val());
    if (iter == logs_index_.end()) {
      err = E::NOTFOUND;
      return false;
    }
    return true;
  }

  const LogMap& getLogMap() const {
    return logs_index_;
  }

  size_t size() const {
    return logs_index_.size();
  }

  // returns true if the supplied logrange will clash with any of the log_groups
  // in the tree
  bool doesLogRangeClash(const logid_range_t& range,
                         std::string& failure_reason) const;

  std::unique_ptr<LogsConfigTree> copy() const {
    return std::make_unique<LogsConfigTree>(*this);
  }
  virtual ~LogsConfigTree() {}

  /*
   * This creates a directory with the specified attributes, the path is a FQLGN
   * (Fully Qualified LogGroup Name) and if recursive = true, this will create
   * the whole tree while applying the supplied attributes to only the leaf
   */
  DirectoryNode* addDirectory(const std::string& path,
                              bool recursive,
                              const LogAttributes& attrs,
                              std::string& failure_reason);

  // Same as above except that it logs the failure reason
  DirectoryNode* addDirectory(const std::string& path,
                              bool recursive = false,
                              const LogAttributes& attrs = LogAttributes());

  /*
   * Given a parent, create a directory directly under it.
   * ** This mutates the current tree, and should only be used from test cases
   * or Codecs.
   */
  DirectoryNode* addDirectory(DirectoryNode* parent,
                              const std::string& name,
                              const LogAttributes& attrs = LogAttributes());

  // maximum finite backlog duration of a log
  std::chrono::seconds getMaxBacklogDuration() const {
    return max_backlog_duration_;
  }
  /**
   * Returns the "minimum" of the two replication properties. More precisely,
   * returns a replication property that is satisfied by every copyset
   * that satisfies at least one of the two properties.
   * E.g.:
   *  * {node: 3}, {node: 2}  =>  {node: 2}
   *  * {node: 3, rack: 2}, {node: 4}  =>  {node: 3}
   *  * {rack: 3}, {region: 2}  => {rack: 2}
   */
  ReplicationProperty getNarrowestReplication() const;

  /*
   * A helper function that generates a pretty presentation of the logs-config
   * tree. This is mainly used for debugging
   */
  std::string print() const {
    std::stringstream stream;
    stream << "=== LogsConfig Tree ===" << std::endl;
    printDirectory(0, root(), stream);
    stream << "=======================" << std::endl;
    return stream.str();
  }

  // deconstructs a path into a set of tokens. The token vector only hold
  // pointers to the original string. It's important that the original string
  // outlives the lifetime of this returned vector.
  std::deque<folly::StringPiece> tokensOfPath(const std::string& path) const;
  std::string pathForTokens(const std::deque<folly::StringPiece>& tokens) const;
  std::pair<std::string, std::string>
  splitParentPath(const std::string& path) const;

 protected:
  LogsConfigTree& copy(const LogsConfigTree& other) {
    delimiter_ = other.delimiter_;
    root_ = std::make_unique<DirectoryNode>(*other.root_);
    version_ = other.version_;
    // We cannot copy the index because the Parent pointer in
    // LogGroupInDirectory will be pointing to the old tree.
    rebuildIndex();
    return *this;
  }

  /*
   * Adds a log group to a specific directory
   */
  std::shared_ptr<LogGroupNode> addLogGroup(DirectoryNode* parent,
                                            const LogGroupNode& log_group,
                                            std::string& failure_reason);
  // This refreshes the internal lookup index with the supplied log_group
  // The lookup index is used to locate a LogGroup object using a logid_t
  void updateLookupIndex(const DirectoryNode* parent,
                         const std::shared_ptr<LogGroupNode> log_group,
                         const bool delete_old);

  /**
   * Deletes a log group from the logid lookup index
   */
  bool deleteLogGroupFromLookupIndex(const logid_range_t& range);
  /**
   * Deletes a full directory including all its nodes from the lookup index
   */
  void deleteDirectoryFromLookupIndex(const DirectoryNode* dir);

  // recursively updates the lookup index for all logs in this directory and its
  // children. if delete_old == true, removes old index entries first
  // (only for first level)
  void rebuildIndexForDir(const DirectoryNode* dir, const bool delete_old);

  // same as rebuildIndexForDir except that this rebuilds the whole tree
  void rebuildIndex();

  std::string normalize_path(const std::string& path) const;
  /*
   * Used by print() to print a specific directory level
   */
  void printDirectory(int level,
                      DirectoryNode* dir,
                      std::stringstream& stream) const {
    std::string indent(level * 2, ' ');
    stream << indent << dir->getFullyQualifiedName() << std::endl;
    stream << indent << "Narrowest Replication: "
           << dir->getNarrowestReplication().toString() << std::endl;

    for (const auto& iter : dir->logs()) {
      stream << indent << "- " << iter.second->getFullyQualifiedName(dir)
             << " (" << iter.second->range().first.val() << ".."
             << iter.second->range().second.val()
             << ") R=" << iter.second->getReplicationProperty().toString()
             << std::endl;
    }
    for (const auto& iter : dir->children()) {
      printDirectory(level + 1, iter.second.get(), stream);
    }
  }

  // returns the found directory and a list of the tokens that were not found.
  // This is used to find the existing parent in a tree of directory where
  // parts exist and others don't.
  std::pair<DirectoryNode*, std::deque<folly::StringPiece>>
  partialFindDirectory(DirectoryNode* parent,
                       const std::deque<folly::StringPiece>& tokens) const;
  std::unique_ptr<DirectoryNode> root_;
  lsn_t version_ = 0l;
  std::string delimiter_;
  // maximum finite backlog duration of a log
  std::chrono::seconds max_backlog_duration_{0};
  LogMap logs_index_;
  // Max version seen, this is meant to be used if this tree is not backed by
  // LogsConfigManager.
  static std::atomic<uint64_t> max_version;
};

// A flat structure that contains a Log group and a denormalized parent name
// This is only used for messaging
struct LogGroupWithParentPath {
 public:
  std::shared_ptr<LogGroupNode> log_group;
  std::string parent_path;

  const std::string getFullyQualifiedName() const {
    if (log_group != nullptr) {
      return parent_path + log_group->name();
    } else {
      return "";
    }
  }
};
}}} // namespace facebook::logdevice::logsconfig
