/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigTree.h"

#include <deque>
#include <iostream>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <folly/CppAttributes.h>
#include <folly/Format.h>
#include <folly/json.h>

#include "logdevice/common/SlidingWindow.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;

using namespace facebook::logdevice::configuration::parser;

namespace facebook { namespace logdevice { namespace logsconfig {

std::atomic<uint64_t> LogsConfigTree::max_version{0};

bool operator==(const LogGroupInDirectory& left,
                const LogGroupInDirectory& right) {
  return left.log_group == right.log_group && left.parent == right.parent;
}

static std::string intervalToString(const logid_range_t& range) {
  return std::to_string(range.first.val()) + ".." +
      std::to_string(range.second.val());
}

folly::dynamic LogGroupInDirectory::toFollyDynamic(bool metadata_logs) const {
  ld_check(this->log_group != nullptr);

  const LogAttributes& attrs = this->log_group->attrs();
  folly::dynamic json_log = folly::dynamic::object;

  if (attrs.replicationFactor().hasValue()) {
    json_log[REPLICATION_FACTOR] = *attrs.replicationFactor();
  }

  if (attrs.syncReplicationScope().hasValue()) {
    json_log[SYNC_REPLICATION_SCOPE] =
        NodeLocation::scopeNames()[*attrs.syncReplicationScope()];
  }

  if (attrs.replicateAcross().hasValue()) {
    folly::dynamic json_replicate = folly::dynamic::object;
    for (const auto& item : *attrs.replicateAcross()) {
      json_replicate[NodeLocation::scopeNames()[item.first]] = item.second;
    }
    json_log[REPLICATE_ACROSS] = json_replicate;
  }

  if (metadata_logs) {
    return json_log;
  }

  std::string interval_str = intervalToString(this->log_group->range());
  json_log["id"] = interval_str;
  json_log["name"] = getFullyQualifiedName();
  json_log[EXTRA_COPIES] = *attrs.extraCopies();
  json_log[SYNCED_COPIES] = *attrs.syncedCopies();
  json_log[MAX_WRITES_IN_FLIGHT] = *attrs.maxWritesInFlight();
  json_log[SINGLE_WRITER] = *attrs.singleWriter();

  // Since all attributes defined as Attribute<Optional> also handle
  // 'null' as a special value we explicitly set them here.
  // This is different from the old Log.cpp serialization code
  // and might have not been correct for RemoteLogsConfig in the first place
  if (attrs.backlogDuration().value()) {
    json_log[BACKLOG] = chrono_string(attrs.backlogDuration().value().value());
  } else {
    json_log[BACKLOG] = nullptr;
  }

  if (attrs.nodeSetSize().value()) {
    json_log[NODESET_SIZE] = attrs.nodeSetSize().value().value();
  } else {
    json_log[NODESET_SIZE] = nullptr;
  }

  if (attrs.deliveryLatency().value()) {
    json_log[DELIVERY_LATENCY] =
        chrono_string(attrs.deliveryLatency().value().value());
  } else {
    json_log[DELIVERY_LATENCY] = nullptr;
  }

  if (attrs.stickyCopySets().value()) {
    json_log[STICKY_COPYSETS] = attrs.stickyCopySets().value();
  }
  json_log[SCD_ENABLED] = attrs.scdEnabled().value();
  json_log[LOCAL_SCD_ENABLED] = attrs.localScdEnabled().value();

  if (attrs.writeToken().value()) {
    json_log[WRITE_TOKEN] = attrs.writeToken().value().value();
  } else {
    json_log[WRITE_TOKEN] = nullptr;
  }

  if (attrs.mutablePerEpochLogMetadataEnabled().value()) {
    json_log[MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED] =
        attrs.mutablePerEpochLogMetadataEnabled().value();
  }

  if (attrs.sequencerAffinity().value()) {
    json_log[SEQUENCER_AFFINITY] = attrs.sequencerAffinity().value().value();
  } else {
    json_log[SEQUENCER_AFFINITY] = nullptr;
  }

  if (attrs.sequencerBatching().hasValue()) {
    json_log[SEQUENCER_BATCHING] = attrs.sequencerBatching().value();
  }

  if (attrs.sequencerBatchingTimeTrigger().hasValue()) {
    json_log[SEQUENCER_BATCHING_TIME_TRIGGER] =
        chrono_string(attrs.sequencerBatchingTimeTrigger().value());
  }

  if (attrs.sequencerBatchingSizeTrigger().hasValue()) {
    json_log[SEQUENCER_BATCHING_SIZE_TRIGGER] =
        attrs.sequencerBatchingSizeTrigger().value();
  }

  if (attrs.sequencerBatchingCompression().hasValue()) {
    json_log[SEQUENCER_BATCHING_COMPRESSION] =
        compressionToString(attrs.sequencerBatchingCompression().value());
  }

  if (attrs.sequencerBatchingPassthruThreshold().hasValue()) {
    json_log[SEQUENCER_BATCHING_PASSTHRU_THRESHOLD] =
        attrs.sequencerBatchingPassthruThreshold().value();
  }

  if (attrs.extras().hasValue() && !attrs.extras().value().empty()) {
    for (const auto& it : attrs.extras().value()) {
      json_log[it.first] = it.second;
    }
  }

  if (attrs.permissions().hasValue() && !attrs.permissions().value().empty()) {
    json_log[PERMISSIONS] = folly::dynamic::object;
    for (const auto& kv : attrs.permissions().value()) {
      ld_check(kv.second.size() == static_cast<int>(ACTION::MAX));

      folly::dynamic permissions_list = folly::dynamic::array;

      if (kv.second[static_cast<int>(ACTION::APPEND)]) {
        permissions_list.push_back("APPEND");
      }
      if (kv.second[static_cast<int>(ACTION::READ)]) {
        permissions_list.push_back("READ");
      }
      if (kv.second[static_cast<int>(ACTION::TRIM)]) {
        permissions_list.push_back("TRIM");
      }

      json_log[PERMISSIONS][kv.first] = permissions_list;
    }
  }

  if (attrs.acls().hasValue() && !attrs.acls().value().empty()) {
    json_log[ACLS] = folly::dynamic::array;
    for (const auto& acl : attrs.acls().value()) {
      json_log[ACLS].push_back(acl);
    }
  }

  if (attrs.aclsShadow().hasValue() && !attrs.aclsShadow().value().empty()) {
    json_log[ACLS_SHADOW] = folly::dynamic::array;
    for (const auto& acl : attrs.aclsShadow().value()) {
      json_log[ACLS_SHADOW].push_back(acl);
    }
  }

  if (attrs.sequencerAffinity().hasValue() &&
      attrs.sequencerAffinity().value().hasValue()) {
    json_log[SEQUENCER_AFFINITY] = attrs.sequencerAffinity().value().value();
  }

  json_log[TAIL_OPTIMIZED] = attrs.tailOptimized().value();

  if (attrs.shadow().hasValue() &&
      !attrs.shadow().value().destination().empty()) {
    json_log[SHADOW] = folly::dynamic::object();
    json_log[SHADOW][SHADOW_DEST] = attrs.shadow().value().destination();
    json_log[SHADOW][SHADOW_RATIO] = attrs.shadow().value().ratio();
  }

  return json_log;
}

std::string LogGroupInDirectory::toJson(bool metadata_logs) const {
  auto contents = this->toFollyDynamic(metadata_logs);
  folly::json::serialization_opts opts;
  return folly::json::serialize(contents, opts);
}

std::string
LogGroupNode::getFullyQualifiedName(const DirectoryNode* parent) const {
  ld_check(parent != nullptr);
  return parent->getFullyQualifiedName() + name_;
}

bool LogGroupNode::isValid(const DirectoryNode* parent,
                           const LogGroupNode* lg,
                           std::string& failure_reason) {
  if (lg == nullptr) {
    return false;
  }

  // Name is required
  if (lg->name().size() == 0) {
    failure_reason =
        folly::format(
            "Name is required for LogGroups, offending log(s) \"{}..{}\"",
            lg->range().first.val(),
            lg->range().second.val())
            .str();
    return false;
  }
  std::string lg_path;
  if (parent != nullptr) {
    lg_path = parent->getFullyQualifiedName() + lg->name();
  } else {
    lg_path = lg->name();
  }

  // Optional but is defaulted.
  if (!lg->attrs().maxWritesInFlight() ||
      lg->attrs().maxWritesInFlight().value() < SLIDING_WINDOW_MIN_CAPACITY) {
    failure_reason =
        folly::format(
            "Missing or invalid \"max-writes-in-flight\" value for "
            "log(s) {}. Expected a positive integer >= {}, Current is {}",
            lg_path.c_str(),
            SLIDING_WINDOW_MIN_CAPACITY,
            lg->attrs().maxWritesInFlight().describe().c_str())
            .str();

    return false;
  }

  // Optional but defaulted
  // hasValue() explicitly to avoid confusion.
  if (!lg->attrs().singleWriter().hasValue()) {
    failure_reason =
        folly::format(
            "Invalid value of \"single_writer\" attribute for log(s) {}. "
            "Expected a bool.",
            lg_path.c_str())
            .str();
    return false;
  }

  // backlog duration must be larger than 0 if set.
  if (lg->attrs().backlogDuration() &&
      lg->attrs().backlogDuration().value().hasValue() &&
      lg->attrs().backlogDuration().value().value().count() <= 0) {
    failure_reason = folly::format("Invalid value of \"backlog\" attribute "
                                   "for log(s) {}",
                                   lg_path.c_str())
                         .str();
    return false;
  }

  // validate log range specs
  if (!LogGroupNode::isRangeValid(lg)) {
    failure_reason =
        folly::format("Invalid LogGroup range for log(s) {}.", lg_path.c_str())
            .str();
    return false;
  }

  std::string replication_error;
  if (ReplicationProperty::validateLogAttributes(
          lg->attrs(), &replication_error) != 0) {
    failure_reason =
        folly::format("Invalid replication settings for log(s) {}. {}",
                      lg_path.c_str(),
                      replication_error.c_str())
            .str();
    return false;
  }

  // validate shadow parameters
  if (lg->attrs().shadow()) {
    if (lg->attrs().shadow().value().destination().size() == 0) {
      failure_reason =
          folly::format("Destination is required for shadowing log(s) {}",
                        lg_path.c_str())
              .str();
      return false;
    }
    double ratio = lg->attrs().shadow().value().ratio();
    if (ratio < 0.0 || ratio > 1.0) {
      failure_reason =
          folly::format("Invalid ratio for shadowing log(s) {}: {}, should be "
                        "in range [0.0, 1.0]",
                        lg_path.c_str(),
                        ratio)
              .str();
      return false;
    }
  }

  return true;
}

bool LogGroupNode::isRangeValid(const LogGroupNode* lg) {
  if (lg->range().first.val() > lg->range().second.val() ||
      (lg->range().first.val() == 0 ||
       lg->range().second.val() > LOGID_MAX.val_)) {
    ld_error(
        "invalid log range attribute for Log Group: \"%s\"."
        "The log range specified must be positive %zu-bit with "
        "start smaller or equal than end of range, supplied range is %zu..%zu",
        lg->name().c_str(),
        LOGID_BITS,
        lg->range().first.val(),
        lg->range().second.val());
    err = E::INVALID_CONFIG;
    return false;
  }
  return true;
}

std::string DirectoryNode::getFullyQualifiedName() const {
  if (!parent_) {
    return delimiter_;
  }
  return parent_->getFullyQualifiedName() + name_ + delimiter_;
}

DirectoryNode::DirectoryNode(const DirectoryNode& other)
    : DirectoryNode(other, other.parent_) {}

DirectoryNode::DirectoryNode(const DirectoryNode& other, DirectoryNode* parent)
    : LogsConfigTreeNode(other.name_, other.attrs_),
      parent_(parent),
      delimiter_(other.delimiter_) {
  // a simple copy of the log groups map
  logs_ = other.logs_;
  for (const auto& item : other.children_) {
    children_[item.first] = std::make_unique<DirectoryNode>(*item.second, this);
  }
}

ReplicationProperty DirectoryNode::getNarrowestReplication() const {
  ReplicationProperty narrowest;
  for (const auto& item : children_) {
    narrowest = narrowest.narrowest(item.second->getNarrowestReplication());
  }
  for (const auto& item : logs_) {
    narrowest = narrowest.narrowest(item.second->getReplicationProperty());
  }
  return narrowest;
}

DirectoryNode* DirectoryNode::addChild(const std::string& name,
                                       const LogAttributes& child_attrs) {
  auto child = std::make_unique<DirectoryNode>(
      name, this, LogAttributes(child_attrs, attrs_), delimiter_);
  DirectoryNode* node = child.get();
  setChild(name, std::move(child));
  return node;
}

void DirectoryNode::setChild(const std::string& name,
                             std::unique_ptr<DirectoryNode> child) {
  children_[name] = std::move(child);
}

std::shared_ptr<LogGroupNode>
DirectoryNode::addLogGroup(std::shared_ptr<LogGroupNode> group,
                           bool overwrite,
                           std::string& failure_reason) {
  if (!overwrite && exists(group->name())) {
    failure_reason = "Log group already exists!";
    err = E::EXISTS;
    return nullptr;
  }
  logs_[group->name()] = group;
  return group;
}

std::shared_ptr<LogGroupNode>
DirectoryNode::addLogGroup(const std::string& name,
                           const logid_range_t& range,
                           const LogAttributes& log_attrs,
                           bool overwrite,
                           std::string& failure_reason) {
  auto log = std::make_shared<LogGroupNode>(
      name, LogAttributes(log_attrs, attrs_), range);
  if (LogGroupNode::isValid(this, log.get(), failure_reason)) {
    return addLogGroup(log, overwrite, failure_reason);
  } else {
    err = E::INVALID_ATTRIBUTES;
    return nullptr;
  }
}

std::shared_ptr<LogGroupNode>
DirectoryNode::addLogGroup(const LogGroupNode& log_group,
                           bool overwrite,
                           std::string& failure_reason) {
  return addLogGroup(log_group.name(),
                     log_group.range(),
                     log_group.attrs(),
                     overwrite,
                     failure_reason);
}

bool DirectoryNode::refreshAttributesInheritance(std::string& failure_reason) {
  // create new attributes that apply the parent to the supplied attributes
  if (parent_ != nullptr) {
    attrs_ = LogAttributes(attrs_, parent_->attrs());
  }
  // for all dirs, set new attribute
  for (auto& it : children_) {
    if (!it.second->refreshAttributesInheritance(failure_reason)) {
      // refreshing attributes for this directory failed.
      return false;
    }
  }
  // for all log groups, replace.
  for (auto& it : logs_) {
    std::string log_path = getFullyQualifiedName() + delimiter_ + it.first;
    // create a replacement log group node that has our attributes applied
    LogGroupNode replacement =
        it.second->withLogAttributes(LogAttributes(it.second->attrs(), attrs_));
    if (!LogGroupNode::isValid(this, &replacement, failure_reason)) {
      err = E::INVALID_ATTRIBUTES;
      return false;
    }
    // should not fail since we pass overwrite = true
    addLogGroup(replacement, true /* overwrite */, failure_reason);
  }
  return true;
}

std::shared_ptr<LogGroupNode>
DirectoryNode::deleteLogGroup(const std::string& name) {
  auto iter = logs_.find(name);
  if (iter != logs_.end()) {
    std::shared_ptr<LogGroupNode> old_node = iter->second;
    logs_.erase(iter);
    return old_node;
  }
  return nullptr;
}

void DirectoryNode::deleteChild(const std::string& name) {
  auto iter = children_.find(name);
  if (iter != children_.end()) {
    children_.erase(iter);
  }
}

std::deque<folly::StringPiece>
LogsConfigTree::tokensOfPath(const std::string& path) const {
  std::deque<folly::StringPiece> tokens;
  folly::splitTo<folly::StringPiece>(
      delimiter_, path, std::inserter(tokens, tokens.begin()));
  return tokens;
}

std::string LogsConfigTree::pathForTokens(
    const std::deque<folly::StringPiece>& tokens) const {
  return folly::join(delimiter_, tokens);
}

std::pair<std::string, std::string>
LogsConfigTree::splitParentPath(const std::string& path) const {
  std::string parent = delimiter_;
  std::string name;
  auto tokens = tokensOfPath(path);
  if (tokens.size() > 0) {
    name = tokens.back().str();
    tokens.pop_back();
    parent = pathForTokens(tokens);
  }
  return std::make_pair(parent, name);
}

DirectoryNode* FOLLY_NULLABLE
LogsConfigTree::findDirectory(const std::string& path) const {
  DirectoryNode* found;
  std::string clean_path = normalize_path(path);
  std::deque<folly::StringPiece> remaining_tokens;
  std::tie(found, remaining_tokens) =
      partialFindDirectory(root_.get(), tokensOfPath(clean_path));
  if (!remaining_tokens.empty()) {
    return nullptr;
  } else {
    return found;
  }
}

LogsConfigTreeNode* FOLLY_NULLABLE
LogsConfigTree::find(const std::string& raw_path) const {
  std::string path = normalize_path(raw_path);
  auto tokens = tokensOfPath(path);
  DirectoryNode* found;
  std::deque<folly::StringPiece> remaining_tokens;
  std::tie(found, remaining_tokens) = partialFindDirectory(root_.get(), tokens);
  if (remaining_tokens.size() > 1) {
    // this means that we have a missing directory(ies) in the path, we fail.
    err = E::NOTFOUND;
    return nullptr;
  }
  if (remaining_tokens.size() == 0) {
    // the path exists and it's a directory
    return found;
  }
  // we have one token left and it cannot be a directory, it must be a LogGroup
  std::string group_name = remaining_tokens.front().str();
  auto search = found->logs().find(group_name);
  if (search != found->logs().end()) {
    // we found it.
    return search->second.get();
  }
  err = E::NOTFOUND;
  return nullptr;
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::findLogGroup(const std::string& path) const {
  DirectoryNode* parent;
  std::shared_ptr<LogGroupNode> group;
  std::tie(parent, group) = getLogGroupAndParent(path);
  if (parent == nullptr || group == nullptr) {
    err = E::NOTFOUND;
    return nullptr;
  }
  return group;
}

std::pair<DirectoryNode*, std::shared_ptr<LogGroupNode>>
LogsConfigTree::getLogGroupAndParent(const std::string& raw_path) const {
  DirectoryNode* found;
  std::string path = normalize_path(raw_path);
  std::deque<folly::StringPiece> remaining_tokens;
  std::tie(found, remaining_tokens) =
      partialFindDirectory(root_.get(), tokensOfPath(path));
  if (remaining_tokens.empty() || remaining_tokens.size() > 1) {
    // it's found but it's a directory!
    // or a parent directory doesn't exist.
    err = E::NOTFOUND;
    return std::make_pair(nullptr, nullptr);
  } else {
    const auto& logs = found->logs();
    auto search = logs.find(remaining_tokens.front().str());
    if (search != logs.end()) {
      return std::make_pair(found, search->second);
    }
    err = E::NOTFOUND;
    return std::make_pair(nullptr, nullptr);
  }
}

std::pair<DirectoryNode*, std::deque<folly::StringPiece>>
LogsConfigTree::partialFindDirectory(
    DirectoryNode* parent,
    const std::deque<folly::StringPiece>& path_tokens) const {
  // special handling for finding the root directory
  if (path_tokens.size() == 0 ||
      (path_tokens.size() == 1 && path_tokens.front().size() == 0)) {
    return std::make_pair(parent, std::deque<folly::StringPiece>());
  }
  // copy
  std::deque<folly::StringPiece> tokens = path_tokens;
  DirectoryNode* current_dir = parent;
  while (!tokens.empty()) {
    ld_check(current_dir != nullptr);
    std::string current_token = tokens.front().str();
    const auto& children = current_dir->children();
    auto search = children.find(current_token);
    if (search != children.end()) {
      tokens.pop_front();
      current_dir = search->second.get();
    } else {
      break;
    }
  }
  return std::make_pair(current_dir, std::move(tokens));
}

DirectoryNode* FOLLY_NULLABLE
LogsConfigTree::addDirectory(DirectoryNode* parent,
                             const std::string& name,
                             const LogAttributes& attrs) {
  if (parent && !parent->exists(name)) {
    return parent->addChild(name, attrs);
  }
  return nullptr;
}

DirectoryNode* FOLLY_NULLABLE
LogsConfigTree::addDirectory(const std::string& path,
                             bool recursive,
                             const LogAttributes& attrs) {
  std::string failure_reason;
  DirectoryNode* ret = addDirectory(path, recursive, attrs, failure_reason);
  if (!ret) {
    ld_error("Failed to add directory '%s' because %s: %s",
             path.c_str(),
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

DirectoryNode* FOLLY_NULLABLE
LogsConfigTree::addDirectory(const std::string& raw_path,
                             bool recursive,
                             const LogAttributes& attrs,
                             std::string& failure_reason) {
  DirectoryNode* found;
  std::string path = normalize_path(raw_path);
  std::deque<folly::StringPiece> remaining_tokens;
  std::tie(found, remaining_tokens) =
      partialFindDirectory(root_.get(), tokensOfPath(path));

  if (remaining_tokens.size() > 1 && !recursive) {
    // we need to create more than one directory and we are not recursive, log
    // and return nullptr
    ld_spew(
        "Trying to create log directory %s but one of the parents (at %s) does "
        "not exist, use recursive to fix this",
        path.c_str(),
        remaining_tokens.front().str().c_str());
    failure_reason = folly::format("One of the parents directories of '{}' does"
                                   " not exist.",
                                   path)
                         .str();
    err = E::NOTFOUND;
    return nullptr;
  }
  if (remaining_tokens.empty()) {
    // Directory exists!
    failure_reason =
        folly::format("Directory '{}' already exists!", path).str();
    err = E::EXISTS;
    return nullptr;
  } else {
    DirectoryNode* current_parent = found;
    while (remaining_tokens.size() > 0) {
      LogAttributes dir_attrs =
          remaining_tokens.size() == 1 ? attrs : LogAttributes();
      current_parent =
          current_parent->addChild(remaining_tokens.front().str(), dir_attrs);
      remaining_tokens.pop_front();
    }
    return current_parent;
  }
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(const std::string& parent_path,
                            const std::string& name,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs) {
  std::string failure_reason;
  auto ret = addLogGroup(parent_path, name, range, log_attrs, failure_reason);
  if (!ret) {
    ld_error("Failed to add log group '%s' in '%s': %s, %s",
             name.c_str(),
             parent_path.c_str(),
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(const std::string& parent_path,
                            const std::string& name,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs,
                            std::string& failure_reason) {
  return addLogGroup(findDirectory(parent_path),
                     name,
                     range,
                     log_attrs,
                     false,
                     failure_reason);
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(const std::string& path,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs,
                            bool addIntermediateDirectories,
                            std::string& failure_reason) {
  // we now require name to be defined in all logs

  return addLogGroup(root_.get(),
                     path,
                     range,
                     log_attrs,
                     addIntermediateDirectories,
                     failure_reason);
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(const std::string& path,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs,
                            bool addIntermediateDirectories) {
  std::string failure_reason;
  auto ret = addLogGroup(
      path, range, log_attrs, addIntermediateDirectories, failure_reason);
  if (!ret) {
    ld_error("Cannot add log group: %s, %s",
             error_name(err),
             failure_reason.c_str());
  }

  return ret;
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(DirectoryNode* parent,
                            const std::string& name,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs,
                            bool addIntermediateDirectories) {
  std::string failure_reason;
  auto ret = addLogGroup(parent,
                         name,
                         range,
                         log_attrs,
                         addIntermediateDirectories,
                         failure_reason);
  if (!ret) {
    ld_error("Cannot add log group: %s, %s",
             error_name(err),
             failure_reason.c_str());
  }

  return ret;
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(DirectoryNode* parent,
                            const std::string& name,
                            const logid_range_t& range,
                            const LogAttributes& log_attrs,
                            bool addIntermediateDirectories,
                            std::string& failure_reason) {
  // make sure that name doesn't have any _delimiter_
  std::string clean_name = normalize_path(name);
  // copied since tokensOfPath references the string clean_name and changing
  // clean_name means corrupting the folly::StringPiece
  std::string target_name = clean_name;
  auto name_tokens = tokensOfPath(clean_name);
  if (name_tokens.size() == 0) {
    err = E::INVALID_PARAM;
    return nullptr;
  } else if (name_tokens.size() > 1) {
    // find the parent that actually exists and add directories recursively
    // until we have the direct parent for this log group.

    std::deque<folly::StringPiece> remaining_tokens;
    std::tie(parent, remaining_tokens) =
        partialFindDirectory(parent, name_tokens);
    if (remaining_tokens.size() == 0) {
      // the name turned out to be a directory, failing.
      ld_error(
          "Cannot create the log group \"%s\" because a directory exists with "
          "the same path",
          name.c_str());
      err = E::EXISTS;
      return nullptr;
    } else if (remaining_tokens.size() > 1) {
      if (!addIntermediateDirectories) {
        // name has delimiter in it, and we are not allowed to add intermediate
        // directories, failing.
        ld_error(
            "Cannot create the log group \"%s\" because the parent directory "
            "doesn't exist",
            name.c_str());
        err = E::NOTDIR;
        return nullptr;
      }
      // some directories missing, let's create them
      target_name = remaining_tokens.back().str();
      remaining_tokens.pop_back();
      parent = addDirectory(
          parent->getFullyQualifiedName() + pathForTokens(remaining_tokens),
          true /* recursive */,
          LogAttributes(),
          failure_reason);
      ld_check(parent != nullptr);
    } else if (remaining_tokens.size() == 1) {
      target_name = remaining_tokens.back().str();
    }
  }
  return addLogGroup(
      parent, LogGroupNode(target_name, log_attrs, range), failure_reason);
}

std::shared_ptr<LogGroupNode>
LogsConfigTree::addLogGroup(DirectoryNode* parent,
                            const LogGroupNode& log_group,
                            std::string& failure_reason) {
  if (!parent) {
    ld_error(
        "Adding a LogGroup \"%s\" without a parent, this is possibly a code "
        "bug",
        log_group.name().c_str());
    failure_reason = folly::format("Adding a LogGroup \"{}\" without a parent, "
                                   "this is possibly a code bug!",
                                   log_group.name())
                         .str();
    return nullptr;
  }
  // check if the name exists
  if (parent->exists(log_group.name())) {
    failure_reason =
        folly::format(
            "Adding LogGroup with the path \"{}\" which already exists in the "
            "tree!",
            log_group.getFullyQualifiedName(parent))
            .str();
    err = E::EXISTS;
    return nullptr;
  }
  if (!LogGroupNode::isRangeValid(&log_group)) {
    failure_reason = folly::format("Log group '{}' has an invalid range!",
                                   log_group.getFullyQualifiedName(parent))
                         .str();
    err = E::INVALID_CONFIG;
    return nullptr;
  }

  // validate if this results in no ID clash
  if (doesLogRangeClash(log_group.range(), failure_reason)) {
    // failure_reason set by doesLogRangeClash
    err = E::ID_CLASH;
    return nullptr;
  }
  auto lg = parent->addLogGroup(log_group,
                                /* overwrite= */ false,
                                failure_reason);
  // update the lookup-index.
  if (lg) {
    updateLookupIndex(parent, lg, false);
  }
  return lg;
}

bool LogsConfigTree::replaceLogGroup(const std::string& path,
                                     const LogGroupNode& new_log_group) {
  std::string failure_reason;
  bool ret = replaceLogGroup(path, new_log_group, failure_reason);
  if (!ret) {
    ld_error("Failed to replace log group: %s %s",
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

bool LogsConfigTree::replaceLogGroup(const std::string& raw_path,
                                     const LogGroupNode& new_log_group,
                                     std::string& failure_reason) {
  std::string path = normalize_path(raw_path);
  if (!LogGroupNode::isRangeValid(&new_log_group)) {
    // the new log group range is reserved or invalid.
    err = E::INVALID_ATTRIBUTES;
    failure_reason =
        folly::format("Log group '{}' has an invalid range!", path).str();
    return false;
  }
  DirectoryNode* parent;
  std::deque<folly::StringPiece> remaining_tokens;
  std::tie(parent, remaining_tokens) =
      partialFindDirectory(root_.get(), tokensOfPath(path));
  if (parent == nullptr || remaining_tokens.size() == 0 ||
      remaining_tokens.size() > 1) {
    failure_reason = folly::format("Cannot replace log group \"{}\" because "
                                   "the same path appears to "
                                   "be a directory instead!",
                                   path)
                         .str();
    err = E::NOTFOUND;
    return false;
  }

  std::string group_name = remaining_tokens.front().str();

  const auto& old_node_iter = parent->logs().find(group_name);
  if (old_node_iter == parent->logs().end()) {
    failure_reason =
        folly::format(
            "Cannot replace log group \"{}\" because it wasn't found in the "
            "tree",
            path)
            .str();
    // we didn't find the original log group name!
    err = E::NOTFOUND;
    return false;
  }
  std::shared_ptr<LogGroupNode> old_node = old_node_iter->second;

  if (!deleteLogGroupFromLookupIndex(old_node->range())) {
    return false;
  }
  // check if the new range clashes or not, if clash detected we re-add the
  // original
  if (doesLogRangeClash(new_log_group.range(), failure_reason)) {
    // add it back
    updateLookupIndex(parent, old_node, false);
    // failure_reason is set by doesLogRangeClash
    err = E::ID_CLASH;
    return false;
  }
  parent->deleteLogGroup(group_name);

  if (addLogGroup(parent, new_log_group, failure_reason)) {
    return true;
  }
  // we couldn't add the new log group, this is bad, really bad because we have
  // This could happen if after applying the attribute inheritance we
  // discovered that the new log group is not valid
  parent->addLogGroup(old_node, true, failure_reason);
  updateLookupIndex(parent, old_node, false);
  err = E::INVALID_ATTRIBUTES;
  return false;
}

int LogsConfigTree::deleteLogGroup(DirectoryNode* parent,
                                   std::shared_ptr<LogGroupNode> node,
                                   std::string& failure_reason) {
  if (node == nullptr || parent == nullptr) {
    failure_reason = "Path was not found!";
    err = E::NOTFOUND;
    return -1;
  }
  // we need the parent
  deleteLogGroupFromLookupIndex(node->range());
  // we need to remove this from the index after removing from parent (exists)
  parent->deleteLogGroup(node->name());
  return 0;
}

int LogsConfigTree::deleteLogGroup(const std::string& raw_path) {
  std::string failure_reason;
  std::string path = normalize_path(raw_path);
  int ret = deleteLogGroup(path, failure_reason);
  if (ret != 0) {
    ld_error("Failed to delete log group '%s': %s, %s",
             path.c_str(),
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

int LogsConfigTree::deleteLogGroup(const std::string& path,
                                   std::string& failure_reason) {
  std::shared_ptr<LogGroupNode> node;
  DirectoryNode* parent;
  std::tie(parent, node) = getLogGroupAndParent(path);
  return deleteLogGroup(parent, node, failure_reason);
}

int LogsConfigTree::deleteDirectory(DirectoryNode* FOLLY_NULLABLE dir,
                                    bool recursive,
                                    std::string& failure_reason) {
  if (dir == nullptr) {
    err = E::NOTFOUND;
    return -1;
  }
  if (dir == root_.get() || dir->parent() == nullptr) {
    // cannot delete the root directory
    failure_reason = "Cannot delete the root directory!";
    err = E::INVALID_PARAM;
    return -1;
  }
  if ((dir->children().size() || dir->logs().size() > 0) > 0 && !recursive) {
    failure_reason = folly::format("Directory '{}' is not empty!",
                                   dir->getFullyQualifiedName())
                         .str();
    err = E::NOTEMPTY;
    return -1;
  }

  deleteDirectoryFromLookupIndex(dir);

  DirectoryNode* parent = dir->parent();
  parent->deleteChild(dir->name());
  return 0;
}

int LogsConfigTree::deleteDirectory(const std::string& path, bool recursive) {
  std::string failure_reason;
  int ret = deleteDirectory(path, recursive, failure_reason);
  if (ret != 0) {
    ld_error("Failed to delete a directory '%s': %s, %s",
             path.c_str(),
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

int LogsConfigTree::deleteDirectory(const std::string& path,
                                    bool recursive,
                                    std::string& failure_reason) {
  DirectoryNode* dir = findDirectory(path);
  return deleteDirectory(dir, recursive, failure_reason);
}

int LogsConfigTree::rename(const std::string& from_path,
                           const std::string& to_path) {
  std::string failure_reason;
  int ret = rename(from_path, to_path, failure_reason);
  if (ret != 0) {
    ld_error(
        "Cannot rename path: %s, %s", error_name(err), failure_reason.c_str());
  }
  return ret;
}

int LogsConfigTree::rename(const std::string& raw_from_path,
                           const std::string& raw_to_path,
                           std::string& failure_reason) {
  // we cannot do this operation halfway, so the strategy here is to validate
  // the following:
  //  - the destination must not be the root directory. e.g., '/'
  //  - the source must not be the root directory
  //  - only allowed to change the name of the last token.
  std::string from_path = normalize_path(raw_from_path);
  std::string to_path = normalize_path(raw_to_path);
  std::deque<folly::StringPiece> from_tokens = tokensOfPath(from_path);
  std::deque<folly::StringPiece> to_tokens = tokensOfPath(to_path);
  // source and destination cannot be the same
  if (from_tokens == to_tokens) {
    err = E::INVALID_PARAM;
    failure_reason =
        "The source path is exactly the same as the destination path";
    return -1;
  }

  if (to_tokens.size() != from_tokens.size()) {
    // destination is not the same length as source
    failure_reason =
        folly::format(
            "Destination is not a sibling of the parent. You cannot change the "
            "parent of {}",
            from_path)
            .str();
    err = E::INVALID_PARAM;
    return -1;
  }
  // make sure neither source nor destination are the root directory
  // boost::split will generate a deque with a single element which is an empty
  // string if the input is empty!
  if (from_tokens.size() == 0 || to_tokens.size() == 0) {
    failure_reason = "You cannot rename the root directory!";
    err = E::INVALID_PARAM;
    return -1;
  }
  // more protection for root directory
  if ((from_tokens.size() == 1 && from_tokens.front() == "") ||
      (to_tokens.size() == 1 && to_tokens.front() == "")) {
    failure_reason = "You cannot rename the root directory!";
    err = E::INVALID_PARAM;
    return -1;
  }

  if (find(to_path) != nullptr) {
    // destination already exists
    failure_reason =
        folly::format("Destination directory '{}' already exists!", to_path)
            .str();
    err = E::EXISTS;
    return -1;
  }
  // check that the source actually exists (whether it's a dir or a log-group)
  LogsConfigTreeNode* source = find(from_path);
  if (source == nullptr) {
    failure_reason =
        folly::format("The path '{}' does not exist!", from_path).str();
    err = E::NOTFOUND;
    return -1;
  }
  // check that destination parent exists
  std::string old_name = from_tokens.back().str();
  std::string new_name = to_tokens.back().str();
  to_tokens.pop_back();
  DirectoryNode* dest_dir;
  if (to_tokens.size() == 0) {
    dest_dir = root_.get();
  } else {
    dest_dir = findDirectory(pathForTokens(to_tokens));
  }
  ld_check(dest_dir != nullptr);
  if (source->type() == NodeType::DIRECTORY) {
    // rename a directory
    static_cast<DirectoryNode*>(source)->setName(new_name);
    dest_dir->replaceChild(old_name, new_name);
  } else {
    // rename a log group node by creating a new one
    LogGroupNode new_group =
        static_cast<LogGroupNode*>(source)->withName(new_name);
    // should not fail since we are not changing attributes
    if (!replaceLogGroup(from_path, new_group, failure_reason)) {
      return -1;
    }
  }
  return 0;
}

int LogsConfigTree::setAttributes(const std::string& path,
                                  const LogAttributes& attrs) {
  std::string failure_reason;
  int ret = setAttributes(path, attrs, failure_reason);
  if (ret != 0) {
    ld_error("Failed to set attributes of '%s': %s, %s",
             path.c_str(),
             error_name(err),
             failure_reason.c_str());
  }
  return ret;
}

int LogsConfigTree::setAttributes(const std::string& path,
                                  const LogAttributes& attrs,
                                  std::string& failure_reason) {
  LogsConfigTreeNode* node = find(path);
  if (node == nullptr) {
    err = E::NOTFOUND;
    return -1;
  }
  if (node->type() == NodeType::DIRECTORY) {
    DirectoryNode* dir = static_cast<DirectoryNode*>(node);
    // copy the directory
    std::unique_ptr<DirectoryNode> new_dir =
        std::make_unique<DirectoryNode>(*dir);
    LogAttributes new_attrs = attrs;
    if (dir == root_.get()) {
      // If the directory is root, we need to re-apply the defaults to the
      // supplied attributes.
      new_attrs = LogAttributes(new_attrs, DefaultLogAttributes());
    }
    if (new_dir->setAttributes(new_attrs, failure_reason)) {
      DirectoryNode* new_dir_ptr = new_dir.get();
      if (dir->parent() == nullptr) {
        // this is the root dir.
        root_ = std::move(new_dir);
      } else {
        dir->parent_->setChild(dir->name(), std::move(new_dir));
      }
      rebuildIndexForDir(new_dir_ptr, true);
      return 0;
    } else {
      // new_dir->setAttributes will set failure_reason
      err = E::INVALID_ATTRIBUTES;
      return -1;
    }
  } else {
    // Log Group
    LogGroupNode replacement =
        static_cast<LogGroupNode*>(node)->withLogAttributes(attrs);
    if (!replaceLogGroup(path, replacement, failure_reason)) {
      return -1;
    }
    return 0;
  }
}

// returns true if the supplied logrange will clash with any of the log_groups
// in the tree
bool LogsConfigTree::doesLogRangeClash(const logid_range_t& range,
                                       std::string& failure_reason) const {
  // use a right open interval because that's what
  // icl::intersect<interval_map,...> expects for default interval_map
  // instantiations
  boost::icl::right_open_interval<logid_t::raw_type> logid_interval(
      range.first.val(), range.second.val() + 1);
  LogMap::const_iterator found = logs_index_.find(logid_interval);
  if (found != logs_index_.end()) {
    boost::icl::right_open_interval<logid_t::raw_type> overlap = found->first;
    failure_reason =
        folly::format(
            "Log id or range \"[{}, {}]\" overlaps with another range: "
            "[{},{}] from Log Group \"{}\"",
            range.first.val(),
            range.second.val(),
            overlap.lower(),
            overlap.upper() - 1,
            found->second.getFullyQualifiedName())
            .str();
    return true;
  }
  return false;
}

void LogsConfigTree::updateLookupIndex(
    const DirectoryNode* parent,
    const std::shared_ptr<LogGroupNode> log_group,
    const bool delete_old) {
  ld_check(log_group != nullptr);
  if (log_group == nullptr) {
    return;
  }
  auto backlogDuration = log_group->attrs().backlogDuration();
  // update the max_backlog_duration
  if (backlogDuration.hasValue() && backlogDuration.value().hasValue()) {
    max_backlog_duration_ =
        std::max(max_backlog_duration_, backlogDuration.value().value());
  }
  const auto& range = log_group->range();
  auto interval = boost::icl::right_open_interval<logid_t::raw_type>(
      range.first.val(), range.second.val() + 1);
  if (delete_old) {
    deleteLogGroupFromLookupIndex(range);
  } else {
    if (folly::kIsDebug) {
      std::string failure_reason;
      bool log_range_clash = doesLogRangeClash(range, failure_reason);
      if (log_range_clash) {
        ld_error("Unexpected log range clash: %s", failure_reason.c_str());
        ld_assert(false);
      }
    }
  }
  logs_index_.insert(
      logs_index_.end(),
      std::make_pair(interval, LogGroupInDirectory{log_group, parent}));
}

bool LogsConfigTree::deleteLogGroupFromLookupIndex(const logid_range_t& range) {
  // delete that log range from the index
  auto iter = logs_index_.find(range.first.val());
  if (iter == logs_index_.end()) {
    // this is wrong, we should always be able to find this here, failing.
    err = E::INVALID_CONFIG;
    ld_error("The log group range '%zu..%zu' doesn't seem to have the "
             "corresponding entry in the logid index. This makes this log "
             "group un-findable by logids",
             range.first.val(),
             range.second.val());
    return false;
  }
  // clears that interval from the index
  logs_index_.erase(iter->first);
  return true;
}

void LogsConfigTree::deleteDirectoryFromLookupIndex(const DirectoryNode* dir) {
  if (dir == nullptr) {
    return;
  }
  for (auto& iter : dir->children()) {
    deleteDirectoryFromLookupIndex(iter.second.get());
  }
  for (auto& iter : dir->logs()) {
    deleteLogGroupFromLookupIndex(iter.second->range());
  }
}

void LogsConfigTree::rebuildIndexForDir(const DirectoryNode* dir,
                                        const bool delete_old) {
  for (const auto& log_item : dir->logs()) {
    updateLookupIndex(dir, log_item.second, delete_old);
  }
  for (const auto& dir_item : dir->children()) {
    rebuildIndexForDir(dir_item.second.get(), delete_old);
  }
}

void LogsConfigTree::rebuildIndex() {
  logs_index_.clear();
  rebuildIndexForDir(root_.get(), false);
}

ReplicationProperty LogsConfigTree::getNarrowestReplication() const {
  return root_->getNarrowestReplication();
}

std::string LogsConfigTree::normalize_path(const std::string& path) const {
  char delimiter = delimiter_.front();
  std::string normal = path;
  boost::trim_if(normal, [&](char c) { return c == delimiter; });
  return normal;
}

std::unique_ptr<LogGroupNode>
LogGroupNode::createFromJson(const std::string& json,
                             const std::string& namespace_delimiter) {
  auto map = folly::parseJson(json);
  return createFromFollyDynamic(map, namespace_delimiter).second;
}

std::pair<std::string, std::unique_ptr<LogGroupNode>>
LogGroupNode::createFromFollyDynamic(const folly::dynamic& log_entry,
                                     const std::string& namespace_delimiter) {
  std::string interval_string;
  interval_t interval_raw;
  logid_range_t log_range;

  if (!parseLogInterval(log_entry, interval_string, interval_raw, false)) {
    log_range = std::make_pair(logid_t(LSN_INVALID), logid_t(LSN_INVALID));
  } else {
    log_range =
        std::make_pair(logid_t(interval_raw.lo), logid_t(interval_raw.hi));
  }

  std::string name;
  bool success = getStringFromMap(log_entry, "name", name);
  if (!success && err != E::NOTFOUND) {
    ld_error("Invalid value for \"name\" attribute of log range '%s'. "
             "Expected a string.",
             interval_string.c_str());
    err = E::INVALID_CONFIG;
    return std::make_pair(std::string(), nullptr);
  }

  folly::Optional<LogAttributes> log_attrs;
  log_attrs =
      parseAttributes(log_entry, interval_string, true /* allow_permissions */);
  if (!log_attrs) {
    // empty log attributes if we couldn't parse
    log_attrs = LogAttributes();
  }

  // Old log API sent the full path to the log as its name
  // The new API no longer does this and only returns the leaf node within the
  // tree, hence we try to extract this from its absolute name
  auto name_offset = name.rfind(namespace_delimiter);
  if (name_offset != std::string::npos && name_offset >= name.length() - 1) {
    ld_error("Invalid value for \"name\" attribute of log range '%s'. "
             "Failed to parse log name because of wrong delimiter. Name: '%s'",
             interval_string.c_str(),
             name.c_str());
    return std::make_pair(std::string(), nullptr);
  }

  return std::make_pair(
      name,
      std::make_unique<LogGroupNode>(name_offset == std::string::npos
                                         ? name
                                         : name.substr(name_offset + 1),
                                     log_attrs.value(),
                                     log_range));
}

}}} // namespace facebook::logdevice::logsconfig
