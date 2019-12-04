/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/algorithm/string/predicate.hpp>

#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/LogsConfigTypes.h"

namespace facebook { namespace logdevice { namespace client {

class LogGroupImpl;
class DirectoryImpl;

class LogGroupImpl : public LogGroup {
 public:
  using LogGroupNode = facebook::logdevice::logsconfig::LogGroupNode;
  explicit LogGroupImpl(std::shared_ptr<LogGroupNode> lgn,
                        const std::string& full_path,
                        uint64_t version)
      : node_(std::move(lgn)),
        fully_qualified_name_(full_path),
        version_(version) {}

  const logid_range_t& range() const override {
    return node_->range();
  }

  const std::string& name() const override {
    return node_->name();
  }

  // Returns a the attributes associated with this node
  const LogAttributes& attrs() const override {
    return node_->attrs();
  }

  const std::string& getFullyQualifiedName() const override {
    return fully_qualified_name_;
  }

  uint64_t version() const override {
    return version_;
  }

 private:
  std::shared_ptr<LogGroupNode> node_;
  std::string fully_qualified_name_;
  uint64_t version_;
};

class DirectoryImpl : public Directory {
 public:
  explicit DirectoryImpl(DirectoryImpl* parent) : parent_(parent) {}
  explicit DirectoryImpl(const logsconfig::DirectoryNode& node,
                         DirectoryImpl* parent,
                         const std::string& full_path,
                         const std::string& delim,
                         uint64_t version)
      : name_(node.name()),
        parent_(parent),
        attrs_(node.attrs()),
        version_(version) {
    // make sure that our path ends with the delimiter
    if (!boost::algorithm::ends_with(full_path, delim)) {
      fully_qualified_name_ = full_path + delim;
    } else {
      fully_qualified_name_ = full_path;
    }
    // populate the children and logs map(s)
    for (const auto& dir : node.children()) {
      children_[dir.first] =
          std::make_unique<DirectoryImpl>(*dir.second,
                                          this,
                                          fully_qualified_name_ + dir.first,
                                          delim,
                                          version_);
    }
    for (const auto& lg : node.logs()) {
      logs_[lg.first] = std::make_shared<LogGroupImpl>(
          lg.second, fully_qualified_name_ + lg.first, version_);
    }
  }

  const std::string& name() const override {
    return name_;
  }

  // Returns a the attributes associated with this node
  const LogAttributes& attrs() const override {
    return attrs_;
  }

  const DirectoryMap& children() const override {
    return children_;
  }

  DirectoryImpl* parent() const override {
    return parent_;
  }

  const LogGroupMap& logs() const override {
    return logs_;
  }

  const std::string& getFullyQualifiedName() const override {
    return fully_qualified_name_;
  }

  uint64_t version() const override {
    return version_;
  }

 private:
  DirectoryMap children_;
  std::string name_;
  std::string fully_qualified_name_;
  LogGroupMap logs_;
  DirectoryImpl* parent_;
  LogAttributes attrs_;
  uint64_t version_;
};
}}} // namespace facebook::logdevice::client
