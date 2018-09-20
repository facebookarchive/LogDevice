/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace client {

// a wrapper around facebook::logdevice::logsconfig::LogAttributes
using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;

class LogGroup;
class Directory;

using DirectoryMap =
    std::unordered_map<std::string, std::unique_ptr<Directory>>;
using LogGroupMap = std::unordered_map<std::string, std::shared_ptr<LogGroup>>;

/**
 * Defines a group of logid (log range) in the general LogsConfig tree, this
 * holds the attributes set for this group, the id range for logs, and the name
 * of this log group.
 *
 * Note that name() does not return the FQLN (Fully Qualified Log Name), it only
 * returns the name defined for this group.
 */
class LogGroup {
 public:
  virtual const logid_range_t& range() const = 0;
  virtual const std::string& name() const = 0;
  virtual const std::string& getFullyQualifiedName() const = 0;
  virtual const LogAttributes& attrs() const = 0;
  virtual uint64_t version() const = 0;

  virtual ~LogGroup() = default;

 protected:
  LogGroup() = default;
  // non-copyable && non-assignable
  LogGroup(const LogGroup&) = delete;
  LogGroup& operator=(const LogGroup&) = delete;
};

/**
 * Defines a directory of logs in the general LogsConfig tree, this holds the
 * children directories and LogGroup objects in this directory.
 */
class Directory {
 public:
  virtual const std::string& name() const = 0;
  virtual const std::string& getFullyQualifiedName() const = 0;
  virtual const LogAttributes& attrs() const = 0;
  virtual const DirectoryMap& children() const = 0;
  virtual const LogGroupMap& logs() const = 0;
  virtual Directory* parent() const = 0;
  virtual uint64_t version() const = 0;

  virtual ~Directory() = default;

 protected:
  Directory() = default;
  // non-copyable && non-assignable
  Directory(const Directory&) = delete;
  Directory& operator=(const Directory&) = delete;
};

}}} // namespace facebook::logdevice::client
