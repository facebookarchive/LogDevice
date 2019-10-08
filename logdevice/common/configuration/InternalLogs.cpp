/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/InternalLogs.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace configuration {

constexpr logid_t InternalLogs::CONFIG_LOG_SNAPSHOTS;
constexpr logid_t InternalLogs::CONFIG_LOG_DELTAS;
constexpr logid_t InternalLogs::EVENT_LOG_SNAPSHOTS;
constexpr logid_t InternalLogs::EVENT_LOG_DELTAS;
constexpr logid_t InternalLogs::MAINTENANCE_LOG_DELTAS;
constexpr logid_t InternalLogs::MAINTENANCE_LOG_SNAPSHOTS;

const InternalLogs::NameLookupMap& InternalLogs::nameLookup() {
  static NameLookupMap inst = {
      {"config_log_snapshots", CONFIG_LOG_SNAPSHOTS},
      {"config_log_deltas", CONFIG_LOG_DELTAS},
      {"event_log_snapshots", EVENT_LOG_SNAPSHOTS},
      {"event_log_deltas", EVENT_LOG_DELTAS},
      {"maintenance_log_deltas", MAINTENANCE_LOG_DELTAS},
      {"maintenance_log_snapshots", MAINTENANCE_LOG_SNAPSHOTS}};
  return inst;
}

uint32_t InternalLogs::numInternalLogs() {
  return nameLookup().size();
}

logid_t InternalLogs::lookupByName(const std::string& name) {
  auto it = nameLookup().find(name);
  if (it == nameLookup().end()) {
    return LOGID_INVALID;
  }
  return it->second;
}

std::string InternalLogs::lookupByID(logid_t log_id) {
  std::string out;
  const auto& name_lookup = nameLookup();
  auto it = std::find_if(name_lookup.begin(), name_lookup.end(), [&](auto&& p) {
    return p.second == log_id;
  });

  if (it != name_lookup.end()) {
    out = it->first;
  }
  return out;
}

InternalLogs::InternalLogs(std::string ns_delimiter)
    : ns_delimiter_(std::move(ns_delimiter)) {
  reset();
}

bool InternalLogs::isInternal(logid_t logid) {
  return !MetaDataLog::isMetaDataLog(logid) && logid > USER_LOGID_MAX;
}

void InternalLogs::reset() {
  root_ = std::make_unique<logsconfig::DirectoryNode>(
      ns_delimiter_, logsconfig::DefaultLogAttributes());
  logs_index_.clear();
}

InternalLogs::InternalLogs(const InternalLogs& other) {
  *this = other;
}

InternalLogs& InternalLogs::operator=(const InternalLogs& other) {
  reset();

  for (auto it = other.logsBegin(); it != other.logsEnd(); ++it) {
    auto n =
        insert(it->second.log_group->name(), it->second.log_group->attrs());
    ld_check(n);
  }

  return *this;
}

std::shared_ptr<logsconfig::LogGroupNode>
InternalLogs::insert(const std::string& name, logsconfig::LogAttributes attrs) {
  const logid_t logid = lookupByName(name);
  if (logid == LOGID_INVALID) {
    ld_error("Internal log \"%s\" does not exist", name.c_str());
    err = E::NOTFOUND;
    return nullptr;
  }

  ld_check(logid > USER_LOGID_MAX);
  const logid_range_t range(logid, logid);
  std::string failure_reason;
  auto res = root_->addLogGroup(name, range, attrs, false, failure_reason);
  if (!res) {
    ld_info("Cannot add internal log: %s, %s",
            error_name(err),
            failure_reason.c_str());
    return nullptr;
  }

  auto p = logsconfig::LogGroupInDirectory{res, root_.get()};
  auto interval = boost::icl::right_open_interval<logid_t::raw_type>(
      range.first.val(), range.second.val() + 1);
  logs_index_.insert(logs_index_.end(), std::make_pair(interval, p));

  return res;
}

bool InternalLogs::isValid() const {
  for (auto& l : nameLookup()) {
    auto it = logs_index_.find(l.second.val_);
    if (it == logs_index_.end()) {
      continue;
    }
    const logsconfig::LogGroupInDirectory& p = it->second;
    std::string failure;
    if (!logsconfig::LogGroupNode::isValid(
            p.parent, p.log_group.get(), failure)) {
      ld_warning("InternalLogs are invalid: %s", failure.c_str());
      return false;
    }

    // backlog duration must be not be set.
    if (p.log_group->attrs().backlogDuration() &&
        p.log_group->attrs().backlogDuration().value().hasValue()) {
      ld_error("\"backlog\" attribute must not be set for internal log "
               "\"%s\" (%lu)",
               l.first.c_str(),
               l.second.val_);
      return false;
    }
  }

  return true;
}

const logsconfig::LogGroupInDirectory*
InternalLogs::getLogGroupByID(logid_t logid) const {
  auto iter = logs_index_.find(logid.val_);
  if (iter == logs_index_.end()) {
    err = E::NOTFOUND;
    return nullptr;
  }
  return &iter->second;
}

bool InternalLogs::logExists(logid_t logid) const {
  auto iter = logs_index_.find(MetaDataLog::dataLogID(logid).val_);
  if (iter == logs_index_.end()) {
    err = E::NOTFOUND;
    return false;
  }
  return true;
}

folly::dynamic InternalLogs::toDynamic() const {
  folly::dynamic logs = folly::dynamic::object;

  for (auto& l : nameLookup()) {
    auto it = logs_index_.find(l.second.val_);
    if (it == logs_index_.end()) {
      continue;
    }
    logs[l.first] = it->second.toFollyDynamic();
  }

  return logs;
}

}}} // namespace facebook::logdevice::configuration
