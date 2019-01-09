/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/LogsConfig.h"

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using facebook::logdevice::configuration::LocalLogsConfig;

constexpr char LogsConfig::default_namespace_delimiter_[];

std::atomic<uint64_t> LogsConfig::max_version{0};

std::string
LogsConfig::getNamespacePrefixedLogRangeName(const std::string& ns,
                                             const std::string& name) const {
  if (ns.empty()) {
    return namespace_delimiter_ + name;
  }
  if (name.empty()) {
    return ns;
  }
  return namespace_delimiter_ + ns + namespace_delimiter_ + name;
}

LogsConfig::NamespaceRangeLookupMap
LogsConfig::getLogRangesByNamespace(const std::string& ns) const {
  LogsConfig::NamespaceRangeLookupMap res;
  Status status = E::INTERNAL;
  folly::Baton<> baton;
  getLogRangesByNamespaceAsync(
      std::move(ns), [&](Status st, LogsConfig::NamespaceRangeLookupMap map) {
        status = st; // can't assign to err because we're on the wrong thread
        res = std::move(map);
        baton.post();
      });
  baton.wait();
  err = status;
  return res;
}

logid_range_t LogsConfig::getLogRangeByName(std::string name) const {
  logid_range_t res;
  Status status = E::INTERNAL;
  folly::Baton<> baton;
  getLogRangeByNameAsync(std::move(name), [&](Status st, logid_range_t range) {
    status = st;
    res = range;
    baton.post();
  });
  baton.wait();
  err = status;
  return res;
}

folly::dynamic LogsConfig::toJson() const {
  folly::dynamic logs = folly::dynamic::array;
  if (isLocal()) {
    auto* local_logs_config = dynamic_cast<const LocalLogsConfig*>(this);
    for (const auto& map_entry : local_logs_config->getLogMap()) {
      std::string interval_str = std::to_string(map_entry.first.lower()) +
          ".." + std::to_string(map_entry.first.upper() - 1);

      auto json_log = map_entry.second.toFollyDynamic();
      logs.push_back(json_log);
    }
  }
  return logs;
}

std::string LogsConfig::toStringImpl() const {
  folly::dynamic logs = toJson();
  folly::json::serialization_opts opts;
  opts.pretty_formatting = true;
  opts.sort_keys = true;
  return folly::json::serialize(logs, opts);
}

std::string LogsConfig::toString() const {
  // Grab the lock and initialize the cached result if this is the first call
  // to toString()
  {
    std::lock_guard<std::mutex> guard(to_string_cache_mutex_);
    if (to_string_cache_.empty()) {
      to_string_cache_ = toStringImpl();
    }
  }
  ld_check(!to_string_cache_.empty());

  // Since the cached string will not change, we can provide a reference to it
  return to_string_cache_;
}
}} // namespace facebook::logdevice
