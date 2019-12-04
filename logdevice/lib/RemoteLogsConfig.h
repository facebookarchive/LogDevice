/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/synchronization/RWSpinLock.h>

#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

/*
 * This class resolves the log configuration via sending requests to servers
 * and parsing the json blobs it receives in response.
 */
class RemoteLogsConfig : public LogsConfig {
 public:
  RemoteLogsConfig(std::chrono::milliseconds timeout,
                   std::chrono::milliseconds cache_ttl)
      : timeout_(timeout),
        max_data_age_(cache_ttl),
        processor_(new std::weak_ptr<Processor>()),
        target_node_info_(std::make_shared<GetLogInfoRequestSharedState>()) {}

  bool isLocal() const override {
    return false;
  }

  bool isFullyLoaded() const override {
    return true;
  }

  std::shared_ptr<LogGroupNode>
  getLogGroupByIDShared(logid_t id) const override;

  void getLogGroupByIDAsync(
      logid_t id,
      std::function<void(std::shared_ptr<LogGroupNode>)> cb) const override;

  bool logExists(logid_t id) const override;

  void getLogRangeByNameAsync(
      std::string name,
      std::function<void(Status, logid_range_t)> cb) const override;

  void getLogRangesByNamespaceAsync(
      const std::string& ns,
      std::function<void(Status, NamespaceRangeLookupMap)> cb) const override;

  std::shared_ptr<logsconfig::LogGroupNode>
  getLogGroup(const std::string& path) const override;

  std::unique_ptr<LogsConfig> copy() const override;
  std::shared_ptr<std::weak_ptr<Processor>> getProcessorPtrPtr();

  RemoteLogsConfig(const RemoteLogsConfig& src);

  /**
   * Returns true if a given node is allowed to send us a CONFIG_CHANGED
   * message to invalidate the config, false otherwise
   */
  bool useConfigChangedMessageFrom(NodeID node_id) const override;

  // used in tests only
  std::shared_ptr<GetLogInfoRequestSharedState> getTargetNodeInfo() const;

 private:
  using RangeLookupMap = LogsConfig::NamespaceRangeLookupMap;

  // Inserts entries into logid->log struct cache
  void insertIntoIdCache(const std::shared_ptr<LogGroupNode>& log) const;

  // attempts to fetch results from cache into res. Returns true on success,
  // false on failure.
  bool getLogRangesByNamespaceCached(const std::string& ns,
                                     NamespaceRangeLookupMap& res) const;

  // Creates the request and posts it to a worker. If posting the request fails,
  // will call the callback with E::FAILED
  int postRequest(LOGS_CONFIG_API_Header::Type request_type,
                  std::string identifier,
                  get_log_info_callback_t callback) const;

  // Recursively processes the result of the BY_NAMESPACE result and adds it
  // to the cache
  static void processDirectoryResult(RemoteLogsConfig* rlc,
                                     RangeLookupMap& map,
                                     const logsconfig::DirectoryNode& directory,
                                     const std::string& parent_name,
                                     const std::string& delimiter);

  std::chrono::milliseconds timeout_ = std::chrono::milliseconds(5000);

  // TTL for the result cache
  std::chrono::milliseconds max_data_age_ = std::chrono::milliseconds(60000);
  // This is a pointer to the client's processor. It is shared for easier
  // substitution across all RemoteLogsConfig instances
  std::shared_ptr<std::weak_ptr<Processor>> processor_;

  // log id -> log struct cache
  struct IDMapEntry {
    std::shared_ptr<LogGroupNode> log;
    std::chrono::steady_clock::time_point time_fetched;
    bool operator==(const IDMapEntry& other) const {
      return other.log.get() == this->log.get();
    }
  };
  mutable folly::RWSpinLock id_cache_mutex;
  typedef boost::icl::interval_map<
      logid_t::raw_type,
      IDMapEntry,
      boost::icl::partial_enricher,
      std::less,
      boost::icl::inplace_plus,
      boost::icl::inter_section,
      boost::icl::right_open_interval<logid_t::raw_type, std::less>>
      IDMap;
  mutable IDMap id_result_cache;

  // Name -> log id range entry cache
  struct NameMapEntry {
    std::pair<logid_t, logid_t> range;
    std::chrono::steady_clock::time_point time_fetched;
  };
  mutable folly::RWSpinLock name_cache_mutex;

  mutable std::map<std::string, NameMapEntry> name_result_cache;
  mutable std::unordered_map<std::string, std::chrono::steady_clock::time_point>
      namespace_last_fetch_times;

  // This is a structure shared between all GetLogInfoRequest instances, which
  // defines where the request should be sent to
  std::shared_ptr<GetLogInfoRequestSharedState> target_node_info_;
};

}} // namespace facebook::logdevice
