/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Map.h>
#include <folly/dynamic.h>

#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/types.h"

/**
 * @file InternalLogs defines the list of internal logs that may be configured
 * in all clusters.
 *
 * In order to "activate" an internal log, one must configure it in the
 * "internal_logs" section of the main config:
 *
 * "internal_logs": {
 *   "event_log_deltas": {
 *     "replication_factor": 5,
 *     "nodeset_size": 10,
 *   },
 *   "event_log_snapshots": {
 *     "replication_factor": 7,
 *     "nodeset_size": 10,
 *   }
 * },
 *
 */

namespace facebook { namespace logdevice { namespace configuration {

class InternalLogs {
 public:
  static constexpr logid_t CONFIG_LOG_SNAPSHOTS{4611686018427387900};
  static constexpr logid_t CONFIG_LOG_DELTAS{4611686018427387901};
  static constexpr logid_t EVENT_LOG_SNAPSHOTS{4611686018427387902};
  static constexpr logid_t EVENT_LOG_DELTAS{4611686018427387903};
  static constexpr logid_t MAINTENANCE_LOG_SNAPSHOTS{4611686018427387898};
  static constexpr logid_t MAINTENANCE_LOG_DELTAS{4611686018427387899};

  using NameLookupMap = folly::F14FastMap<std::string, logid_t>;
  static uint32_t numInternalLogs();
  static const NameLookupMap& nameLookup();
  static logid_t lookupByName(const std::string& name);
  static std::string lookupByID(logid_t);

  explicit InternalLogs(std::string ns_delimiter = "/");
  InternalLogs(InternalLogs&& other) = default;
  InternalLogs& operator=(InternalLogs&& other) = default;
  InternalLogs(const InternalLogs& other);
  InternalLogs& operator=(const InternalLogs& other);

  /**
   * Get the configuration of an internal log.
   *
   * @param logid Id of the internal log.
   * @return a pointer to the LogGroupInDirectory object on success, or nullptr
   *         and err is set to E::NOTFOUND.
   */
  const logsconfig::LogGroupInDirectory* getLogGroupByID(logid_t logid) const;

  /**
   * Check if an internal log exists, ie has been configured by the user.
   *
   * @param logid Log id of the internal log.
   * @return true if the internal log has been successfully configured by the
   *         user, or false otherwise and err is set to E::NOTFOUND.
   */
  bool logExists(logid_t logid) const;

  /**
   * Called by config parsers to register the configuration for an internal log.
   *
   * @param name of the internal log.
   * @param attrs attributes of the internal log.
   *
   * @return the new LogGroupNode on success (that log group only contains one
   * log id), or nullptr and err is set to:
   * - E::NOTFOUND if there is not internal* log with the given name;
   * - E::EXISTS if that internal log has already been configured;
   * - E::INVALID_CONFIG if the attributes are not valid.
   */
  std::shared_ptr<logsconfig::LogGroupNode>
  insert(const std::string& name, logsconfig::LogAttributes attrs);

  using const_iterator = logsconfig::LogMap::element_const_iterator;
  using const_reverse_iterator =
      logsconfig::LogMap::element_const_reverse_iterator;

  const_iterator logsBegin() const {
    return boost::icl::elements_begin(logs_index_);
  }

  const_iterator logsEnd() const {
    return boost::icl::elements_end(logs_index_);
  }

  const_reverse_iterator logsRBegin() const {
    return boost::icl::elements_rbegin(logs_index_);
  }

  const_reverse_iterator logsREnd() const {
    return boost::icl::elements_rend(logs_index_);
  }

  /**
   * @return number of internal logs that have been activated.
   */
  size_t size() const {
    return logs_index_.size();
  }

  bool isValid() const;

  /**
   * @return whether or not the given log id is within the reserved log id range
   * for internal logs.
   */
  static bool isInternal(logid_t logid);

  /**
   * @return folly::dynamic representation of this object. Used to generate the
   * "internal_logs" section of the main config when dumping it in json format.
   */
  folly::dynamic toDynamic() const;

 private:
  void reset();

  const std::string ns_delimiter_;

  // A single directory that contains all the internal logs that have been
  // activated by the user.
  std::unique_ptr<logsconfig::DirectoryNode> root_;
  // An index to iterate or search the activated logs by log id.
  logsconfig::LogMap logs_index_;
};

}}} // namespace facebook::logdevice::configuration
