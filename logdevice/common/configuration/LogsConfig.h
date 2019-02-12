/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <mutex>

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

/**
 * @file An abstract class that can be queried for logs config data.
 */

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice {

namespace logsconfig {
class LogGroupNode;
};

class LogsConfig {
 public:
  using LogGroupInDirectory =
      facebook::logdevice::logsconfig::LogGroupInDirectory;
  using LogGroupNode = facebook::logdevice::logsconfig::LogGroupNode;
  using LogAttributes = facebook::logdevice::logsconfig::LogAttributes;
  /**
   * the type that getLogRangesByNamespace() returns
   */
  typedef std::map<std::string, logid_range_t> NamespaceRangeLookupMap;
  using LoadFileCallback = std::function<Status(const char*, std::string*)>;

  LogsConfig() {
    version_ = ++max_version;
  }
  // copy constructor has to be explicitly defined because mutex is not
  // copyable
  LogsConfig(const LogsConfig& other) {
    version_ = other.version_;
    namespace_delimiter_ = other.namespace_delimiter_;
  }

  virtual std::string toString() const;
  virtual folly::dynamic toJson() const;
  /**
   * Returns true if the full logs config is available locally.
   */
  virtual bool isLocal() const = 0;

  /**
   * Gets a particular log's config struct synchronously. Can be used when
   * the caller doesn't care about blocking. Shouldn't be called from a worker
   * thread on the client.
   * On error returns nullptr and sets err.
   * See getLogGroupByIDAsync() for the list of error codes.
   *
   * @param id                    ID of the log
   * @return                      Returns a shared_ptr to the log struct.
   */
  virtual std::shared_ptr<LogGroupNode>
  getLogGroupByIDShared(logid_t id) const = 0;

  /**
   * Will call the submitted callback asynchronously with a shared_ptr to a
   * particular log's config struct. Should be used for non-blocking
   * operations.
   * Note that callback might be called inline.
   *
   * @param id                    ID of the log
   *
   * On error call back is called with nullptr argument and err set to:
   *  - NOTFOUND if the log doesn't exist.
   *  - SHUTDOWN if Client was destroyed while the request was outstanding.
   */
  virtual void getLogGroupByIDAsync(
      logid_t id,
      std::function<void(std::shared_ptr<LogGroupNode>)> cb) const = 0;

  /**
   * Checks if a log exists in the logs config.
   *
   * @param id ID of the log
   */
  virtual bool logExists(logid_t id) const = 0;

  /**
   * Looks up the boundaries of a log range by its name. Note that this method
   * should not be called on a worker thread on a client.
   *
   * @return  if configuration has a JSON object in the "logs" section
   *          with "name" attribute @param name, returns a pair containing
   *          the lowest and  highest log ids in the range (this may be the
   *          same id for log ranges of size 1). Otherwise returns a pair
   *          where both ids are set to LOGID_INVALID.
   */
  virtual logid_range_t getLogRangeByName(std::string name) const;

  /**
   * Looks up the logs config tree for a log group defined at the supplied path.
   *
   * @return  if a log group was found at the supplied path, the returned object
   *          will hold the attributes and name (not full path) for that
   *          log group.
   */
  virtual std::shared_ptr<logsconfig::LogGroupNode>
  getLogGroup(const std::string& path) const = 0;

  /**
   * Looks up the boundaries of all log ranges that have a "name" attribute
   * set and belong to the namespace @param ns. Note that this method should not
   * be called on a worker thread on a client.
   *
   * @return  A map from log range name to a pair of the lowest and highest log
   *          ids in the range (this may be the same id for log ranges of size
   *          1). If empty, err contains an error code if the operation failed,
   *          or E::OK if the operation succeeded but there are no log ranges
   *          in the namespace.
   */
  virtual std::map<std::string, logid_range_t>
  getLogRangesByNamespace(const std::string& ns) const;

  /**
   * Looks up the boundaries of all log ranges that have a "name" attribute
   * set and belong to the namespace @param ns. Calls cb with status (E::OK,
   * E::FAILED or E::NOTFOUND) and the result (identical to the return value of
   * getLogRangesByNamespace()).
   *
   * The callback can be invoked synchronously if the logs config is
   * immediately available. This comes with risk of deadlock, for example if the
   * client-supplied callback needs to acquire a mutex that is already held when
   * this call is made. For safety in such cases, it may be best to release
   * unneeded mutexes before calling this method, or use a recursive mutex.
   */
  virtual void getLogRangesByNamespaceAsync(
      const std::string& ns,
      std::function<void(Status, std::map<std::string, logid_range_t>)> cb)
      const = 0;

  /**
   * Gets a [from,to] range of logids corresponding to the named log group.
   * Calls cb with a status (E::OK, E::FAILED or E::NOTFOUND) and the result
   *
   * @param name  name of the log group
   * @param cb    result callback
   */
  virtual void getLogRangeByNameAsync(
      std::string name,
      std::function<void(Status, logid_range_t)> cb) const = 0;

  /**
   * Makes a copy of *this and returns a unique_ptr to it.
   */
  virtual std::unique_ptr<LogsConfig> copy() const = 0;

  virtual ~LogsConfig(){};

  // Returns the namespace delimiter character
  const std::string& getNamespaceDelimiter() const {
    return namespace_delimiter_;
  }

  // Sets the namespace delimiter character - this should be imported from
  // the config file
  void setNamespaceDelimiter(const std::string delim) {
    namespace_delimiter_ = delim;
  }

  /**
   * Whether this configuration is fully loaded or not is important for other
   * logdeviced components to decide whether to rely on this config or not.
   * An initial config that a server creates might be
   * empty, a full config will be loaded from a config-file, LogsConfigManager
   * or RemoteLogsConfig
   */
  virtual bool isFullyLoaded() const {
    // if it's not explicitly overridden, it's false;
    return false;
  }

  /**
   * Whether this logid is an internal (reserved) logid that is internally used
   * by logdevice or not
   */
  virtual bool isInternalLogID(logid_t logid) const {
    return configuration::InternalLogs::isInternal(logid);
  }

  /**
   * Returns the log name prefixed with the given namespace.
   */
  std::string getNamespacePrefixedLogRangeName(const std::string& ns,
                                               const std::string& name) const;

  constexpr static char default_namespace_delimiter_[] = "/";

  virtual uint64_t getVersion() const {
    return version_;
  }

  /**
   * Returns true if a given node is allowed to send us a CONFIG_CHANGED
   * message to invalidate the config, false otherwise
   */
  virtual bool useConfigChangedMessageFrom(NodeID /*node_id*/) const {
    // used for RemoteLogsConfig only
    return false;
  }

 protected:
  // toString() can be expensive for large configs and useful for monitoring
  // in production, so cache the output
  virtual std::string toStringImpl() const;
  mutable std::mutex to_string_cache_mutex_;
  mutable std::string to_string_cache_;

 private:
  // Configured delimiter for namespaces
  std::string namespace_delimiter_;
  // Version of this LogsConfig
  uint64_t version_;
  // Max version seen
  static std::atomic<uint64_t> max_version;
};

}} // namespace facebook::logdevice
