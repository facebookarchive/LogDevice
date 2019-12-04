/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/RemoteLogsConfig.h"

#include <deque>
#include <shared_mutex>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <folly/Random.h>
#include <folly/json.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/include/LogAttributes.h"

using folly::RWSpinLock;
using std::chrono::duration;
using std::chrono::steady_clock;
using namespace facebook::logdevice::configuration::parser;
using facebook::logdevice::logsconfig::LogGroupNode;

namespace facebook { namespace logdevice {

std::shared_ptr<LogGroupNode>
RemoteLogsConfig::getLogGroupByIDShared(logid_t id) const {
  std::shared_ptr<LogsConfig::LogGroupNode> res;
  Semaphore sem;
  this->getLogGroupByIDAsync(
      id, [&res, &sem](const std::shared_ptr<LogsConfig::LogGroupNode> loggrp) {
        res = loggrp;
        sem.post();
      });
  sem.wait();
  return res;
}

int RemoteLogsConfig::postRequest(LOGS_CONFIG_API_Header::Type request_type,
                                  std::string identifier,
                                  get_log_info_callback_t callback) const {
  ld_check(processor_);
  std::shared_ptr<Processor> processor = processor_->lock();
  if (!processor) {
    ld_critical("Attempting to post request to a processor when there is none");
    ld_check(false);
    err = E::INTERNAL;
    return -1;
  }

  // Set the callback worker ID
  auto w = Worker::onThisThread(false);
  worker_id_t callback_thread_affinity = (w ? w->idx_ : worker_id_t(-1));

  // Set the processing worker ID
  worker_id_t worker_id;
  {
    std::lock_guard<std::mutex> lock(target_node_info_->mutex_);
    if (target_node_info_->worker_id_.val_ == -1) {
      // select random worker
      target_node_info_->worker_id_ = processor->selectWorkerRandomly(
          folly::Random::rand64(), WorkerType::GENERAL);
    }
    worker_id = target_node_info_->worker_id_;
  }

  std::unique_ptr<Request> req(new GetLogInfoRequest(request_type,
                                                     identifier,
                                                     timeout_,
                                                     target_node_info_,
                                                     callback,
                                                     callback_thread_affinity));
  int rv = processor->postWithRetrying(req);
  if (rv == -1) {
    ld_error("Failed to post request to %s: %s",
             Worker::getName(req->getWorkerTypeAffinity(), worker_id).c_str(),
             error_description(err));
    static_cast<GetLogInfoRequest*>(req.get())->finalize(E::FAILED, false);
  }
  return rv;
}

void RemoteLogsConfig::getLogGroupByIDAsync(
    logid_t id,
    std::function<void(std::shared_ptr<LogGroupNode>)> cb) const {
  // Process internal logs and metadata logs separately since their attributes
  // are in server config, not in logs config.
  // We have server config locally, so no need to talk to servers.
  if (MetaDataLog::isMetaDataLog(id) ||
      configuration::InternalLogs::isInternal(id)) {
    std::shared_ptr<Processor> processor = processor_->lock();
    if (!processor) {
      ld_critical("Attempting to post request to a processor when there is none");
      ld_check(false);
      err = E::INTERNAL;
      cb(nullptr);
      return;
    }

    auto server_config = processor->config_->getServerConfig();
    if (MetaDataLog::isMetaDataLog(id)) {
      // Don't check that the corresponding data log exists, to allow reading
      // metadata log left over after data log was deleted.
      cb(server_config->getMetaDataLogsConfig().metadata_log_group);
    } else {
      const logsconfig::LogGroupInDirectory* in_dir =
          server_config->getInternalLogsConfig().getLogGroupByID(id);
      if (in_dir == nullptr) {
        err = E::NOTFOUND;
        cb(nullptr);
      } else {
        cb(in_dir->log_group);
      }
    }
    return;
  }

  {
    // Attempting to fetch result from cache
    std::shared_lock<RWSpinLock> lock(id_cache_mutex);
    auto it = id_result_cache.find(id.val_);
    if (it != id_result_cache.end()) {
      // potential cache hit - check timestamp
      steady_clock::time_point tnow = steady_clock::now();
      if (tnow - it->second.time_fetched <= max_data_age_) {
        // data not too old - return from cache
        auto log_sptr_copy = it->second.log;
        lock.unlock();
        cb(std::move(log_sptr_copy));
        return;
      }
    }
  }

  std::string delimiter = getNamespaceDelimiter();
  // No suitable results in cache - proceed to get them from remote hosts.
  auto request_callback = [cb, delimiter](Status st, std::string payload) {
    auto config = Worker::onThisThread()->getConfig();
    RemoteLogsConfig* rlc = checked_downcast<RemoteLogsConfig*>(
        const_cast<LogsConfig*>(config->logsConfig().get()));
    if (!rlc || st != E::OK) {
      // failing if the status is not okay or the LogsConfig is not an instance
      // of RemoteLogsConfig anymore
      err = !rlc ? E::FAILED : st;
      cb(nullptr);
      return;
    }

    ld_assert(payload.size() > 0);

    std::unique_ptr<logsconfig::LogGroupWithParentPath> lid =
        logsconfig::FBuffersLogsConfigCodec::deserialize<
            logsconfig::LogGroupWithParentPath>(
            Payload(payload.data(), payload.size()), delimiter);
    ld_check(lid != nullptr);
    if (lid == nullptr) {
      ld_error("Failed to deserialize LogGroupWithParentPath result");
      cb(nullptr);
      return;
    }

    const std::shared_ptr<LogGroupNode> log_shared = lid->log_group;

    // Placing result in cache
    rlc->insertIntoIdCache(log_shared);

    // Returning to client
    cb(std::move(log_shared));
  };
  int rv = this->postRequest(LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID,
                             std::to_string(id.val()),
                             request_callback);
  if (rv != 0) {
    err = E::INTERNAL;
    cb(nullptr);
  }
}

void RemoteLogsConfig::insertIntoIdCache(
    const std::shared_ptr<LogGroupNode>& log) const {
  auto range = log->range();
  boost::icl::right_open_interval<logid_t::raw_type> interval(
      range.first.val_, range.second.val_ + 1);
  IDMapEntry entry{log, steady_clock::now()};
  std::unique_lock<RWSpinLock> lock(id_cache_mutex);
  id_result_cache.erase(interval);
  id_result_cache.insert(std::make_pair(interval, entry));
}

bool RemoteLogsConfig::logExists(logid_t id) const {
  bool res = false;
  Semaphore sem;
  this->getLogGroupByIDAsync(
      id,
      [&res, &sem](const std::shared_ptr<const LogsConfig::LogGroupNode> log) {
        res = (bool)log;
        sem.post();
      });
  sem.wait();
  return res;
}

void RemoteLogsConfig::getLogRangeByNameAsync(
    std::string name,
    std::function<void(Status, logid_range_t)> cb) const {
  std::string delimiter = getNamespaceDelimiter();
  // clean up the name to be resilient to superfluous (or missing)
  // delimiters
  boost::trim_if(name, boost::is_any_of(delimiter));
  // Attempting to fetch result from cache
  {
    std::shared_lock<RWSpinLock> lock(name_cache_mutex);
    auto it = name_result_cache.find(name);
    if (it != name_result_cache.end()) {
      // potential cache hit - check timestamp
      steady_clock::time_point tnow = steady_clock::now();
      if (tnow - it->second.time_fetched <= max_data_age_) {
        // data not too old - return from cache
        cb(E::OK, it->second.range);
        return;
      }
    }
  }

  auto request_callback = [delimiter, name, cb](
                              Status st, std::string payload) {
    auto config = Worker::onThisThread()->getConfig();
    RemoteLogsConfig* rlc = dynamic_cast<RemoteLogsConfig*>(
        const_cast<LogsConfig*>(config->logsConfig().get()));
    if (!rlc || st != E::OK) {
      // The request failed, or the LogsConfig isn't RemoteLogsConfig anymore
      cb(st == E::NOTFOUND ? E::NOTFOUND : E::FAILED,
         std::make_pair(logid_t(LSN_INVALID), logid_t(LSN_INVALID)));
      return;
    }

    ld_assert(payload.size() > 0);

    // deserialize the payload
    std::shared_ptr<logsconfig::LogGroupNode> log_group =
        LogsConfigStateMachine::deserializeLogGroup(payload, delimiter);
    ld_assert(log_group != nullptr);
    if (log_group == nullptr) {
      ld_error("Failed to parse LogGroupNode result");
      cb(E::FAILED, std::make_pair(logid_t(LSN_INVALID), logid_t(LSN_INVALID)));
      return;
    }

    logid_range_t res = log_group->range();
    {
      // writing to name->range cache
      std::unique_lock<RWSpinLock> lock(rlc->name_cache_mutex);
      NameMapEntry entry{res, steady_clock::now()};
      rlc->name_result_cache[name] = std::move(entry);
    }
    rlc->insertIntoIdCache(log_group);
    cb(E::OK, res);
  };

  this->postRequest(LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_NAME,
                    name,
                    request_callback);
}

std::shared_ptr<logsconfig::LogGroupNode>
RemoteLogsConfig::getLogGroup(const std::string& path) const {
  std::shared_ptr<LogGroupNode> result;
  Semaphore sem;
  auto range = this->getLogRangeByName(path);
  // the log range name is the last piece of the path
  std::string rangeName = path;
  boost::trim_if(rangeName, boost::is_any_of(getNamespaceDelimiter()));
  std::deque<std::string> tokens;
  boost::split(tokens, rangeName, boost::is_any_of(getNamespaceDelimiter()));
  rangeName = tokens.back();
  this->getLogGroupByIDAsync(
      range.first,
      [&result, &sem](const std::shared_ptr<LogsConfig::LogGroupNode> log) {
        result = log;
        sem.post();
      });
  sem.wait();
  return result;
}

// attempts to fetch results from cache into res. Returns true on success,
// false on failure.
bool RemoteLogsConfig::getLogRangesByNamespaceCached(
    const std::string& ns,
    RangeLookupMap& res) const {
  // using the same mutex as the name cache
  std::shared_lock<RWSpinLock> lock(name_cache_mutex);
  auto ns_it = namespace_last_fetch_times.find(ns);
  if (ns_it == namespace_last_fetch_times.end()) {
    // no result in cache
    return false;
  }

  steady_clock::time_point tnow = steady_clock::now();
  if (tnow - ns_it->second > max_data_age_) {
    // result too old
    return false;
  }

  // returning result from name cache if all the entries in the cache have
  // the same fetch time as the namespace entry. This check is necessary so
  // the result doesn't contain mixed older and newer entries (which might be
  // important on config change).
  std::string prefix = ns;
  if (!ns.empty()) {
    prefix += getNamespaceDelimiter();
  }

  for (auto name_iter = name_result_cache.lower_bound(prefix);
       name_iter != name_result_cache.end();
       ++name_iter) {
    auto& name = name_iter->first;
    if ((prefix.size() > name.size()) ||
        (name.compare(0, prefix.size(), prefix) != 0)) {
      // prefix doesn't match the iterator anymore
      break;
    }
    if (name_iter->second.time_fetched != ns_it->second) {
      // Time mismatch - can't mix entries with different times
      res.clear();
      return false;
    }
    res.insert(std::make_pair(name_iter->first, name_iter->second.range));
  }
  return true;
}

void RemoteLogsConfig::processDirectoryResult(
    RemoteLogsConfig* rlc,
    RangeLookupMap& map,
    const logsconfig::DirectoryNode& directory,
    const std::string& parent_name,
    const std::string& delimiter) {
  std::string directory_name;

  // Directory names should always end with delimiter
  if (!boost::algorithm::ends_with(parent_name, delimiter)) {
    directory_name = parent_name + delimiter + directory.name();
  } else {
    directory_name = parent_name + directory.name();
  }

  for (const auto& lg : directory.logs()) {
    // We have to flatten the LogGroup structures so they fit into the
    // cache and do not depend on RAW DirectoryNode pointers
    std::shared_ptr<logsconfig::LogGroupNode> flat_log_group =
        std::make_shared<logsconfig::LogGroupNode>(
            directory_name + delimiter + lg.second->name(),
            lg.second->attrs(),
            lg.second->range());
    map.insert(std::make_pair(flat_log_group->name(), flat_log_group->range()));

    // writing to name->range cache
    NameMapEntry entry{flat_log_group->range(), steady_clock::now()};
    rlc->name_result_cache[flat_log_group->name()] = std::move(entry);

    // writing to id -> log info cache
    rlc->insertIntoIdCache(flat_log_group);
  }

  // Process children recursively
  for (const auto& dir : directory.children()) {
    processDirectoryResult(rlc, map, *dir.second, directory_name, delimiter);
  }
}

void RemoteLogsConfig::getLogRangesByNamespaceAsync(
    const std::string& ns,
    std::function<void(Status, RangeLookupMap)> cb) const {
  std::string delimiter = getNamespaceDelimiter();

  // Calls from Client are automatically namespaced / absolute, but internally
  // they might not be.
  std::string path;
  if (ns.size() > 0 && ns.compare(0, delimiter.size(), delimiter) == 0) {
    path = ns;
  } else {
    path = delimiter + ns;
  }

  RangeLookupMap rangeMap;
  // Attempting to fetch result from cache
  if (getLogRangesByNamespaceCached(path, rangeMap)) {
    if (rangeMap.empty()) {
      cb(E::NOTFOUND, std::move(rangeMap));
    } else {
      cb(E::OK, std::move(rangeMap));
    }
    return;
  }

  auto request_callback = [path, cb, delimiter](
                              Status st, std::string payload) {
    auto config = Worker::onThisThread()->getConfig();
    RemoteLogsConfig* rlc = dynamic_cast<RemoteLogsConfig*>(
        const_cast<LogsConfig*>(config->logsConfig().get()));
    if (!rlc || st != E::OK) {
      // The request failed, or the LogsConfig isn't RemoteLogsConfig anymore
      cb(st == E::NOTFOUND ? E::NOTFOUND : E::FAILED, RangeLookupMap());
      return;
    }

    ld_check(payload.size() > 0);

    // deserialize the payload
    std::unique_ptr<logsconfig::DirectoryNode> directory =
        LogsConfigStateMachine::deserializeDirectory(payload, delimiter);
    if (directory == nullptr) {
      ld_error("Error parsing DirectoryNode result");
      cb(E::FAILED, RangeLookupMap());
      return;
    }

    // writing to namespace timing cache
    auto now = steady_clock::now();
    std::unique_lock<RWSpinLock> lock(rlc->name_cache_mutex);
    rlc->namespace_last_fetch_times[path] = now;

    RangeLookupMap res;
    processDirectoryResult(rlc, res, *directory, "", delimiter);
    cb(res.empty() ? E::NOTFOUND : E::OK, res);
  };

  this->postRequest(
      LOGS_CONFIG_API_Header::Type::GET_DIRECTORY, path, request_callback);
}

std::unique_ptr<LogsConfig> RemoteLogsConfig::copy() const {
  return std::unique_ptr<LogsConfig>(new RemoteLogsConfig(*this));
}

std::shared_ptr<std::weak_ptr<Processor>>
RemoteLogsConfig::getProcessorPtrPtr() {
  return processor_;
}

RemoteLogsConfig::RemoteLogsConfig(const RemoteLogsConfig& src)
    : LogsConfig(src) {
  // not copying the cache
  timeout_ = src.timeout_;
  max_data_age_ = src.max_data_age_;
  processor_ = src.processor_;
  target_node_info_ = src.target_node_info_;

  // Enabling sending LOGS_CONFIG_API messages if it's disabled
  std::unique_lock<std::mutex> lock(target_node_info_->mutex_);
  if (!target_node_info_->message_sending_enabled_) {
    target_node_info_->message_sending_enabled_ = true;
  }
}

std::shared_ptr<GetLogInfoRequestSharedState>
RemoteLogsConfig::getTargetNodeInfo() const {
  return target_node_info_;
}

bool RemoteLogsConfig::useConfigChangedMessageFrom(NodeID node_id) const {
  return target_node_info_->useConfigChangedMessageFrom(node_id);
}

}} // namespace facebook::logdevice
