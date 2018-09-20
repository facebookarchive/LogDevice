/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/RemoteLogsConfig.h"

#include <deque>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

#include <folly/Random.h>
#include <folly/json.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/include/LogAttributes.h"

using folly::RWSpinLock;
using std::chrono::duration;
using std::chrono::steady_clock;
using namespace facebook::logdevice::configuration::parser;
using facebook::logdevice::logsconfig::LogGroupNode;

namespace facebook { namespace logdevice {

using RangeLookupMap = LogsConfig::NamespaceRangeLookupMap;

std::shared_ptr<LogGroupNode> RemoteLogsConfig::getLogGroupByIDShared(
    logid_t id,
    const std::shared_ptr<LogsConfig::LogGroupNode>& metadata_log_fallback)
    const {
  if (MetaDataLog::isMetaDataLog(id)) {
    return metadata_log_fallback;
  }

  std::shared_ptr<LogsConfig::LogGroupNode> res;
  Semaphore sem;
  this->getLogGroupByIDAsync(
      id,
      metadata_log_fallback,
      [&](const std::shared_ptr<LogsConfig::LogGroupNode> loggrp) {
        res = loggrp;
        sem.post();
      });
  sem.wait();
  return res;
}

int RemoteLogsConfig::postRequest(GET_LOG_INFO_Header::Type request_type,
                                  logid_t log_id,
                                  std::string log_group_name,
                                  get_log_info_callback_t callback) const {
  ld_check(processor_);
  std::shared_ptr<Processor> processor = processor_->lock();
  if (!processor) {
    ld_spew("Attempting to post request to a processor when there is none");
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
          request_id_t(folly::Random::rand64()), WorkerType::GENERAL);
    }
    worker_id = target_node_info_->worker_id_;
  }

  std::unique_ptr<Request> req =
      std::make_unique<GetLogInfoRequest>(request_type,
                                          log_id,
                                          log_group_name,
                                          timeout_,
                                          target_node_info_,
                                          callback,
                                          callback_thread_affinity);
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
    const std::shared_ptr<LogsConfig::LogGroupNode>& metadata_log_fallback,
    std::function<void(std::shared_ptr<LogGroupNode>)> cb) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    cb(metadata_log_fallback);
    return;
  }

  {
    // Attempting to fetch result from cache
    shared_lock<RWSpinLock> lock(id_cache_mutex);
    auto it = id_result_cache.find(id.val_);
    if (it != id_result_cache.end()) {
      // potential cache hit - check timestamp
      steady_clock::time_point tnow = steady_clock::now();
      if (it->second.time_fetched - tnow <= max_data_age_) {
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
  auto request_callback = [cb, delimiter](Status st, std::string json) {
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

    auto log_group = LogGroupNode::createFromJson(json, delimiter);
    if (!log_group) {
      ld_error("Failed to parse LogGroupNode result");
      cb(nullptr);
      return;
    }

    const std::shared_ptr<LogGroupNode> log_shared(log_group.release());

    // Placing result in cache
    rlc->insertIntoIdCache(log_shared);

    // Returning to client
    cb(std::move(log_shared));
  };
  this->postRequest(
      GET_LOG_INFO_Header::Type::BY_ID, id, std::string(), request_callback);
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
      nullptr,
      [&](const std::shared_ptr<const LogsConfig::LogGroupNode> log) {
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
    shared_lock<RWSpinLock> lock(name_cache_mutex);
    auto it = name_result_cache.find(name);
    if (it != name_result_cache.end()) {
      // potential cache hit - check timestamp
      steady_clock::time_point tnow = steady_clock::now();
      if (it->second.time_fetched - tnow <= max_data_age_) {
        // data not too old - return from cache
        cb(E::OK, it->second.range);
        return;
      }
    }
  }

  auto request_callback = [delimiter, name, cb](Status st, std::string json) {
    auto config = Worker::onThisThread()->getConfig();
    RemoteLogsConfig* rlc = dynamic_cast<RemoteLogsConfig*>(
        const_cast<LogsConfig*>(config->logsConfig().get()));
    if (!rlc || st != E::OK) {
      // The request failed, or the LogsConfig isn't RemoteLogsConfig anymore
      cb(st == E::NOTFOUND ? E::NOTFOUND : E::FAILED,
         std::make_pair(logid_t(LSN_INVALID), logid_t(LSN_INVALID)));
      return;
    }

    auto log_group = LogGroupNode::createFromJson(json, delimiter);
    if (!log_group) {
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
    // writing to id -> log info cache
    const std::shared_ptr<LogGroupNode> log_shared(log_group.release());
    rlc->insertIntoIdCache(log_shared);
    cb(E::OK, res);
  };

  this->postRequest(
      GET_LOG_INFO_Header::Type::BY_NAME, logid_t(0), name, request_callback);
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
      nullptr,
      [&](const std::shared_ptr<LogsConfig::LogGroupNode> log) {
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
  shared_lock<RWSpinLock> lock(name_cache_mutex);
  auto ns_it = namespace_last_fetch_times.find(ns);
  if (ns_it == namespace_last_fetch_times.end()) {
    // no result in cache
    return false;
  }

  steady_clock::time_point tnow = steady_clock::now();
  if (ns_it->second - tnow > max_data_age_) {
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

void RemoteLogsConfig::getLogRangesByNamespaceAsync(
    const std::string& ns,
    std::function<void(Status, RangeLookupMap)> cb) const {
  std::string delimiter = getNamespaceDelimiter();
  RangeLookupMap rangeMap;
  // Attempting to fetch result from cache
  if (getLogRangesByNamespaceCached(ns, rangeMap)) {
    if (rangeMap.empty()) {
      cb(E::NOTFOUND, std::move(rangeMap));
    } else {
      cb(E::OK, std::move(rangeMap));
    }
    return;
  }

  auto request_callback = [ns, cb, delimiter](Status st, std::string json) {
    auto config = Worker::onThisThread()->getConfig();
    RemoteLogsConfig* rlc = dynamic_cast<RemoteLogsConfig*>(
        const_cast<LogsConfig*>(config->logsConfig().get()));
    if (!rlc || st != E::OK) {
      // The request failed, or the LogsConfig isn't RemoteLogsConfig anymore
      cb(st == E::NOTFOUND ? E::NOTFOUND : E::FAILED, RangeLookupMap());
      return;
    }

    // parsing JSON
    folly::dynamic parsed = folly::dynamic::object;
    try {
      folly::json::serialization_opts opts;
      opts.allow_trailing_comma = true;
      parsed = folly::parseJson(json, opts);
    } catch (std::exception& e) {
      ld_error("Error parsing JSON received from remote server: %s", e.what());
      cb(E::FAILED, RangeLookupMap());
      return;
    }

    if (!parsed.isArray()) {
      ld_error("Error parsing JSON received from remote server: "
               "top-level JSON item is not an array");
      cb(E::FAILED, RangeLookupMap());
      return;
    }

    // writing to namespace timing cache
    auto now = steady_clock::now();
    std::unique_lock<RWSpinLock> lock(rlc->name_cache_mutex);
    rlc->namespace_last_fetch_times[ns] = now;

    RangeLookupMap res;
    for (auto& iter : parsed) {
      // LogGroupNode forgets its full path, hence we have to return it from
      // the parse method as an extra return argument
      auto grp = LogGroupNode::createFromFollyDynamic(iter, delimiter);
      ld_check(grp.second != nullptr);
      auto& logs_config = grp.second;
      auto& name = grp.first;
      auto& range = logs_config->range();

      res.insert(std::make_pair(name, range));

      // writing to name->range cache
      NameMapEntry entry{range, steady_clock::now()};
      rlc->name_result_cache[name] = std::move(entry);

      // writing to id -> log info cache
      const std::shared_ptr<LogGroupNode> log_shared(logs_config.release());
      rlc->insertIntoIdCache(log_shared);
    }
    cb(res.empty() ? E::NOTFOUND : E::OK, res);
  };

  this->postRequest(GET_LOG_INFO_Header::Type::BY_NAMESPACE,
                    logid_t(0),
                    ns,
                    request_callback);
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

  // Enabling sending GET_LOG_INFO messages if it's disabled
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
