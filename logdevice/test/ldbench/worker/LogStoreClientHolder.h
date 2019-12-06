/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <string>
#include <vector>

#include <folly/Function.h>
#include <folly/dynamic.h>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/LogStoreTypes.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogStoreClient;
class BenchStatsHolder;
class StatsStore;
class BenchStatsCollectionThread;
class BenchTracer;
class EventStore;
class Options;
class LogStoreReader;

class LogStoreClientHolder {
 public:
  explicit LogStoreClientHolder();

  ~LogStoreClientHolder();

  /**
   * Create a reader based on the options.sys_name
   * @param
   *  read -- the reader to create
   */
  std::unique_ptr<LogStoreReader> createReader();

  /**
   * aync append log
   * @param
   *  id -- log id
   *  payload
   *  Context -- a void * to identify the record
   */
  bool append(LogIDType log_id, std::string payload, Context context);

  /**
   * Get all logs in the system
   */
  bool getLogs(std::vector<LogIDType>* all_logs);

  /**
   * Get the tail of a log
   */
  bool getTail(LogIDType logid, std::function<void(bool, LogPositionType)>);

  /**
   * Return a position of a timestamp in a log
   */
  bool findTime(LogIDType log,
                std::chrono::milliseconds timestamp,
                std::function<void(bool, uint64_t)> cb);
  /**
   * Get BenchStats from all threads
   */
  folly::dynamic getAllStats();

  /**
   * Get pointer to the implementation-specific client object.
   * E.g. logdevice::Client or RdKafka::Producer."
   */

  std::shared_ptr<void> getRawClient();

  /**
   * Get BenchStatsHolder
   */
  BenchStatsHolder* getBenchStatsHolder();

  /**
   * Set the callback that will be called for completed append.
   * If callback was already set, replaces it.
   */
  void setWorkerCallBack(write_worker_callback_t);

  /**
   * Final callback function where the Callback of clients will be
   * finally terminated. We also do stats updates and event sampling here.
   * @param
   *   successful -- if appends are successful
   *   log_id: log id
   *   lsn: log record sequence number
   *        (mutilple records may share a lsn in buffered writes)
   *   ContextSet: vector of kv pairs <context, payload> returned
   */
  void onAppendDone(bool successful,
                    LogIDType log_id,
                    LogPositionType lsn,
                    ContextSet contexts);

  /**
   * Final callback function where the Callback of clients will be
   * finally terminated. We also do stats updates and event sampling here.
   * @param
   *   successful: if the requests are successful
   *   logid
   *   lsn
   *   num_records: if succeed, it will be 1
   *   vector<string>: payloads of the requests (inlcuding time stamps)
   */
  void onReadDone(bool successful,
                  LogIDType log_id,
                  LogPositionType lsn,
                  uint64_t num_records,
                  std::string payload);

 private:
  /**
   * Do sampling for events
   * 1. get timestamp from the payload
   * 2. calculate latency
   * 3. write to event store
   * @param
   *  logid
   *  lsn
   *  payload
   */
  void doSample(LogIDType log_id,
                LogPositionType lsn,
                const std::string& payload,
                std::string& event_name);

  // Explaination of define order
  // callbacks of client use everything so client go first
  // bench_tracer uses event_store
  // collection thread use stats_store and holder
  write_worker_callback_t worker_cb_;
  std::shared_ptr<StatsStore> stats_store_;
  std::shared_ptr<BenchStatsHolder> bench_stats_holder_;
  std::unique_ptr<BenchStatsCollectionThread> collect_thread_;
  std::shared_ptr<EventStore> event_store_;
  std::unique_ptr<BenchTracer> bench_tracer_;
  std::unique_ptr<LogStoreClient> client_;
};
}}} // namespace facebook::logdevice::ldbench
