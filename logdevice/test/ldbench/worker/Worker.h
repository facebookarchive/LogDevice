/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/noncopyable.hpp>
#include <folly/Memory.h>
#include <folly/memory/EnableSharedFromThis.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/ServerRecordFilter.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/LogStoreTypes.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice {

class AsyncReader;
class DataRecord;
class GapRecord;

namespace ldbench {

class Worker;
class LogStoreClientHolder;
class BenchStatsHolder;

/**
 * Benchmark worker base class.
 */
class Worker : private boost::noncopyable {
 public:
  using RecordCallback = std::function<bool(std::unique_ptr<DataRecord>&)>;
  using GapCallback = std::function<bool(const GapRecord&)>;

  explicit Worker();

  /**
   * Run the worker. To be defined by derived classes.
   *
   * @return
   *  Exit code. 0 on success, something else otherwise.
   */
  virtual int run() = 0;

  /**
   * Signal the worker to stop.
   *
   * Async-safe (may be called from POSIX signal handlers).
   */
  inline void stop() noexcept {
    stopped_ = true;
  }

  /**
   * Posts a request to ev_ asking the Worker to dump some information about
   * its state to the log (usually at error level).
   */
  void requestDebugInfoDump();

  /**
   * @return whether stop() has been called.
   */
  inline bool isStopped() const noexcept {
    return stopped_;
  }

  /**
   * Wait until the configured start time
   */
  void waitUntilStartTime();

  /**
   * Destroys Client.
   * Call this to make sure no more Client callbacks will be called.
   * All other instances of Reader and AsyncReader must be destroyed before
   * calling this method.
   */
  void destroyClient();

  virtual void onAppendDone(LogIDType /* log_id */,
                            bool /* status */,
                            bool /* buffered */,
                            uint64_t /* num_records */,
                            uint64_t /* payload_bytes */) {}

 protected:
  using LogIdDist = std::function<logid_t()>;
  using LogToLsnMap = std::unordered_map<logid_t::raw_type, lsn_t>;

  virtual ~Worker();

  /**
   * Called from ev_ thread soon after requestDebugInfoDump() is called.
   * Use it to print some worker-type-specific state information at error level.
   * Note that it might be called before run() is called, as well as after run()
   * returns.
   */
  virtual void dumpDebugInfo() {
    ld_error("Dumping debug info is not implemented for this worker type.");
  }

  /**
   * Called from sleepForDurationOfTheBench() every few seconds.
   * Use it to print things like throughput and number of logs, at info level.
   */
  virtual void printProgress(double /* seconds_since_start */,
                             double /* seconds_since_last_call */) {}

  /**
   * Gets the logs according to options.log_range_names, log_hash_salt
   * and log_hash_range. NOT partitioned (options.partition_by). To get this
   * worker's portion of the logs, you can use getLogsPartition().
   *
   * @return
   *  false on success, true on failure. Logs error on failure and leaves
   *  err set.
   */
  bool getLogs(std::vector<logid_t>& logs_out) const;

  /**
   * Get the logs assigned to this worker, as implied by options.worker_id.
   * Requires: options.partition_by == LOG
   *
   * @param logs
   *  Logs of all workers, as produced by getLogs().
   */
  static std::vector<logid_t>
  getLogsPartition(const std::vector<logid_t>& logs);

  /**
   * Get uniform random distribution across given logs.
   *
   * @return
   *  Uniform random distribution across input logs. nullptr if logs
   *  is empty.
   */
  static LogIdDist getLogIdDist(std::vector<logid_t> logs);

  /**
   * Generate a random payload.
   *
   * @param size
   *  Payload size in number of bytes.
   * @return
   *  Random payload.
   */
  virtual std::string generatePayload(size_t size = options.payload_size);

  /**
   * Try an async append to the desired log. If options.pretend, doesn't append
   * but calls `cb` soon, with an OK status and a fake LSN.
   *
   * @return
   *  false on success, true on failure. Leaves err set on failure.
   */
  bool tryAppend(logid_t log_id,
                 std::string payload,
                 const append_callback_t& cb,
                 const AppendAttributes* attrs = nullptr);

  /**
   * Get tail LSNs for given logs.
   *
   * @param tail_lsns
   *  Output map of log id to tail LSN.
   * @param logs
   *  Logs to get tail LSNs for.
   * @return
   *  false on success, true on failure. Logs first error on failure and leaves
   *  err set.
   */
  bool getTailLSNs(LogToLsnMap& tail_lsns,
                   const std::vector<logid_t>& logs) const;

  /**
   * Start reading from the given logs.
   *
   * @param from_lsns
   *  Map of log id to from-LSN (inclusive) of all logs to be read.
   * @param record_cb
   *  Callback to be called on record read.
   * @param gap_cb
   *  Optional callback to be called on gap read.
   * @param until_lsns
   *  Optional map of log id to until-LSN (inclusive) of all logs to be read.
   *  Logs that are present in from_lsns but absent in until_lsns will be
   *  tailed indefinitely (until stopReading() is called).
   * @return
   *  false on success, true on failure. Logs first error on failure and leaves
   *  err set.
   */
  virtual bool startReading(const LogToLsnMap& from_lsns,
                            RecordCallback record_cb,
                            GapCallback gap_cb = nullptr,
                            const LogToLsnMap* until_lsns = nullptr);

  /**
   * Stop tailing logs.
   *
   * Waits for all reads to complete before returning. May fail on individual
   * logs, but will continue until success or failure on every single log.
   *
   * @return
   *  false on success, true on failure. Logs first error on failure and leaves
   *  err set.
   */
  virtual bool stopReading();

  /**
   * Returns server-side filtering options based on options.filter_type.
   * Useful if you're using Reader/AsyncReader directly instead of
   * Worker::startReading().
   */
  ReadStreamAttributes getReadAttrs() const;

  // Wrappers around client_'s async methods. The callback is called on the ev_
  // thread, only if isStopped() is false.
  int getTailLSN(logid_t log, get_tail_lsn_callback_t cb);
  int getTailLSN(logid_t log, std::function<void(bool, uint64_t)> cb);
  int findTime(logid_t log,
               std::chrono::milliseconds timestamp,
               find_time_callback_t cb);
  int findTime(logid_t log,
               std::chrono::milliseconds timestamp,
               std::function<void(bool, uint64_t)> cb);

  int isLogEmpty(logid_t log, is_empty_callback_t cb);

  // Blocks until options.duration seconds passed, or a stop signal was
  // received, or stop() was called. In all cases, after this method returns,
  // isStopped() is true.
  // Usually a subclass would start some work in other threads (e.g. in ev_),
  // then call this method, then stop the work.
  // Returns how long the sleep lasted.
  std::chrono::milliseconds sleepForDurationOfTheBench();

  // Convenience function for synchronous execution on EventLoop
  void executeOnEventLoopSync(folly::Func f);

  // Custom stats for ldbench. Above client_ as to be destroyed after it.
  std::unique_ptr<StatsHolder> stats_;

  // Abstract interface for generic mode
  // Client_ will take raw client from client_holder_, therefore
  // we make sure that we release client_ before destroying client_holder_
  std::unique_ptr<LogStoreClientHolder> client_holder_;

  // nullptr if options.pretend is true.
  // Ownership of the Client is _not_ actually shared. This is the only
  // reference. Once you destroy it and all Reader-s/AsyncReader-s, the Client
  // is gone and won't call callbacks anymore.
  std::shared_ptr<Client> client_;

  // An event loop that subclasses can use for things like retries and timers.
  // Created and started by Worker constructor.
  std::unique_ptr<EventLoop> ev_;

  std::vector<uint32_t> payload_buf_;

 private:
  template <typename T>
  friend class std::default_delete;

  void parseFilterOptions();

  std::unique_ptr<AsyncReader> reader_;
  std::atomic<bool> stopped_{false};
  std::vector<logid_t> tailed_logs_;

  static constexpr const char* FILTER_KEY = "abcdefg";
};
} // namespace ldbench

}} // namespace facebook::logdevice
