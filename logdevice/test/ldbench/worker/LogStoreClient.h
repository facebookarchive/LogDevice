/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/test/ldbench/worker/LogStoreTypes.h"
#include "logdevice/test/ldbench/worker/Options.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogStoreReader;

/**
 * Clients of different systems can inherit this class
 */
class LogStoreClient {
 public:
  virtual ~LogStoreClient() {}

  /**
   * Append a record.
   * This is a async function and its results will be reported to onAppendDone.
   * @param
   *  id -- log id
   *  payload
   * @return
   *   true -- successful
   *   false -- failed
   */
  virtual bool append(LogIDType id, std::string payload, Context context) = 0;

  /**
   * Get logs belong to options_.log_range_names
   * Logs are idetified with integer number
   * @return
   *   true -- successful
   *   false -- failed
   */
  virtual bool getLogs(std::vector<LogIDType>* all_logs) = 0;

  /**
   * Get the real client
   */
  virtual std::shared_ptr<void> getRawClient() = 0;

  /*
   * Non-blockibng get tail of a log
   */
  virtual bool getTail(LogIDType logid,
                       std::function<void(bool, LogPositionType)>) = 0;
  /**
   * Non-blocking get the position of the timestamp in a log
   */
  virtual bool findTime(LogIDType log,
                        std::chrono::milliseconds timestamp,
                        std::function<void(bool, LogPositionType)> cb) = 0;
  /**
   * Set the callback for the stats update for appends
   * check LogStoreTypes for write_stats_update_callback_t
   * and read_stats_update_callback_t
   */
  void setAppendStatsUpdateCallBack(write_stats_update_callback_t);
  /**
   * Set the callback for the stats update for reads
   */
  void setReadStatsUpdateCallBack(read_stats_update_callback_t);

  /**
   * Create a reader
   */
  virtual std::unique_ptr<LogStoreReader> createReader() = 0;

  /**
   * Callback functions to update stats
   */
  write_stats_update_callback_t writer_stats_update_cb_;
  read_stats_update_callback_t reader_stats_updates_cb_;
};

}}} // namespace facebook::logdevice::ldbench
