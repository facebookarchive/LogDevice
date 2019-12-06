/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <chrono>
#include <functional>

namespace facebook { namespace logdevice { namespace ldbench {
using LogIDType = uint64_t;
using LogPositionType = uint64_t;
using Context = void*;
using ContextSet = std::vector<std::pair<Context, std::string>>;
/**
 * Customized append callbacks for logstore
 * This callback only uses standard data types
 * @params
 *  successful -- successful append or not
 *  logid
 *  lsn
 *  payload
 */
using logstore_append_callback_t = std::function<void(bool successful,
                                                      LogIDType logid,
                                                      LogPositionType lsn,
                                                      std::string payload)>;
/**
 * callback for updating stats for writes
 * @params
 *   successful
 *   logid
 *   lsn
 *   contexts -- void * and payload pairs
 */
using write_stats_update_callback_t = std::function<void(bool successful,
                                                         LogIDType log_id,
                                                         LogPositionType lsn,
                                                         ContextSet contexts)>;
/**
 * write worker callback (WriteWorker::onAppendDone)
 */
using write_worker_callback_t = std::function<void(LogIDType logid,
                                                   bool successful,
                                                   bool buffered,
                                                   uint64_t num_records,
                                                   uint64_t payload_bytes)>;

/**
 * callback for updating stats for reads
 * @params
 *   successful
 *   logid
 *   lsn
 *   num_records -- 1 for most of the time
 *   payload  -- for calculating end2end latency
 */
using read_stats_update_callback_t = std::function<void(bool successful,
                                                        LogIDType log_id,
                                                        LogPositionType lsn,
                                                        uint64_t num_records,
                                                        std::string payload)>;
/**
 * read worker callback
 * record callback is for successful reads
 * @param:
 *  logid
 *  lsn
 *  timestamp -- ts assigned by the sequencer
 *  payload
 */
using logstore_record_callback_t =
    std::function<bool(LogIDType logid,
                       LogPositionType lsn,
                       std::chrono::milliseconds timestamp,
                       std::string payload)>;

enum class LogStoreGapType { ACCESS, DATALOSS, OTHER };
/**
 * gap call backs is for failed reads
 * @params
 *  type
 *  logid
 *  lo_lsn -- the lowest lsn in the gap
 *  hi_lsn -- the highest lsn in the gap
 */
using logstore_gap_callback_t = std::function<bool(LogStoreGapType type,
                                                   LogIDType logid,
                                                   LogPositionType lo_lsn,
                                                   LogPositionType hi_lsn)>;
/**
 * done callback is for stoping reading
 */
using logstore_done_callback_t = std::function<void(LogIDType)>;

}}} // namespace facebook::logdevice::ldbench
