/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/test/ldbench/worker/LogStoreTypes.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogStoreClient;

class LogStoreReader {
 public:
  virtual ~LogStoreReader() {}
  /**
   * Start to reading a log within the range <start, end>
   * This is an async interface. Specific Reader can define and implement
   * its own callbacks and only report the final results to onReadDone, Or
   * use onReadDone as the callback directly.
   */
  virtual bool startReading(LogIDType logid,
                            LogPositionType start,
                            LogPositionType end) = 0;

  /**
   * Stop reading a log.
   * It is also an async call. Same with startReading, specific reader can
   * have their owen callback or use readStopDone directly.
   */
  virtual bool stopReading(LogIDType logid) = 0;

  /**
   * Resume a reading after stop reading
   */
  virtual bool resumeReading(LogIDType logid) = 0;

  /**
   * Set the customized callbacks from callers
   * Note that before calling these callback, a reader should report the
   * stats inforamtion to client holder to update stats
   * RecordCallback is for successful reading
   * @param:
   *  logid
   *  lsn
   *  timestamp -- ts assigned by the sequencer
   *  payload
   */
  void setWorkerRecordCallback(logstore_record_callback_t);
  /**
   * GapCallback is for failed reading
   * @params
   *  type
   *  logid
   *  lo_lsn -- the lowest lsn in the gap
   *  hi_lsn -- the highest lsn in the gap
   */
  void setWorkerGapCallback(logstore_gap_callback_t);
  /**
   * DoneCallback is for stoping reading
   */
  void setWorkerDoneCallback(std::function<void(LogIDType)>);

  // customized callback functions from callers
  logstore_record_callback_t worker_record_callback_;
  logstore_gap_callback_t worker_gap_callback_;
  logstore_done_callback_t worker_done_callback_;
  LogStoreClient* owner_client_;
};
}}} // namespace facebook::logdevice::ldbench
