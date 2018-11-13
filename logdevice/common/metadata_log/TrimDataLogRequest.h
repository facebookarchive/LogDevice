/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// TrimDataLogRequest is the state machine for reading a data log's trim point
// and optionally trimming it. It is only ran by metadata-utility.
//
// If `do_trim == false` it will only read the existing trim point (the max
// right side of the trim gaps it gets) and returns that.
//
// If `do_trim == true`, it will trim the data log to the newly calculated trim
// point and return the new trim point. Typically a log will have its trim point
// set right before the first available record in the log, and thus requires no
// further trimming. However, in some cases (e.g. when the log hasn't received
// writes in a while and there is no automatic trimming) the trim point will be
// stale and needs to be updated to ensure we can delete metadata log records
// that cover epochs that contain no data. TrimDataLogRequest will read the data
// log from LSN_OLDEST until the tail and calculate the new trim point LSN to be
// either (first_record-1) or to (tail_lsn) if there are no records in the log.

class TrimDataLogRequest : public Request {
 public:
  // callback function called when the operation is complete
  using Callback = std::function<void(Status, lsn_t)>;

  // create a TrimMetaDataLogRequest
  //
  // @param log_id        data log id to perform the trim operation on
  // @param worker_id     worker on which this request will run
  // @param read_timeout  timeout for reading the data log
  // @param trim_timeout  timeout for performing trim action using TrimRequest
  // @param do_trim       If true, will perform trimming. Otherwise, will return
  //                      existing data log trim point.
  TrimDataLogRequest(logid_t log_id,
                     worker_id_t worker_id,
                     std::chrono::milliseconds read_timeout,
                     std::chrono::milliseconds trim_timeout,
                     bool do_trim,
                     Callback callback);

  Request::Execution execute() override;

  virtual int getThreadAffinity(int nthreads) override {
    ld_check(current_worker_.val() < nthreads);
    return current_worker_.val();
  }

  // callback functions for the internal ClientReadStream that reads the
  // data log
  bool onDataRecord(std::unique_ptr<DataRecord>&);
  bool onGapRecord(const GapRecord&);
  void onDoneReading(logid_t log_id);

 protected:
  virtual void getTailLSN(SyncSequencerRequest::Callback cb);
  virtual void startReader(lsn_t from_lsn, lsn_t until_lsn);
  virtual void finalizeReading(Status st);
  void onDestroyReadStreamTimer(Status st);
  virtual void stopReading();
  virtual void runTrim();
  void onTrimComplete(Status st);
  virtual void finalize(Status st, lsn_t trim_point);

  virtual void verifyWorker();

  // indicate if reading data log is finalized and the read stream is
  // scheduled to be destroyed
  bool reading_finalized_{false};

  // Newly calculated trim point - either (lsn_of_first_record-1) or (tail_lsn)
  lsn_t new_trim_point_{LSN_INVALID};

 private:
  void onTailReceived(Status st, lsn_t tail_lsn);
  void onReadTimeout();
  void considerTrimming();

  const logid_t log_id_;

  // Tail LSN of the log
  lsn_t tail_lsn_{LSN_INVALID};

  // Trim point as read from the log - upper bound of any existing TRIM gap
  lsn_t current_trim_point_{LSN_INVALID};

  // LSN of first record, used for sanity checking
  lsn_t first_record_lsn_{LSN_MAX};

  // readstream id of the internal readstream that reads the data log
  read_stream_id_t rsid_;

  // callback function provided by user of the class. Called when the state
  // machine completes.
  Callback callback_;

  // time out for reading the data log for the first trim gap
  const std::chrono::milliseconds read_timeout_;
  std::unique_ptr<Timer> reader_timer_;

  // timer used to schedule the destruction of the read stream
  std::unique_ptr<Timer> destroy_readstream_timer_;

  // timeout for trimming the metadata log
  const std::chrono::milliseconds trim_timeout_;

  // Determines whether this request will issue trims or just returns the
  // existing trim point
  bool do_trim_;

  // worker index to run the request
  worker_id_t current_worker_{-1};
};

}} // namespace facebook::logdevice
