/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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
#include "logdevice/common/Worker.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// TrimMetaDataLogRequest is the state machine for trimming the metadata log
// for a data log. It is only ran by the metadata utility program. In
// logdevice, metadata logs are not automatically trimmed on storage nodes
// and the only way to trim them is through the metadata utility. To
// trim the metadata log for a data log, it needs to first decide which
// record in the metadata log it can trim upto. It is OK to trim a metadata
// record whose effective epoch range is [e_1, e_2] iff its corresponding data
// log has moved its trim point to a LSN whose epoch is strictly larger than
// e_2. Therefore, the first stage of the operation is to figure out the trim
// point for the data log. The state machine performs the following operations
// for the entire trim operation:

// 1. reads the data log from LSN_OLDEST and examines the first gap received to
//    get the trim point for the data log.
// 2. read the metadata log record by record, and determine how the trim point
//    target of the metadata log using the trim point information of the data
//    log
// 3. trim the metadata log using the TrimRequest state machine.

class ClientImpl;

class TrimMetaDataLogRequest : public Request {
 public:
  // callback function called when the operation is complete
  using Callback = std::function<void(Status, logid_t)>;

  // create a TrimMetaDataLogRequest
  //
  // @param log_id        data log id to perform the trim operation
  // @param config        cluster configuration
  // @param processor     logdevice Processor object
  // @param read_timeout  timeout for reading data log and metadata log
  // @param trim_timeout  timeout for performing trim action using TrimRequest
  // @param do_trim_data_log
  //                      move the data log's trim point to the maximum position
  //                      that doesn't drop any data before trimming the
  //                      metadata log
  //
  // @param metadata_log_record_time_grace_period
  //                      extra grace period for considering a metadata log
  //                      record stale based on data log's rentention
  // @param trim_all_UNSAFE
  //                      if true, trim ALL metadata log records for the log.
  //                      used in emergency.
  TrimMetaDataLogRequest(
      logid_t log_id,
      std::shared_ptr<Configuration> config,
      std::shared_ptr<Processor> processor,
      std::chrono::milliseconds read_timeout,
      std::chrono::milliseconds trim_timeout,
      bool do_trim_data_log,
      Callback callback,
      std::chrono::seconds metadata_log_record_time_grace_period,
      bool trim_all_UNSAFE = false);

  ~TrimMetaDataLogRequest() override;

  Request::Execution execute() override;

  // set an inital delay so that the entire operation will be delayed
  // for the specified amount of time before start
  void setStartDelay(std::chrono::milliseconds start_delay) {
    start_delay_ = start_delay;
  }

 private:
  enum class State {
    // initial state before start
    INVALID = 0,
    // read the first trim gap from the data log
    READ_DATALOG_TRIMGAP,
    // read the metadata log to determine how many records to trim
    READ_METADATA_LOG,
    // trim the metadata log
    TRIM_METADATA_LOG,
    // successfully finished all operations
    FINISHED
  };

  void start();
  void complete(Status st);

  // data log reading
  void readTrimGapDataLog();
  void onDataLogTrimComplete(Status st, lsn_t data_trim_point);

  // metadata log reading
  void readMetaDataLog();
  void onReadTimeout();

  // callback for delivering a metadata log record
  void onEpochMetaData(Status st, MetaDataLogReader::Result result);
  void trimWithMetaDataTimestamp(MetaDataLogReader::Result result);
  void trimWithDataLog(MetaDataLogReader::Result result);
  void trimAllUnsafe(MetaDataLogReader::Result result);

  void finalizeReadingMetaDataLog();
  void trimMetaDataLog();
  // callback for the TrimRequest
  void onTrimComplete(Status st);

  static const char* getStateName(State state);

  State state_;
  const logid_t log_id_;
  std::shared_ptr<Configuration> config_;
  std::shared_ptr<Processor> processor_;

  folly::Optional<std::chrono::seconds> backlog_;

  // trim point of the data log
  folly::Optional<lsn_t> trim_point_datalog_;

  // target trim point of the metadata data log
  lsn_t trim_target_metadatalog_{LSN_INVALID};

  // callback function provided by user of the class. Called when the state
  // machine completes.
  Callback callback_;

  // MetaDataLogReader that reads the metadata log
  std::unique_ptr<MetaDataLogReader> meta_reader_;

  // initial delay for executing the entire operation, used by upper
  // layer retries
  std::chrono::milliseconds start_delay_{std::chrono::milliseconds::zero()};
  std::unique_ptr<Timer> start_delay_timer_;

  // time out for reading the data log or the metadata log
  const std::chrono::milliseconds read_timeout_;
  std::unique_ptr<Timer> reader_timer_;

  // timeout for trimming the metadata log
  const std::chrono::milliseconds trim_timeout_;

  // move the data log trim point to the max position that doesn't lose any
  // records before trimming the metadata log
  const bool do_trim_data_log_;

  const std::chrono::seconds metadata_log_record_time_grace_period_;

  const bool trim_all_UNSAFE_;

  // worker index to run the request
  worker_id_t current_worker_{-1};
};

}} // namespace facebook::logdevice
