/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/AtomicOptional.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/types_internal.h"

namespace facebook::logdevice {

class Sequencer;

/**
 * Helper class to ensure metadata logs are not growing to big by periodically
 * trimming them. To trim the metadata log for a data log, it needs to first
 * decide which record in the metadata log it can trim upto. It is OK to trim a
 * metadata record whose effective epoch range is [e_1, e_2] iff its
 * corresponding data log has moved its trim point to a LSN whose epoch is
 * strictly larger than e_2.
 *
 * All methods are expected to be called on Worker thread.
 */
class MetaDataLogTrimmer {
 public:
  /**
   * Creates a new trimmer for metadata log of data log in given sequencer. The
   * newly created trimmer is disabled and should be started by setting its run
   * interval. It can be disabled by setting run interval to 0 or by
   * destruction.
   */
  explicit MetaDataLogTrimmer(Sequencer* sequencer);

  virtual ~MetaDataLogTrimmer() = default;

  /**
   * Updates interval for periodic trimming. Setting interval to 0 effectively
   * disables periodic runs, any other value different from current will cause
   * the timer to restart with new interval.
   *
   * @param run_interval    How often trimmer will attempt to find and trim
   *                        metadata log entries
   */
  void setRunInterval(std::chrono::milliseconds run_interval);

  /**
   * Returns the most recent LSN metadata log was trimmed up to by this object.
   **/
  folly::Optional<lsn_t> getTrimPoint();

 protected:
  // Sends request to trim metadata log up to given LSN
  virtual void sendTrimRequest(lsn_t metadata_lsn);

  // Trims metadata with respect to current data trim point
  void trim();

 private:
  // Schedules periodic trimming of metadata log with respect to current run
  // interval.
  void schedulePeriodicTrimming();
  // Looks up the latest LSN in Metadata log which can be trimmed safely.
  // @return    LSN found or folly::none if nothing to trim or metadata log is
  //            not available
  folly::Optional<lsn_t> findMetadataTrimLSN();
  // Handles complition of trim request for given data log id
  void onTrimComplete(Status, lsn_t trim_point);

  // Sequence responsible for data log whose corresponding metadata log should
  // be trimmed
  const Sequencer* sequencer_;
  // Data log id
  const logid_t log_id_;
  // Furthest LSN of metadata log up to which we trimmed
  AtomicOptional<lsn_t> metadata_trim_point_;
  // Current interval of periodic trimming
  std::chrono::milliseconds current_run_interval_;
  // Reference to the instance of timer responsible for periodic trimming of
  // metadata log.
  Timer trim_timer_;
};

} // namespace facebook::logdevice
