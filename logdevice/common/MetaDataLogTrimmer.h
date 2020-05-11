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
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/types_internal.h"

namespace facebook::logdevice {

class Sequencer;
class Processor;

/**
 * Helper class to ensure metadata logs are not growing to big by periodically
 * trimming them. To trim the metadata log for a data log, it needs to first
 * decide which record in the metadata log it can trim upto. It is OK to trim a
 * metadata record whose effective epoch range is [e_1, e_2] iff its
 * corresponding data log has moved its trim point to a LSN whose epoch is
 * strictly larger than e_2.
 *
 * This class is thread safe and its methods may ba called from any thread.
 */
class MetaDataLogTrimmer {
 public:
  /**
   * Creates a new trimmer for metadata log of data log in given sequencer. The
   * newly created trimmer is disabled and should be started by setting its run
   * interval. It can be disabled by setting run interval to 0 or by
   * destruction.
   *
   * @param sequencers    Sequencer owning this trimmer
   * @param worker_id     ID of worker used for operations on internal timer
   * @param worker_type   Type of the worker used for operations on internal
   *                      timer
   */
  explicit MetaDataLogTrimmer(Sequencer* sequencer);

  virtual ~MetaDataLogTrimmer() = default;

  /**
   * Updates interval for periodic trimming. Setting interval to 0 effectively
   * disables periodic runs, any other value different from current will cause
   * the timer to restart with new interval. This function may be called from
   * arbitrary thread.
   *
   * @param run_interval    How often trimmer will attempt to find and trim
   *                        metadata log entries
   */
  void setRunInterval(std::chrono::milliseconds run_interval);

  /**
   * Stops periodic trimming and invalidates the trimmer.
   */
  void shutdown();

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
  // The current state of trimmer that is being carried over through callbacks.
  // It is extracted into separate structure to decouple lifecycle of from state
  // (references in callbacks) from lifecycle of trimmer itself. Most fields
  // (apart from atomics) must only be accessed from single Worker thread.
  struct State {
    // Current interval of periodic trimming, accessed from Worker thread only
    std::chrono::milliseconds run_interval{0};
    // Reference to the instance of timer responsible for periodic trimming of
    // metadata log. Accessed from Worker thread only. It is not safe to access
    // timer in stopped state.
    std::unique_ptr<Timer> timer{nullptr};
    // Indicates this trimmer is requested to stop. Might be accessed/updated
    // from any thread.
    std::atomic<bool> stopped = false;
    // Furthest LSN of metadata log up to which we trimmed
    AtomicOptional<lsn_t> metadata_trim_point;
    // Picks randomized (with jitter) delay for the next run based on run
    // interval
    std::chrono::milliseconds pickRandomizedDelay();
  };

  // Schedules periodic trimming of metadata log with respect to current run
  // interval. Must be called from the same Worker thread all the time.
  void schedulePeriodicTrimming();
  // Looks up the latest LSN in Metadata log which can be trimmed safely.
  // @return    LSN found or folly::none if nothing to trim or metadata log is
  //            not available
  folly::Optional<lsn_t> findMetadataTrimLSN();
  // Handles complition of trim request for given data log id
  static void onTrimComplete(std::shared_ptr<State>,
                             logid_t,
                             Status,
                             lsn_t trim_point);

  // Applies new value for run interval. This function must be called on Worker
  // owning the timer.
  void updateRunInterval(std::shared_ptr<State>,
                         std::chrono::milliseconds run_interval);

  // Sequence responsible for data log whose corresponding metadata log should
  // be trimmed
  const Sequencer* sequencer_;
  Processor* processor_;
  // Data log id
  const logid_t log_id_;
  // Pair of ID and type uniquely identifying worker for accessing trim timer
  const worker_id_t worker_id_;
  const WorkerType worker_type_;
  std::shared_ptr<State> state_;
};

} // namespace facebook::logdevice
