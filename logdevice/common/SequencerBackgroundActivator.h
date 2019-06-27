/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <queue>
#include <unordered_map>

#include <logdevice/common/Timestamp.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file State machine that triggers sequencer reactivations when nodesets or
 *       window size need to be changed.
 */

class AllSequencers;
class EpochMetaData;
class Sequencer;

class SequencerBackgroundActivator {
 public:
  SequencerBackgroundActivator();

  static WorkerType getWorkerType(Processor* processor) {
    // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
    // on whether we have background workers.
    if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
      return WorkerType::BACKGROUND;
    }
    return WorkerType::GENERAL;
  }
  static constexpr int getThreadAffinity(int nthreads) {
    // Assigning to one specific worker. We already have a bunch of stuff on
    // W0, so picking a different worker for sequencer reactivations
    return 7 % nthreads;
  }

  // Schedules the given logs for checking whether recativation is needed.
  // If no updates are needed, this should be cheap.
  void schedule(std::vector<logid_t> logs,
                bool queued_by_alarm_callback = false);

  // Called when a sequencer activation completes, successfully or not.
  void notifyCompletion(logid_t log, Status st);

  void onSettingsUpdated();

  // Posts a request to call schedule() from the correct thread.
  static void requestSchedule(Processor* processor, std::vector<logid_t> logs);

  // Posts a request to call notifyCompletion() from the correct thread.
  static void requestNotifyCompletion(Processor* processor,
                                      logid_t log,
                                      Status st);

  struct LogDebugInfo {
    std::chrono::steady_clock::time_point next_nodeset_adjustment_time =
        std::chrono::steady_clock::time_point::max();
  };

  // Looks up information about the given logs. The returned list has the same
  // length and is in the same order as `logs`.
  // Must be called from the correct worker thread.
  std::vector<LogDebugInfo> getLogsDebugInfo(const std::vector<logid_t>& logs);

  // A wrapper around getLogsDebugInfo() that forwards the request to the
  // correct worker thread, waits for it, and forwards the result back.
  // Must be called from a non-worker thread.
  // Returns empty vector if the processor is being shut down.
  static std::vector<LogDebugInfo>
  requestGetLogsDebugInfo(Processor* processor,
                          const std::vector<logid_t>& logs);

  enum class ProcessLogDecision : uint8_t {
    NOOP = 0,
    SUCCESS = 1,
    POSTPONED = 2,
    FAILED = 3,
  };

  // These states capture the suggested rectivation decision based on the extent
  // of the changes to the epoch metadata:
  // UPDATE_METADATA : Update the epoch_metadata without reactivating the
  // sequencer.
  // POSTPONE: Reactivate the sequencer but delay the reactivation.
  // REACTIVATE: Immediately reactivate the sequencer.
  // The order here is important as the actual place it is used compares the
  // decision returned from multiple changes and selects the highest.
  enum class ReactivationDecision : uint8_t {
    NOOP = 0,
    UPDATE_METADATA = 1,
    POSTPONE = 2,
    REACTIVATE = 3,
  };

 private:
  struct NodesetAdjustment {
    // The adjustment is conditional on latest sequencer having this epoch.
    // If epoch store turns out to have epoch greater than that, we
    // forget about this adjustment attempt as if it never happened - this
    // usually means that current sequencer is on some other node, and that node
    // is responsible for nodeset adjustments.
    epoch_t epoch;

    folly::Optional<nodeset_size_t> new_size;
    folly::Optional<uint64_t> new_seed;
  };

  struct LogState {
    // If a reactivation is in progress, this token is holding a unit of the
    // in-flight reactivations budget.
    ResourceBudget::Token token;

    // True if this log is in queue_.
    bool in_queue = false;

    // This log was already postponed once for the purpose of throttling
    bool queued_by_alarm_callback = false;

    folly::Optional<NodesetAdjustment> pending_adjustment;

    // Fires every nodeset_adjustment_period to consider changing nodeset size
    // based on log's throughput.
    // For simplicity, this timer is always spinning for all logs in logs_,
    // regardless of whether there's an active sequencer or not.
    Timer nodeset_adjustment_timer;
    // Fires every nodeset_randomization_period to change nodeset seed.
    Timer nodeset_randomization_timer;

    // Timestamps when the above two timers will fire.
    std::chrono::steady_clock::time_point next_nodeset_adjustment_time =
        std::chrono::steady_clock::time_point::max();
    std::chrono::steady_clock::time_point next_nodeset_randomization_time =
        std::chrono::steady_clock::time_point::max();

    // How often to re-generate nodeset with a new random seed.
    // Calculated based on settings and unclamped target nodeset size.
    std::chrono::milliseconds nodeset_randomization_period =
        std::chrono::milliseconds::max();

    // This timer is used to delay sequencer reactivations when possible.
    Timer reactivation_delay_timer;
  };

  // internal method that checks that SequencerBackgroundActivator methods are
  // running on the right thread
  void checkWorkerAsserts();

  // Processes the given log. Returns NOOP, SUCCESS, POSTPONED, or FAILED
  // depending on the conditions.
  ProcessLogDecision processOneLog(logid_t log_id,
                                   LogState& state,
                                   ResourceBudget::Token& token);

  // Called every nodeset_adjustment_period for each log.
  // Updates target_nodeset_size if needed.
  void maybeAdjustNodesetSize(logid_t log_id, LogState& state);

  // Called every nodeset_randomization_period for each log.
  // Updates nodeset seed.
  void randomizeNodeset(logid_t log_id, LogState& state);

  // Internal function that queues up a sequencer reactivation
  // with a delay.
  ProcessLogDecision postponeSequencerReactivation(logid_t logid);

  // queues up a job to update Epoch metadata
  ProcessLogDecision
  updateEpochMetadata(logid_t logid,
                      std::shared_ptr<Sequencer> seq,
                      std::unique_ptr<EpochMetaData>& new_metadata,
                      const epoch_t& current_epoch);

  // Processes changes to log medatadata
  ReactivationDecision
  processMetadataChanges(logid_t logid,
                         std::shared_ptr<const EpochMetaData>& current_metadata,
                         std::unique_ptr<EpochMetaData>& new_metadata);

  // Does the actual useful work.
  // Checks if the current sequencer's epoch metadata (nodeset, replication
  // factor etc) and settings (window size) matches the config. If not, starts
  // either sequencer reactivation or an epoch store update.
  //
  // If the sequencer is not active or not ready for updating metadata (e.g.
  // current metadata is not written to metadata log yet), returns an error.
  // When sequencer becomes ready for metadata updates, it'll notify us
  // through notifyCompletion(), and we'll schedule another call to this
  // check.
  //
  // @return 0 if an update was started. Otherwise returns -1 and assigns err.
  // If update was started, notifyCompletion() will be called when the update is
  // finished.
  // If no update is needed, err is set to UPTODATE.
  // The caller should retry later in case of the following errors:
  // FAILED, NOBUFS, TOOMANY, NOTCONN, ACCESS.
  // The caller shouldn't retry on the following errors:
  // NOSEQUENCER, INPROGRESS, NOTFOUND, SYSLIMIT, INTERNAL, TOOBIG.
  ProcessLogDecision reprovisionOrReactivateIfNeeded(
      logid_t logid,
      LogState& state,
      std::shared_ptr<Sequencer> seq,
      std::shared_ptr<const EpochMetaData> epoch_metadata);

  // Decide how often we need to re-generate nodeset for the given log, and
  // activate LogState::nodeset_randomization_timer accordingly.
  void recalculateNodesetRandomizationTime(
      logid_t logid,
      LogState& state,
      std::shared_ptr<const EpochMetaData> epoch_metadata);

  // processes the queue if there are pending requests and the number of
  // requests currently in flight does not exceed maxRequestsInFlight()
  void maybeProcessQueue();

  // Activates the timer for queue processing. If timeout is not provided,
  // uses sequencer_background_activation_retry_interval setting.
  void activateQueueProcessingTimer(
      folly::Optional<std::chrono::microseconds> timeout);

  // Initializes nodeset_adjustment_timer. Call this after inserting into
  // logs_.
  void activateNodesetAdjustmentTimerIfNeeded(logid_t log_id, LogState& state);

  // deactivates the timer for queue processing
  void deactivateQueueProcessingTimer();

  // Called when a log is added to the background reactivation queue_.
  // These jobs on this queue are throttled based on the setting
  // max-sequencer-background-activations-in-flight. When a job
  // is taken off this queue, it may be processed immediately or
  // further postponed.
  void bumpScheduledStat(uint64_t val = 1);

  // Called when a Token is released. One token is acquired for every log
  // removed from queue_, so the calls to bumpScheduledStat() and
  // bumpCompletedStat() should balance out.
  void bumpCompletedStat(uint64_t val = 1);

  // Called when a log is taken for processing from the
  // queue_ but instead selected to be further postponed.
  // The delay horizon of these jobs is a random time point
  // over a window specified by the settings
  // sequencer-reactivation-max/max-delay-secs".
  void bumpDelayedStat(uint64_t val = 1);

  // Updated when associated delay timer fires. Calls to  bumpDelayedStat()
  // and bumpCompletedDelayStat() should balance out.
  void bumpCompletedDelayStat(uint64_t val = 1);

  std::unordered_map<logid_t, LogState, logid_t::Hash> logs_;

  // Queue of log_ids to process in the background but shortly.
  std::queue<logid_t> queue_;

  // timer for retrying processing the queue later in case of failures
  Timer retry_timer_;

  // limiter on the number of concurrent activations
  std::unique_ptr<ResourceBudget> budget_;

  // Some of the settings we're using.
  // Used for detecting when settings change.
  std::chrono::milliseconds nodeset_adjustment_period_;
  bool unconditional_nodeset_randomization_enabled_;
  size_t nodeset_max_randomizations_;
};
}} // namespace facebook::logdevice
