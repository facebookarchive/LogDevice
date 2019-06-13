/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>
#include <memory>
#include <string>

#include <folly/AtomicBitSet.h>
#include <folly/Optional.h>
#include <folly/Portability.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/AtomicOptional.h"
#include "logdevice/common/GetSeqStateRequest-fwd.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/locallogstore/LogStorageState_PurgeCoordinator_Bridge.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * On storage nodes, contains state for one log that we need to keep in
 * memory for fast access.
 */

struct ExponentialBackoffTimerNode;
class ServerProcessor;
class LogStorageStateMap;
enum class ReleaseType : uint8_t;

class LogStorageState {
 public:
  // Used by updateLastReleasedLSN() to indicate where the last released LSN
  // came from (for callers that care, such as rebuilding)
  enum class LastReleasedSource : uint8_t {
    LOCAL_LOG_STORE = 1 << 0, // LSN was read from the local log store
    RELEASE = 1 << 1          // got it from the sequencer (through a RELEASE)
  };

  enum class RecoverContext : uint8_t {
    UNKNOWN,
    CATCHUP_QUEUE,
    FINDKEY_MESSAGE,
    IS_LOG_EMPTY_MESSAGE,
    REBUILDING_THREAD,
    ROCKSDB_CF,
    START_MESSAGE,
    STORE_MESSAGE,
    GET_TRIM_POINT,
    SEAL_STORAGE_TASK,
    // NOTE: Make corresponding changes in GetSeqStateRequest.h
  };

  // Wrapper around lsn_t representing the last released LSN, containing its
  // initialization status and source from which it was initialized.
  class LastReleasedLSN {
   public:
    LastReleasedLSN() : lsn_(LSN_INVALID), state_(0) {}
    LastReleasedLSN(lsn_t lsn, uint8_t state) : lsn_(lsn), state_(state) {}

    bool hasValue() const {
      return state_;
    }

    lsn_t value() const {
      ld_check(hasValue());
      return lsn_;
    }

    LastReleasedSource source() const {
      ld_check(hasValue());
      return (state_ & (int)LastReleasedSource::RELEASE)
          ? LastReleasedSource::RELEASE
          : LastReleasedSource::LOCAL_LOG_STORE;
    }

   private:
    lsn_t lsn_;
    uint8_t state_;
  };

  /**
   * Currently in LogDevice we have two different types of seals:
   *   - normal seal which is updated when receiving SEAL messages sent by
   *     log recovery
   *   - soft seal which is updated when the storage node receives a STORE
   *     with a new epoch
   *
   *   In general, the storage node should not accept writes in epoch that is
   *   smaller than or equal to the epoch in seals. But it is necessary to
   *   differentiate normal and soft seals so that soft seals would not
   *   preempt STOREs for draining during graceful migration of sequencers.
   */
  enum class SealType : uint8_t {
    NORMAL = 0,
    SOFT,

    Count
  };

  // wraps all kinds of seals, used in seal_callback_t
  class Seals {
   public:
    Seal getSeal(SealType type) const {
      ld_check(type < SealType::Count);
      return seals_[static_cast<size_t>(type)];
    }

    void setSeal(SealType type, Seal seal) {
      ld_check(type < SealType::Count);
      seals_[static_cast<size_t>(type)] = seal;
    }

   private:
    std::array<Seal, static_cast<size_t>(SealType::Count)> seals_;
  };

  // Function object called by recoverSeal() with two arguments:
  // status indicating the success of the operation, and the recovered
  // LogStorageState::Seals.
  // Status is one of: OK, MALFORMED_RECORD, LOCAL_LOG_STORE_READ, DROPPED.
  using seal_callback_t = std::function<void(Status, Seals)>;

  // create the LogStorageState object, if @param cache_deps is not nullptr,
  // enable record caching by creating the RecordCache object. Otherwise,
  // do not create the cache.
  explicit LogStorageState(logid_t log_id,
                           shard_index_t shard,
                           LogStorageStateMap* owner,
                           RecordCacheDependencies* cache_deps = nullptr);

  /**
   * Finds the last released LSN for the log.
   *
   * @return Returns the requested LSN, or an empty Optional if the log was
   *         not found in the map.
   */
  LastReleasedLSN getLastReleasedLSN(
      std::memory_order mem_order = std::memory_order_seq_cst) const {
    LastReleasedLSN result; // initially empty
    uint8_t released_state = last_released_lsn_state_.load(mem_order);
    if (released_state) {
      result =
          LastReleasedLSN(last_released_lsn_.load(mem_order), released_state);
    }
    return result;
  }

  /**
   * Finds the last released LSN for the log.
   *
   * @return Returns the requested LSN, without information about the source of
   *         initialization.
   */
  lsn_t getLastReleasedLSNWithoutSource(
      std::memory_order mem_order = std::memory_order_seq_cst) const {
    return last_released_lsn_.load(mem_order);
  }

  /**
   * @return The last per-epoch released LSN for the log.
   */
  lsn_t getLastPerEpochReleasedLSN(
      std::memory_order mem_order = std::memory_order_seq_cst) const {
    return last_per_epoch_released_lsn_.load(mem_order);
  }

  /**
   * Gets the trim point, if initialized.
   */
  folly::Optional<lsn_t> getTrimPoint() const;

  /**
   * Gets the per-epoch log metadata trim point, if initialized.
   */
  folly::Optional<epoch_t> getPerEpochLogMetadataTrimPoint() const;

  /**
   * Gets the seal of @param type, if initialized
   */
  folly::Optional<Seal> getSeal(SealType type) const;

  folly::Optional<epoch_t> getLastCleanEpoch() const;

  const folly::Optional<std::pair<epoch_t, OffsetMap>>&
  getEpochOffsetMap() const;

  std::chrono::seconds getLogRemovalTime() const {
    return log_removal_time_.load();
  }

  void setLogRemovalTime(std::chrono::seconds t) {
    log_removal_time_.store(t);
  }

  /**
   * Updates the last released LSN. Will also update the last per-epoch
   * released LSN, since the latter is at least as high as the last released
   * LSN, by definition.
   *
   * @seealso updateLastPerEpochReleasedLSN
   *
   * @return If the last released LSN was initialized for the first time or the
   *         existing value was increased, returns 0 to signal success.
   *         Returns -1 on failure, setting err to:
   *           UPTODATE  last released was already >= lsn
   */
  int updateLastReleasedLSN(lsn_t last_released_lsn, LastReleasedSource source);

  /**
   * Updates the last per-epoch released LSN. This is the maximum last known
   * good of any epoch that is safe to read (has metadata available). It is
   * thus the maximum LSN that readers can safely read.
   *
   * @return If the last per-epoch released LSN was initialized for the first
   *         time or the existing value was increased, returns 0 to signal
   *         success. Returns -1 on failure, setting err to:
   *           UPTODATE  last released was already >= lsn
   */
  int updateLastPerEpochReleasedLSN(lsn_t last_per_epoch_released_lsn);

  /**
   * Updates the trim point.
   *
   * @return 0 on success (trim point initialized or increased). Returns -1 on
   *         failure, with err set to:
   *           UPTODATE  trim point was already >= lsn
   */
  int updateTrimPoint(lsn_t trim_point);

  /**
   * Updates the trim point of the per-epoch log metadata.
   *
   * @return same as updateTrimPoint()
   */
  int updatePerEpochLogMetadataTrimPoint(epoch_t trim_point);

  /**
   * Updates the seal of the specific type
   *
   * @param type   type of the Seal to update
   * @return 0 on success, -1 on failure with err set to:
   *            UPTODATE  seal was already >= epoch
   */
  int updateSeal(Seal seal, SealType type);

  void updateLastCleanEpoch(epoch_t epoch);

  void updateEpochOffsetMap(std::pair<epoch_t, OffsetMap>);

  /**
   * Looks up the worker in the set of workers subscribed to the log.
   */
  bool isWorkerSubscribed(worker_id_t id) const {
    ld_check(id.val_ >= 0);
    return subscribed_workers_.test(id.val_);
  }

  void subscribeWorker(worker_id_t id) {
    ld_check(id.val_ >= 0);
    subscribed_workers_.set(id.val_);
  }

  void unsubscribeWorker(worker_id_t id) {
    ld_check(id.val_ >= 0);
    subscribed_workers_.reset(id.val_);
  }

  void noteLogStateRecovered() {
    recover_log_state_task_in_flight_.store(false);
  }

  /**
   * Implementation of LogStorageStateMap::recoverLogState().
   */
  int recover(std::chrono::microseconds interval,
              LogStorageState::RecoverContext ctx,
              bool force_ask_sequencer = false);

  /**
   * If seal record for log_id is not known, this function attempts to recover
   * it by reading it from the local log store. If local log store doesn't
   * contain a seal record for `log_id', a record with EPOCH_INVALID will be
   * returned.
   *
   * This method should be called on a worker thread. Once the seal record is
   * available, `callback' will be called _on the same worker thread_.
   *
   * Recovery of the seal record is separate from the rest of the state since
   * it's used when writing new records -- storage nodes need to wait until
   * seal record is available (on the read path, CatchupQueue handles retrying
   * when last_released_lsn or trim_point is not known, so calling back is not
   * necessary).
   *
   * Note: this method recovers both normal and soft seals
   *
   * @return 0 if the recover task was successfully scheduled.
   *         -1 if the log has a permanent error, and you shouldn't expect
   *         this LogStorageState to ever be recovered.
   *         Sets `err` to E::FAILED when returning -1.
   */
  int recoverSeal(seal_callback_t callback);

  /**
   * Called when ReleaseRequest::broadcastReleaseRequest() fails to post a
   * ReleaseRequest to a worker.  Schedules a timer to retry again in a while.
   */
  void retryRelease(worker_id_t id, bool force);

  void getDebugInfo(InfoLogStorageStateTable& table) const;

  ~LogStorageState();

  // TODO change to inline PurgeCoordinator instance (which was moved to
  // server/) once this class is in server/
  std::unique_ptr<LogStorageState_PurgeCoordinator_Bridge> purge_coordinator_;

  std::unique_ptr<RecordCache> record_cache_;

  // Static callback for GetSeqStateRequest; looks up the correct
  // LogStorageState instance and passes it the result to populate the
  // in-memory state
  static void getSeqStateRequestCallback(shard_index_t shard_idx,
                                         const GetSeqStateRequestResult&);

  // @see permanent_error_
  // @param context should fit in sentence "<context> failed for log %lu".
  // @return false if the permanent error flag was already set
  bool notePermanentError(const char* context);
  // @return whether the permanent error flag is set
  bool hasPermanentError();

  shard_index_t getShardIdx() const {
    return shard_;
  }

 private:
  const logid_t log_id_;
  const shard_index_t shard_;
  LogStorageStateMap* const owner_;

  // The last released LSN for the log. Updated when a global RELEASE message
  // is received from the log's sequencer. Read by CatchupQueue when reading
  // from the local log store. Anything up to the last released LSN can be
  // safely read.
  std::atomic<lsn_t> last_released_lsn_{LSN_INVALID};

  // Set to true if a permanent error was encountered and this LogStorageState
  // likely will never be fully up-to-date. If true, we should avoid creating
  // ServerReadStreams for this log because they are likely to get stuck waiting
  // for this LogStorageState to be updated. In particular this flag is set if:
  //  * RecoverLogStateTask failed for this log. Things like trim_point_ will
  //    most likely never be populated.
  //  * Purging was unable to complete due to a permanent error in the local
  //    log store. If a read stream is blocked on last_released_lsn_ being
  //    advanced and this flag is set, it knows it will be permanently stalled
  //    and it needs to instruct the reader to do SCD failover.
  // If trim point is loaded, we do not immediately destroy read streams that
  // may be affected because we want to give them a chance to read all the
  // records up to the current (but stale) value of last released lsn.
  // If only a few sectors of the disk are broken, we might fail to do purging
  // but succeed to send records in clean epochs.
  std::atomic<bool> permanent_error_{false};

  // The last per-epoch released LSN for the log. Updated when a global or
  // per-epoch RELEASE message is received from the log's sequencer. Read by
  // CatchupQueue when reading from the local log store. Anything between the
  // last released LSN and the last per-epoch released LSN can be safely read
  // provided the respective LSN's ESN is not greater than the last known good
  // (LNG) of its epoch.
  std::atomic<lsn_t> last_per_epoch_released_lsn_{LSN_INVALID};

  // Initialization state of last_released_lsn. Zero if uninitialized,
  // otherwise bitwise-or of values from LastReleasedSource
  std::atomic<uint8_t> last_released_lsn_state_{0};

  // Is there a GetSeqStateRequest inflight for this log?  If so, we avoid
  // creating new ones until it comes back.
  std::atomic<bool> get_seq_state_inflight_{false};

  std::atomic<bool> recover_log_state_task_in_flight_{false};

  // Trim point of log.  Allows the local log store to delete trimmed
  // records and read paths to recognize that records are missing because of
  // trimming.  All records up to (and including) this LSN are scheduled for
  // deletion and should not be exposed to clients.
  // If uninitialized, the current thread may try to schedule recovery.
  //
  // Initialized by one of:
  // (1) A TRIM message received by this process
  // (2) A saved trim point lazily read from the local log store metadata
  //     section
  AtomicOptional<lsn_t> trim_point_{LSN_INVALID, EMPTY_OPTIONAL};

  // Trim point of per-epoch log metadata. This is of type epoch_t instead
  // of lsn_t since these metadata are stored per-epoch. PerEpochLogMetadata
  // whose epoch is smaller or equal than the trim point should be trimmed.
  AtomicOptional<epoch_t::raw_type> per_epoch_metadata_trim_point_{
      EPOCH_INVALID.val_,
      EMPTY_OPTIONAL};

  // stores all kinds of Seal for a log. Used to decide whether storage nodes
  // should reject all stores with sequence numbers belonging to sealed epochs.
  // Empty optional means that the seal is unknown and needs to be read
  // from local log store. In contrast, Seal::empty() means that we _know_
  // that this storage node didn't receive any seals for this log.
  std::array<AtomicOptional<Seal>, static_cast<size_t>(SealType::Count)> seals_{
      {{Seal(), EMPTY_OPTIONAL}, {Seal(), EMPTY_OPTIONAL}}};

  // Last clean epoch value, also stored in the local log store.  This is
  // atomic so that on incoming RELEASE messages we can decide without locking
  // if purging is necessary (most often it is not).
  // Initialized by reading the value from the local log store.
  // If uninitialized, we kick off PurgeUncleanEpochs which reads the last clean
  // epoch as its first step and reports back.
  AtomicOptional<epoch_t::raw_type> last_clean_epoch_{0, EMPTY_OPTIONAL};

  // subscribed to broadcasts of RELEASE messages.  These workers are
  // notified, for example, when a new record is released for delivery.
  folly::AtomicBitSet<MAX_WORKERS> subscribed_workers_;

  // Latest time (number of microseconds since steady_clock's epoch) when
  // some storage node tried to recover the state.
  std::atomic<std::chrono::microseconds> last_recovery_time_{};

  // A grace period for delaying the deletion of data for this log.
  // This is useful in cases, when the log is accidentally removed
  // from logs config.
  //
  // set to 0 by default to help figure out that the value hasn't been
  // read from LogRemovalTimeMetadata yet.
  //
  // set to std::chrono:seconds::max() to indicate that log's grace period(
  // used for logs that get removed) is not active.
  std::atomic<std::chrono::seconds> log_removal_time_{std::chrono::seconds(0)};

  using RWLock = folly::SharedMutexWritePriority;
  // Lock to update and read latest_epoch_offsets_ safely.
  mutable RWLock rw_lock_;
  // Pair of latest updated epoch and corresponding epoch offsets.
  // This value get updated from sequencer once recover() get triggered.
  // It is not updated with RELEASE messages, so epoch of last_released_lsn_
  // can be different from epoch in latest_epoch_offsets_ pair.
  folly::Optional<std::pair<epoch_t, OffsetMap>> latest_epoch_offsets_;

  // Data needed to manage retrying sending ReleaseRequests to workers.
  struct RetryRelease {
    std::mutex mutex_;
    // True when there is a timer scheduled to fire or currently running on
    // *some* worker.
    bool timer_scheduled_;
    bool force_;
    std::bitset<MAX_WORKERS> failed_workers_;
  } retry_release_;

  /**
   * Callback for timer to retry sending a ReleaseRequest to workers that we
   * failed to post to because their Request pipes were full.
   */
  void onRetryReleaseTimer(ExponentialBackoffTimerNode* node);
};

}} // namespace facebook::logdevice
