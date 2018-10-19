/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include <boost/noncopyable.hpp>
#include <folly/Memory.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/memory/EnableSharedFromThis.h>

#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

/**
 * @file A background thread that periodically checks for logs which have
 *       unreleased records, and for which no records have been released for
 *       two consecutive unreleased_record_detector_intervals. This may
 *       happen if a sequencer fails after sending some STORE messages but
 *       before sending corresponding RELEASE messages, and the sequencer is
 *       not replaced, because there are no readers, or all the readers have
 *       caught up. In that case, the thread will eventually execute a
 *       GetSeqStateRequest, causing sequencer fail-over and eventually release
 *       of the records.
 *
 *       Runs only on storage nodes.
 */
namespace facebook { namespace logdevice {

class ServerProcessor;

class UnreleasedRecordDetector final
    : public folly::enable_shared_from_this<UnreleasedRecordDetector>,
      private boost::noncopyable {
 public:
  UnreleasedRecordDetector(ServerProcessor* processor,
                           UpdateableSettings<Settings> settings);
  ~UnreleasedRecordDetector();

  /**
   * Start worker thread. Assumes that thread is not running already.
   */
  void start();

  /**
   * Stop worker thread. Idempotent.
   */
  void stop();

 private:
  /**
   * Main loop of the worker thread.
   */
  void threadMain() noexcept;

  /**
   * Wait for next unreleased_record_detector_interval, or stop(), or any
   * update to unreleased_record_detector_interval.
   *
   * @return true iff stop() was called
   */
  bool waitNextInterval(std::unique_lock<std::mutex>& lock);

  /**
   * Collect new log states from local log store.
   *
   * @return true iff some local log store does not support
   *         getHighestInsertedLSN() (fatal error)
   */
  bool collectLogStates();

  /**
   * Compare previous log states and new log states.
   *
   * @return true iff stop() was called
   */
  bool compareLogStates();

  /**
   * @return Options for GetSeqStateRequest.
   */
  GetSeqStateRequest::Options makeOptions(shard_index_t shard_idx);

  /**
   * A map of (logid_t, shard_index_t) to pairs of (highest_inserted_lsn,
   * last_released_lsn)
   */
  using Key = std::pair<logid_t::raw_type, shard_index_t>;
  using LogStates = std::unordered_map<Key, std::pair<lsn_t, lsn_t>>;

  /// pointer to the processor object for accessing other logdevice
  /// components
  ServerProcessor* const processor_;

  /// settings, we only care about the unreleased_record_detector_interval
  UpdateableSettings<Settings> settings_;

  /// flag to stop the thread, protected by mutex_
  bool should_stop_ = true;

  /// thread object for the detector thread
  std::thread thread_;

  /// condition variable and related mutex used by the detector thread
  std::condition_variable cv_;
  std::mutex mutex_;

  /// log states at previous iteration
  LogStates prev_states_;

  /// log states at current iteration
  LogStates new_states_;

  /// thread safe map that tells us whether there is an outstanding
  /// GetSeqStateRequest for a given log
  /// TODO(T15517759): With Flexible Log Sharding: in order to be able to have
  /// more than one shard on this storage node have data for each log, we may
  /// need to either have this map key'ed by a (logid_t, shard_index_t) pair or
  /// implement or more clever mechanism such that we have at most one
  /// GetSeqStateRequest in flight for each log.
  folly::ConcurrentHashMap<logid_t::raw_type, bool, Hash64<logid_t::raw_type>>
      outstanding_request_map_;

  // subscription to settings, comes last to ensure it is destroyed first
  UpdateableSettings<Settings>::SubscriptionHandle settings_subscription_;
};

}} // namespace facebook::logdevice
