/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/UnreleasedRecordDetector.h"

#include <cinttypes>

#include <folly/Likely.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

using namespace std::literals;

UnreleasedRecordDetector::UnreleasedRecordDetector(
    ServerProcessor* processor,
    UpdateableSettings<Settings> settings)
    : processor_(processor),
      settings_(std::move(settings)),
      outstanding_request_map_(),
      settings_subscription_(settings_.subscribeToUpdates([this]() {
        // the lock here is necessary to avoid a subtle race condition where
        // waitNextInterval() reads unreleased_record_detector_interval == 0,
        // the callback is called, the signal is lost, and waitNextInterval()
        // goes on to wait indefinitely
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.notify_all();
      })) {
  ld_check(processor != nullptr);
}

UnreleasedRecordDetector::~UnreleasedRecordDetector() {
  // make sure detector thread is stopped (stop() is idempotent)
  stop();
}

GetSeqStateRequest::Options
UnreleasedRecordDetector::makeOptions(shard_index_t shard_idx) {
  GetSeqStateRequest::Options options;
  options.wait_for_recovery = true;
  options.include_tail_attributes = false;
  // Ask for epoch offset because we populate LogStorageState and it normally
  // asks for it
  options.include_epoch_offset = true;
  options.merge_type = GetSeqStateRequest::MergeType::GSS_MERGE_INTO_OLD;

  // Request callback, uses a weak_ptr-to-this to make sure it does not crash if
  // this is destroyed concurrently.
  options.on_complete = [this_weak = weak_from_this(),
                         shard_idx](GetSeqStateRequest::Result result) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   2,
                   "GetSeqStateRequest finished for log %" PRIu64 ", "
                   "status = %s.",
                   result.log_id.val(),
                   error_description(result.status));

    // Update LogStorageState for the log since we depend on it being
    // populated; see collectLogStates()
    LogStorageState::getSeqStateRequestCallback(shard_idx, result);

    const auto this_shared = this_weak.lock();
    if (LIKELY(this_shared != nullptr)) {
      // detector still live, decrement pending request stat and atomically
      // unmark request as outstanding by setting the corresponding map value to
      // 'false'
      STAT_DECR(this_shared->processor_->stats_,
                get_seq_state_pending_context_unreleased_record);
      if (folly::kIsDebug) {
        const auto it =
            this_shared->outstanding_request_map_.find(result.log_id.val());
        ld_check(it != this_shared->outstanding_request_map_.cend());
        ld_check(it->first == result.log_id.val());
      }
      this_shared->outstanding_request_map_.assign(result.log_id.val(), false);
    }
  };
  return options;
}

void UnreleasedRecordDetector::start() {
  ld_check(should_stop_ && !thread_.joinable());

  // Create detector thread. Safe to destroy detector object while thread is
  // running, because destructor will call stop().
  std::unique_lock<std::mutex> lock(mutex_);
  should_stop_ = false;
  auto* const this_ptr = this; // suppress false linter warning
  thread_ = std::thread([this_ptr] { this_ptr->threadMain(); });
  ld_info("Unreleased record detector thread started.");
}

void UnreleasedRecordDetector::stop() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!should_stop_) {
    ld_info("Unreleased record detector thread stopping...");
    should_stop_ = true;
    lock.unlock();
    cv_.notify_all();
    thread_.join();
    ld_info("Unreleased record detector thread stopped.");
  }
}

void UnreleasedRecordDetector::threadMain() noexcept try {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:unrel-dec");

  // main loop, loops until stop() is called concurrently
  std::unique_lock<std::mutex> lock(mutex_);
  for (;;) {
    // wait for next interval, waking up on stop() or wait interval update
    if (waitNextInterval(lock)) {
      break;
    }

    // collect new log states
    if (UNLIKELY(collectLogStates())) {
      ld_error("Local log store does not support getHighestInsertedLSN(). "
               "Cannot detect unreleased records.");
      break;
    }

    // compare previous log states and new log states
    if (compareLogStates()) {
      break;
    }

    // forget prev states, new states become prev states
    prev_states_.clear();
    using std::swap;
    swap(prev_states_, new_states_);
  }

  // clean up before exit
  prev_states_.clear();
  new_states_.clear();
  ld_info("Unreleased record detector thread exit.");
} catch (const std::exception& ex) {
  ld_critical("Caught exception in unreleased record detector thread: \"%s\". "
              "Thread exits.",
              ex.what());
} catch (...) {
  ld_critical(
      "Caught something in unreleased record detector thread. Thread exits.");
}

bool UnreleasedRecordDetector::waitNextInterval(
    std::unique_lock<std::mutex>& lock) {
  // how long to wait/sleep
  auto wait_interval = settings_->unreleased_record_detector_interval;

  // predicate function for use with cv_
  const auto wake_cond = [this, wait_interval]() -> bool {
    if (should_stop_) {
      // stop() has been called, stop waiting
      return true;
    }
    const auto new_wait_interval =
        settings_->unreleased_record_detector_interval;
    if (new_wait_interval != wait_interval) {
      // wait interval changed, discard previous record states to
      // ensure we do not take action unless at least two of the new
      // wait intervals have passed
      prev_states_.clear();
      return true;
    } else {
      // nothing of interest happened, keep waiting
      return false;
    }
  };

  // wait for next interval, waking up on stop() or wait interval update
  if (wait_interval.count()) {
    ld_debug("Active. Waiting for %ld seconds...",
             std::chrono::duration_cast<std::chrono::seconds>(wait_interval)
                 .count());
    cv_.wait_for(lock, wait_interval, wake_cond);
  } else {
    ld_debug("Inactive. Waiting for stop or wait interval update...");
    cv_.wait(lock, wake_cond);
  }

  // return true iff stop() has been called
  return should_stop_;
}

bool UnreleasedRecordDetector::collectLogStates() {
  ld_check(processor_ && processor_->sharded_storage_thread_pool_);
  auto* const sharded_local_log_store =
      processor_->sharded_storage_thread_pool_->getShardedLocalLogStore();
  ld_check(sharded_local_log_store);

  // get pointer to latest snapshot of local logs config, may be nullptr in
  // unit tests
  const auto local_logs_config = processor_->config_->updateableLogsConfig()
      ? processor_->config_->getLocalLogsConfig()
      : nullptr;

  // visitor lambda for LogStorageStateMap that collects LogStates for all
  // logs whose last_released_lsn < highest_inserted_lsn; i.e., all logs for
  // which there are unreleased records
  const auto visitor = [this, sharded_local_log_store, &local_logs_config](
                           const logid_t log_id, const LogStorageState& state) {
    shard_index_t shard_idx = state.getShardIdx();

    // read last released LSN with relaxed memory order (we do not care about
    // consistency here, because we are only interested in logs where the last
    // released LSN has not changed in a long time)
    const lsn_t last_released_lsn =
        state.getLastReleasedLSNWithoutSource(std::memory_order_relaxed);

    // read highest inserted LSN from local log store
    lsn_t highest_inserted_lsn;
    if (sharded_local_log_store->getByIndex(shard_idx)->getHighestInsertedLSN(
            log_id, &highest_inserted_lsn)) {
      switch (err) {
        case E::LOCAL_LOG_STORE_READ:
          // shard failed to start or is under repair
          return 0;
        case E::NOTSUPPORTEDLOG:
          // getHighestInsertedLSN() not supported for this particular log
          return 0;
        case E::NOTSUPPORTED:
          // local store does not meet our requirements
          return -1;
        default:
          // unknown error
          ld_check(false);
          return -1;
      }
    }

    // check for unreleased records, ignoring logs that do not exist (have been
    // removed from the config)
    if ((last_released_lsn < highest_inserted_lsn) &&
        (!local_logs_config || local_logs_config->logExists(log_id))) {
      // log has unreleased records and exists, remember state
      const bool insert_happened =
          new_states_
              .emplace(std::make_pair(log_id.val(), shard_idx),
                       std::make_pair(highest_inserted_lsn, last_released_lsn))
              .second;
      ld_check(insert_happened);
      (void)insert_happened;
    }
    return 0;
  };

  ld_debug("Collecting log states...");
  const bool error = processor_->getLogStorageStateMap().forEachLog(visitor);
  ld_debug("Finished collecting log states.");

  // return true iff visitor() returned -1 for some invocation
  return error;
}

bool UnreleasedRecordDetector::compareLogStates() {
  ld_debug("Comparing log states...");

  // iterate over old states (implicitly disregarding any logs for which there
  // is a new state, but no old state)
  for (const auto& old_entry : prev_states_) {
    // skip logs for which there is no new state; i.e., logs which no longer
    // have unreleased records
    const Key key(old_entry.first);
    const logid_t log_id = logid_t(key.first);
    const shard_index_t shard_idx = key.second;
    const auto new_states_it = new_states_.find(key);
    if (new_states_it == new_states_.end()) {
      continue;
    }

    // there is an old state and a new state for the log_id, compare LSNs
    const auto& new_entry = *new_states_it;
    const lsn_t old_last_released_lsn = old_entry.second.second;
    const lsn_t new_last_released_lsn = new_entry.second.second;
    if (new_last_released_lsn == old_last_released_lsn) {
      // last_released_lsn has not changed. This means the log has had
      // unreleased records for two consecutive periods, and nothing has
      // been released in that time. Sequencer may be dead. Check map for
      // outstanding request.
      bool outstanding;
      {
        // This iterator will keep the map entry around until the iterator is
        // destroyed.  So, give it its own scope so we can destroy it ASAP.
        const auto it = outstanding_request_map_.find(log_id.val());
        outstanding = it != outstanding_request_map_.cend() && it->second;
      }
      if (!outstanding) {
        // no outstanding request for this log, we have work to do
        RATELIMIT_INFO(
            10s,
            20,
            "Log %" PRIu64
            " on shard %u has unreleased records and no releases for two "
            "consecutive periods. highest_inserted_lsn = %s, "
            "last_released_lsn = %s. Creating GetSeqStateRequest...",
            log_id.val(),
            shard_idx,
            lsn_to_string(new_entry.second.first).c_str(),
            lsn_to_string(new_last_released_lsn).c_str());

        // create GetSeqState request to make sure sequencer is alive or
        // replace it if necessary
        auto typed_req = std::make_unique<GetSeqStateRequest>(
            log_id,
            GetSeqStateRequest::Context::UNRELEASED_RECORD,
            makeOptions(shard_idx));
        auto req = std::unique_ptr<Request>(std::move(typed_req));

        // Post asynchronous request to processor, making sure the
        // outstanding request map contains 'true' iff the request was posted
        // successfully. We need to first update the outstanding request map,
        // for the (unlikely) case that the request callback is called before
        // Processor::postRequest() returns. If that call throws or fails for
        // whatever reason, the value in the outstanding request map has to be
        // reset to 'false' to keep the detector in a consistent state.
        bool is_first_request;
        {
          // This iterator will keep the map entry around until the iterator is
          // destroyed.  So, give it its own scope so we can destroy it ASAP.
          const auto insert_result(
              outstanding_request_map_.insert(log_id.val(), true));
          is_first_request = insert_result.second;
          if (!is_first_request) {
            // insert failed because entry for this log already existed, just
            // update the existing value from 'false' to 'true'
            ld_check(insert_result.first->first == log_id.val());
            ld_check(!insert_result.first->second);
            outstanding_request_map_.assign(log_id.val(), true);
          }
        }
        E status;
        try {
          status = static_cast<E>(processor_->postRequest(req));
        } catch (...) {
          outstanding_request_map_.assign(log_id.val(), false);
          throw;
        }
        if (status != E::OK) {
          outstanding_request_map_.assign(log_id.val(), false);
        }

        switch (status) {
          case E::OK:
            STAT_INCR(processor_->stats_,
                      get_seq_state_pending_context_unreleased_record);
            ld_debug("Successfully posted %s GetSeqStateRequest for log "
                     "%" PRIu64 ".",
                     (is_first_request ? "first" : "subsequent"),
                     log_id.val());
            break;
          case E::SHUTDOWN:
            ld_info("Processor shutting down. Bailing out...");
            return true;
          case E::NOBUFS:
            RATELIMIT_WARNING(
                std::chrono::seconds(1), 1, "Too many pending requests.");
            break;
          default:
            ld_error("Fatal error, status = %s.", error_description(status));
            return true;
        }
      } else {
        // there is an outstanding request, no need for another one
        ld_debug("Log %" PRIu64
                 " has unreleased records but a request is already "
                 "outstanding.",
                 log_id.val());
      }
    } else {
      // Log has had unreleased records for two consecutive intervals, but
      // there have been releases, so the log is not stuck. Everything in
      // order.
      ld_spew("Log %" PRIu64 " has unreleased records but is making progress. "
              "highest_inserted_lsn = %s, old_last_released_lsn = %s, "
              "new_last_released_lsn = %s.",
              log_id.val(),
              lsn_to_string(new_entry.second.first).c_str(),
              lsn_to_string(old_last_released_lsn).c_str(),
              lsn_to_string(new_last_released_lsn).c_str());
    }
  }
  ld_debug("Finished comparing log states.");

  return false;
}

}} // namespace facebook::logdevice
