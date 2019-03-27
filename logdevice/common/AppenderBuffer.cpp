/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppenderBuffer.h"

#include <folly/Memory.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

AppenderBufferQueue::AppenderBufferQueue(logid_t logid,
                                         AppenderBuffer* appender_buffer)
    : logid_(logid),
      process_timer_([this] { this->onTimerFired(); }),
      resume_callback_(AppenderBuffer::EMPTY_APPENDER_CB),
      appender_buffer_(appender_buffer) {
  ld_check(appender_buffer != nullptr);
}

void AppenderBufferQueue::process(AppenderBuffer::AppenderCallback cb) {
  size_t num_processed = 0;
  size_t num_allowed =
      Worker::onThisThread()->settings().appender_buffer_process_batch;
  ld_check(num_allowed > 0);

  // store appenders that need to be re-enqueued later
  std::queue<AppenderUniqPtr> re_queue;

  while (!queue_.empty() && num_processed < num_allowed) {
    AppenderUniqPtr& appender = queue_.front();
    auto action = cb(logid_, appender);
    switch (action) {
      case AppenderBuffer::Action::DESTROY:
        break;
      case AppenderBuffer::Action::RELEASE:
        appender.release();
        break;
      case AppenderBuffer::Action::REQUEUE:
        re_queue.push(std::move(appender));
        break;
    };

    queue_.pop();
    ++num_processed;
  }

  // re-enqueue appenders with Action::REQUEUE
  while (!re_queue.empty()) {
    queue_.push(std::move(re_queue.front()));
    re_queue.pop();
    WORKER_STAT_INCR(appenderbuffer_appender_requeued);
  }

  if (queue_.empty()) {
    ld_debug("AppenderBufferQueue cleared for log %lu, Worker %d, "
             "queue size %lu",
             logid_.val_,
             (int)(Worker::onThisThread()->idx_),
             num_processed);

    resume_callback_ = AppenderBuffer::EMPTY_APPENDER_CB;
    process_timer_.cancel();

    // remove the AppenderBufferQueue from the map in AppenderBuffer
    appender_buffer_->deleteQueue(logid_);
  } else if (num_processed >= num_allowed) {
    // We reached the limit of how many Appender objects we are allowed to
    // process before returning control to libevent. Schedule the rest of
    // processing in the next iteration of event loop and return control to
    // libevent so that its evbuffer can be processed
    resume_callback_ = cb;
    process_timer_.activate(std::chrono::milliseconds::zero());
  }
}

void AppenderBuffer::bufferedAppenderSendError(logid_t logid, Status st) {
  auto it = map_.find(logid);
  if (it == map_.end()) {
    return;
  }
  ld_check(it->second != nullptr);
  it->second->drainQueueAndSendError(st);
}

void AppenderBufferQueue::drainQueueAndSendError(Status st) {
  while (!queue_.empty()) {
    AppenderUniqPtr& appender = queue_.front();
    appender->sendError(st);
    WORKER_STAT_INCR(appenderbuffer_appender_failed_sequencer_activation);
    queue_.pop();
  }

  ld_debug("AppenderBufferQueue cleared for log %lu, Worker %d, ",
           logid_.val_,
           (int)(Worker::onThisThread()->idx_));

  resume_callback_ = AppenderBuffer::EMPTY_APPENDER_CB;
  process_timer_.cancel();

  // remove the AppenderBufferQueue from the map in AppenderBuffer
  appender_buffer_->deleteQueue(logid_);
}

void AppenderBufferQueue::onTimerFired() {
  process(resume_callback_);
}

const AppenderBuffer::AppenderCallback AppenderBuffer::EMPTY_APPENDER_CB =
    [](logid_t, AppenderUniqPtr&) { return Action::DESTROY; };

AppenderBuffer::AppenderBuffer(size_t queue_cap)
    : appender_buffer_queue_cap_(queue_cap) {}

bool AppenderBuffer::hasBufferedAppenders(logid_t logid) const {
  auto it = map_.find(logid);
  if (it == map_.end()) {
    return false;
  }
  // Since we delete the queue as soon as it is drained,
  // queue must be non-empty when it exists
  ld_check(!it->second->queue_.empty());
  return true;
}

bool AppenderBuffer::bufferAppender(logid_t logid,
                                    std::unique_ptr<Appender>& appender) {
  size_t queue_cap = appender_buffer_queue_cap_;
  if (queue_cap == 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    100,
                    "Dropping Appender because appender buffer queue cap "
                    "is set to 0 for log %lu on %s.",
                    logid.val_,
                    Worker::onThisThread()->getName().c_str());
    err = E::NOBUFS;
    WORKER_STAT_INCR(appenderbuffer_appender_failed_queue_full);
    return false;
  }

  if (appender->maxAppendersHardLimitReached()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Dropping Appender because maximum total size of appenders "
                    "%zu has been reached, log %lu, Worker %s.",
                    Worker::settings().max_total_appenders_size_hard,
                    logid.val_,
                    Worker::onThisThread()->getName().c_str());
    err = E::TEMPLIMIT;
    WORKER_STAT_INCR(appenderbuffer_appender_failed_size_limit);
    return false;
  }

  auto it = map_.find(logid);
  if (it == map_.end()) {
    // create a new AppenderBufferQueue
    auto res = map_.insert(std::make_pair(
        logid, std::make_unique<AppenderBufferQueue>(logid, this)));
    ld_check(res.second);
    it = res.first;
  }

  AppenderBufferQueue* appender_queue = it->second.get();
  ld_check(appender_queue != nullptr);
  if (appender_queue->size() >= queue_cap) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Dropping Appender because the queue of pending Appenders "
                    "is full for log %lu, Worker %s, queue size %lu.",
                    logid.val_,
                    Worker::onThisThread()->getName().c_str(),
                    appender_queue->size());
    err = E::NOBUFS;
    WORKER_STAT_INCR(appenderbuffer_appender_failed_queue_full);
    return false;
  }

  appender_queue->queue_.push(std::move(appender));
  WORKER_STAT_INCR(appenderbuffer_appender_buffered);
  WORKER_STAT_INCR(appenderbuffer_pending_appenders);
  return true;
}

void AppenderBuffer::deleteQueue(logid_t logid) {
  auto it = map_.find(logid);
  if (it != map_.end()) {
    ld_check(it->second != nullptr);
    // All Appender objects in the queue are getting destroyed
    WORKER_STAT_SUB(appenderbuffer_pending_appenders, it->second->size());
    map_.erase(it);
  }
}

void AppenderBuffer::processQueue(logid_t logid, AppenderCallback cb) {
  auto it = map_.find(logid);
  if (it == map_.end()) {
    return;
  }
  ld_check(it->second != nullptr);
  it->second->process(cb);
}

/* static */
AppenderBuffer::Action
AppenderBuffer::processBufferedAppender(logid_t logid,
                                        AppenderUniqPtr& appender) {
  ld_check(appender.get());

  Action action = Action::DESTROY;
  RunAppenderStatus appender_status = RunAppenderStatus::ERROR_DELETE;
  const Worker* w = Worker::onThisThread();

  // find the corresponding data log sequencer
  const logid_t datalog_id = MetaDataLog::dataLogID(logid);
  const bool metadata_log = MetaDataLog::isMetaDataLog(logid);
  std::shared_ptr<Sequencer> sequencer =
      w->processor_->allSequencers().findSequencer(datalog_id);

  if (!sequencer) {
    err = E::NOSEQUENCER;
  } else {
    epoch_t current_epoch = sequencer->getCurrentEpoch();
    NodeID preempted_by = sequencer->checkIfPreempted(current_epoch);
    if (preempted_by.isNodeID()) {
      appender->sendRedirect(preempted_by, E::PREEMPTED);
      WORKER_STAT_INCR(appenderbuffer_appender_failed_retry);
      WORKER_STAT_DECR(appenderbuffer_pending_appenders);
      return Action::DESTROY;
    }
    if (!w->isAcceptingWork()) {
      err = E::SHUTDOWN;
    } else {
      if (!metadata_log) {
        bool nodeset_ok = sequencer->checkNodeSet();
        if (nodeset_ok || err == E::INPROGRESS) {
          appender_status = sequencer->runAppender(appender.get());
        } else {
          // err set by checkNodeSet
          ld_check(err == E::NOSPC || err == E::OVERLOADED ||
                   err == E::UNROUTABLE || err == E::DISABLED);
        }
      } else {
        // appender is for metadata log, redirect it to the MetaDataLogWriter
        // proxy
        MetaDataLogWriter* meta_writer = sequencer->getMetaDataLogWriter();
        ld_check(meta_writer != nullptr);
        appender_status = meta_writer->runAppender(appender.get());
      }

      if (appender_status == RunAppenderStatus::ERROR_DELETE) {
        switch (err) {
          case E::BADPAYLOAD:
          case E::NOSEQUENCER:
          case E::NOBUFS:
          case E::TEMPLIMIT:
          case E::NOSPC:
          case E::OVERLOADED:
          case E::UNROUTABLE:
          case E::REBUILDING:
          case E::DISABLED:
          case E::SYSLIMIT:
            break; // pass through to sendError()
          case E::STALE:
          case E::INPROGRESS: // sequencer is re-initializing
            if (metadata_log) {
              // for metadata log, keep requeuing the request if err is
              // E::INPROGRESS
              return Action::REQUEUE;
            }

            err = E::NOSEQUENCER;
            break;
          case E::TOOBIG:
            RATELIMIT_CRITICAL(
                std::chrono::seconds(1),
                10,
                "INTERNAL ERROR: ran out of ESNs in a new epoch %u "
                "of log %lu",
                sequencer->getCurrentEpoch().val_,
                sequencer->getLogID().val_);
            err = E::INTERNAL;
            break;
          default:
            // unexpected error (INVALID_PARAM?), sendError() will log
            break;
        }
      }
    }
  }

  switch (appender_status) {
    case RunAppenderStatus::SUCCESS_KEEP:
      // Appender object is now owned by Sequencers
      WORKER_STAT_INCR(appenderbuffer_appender_released);
      action = Action::RELEASE;
      break;

    case RunAppenderStatus::SUCCESS_DELETE:
      // Sequencers are done with appender object.  Destroy.
      WORKER_STAT_INCR(appenderbuffer_appender_released);
      WORKER_STAT_INCR(appenderbuffer_appender_deleted);
      action = Action::DESTROY;
      break;

    case RunAppenderStatus::ERROR_DELETE:
      ld_debug("Failed to start a pending appender for log %lu, Worker %d: %s",
               logid.val_,
               (int)(Worker::onThisThread()->idx_),
               error_name(err));

      // we already failed once, just discard and destroy this time
      appender->sendError(err);
      WORKER_STAT_INCR(appenderbuffer_appender_failed_retry);
      action = Action::DESTROY;
  }

  WORKER_STAT_DECR(appenderbuffer_pending_appenders);
  return action;
}

}} // namespace facebook::logdevice
