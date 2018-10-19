/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>

#include "logdevice/common/Appender.h"
#include "logdevice/common/Timer.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file AppenderBuffer stores buffered Appender objects for a particular
 *       Worker. It is currently used for buffering incoming Append requests
 *       during Sequencer reactivation (e.g., maximum ESN for the current epoch
 *       has been reached). These pending Appender objects are stored in per-log
 *       queues whose capacitity can be set by the administrator. Once the
 *       Sequencer finishes its reactivation, buffered Appender objects will be
 *       processed depending on the reactivation status.
 */

class AppenderBufferQueue;
using AppenderUniqPtr = std::unique_ptr<Appender>;

class AppenderBuffer {
 public:
  enum class Action {
    DESTROY, // destory the appender object
    RELEASE, // appender has transfered ownership,
             // release the ownership of the appender object
    REQUEUE  // re-enqueue the appender to the appender buffer
  };

  /**
   * Callback used to process the pending appenders buffered in the queue.
   *
   * @return  Action to advise AppenderBuffer on how to dispose the appender.
   *          Note that it is assumed that ownership of the appender remains to
   *          be AppenderBuffer after the callback. If appender has transfered
   *          owndership in the callback (passed to the sequencer),
   *          RELEASE must be returned.
   */
  using AppenderCallback = std::function<Action(logid_t, AppenderUniqPtr&)>;

  /**
   * Create an AppenderBuffer with the given queue capacity
   */
  explicit AppenderBuffer(size_t queue_cap);

  AppenderBuffer(AppenderBuffer&&) = delete;
  AppenderBuffer(const AppenderBuffer&) = delete;
  AppenderBuffer& operator=(const AppenderBuffer&) = delete;
  AppenderBuffer& operator=(const AppenderBuffer&&) = delete;

  /**
   * Check if there are buffered Appender objects for a particular logid
   */
  bool hasBufferedAppenders(logid_t logid) const;

  /**
   * Try to enqueue an incoming Appender object to buffer
   * If successful, take the ownership from the passed-in appender
   *
   * @param logid      logid of the Appender object
   * @param appender   appender object
   *
   * @return true if enqueue is successful, and appender ownership is claimed
   *         false if the queue is full, and does not claim the ownership.
   *         Sets err to:
   *           E::NOBUFS:    too many appenders queued for this log,
   *           E::TEMPLIMIT: total size of allocated appenders is above limit.
   */
  bool bufferAppender(logid_t logid, AppenderUniqPtr& appender);

  /**
   * Set the capacity limit of the buffer queue. Noted that the limit if for
   * each worker's queue, not for all queues of a log.
   */
  void setQueueCap(size_t cap) {
    appender_buffer_queue_cap_ = cap;
  }

  /**
   * Get the capacity limit of the buffer queue (per Worker).
   */
  size_t getQueueCap() const {
    return appender_buffer_queue_cap_;
  }

  /**
   * Delete an ApperBufferQueue by the given logid
   */
  void deleteQueue(logid_t logid);

  /**
   * Process all the Appender objects in the buffer queue for logid,
   * essentially calls AppenderBufferQueue::process().
   **/
  void processQueue(logid_t logid, AppenderCallback cb);

  /**
   * Send error to all the buffered appender objects
   */
  void bufferedAppenderSendError(logid_t logid, Status st);

  /**
   * AppenderCallback function used to process pending Appender objects
   * in the queue when the sequencer has been successfully reactiavted.
   */
  static Action processBufferedAppender(logid_t logid, AppenderUniqPtr& ap);

  /**
   * Empty AppenderCallback that does not nothing so that Appenders just got
   * destroyed while poped out of the queue
   */
  static const AppenderCallback EMPTY_APPENDER_CB;

 private:
  // capacity of the AppenderBuffer queue
  size_t appender_buffer_queue_cap_;

  // an AppenderBufferQueue for each logid, stored in a hash map
  std::unordered_map<logid_t,
                     std::unique_ptr<AppenderBufferQueue>,
                     logid_t::Hash>
      map_;
};

class AppenderBufferQueue {
 public:
  /**
   * Create an AppenderBufferQueue for the given log and AppenderBuffer
   */
  AppenderBufferQueue(logid_t logid, AppenderBuffer* appender_buffer);

  /**
   * Process all the Appender objects in the buffer queue by dequeuing and
   * calling cb on each item. This function processes Appenders in batch
   * (the batch size is specified in Settings::appender_buffer_process_batch).
   * It periodically returns control to libevent loop after each batch to allow
   * evbuffer to be processed.
   *
   * @param  cb   Callback function that applies on each Appender object
   **/
  void process(AppenderBuffer::AppenderCallback cb);

  /**
   * Send error to all pending Appender objects in the queue when the
   * sequencer activation fails.
   */
  void drainQueueAndSendError(Status st);

  /* return size of the internal queue */
  size_t size() const {
    return queue_.size();
  }

 private:
  // logid for the queue
  logid_t logid_;

  // a std::queue to store the actual Appender objects
  std::queue<AppenderUniqPtr> queue_;

  // This timer is used when the queue of pending Appender objects is
  // sufficiently large, we need to periodically return to libevent loop
  // to prevent it from running out of output buffers
  Timer process_timer_;

  // Current callback to process the queue, used to resume processing
  // upon timer fired
  AppenderBuffer::AppenderCallback resume_callback_;

  // parent pointer to AppenderBuffer object
  AppenderBuffer* appender_buffer_;

  // Callback when process_timer_ fires.
  void onTimerFired();

  friend class AppenderBuffer;
};

}} // namespace facebook::logdevice
