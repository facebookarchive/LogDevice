/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

#include <boost/intrusive/set.hpp>
#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"
#include "logdevice/server/read_path/ReadIoShapingCallback.h"
#include "logdevice/server/read_path/ServerReadStream.h"

namespace facebook { namespace logdevice {

/**
 * @file Manages read streams on the server that belong to one client and are
 *       catching up.  For an explanation of "catching up", see doc for
 *       ServerReadStream::isCatchingUp().
 *
 *       All records going to a single client get multiplexed over a single
 *       socket.  The main responsibility of this class is to transmit the
 *       records in a fair and efficient manner.  This is not trivial --
 *       imagine a client subscribing to many logs with significant backlogs
 *       of records.
 */

class AllServerReadStreams;
class BackoffTimer;
class CatchupQueue;
class Timer;
class LogStorageStateMap;
class ReadIoShapingCallback;
class ReadStorageTask;
class RECORD_Message;
class SenderBase;
class SenderProxy;
class ServerReadStream;
class StatsHolder;

/**
 * External dependencies of CatchupQueueDependencies are isolated into this
 * class.  This gets mocked in unit tests, allowing CatchupQueue output to be
 * inspected.
 */
class CatchupQueueDependencies {
 public:
  explicit CatchupQueueDependencies(
      AllServerReadStreams* all_server_read_streams,
      StatsHolder* stats_holder = nullptr);

  /**
   * Proxy for AllServerReadStreams::erase().
   */
  virtual void eraseStream(ClientID client_id,
                           logid_t log_id,
                           read_stream_id_t read_stream_id,
                           shard_index_t shard);

  /**
   * Proxy for AllServerReadStreams::invalidateIterators().
   */
  virtual void invalidateIterators(ClientID client_id);

  /**
   * Proxy for AllServerReadStreams::distributeNewlyReleasedRecords().
   */
  virtual void distributeNewlyReleasedRecords();

  /**
   * Proxy for AllServerReadStreams::used().
   */
  virtual void used(logid_t logid);

  /**
   * If specified in the configuration for a log, return how much is allowed to
   * artificially delay delivery of newly released records in order to improve
   * performance (due to fewer reads and better batching).
   */
  virtual folly::Optional<std::chrono::milliseconds>
  getDeliveryLatency(logid_t log_id);

  /**
   * Creates a BackoffTimer object to use as the ping timer.  See doc for
   * CatchupQueue::ping_timer_.  In production this will be an
   * ExponentialBackoffTimer attached to the current Worker's event base.
   */
  virtual std::unique_ptr<BackoffTimer>
  createPingTimer(std::function<void()> callback);

  /**
   * Creates a timer to use for iterator invalidation. See doc for
   * CatchupQueue::iterator_invalidation_timer_.
   */
  virtual std::unique_ptr<Timer>
  createIteratorTimer(std::function<void()> callback);

  virtual std::chrono::milliseconds iteratorTimerTTL() const;

  virtual StatsHolder* getStatsHolder() {
    return stats_holder_;
  }

  virtual const Settings& getSettings() const;

  /**
   * Proxy for non-blocking read through LocalLogStoreReader::read().
   */
  virtual Status read(LocalLogStore::ReadIterator* read_iterator,
                      LocalLogStoreReader::Callback& callback,
                      LocalLogStoreReader::ReadContext* read_ctx);

  /**
   * Proxy for PerWorkerStorageTaskQueue::putTask().
   */
  virtual void putStorageTask(std::unique_ptr<ReadStorageTask>&& task,
                              shard_index_t shard);

  /**
   * Proxy for Processor::getLogStorageStateMap().
   */
  virtual LogStorageStateMap& getLogStorageStateMap();

  /**
   * Schedules recovery of the state (last released LSN and trim point) for a
   * log with the given log_id. See LogStorageStateMap::recoverLogState().
   */
  virtual int recoverLogState(logid_t log_id,
                              shard_index_t shard_idx,
                              bool force_ask_sequencer = false);

  virtual NodeID getMyNodeID() const;

  /**
   * Determines how many record bytes we can queue in the output evbuffer for
   * the client.
   */
  virtual size_t getMaxRecordBytesQueued(ClientID client);

  /**
   * Checks with underlying FlowGroup's(corresponding to stream's priority)
   * FlowMeter if sufficient bandwidth exists to allow a read storage task.
   */
  virtual bool canIssueReadIO(ReadIoShapingCallback& on_bw_avail,
                              ServerReadStream* stream);

  virtual ~CatchupQueueDependencies();

 public:
  std::unique_ptr<SenderBase> sender_;

 private:
  AllServerReadStreams* all_server_read_streams_;
  StatsHolder* stats_holder_;
};

class CatchupQueue {
  class ResumeStreamsCallback : public BWAvailableCallback {
   public:
    explicit ResumeStreamsCallback(CatchupQueue* queue)
        : catchup_queue_(queue) {}

    void operator()(FlowGroup&, std::mutex&) override;

   private:
    CatchupQueue* catchup_queue_;
  };

 public:
  enum class PushMode : uint8_t {
    // add a stream to the queue immediately
    IMMEDIATE = 0,
    // delay adding until CatchupQueue is ready to read more records
    DELAYED = 1,
  };

  /**
   *                                AllServerReadStreams instance
   * @param deps                    Class that captures all outgoing
   *                                communication. Used to permit testing.
   * @param client_id               client ID this catchup queue is for
   */
  CatchupQueue(std::unique_ptr<CatchupQueueDependencies>&& deps,
               ClientID client_id);

  virtual ~CatchupQueue();

  ResumeStreamsCallback& resumeCallback() const {
    return resume_cb_;
  }

  /**
   * Tries to make progress delivering records to clients.  Externally called
   * when a client first starts reading or when a new record is released,
   * nudging the class to do some work.
   */
  void pushRecords(CatchupEventTrigger reason = CatchupEventTrigger::OTHER);

  /**
   * Notify ClientReadStream about SHARD failure by sending
   * a STARTED_Message with E::FAILED.
   * Also mark the log as having permanent error to prevent
   * further reading.
   *
   * @return 0 on success, -1 if there was an error sending
   *         the STARTED message.
   */
  int notifyShardError(ServerReadStream* stream);

  /**
   * Called when a record is drained from the output evbuffer and sent over
   * the network.
   */
  void onRecordSent(const RECORD_Message& msg,
                    ServerReadStream*,
                    const SteadyTimestamp enqueue_time);

  /**
   * Called when a gap message is drained from the output evbuffer and
   * sent over the network.
   */
  void onGapSent(const GAP_Message& msg,
                 ServerReadStream*,
                 const SteadyTimestamp enqueue_time);

  /**
   * Called when a started message is drained from the output evbuffer and
   * sent over the network.
   */
  void onStartedSent(const STARTED_Message& msg,
                     ServerReadStream*,
                     const SteadyTimestamp enqueue_time);

  /**
   * Called after a ReadStorageTask completes on a storage thread.  The
   * docblock for LocalLogStoreReader::read() explains the output.
   */
  void onReadTaskDone(const ReadStorageTask& task);

  /**
   * Called after a ReadLngTask completes on a storage thread.
   */
  void onReadLngTaskDone(ServerReadStream* stream);

  /**
   * Called after a ReadStorageTask or ReadLngTask are dropped.
   */
  void onStorageTaskDropped(ServerReadStream* stream);

  /**
   * Adds a read stream to the queue. Depending on the mode argument, the
   * stream will be processed either next time pushRecords() runs, or only
   * when processDelayedQueueNow() indicates that it's ok to read.
   */
  void add(ServerReadStream& stream, PushMode mode = PushMode::IMMEDIATE);

  void getDebugInfo(InfoCatchupQueuesTable& table);

  void blockUnBlock(bool block);

  const ClientID client_id_;

  WeakRefHolder<CatchupQueue> ref_holder_;

 private:
  mutable ResumeStreamsCallback resume_cb_;

  std::unique_ptr<CatchupQueueDependencies> deps_;

  // Queue containing read streams inserted with PushMode::IMMEDIATE. Processed
  // immediately by pushRecords().
  folly::IntrusiveList<ServerReadStream, &ServerReadStream::queue_hook_> queue_;

  // If true, processing of this CatchupQueue has been blocked by the `block
  // catchup_queue` admin command.
  bool blocked_{false};

  struct ServerReadStreamComparer {
    bool operator()(const ServerReadStream& x,
                    const ServerReadStream& y) const {
      auto as_tuple = [](const ServerReadStream& m) {
        return std::tie(m.next_read_time_, m.client_id_, m.id_, m.log_id_);
      };
      return as_tuple(x) < as_tuple(y);
    }
  };

  // Queue containing streams inserted with PushMode::DELAYED, ordered by
  // next_read_time. Streams from this queue are moved to queue_ by
  // pushRecords().
  boost::intrusive::set<
      ServerReadStream,
      boost::intrusive::compare<ServerReadStreamComparer>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::member_hook<ServerReadStream,
                                    ServerReadStream::set_member_hook_type,
                                    &ServerReadStream::queue_delayed_hook_>>
      queue_delayed_;

  // Number of record bytes queued in output evbuffer.  Increased when we
  // queue a message for delivery, decreased when record are sent over the
  // network.
  size_t record_bytes_queued_ = 0;

  // Is there a storage task in flight for this catchup queue?  We only allow
  // one at a time.
  bool storage_task_in_flight_ = false;

  // If true, try a non-blocking read on the worker thread before involving a
  // storage thread.  This is only disabled in tests.
  bool try_non_blocking_read_ = true;

  // Ensures that we do not stall under certain rare error conditions.  The
  // most obvious one is (#2614323) when we fail to put records into the
  // output evbuffer, which interferes with the normal process of doing more
  // work when RECORD messages drain from the output evbuffer.
  std::unique_ptr<BackoffTimer> ping_timer_;

  // Timer used to periodically go through all read streams handled by this
  // CatchupQueue in order to ensure that iterators are not holding references
  // to potentially unused resources.
  std::unique_ptr<Timer> iterator_invalidation_timer_;

  /**
   * Helper method used in pushRecords(). Moves all streams from queue_delayed_
   * scheduled for reading to the end of queue_.
   */
  void processDelayedQueue();

  void adjustPingTimer();

  void onBatchComplete(ServerReadStream* stream);

  void onStorageTaskStopped(const ServerReadStream* stream);

  /**
   * Handle Read Throttling related credits and stats,
   * should be called upon read storage task completion.
   */
  void readThrottlingOnReadTaskDone(const ReadStorageTask& task);

  friend class CatchupQueueTest;
};

}} // namespace facebook::logdevice
