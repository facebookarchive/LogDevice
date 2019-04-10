/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <queue>
#include <set>
#include <utility>

#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index_container.hpp>

#include "logdevice/common/Address.h"
#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/protocol/STOP_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"
#include "logdevice/server/read_path/CatchupQueue.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/read_path/ReadIoShapingCallback.h"
#include "logdevice/server/read_path/ServerReadStream.h"

namespace facebook { namespace logdevice {

/**
 * @file Encapsulates all active subscriptions from readers to a single
 *       server.  They are primarily keyed by (client, log).  This class may
 *       maintain secondary indexes for common access patterns.
 *
 *       This class is not thread-safe.  Each worker thread is meant to have
 *       its own instance.
 */

class EpochOffsetStorageTask;
class ReadStorageTask;
class RECORD_Message;
class StatsHolder;
class ServerProcessor;
class Worker;
enum class ReleaseType : uint8_t;

class AllServerReadStreams : public ShardAuthoritativeStatusSubscriber {
 public:
  /**
   * @param max_read_storage_tasks_mem       Max amount of memory in bytes that
   *                                         can be used by in flight
   *                                         ReadStorageTasks.
   * @param worker_id                        worker->idx_ of worker we are
   *                                         running on
   * @param log_storage_state_map            LogStorageStateMap instance to use
   *                                         to subscribe to and unsubscribe
   *                                         from RELEASE messages for a log
   * @param stats                            Object used to update read stream
   *                                         stats.
   * @param on_worker_thread                 True if all methods will be called
   *                                         on a worker thread.
   */
  AllServerReadStreams(UpdateableSettings<Settings> settings,
                       size_t max_read_storage_tasks_mem,
                       worker_id_t worker_id,
                       LogStorageStateMap* log_storage_state_map,
                       ServerProcessor* processor,
                       StatsHolder* stats = nullptr,
                       bool on_worker_thread = true);

  AllServerReadStreams(const AllServerReadStreams&) = delete;
  AllServerReadStreams& operator=(const AllServerReadStreams&) = delete;

  AllServerReadStreams(AllServerReadStreams&&) = delete;
  AllServerReadStreams& operator=(AllServerReadStreams&&) = delete;

  /**
   * Finds the ServerReadStream object for this (client, log, read stream)
   * triplet, if any.
   *
   * @return Pointer to ServerReadStream object or nullptr if it is not found
   *         (indicating that there is no active read stream from the client
   *         for the log).
   */
  ServerReadStream* get(ClientID client_id,
                        logid_t log_id,
                        read_stream_id_t read_stream_id,
                        shard_index_t shard);

  /**
   * Creates a new ServerReadStream object for the (client, log, read stream)
   * triplet, or returns an already existing one.  Called when processing a
   * START message, to set up a new read stream or update an existing one.
   *
   * If running on a worker thread, this method will register a SocketCallback
   * on the client's Socket to notify us if the client disconnects, and will
   * initialize the iterator cache.
   *
   * @return On success, returns an STL-map-like pair with a pointer to the
   *         ServerReadStream instance and a bool saying if this call inserted
   *         the stream.  On failure, returns pair(nullptr, false) and sets
   *         err to:
   *           TEMPLIMIT  maximum number of readers reached, may succeed later
   *           PERMLIMIT  maximum number of logs reached, cannot accept reads
   *                      for new logs
   */
  std::pair<ServerReadStream*, bool>
  insertOrGet(ClientID client_id,
              logid_t log_id,
              shard_index_t shard,
              read_stream_id_t read_stream_id);

  /**
   * Deletes the ServerReadStream object for the (client, log, read stream)
   * triplet, if any.  Called when processing a STOP message.
   */
  void erase(ClientID client_id,
             logid_t log_id,
             read_stream_id_t read_stream_id,
             shard_index_t shard);

  /**
   * Erase all read streams for the given client ID.  Called when a client
   * disconnects.
   */
  void eraseAllForClient(ClientID client_id);

  /**
   * Called when reading starts or when a new record is released for delivery,
   * effectively notifying this class that the read stream is behind and needs
   * to catch up.
   *
   * @param allow_delay  if true, may delay processing of the stream; set when
   *                     new records are released for delivery
   *
   * NOTE: This may read some data immediately, possibly causing some read
   * streams for this client to finish and be erased.  Because of this, it is
   * not safe to call this while iterating through read streams.
   */
  void notifyNeedsCatchup(ServerReadStream& stream, bool allow_delay);

  /**
   * Walks through the list of all read streams for the given client ID and
   * invalidate cached iterators which haven't been used in the last
   * Settings::iterator_cache_ttl milliseconds.
   */
  void invalidateIterators(ClientID client_id);

  /**
   * Called when the messaging layer drains a GAP message from the output
   * evbuffer.
   */
  void onGapSent(ClientID client_id,
                 const GAP_Message& msg,
                 const SteadyTimestamp enqueue_time);

  /**
   * Called when the messaging layer drains a RECORD message from the output
   * evbuffer, suggesting that there may now be room for new records to be
   * queued up.
   */
  void onRecordSent(ClientID client_id,
                    const RECORD_Message& msg,
                    const SteadyTimestamp enqueue_time);

  /**
   * Called when the messaging layer drains a STARTED message from the output
   * evbuffer.
   */
  void onStartedSent(ClientID client_id,
                     const STARTED_Message& msg,
                     const SteadyTimestamp enqueue_time);

  /**
   * Called after a EpochOffsetStorageTask completed or dropped on a storage
   * thread.
   */
  void onEpochOffsetTask(EpochOffsetStorageTask& task);
  /**
   * Called after a ReadStorageTask completes on a storage thread.  Passes the
   * response to the appropriate CatchupQueue based on the client ID.
   */
  void onReadTaskDone(ReadStorageTask& task);

  /**
   * Called when a ReadStorageTask is dropped.  Passes the information to the
   * appropriate CatchupQueue based on the client ID.
   */
  void onReadTaskDropped(ReadStorageTask& task);

  /**
   * Static handler for incoming WINDOW messages.  Validates a bit then calls
   * the instance method on the current Worker's AllServerReadStreams
   * instance.
   */
  static Message::Disposition onWindowMessage(WINDOW_Message* msg,
                                              const Address& from);

  /**
   * Called when a WINDOW message is received from a client.  Looks up the
   * relevant ServerReadStream and updates its window, switching it to the
   * "catching up" state if needed.
   */
  void onWindowMessage(ClientID client_id, const WINDOW_Header& msg_header);

  /**
   * Find all the read streams that were stalled because they are for a log
   * that belongs to shard `shard` which was rebuilding and wake them up so we
   * can start sending gaps/records.
   */
  void onShardRebuilt(uint32_t shard);

  /**
   * Normally called when the server receives a RELEASE message and broadcasts
   * it to all workers.  This worker should find any clients reading this log
   * and, if they were caught up, instruct them to read and deliver the new
   * records.
   *
   * This method is also called when a storage node is recovering the state
   * for a log (last released LSN in particular). In that case, 'force' flag
   * will be set, which will cause notifyNeedsCatchup() to be called even if
   * record 'rid' was already delivered.
   */
  void onRelease(RecordID rid, shard_index_t shard, bool force);

  /**
   * Notify all clients reading on this worker thread of that update with a
   * SHARD_STATUS_UPDATE_Message.
   */
  void onShardStatusChanged() override;

  /**
   * onSent handler for SHARD_STATUS_UPDATE_Message.
   *
   * If st==E::OK, the message was sent to the client so this resets the retry
   * timer's period.  Otherwise, schedules a retry.
   */
  void onShardStatusUpdateMessageSent(ClientID cid, Status st);

  /**
   *
   * Forces the maps to get cleared and all read streams destroyed.
   */
  void clear() {
    size_t num_streams = streams_.size();
    size_t num_client_states = client_states_.size();

    streams_.clear();
    client_states_.clear();
    // Free all released records that we didn't get around to sending.
    // This moves EpochRecordCacheEntrys to various workers, so
    // must be run before the Worker::~Worker() is called.
    size_t num_record_groups = real_time_record_buffer_.shutdown();

    if (num_streams || num_client_states || num_record_groups) {
      ld_info(
          "Dropped %lu read streams, %lu client states, %lu realtime record "
          "groups",
          num_streams,
          num_client_states,
          num_record_groups);
    }
  }

  /**
   * Enqueue a ReadStorageTask for processing.
   */
  void putStorageTask(std::unique_ptr<ReadStorageTask>&& task,
                      shard_index_t shard);

  /**
   * Adjust the memory budget for read storage tasks (called by Worker when
   * settings are updated).
   */
  void setMemoryBudget(uint64_t max_read_storage_tasks_mem) {
    memory_budget_.setLimit(max_read_storage_tasks_mem);
  }

  ResourceBudget& getMemoryBudget();

  RealTimeRecordBuffer& getRealTimeRecordBuffer() {
    return real_time_record_buffer_;
  }

  /**
   * This method can be called from any thread, including storage threads.  It's
   * called with the EpochRecordCache rw lock held, so it should be as fast as
   * possible.
   */
  void appendReleasedRecords(std::unique_ptr<ReleasedRecords> records);

  /**
   * Evict from the real time buffer until we're under budget.
   */
  void evictRealTime();

  /**
   * Grab any newly released records from read_time_record_buffer_ and add them
   * to all streams for the appropriate log.
   *
   * Used by real time reads.
   */
  void distributeNewlyReleasedRecords();

  /**
   * Tell the real time record cache that we recently used the records from a
   * given log, so it can take this information into account in its eviction
   * strategy.
   */
  void used(logid_t logid) {
    real_time_record_buffer_.used(logid);
  }

  /**
   * Callback, called when settings_ changes.
   */
  void onSettingsUpdate();

  // The following functions return a string of human-readable
  // debug information about ServerReadStream_s.

  void getCatchupQueuesDebugInfo(InfoCatchupQueuesTable& table);

  // All streams associated with a client.
  void getReadStreamsDebugInfo(ClientID client_id,
                               InfoReadersTable& table) const;
  // All streams associated with a logid.
  void getReadStreamsDebugInfo(logid_t log_id, InfoReadersTable& table) const;
  // All streams.
  void getReadStreamsDebugInfo(InfoReadersTable& table) const;

  void blockUnblockClient(ClientID cid, bool block);

  ~AllServerReadStreams() override;

 protected: // tests may override
  /**
   * Initializes the CatchupQueue for the client if necessary, then adds the
   * server read stream identified by the (client_id, log_id, read_stream_id) to
   * the CatchupQueue, and kicks it off.
   *
   * NOTE: This may read some data immediately, possibly causing some read
   * streams for this client to finish and be erased.  Because of this, it is
   * not safe to call this while iterating through read streams.
   */
  virtual void
  scheduleForCatchup(ServerReadStream& stream,
                     bool allow_delay,
                     CatchupEventTrigger reason = CatchupEventTrigger::OTHER);

  /**
   * Send a ReadStorageTask for processing to PerWorkerStorageTaskQueue.
   * @param task Task to be processed.
   */
  virtual void sendStorageTask(std::unique_ptr<ReadStorageTask>&& task,
                               shard_index_t shard);

  // Starts a zero-delay timer to call sendDelayedStorageTasks() on the next
  // event loop iteration.
  virtual void scheduleSendDelayedStorageTasks();

 protected:
  //
  // Main data structure containing ServerReadStream instances.  We use a
  // boost::multi_index to allow three access patterns:
  // - Find/add/remove the stream for a specific (log id, client,
  //   read stream id) triplet.
  // - Find all clients subscribed to a log.  Used by delivery path.  This is
  //   a common high-volume operation.
  // - Find all logs that a client is subscribed to.  Only used to clean up
  //   after a client disconnects.
  //
  // NOTE: updateSubscription(log_id) should be called whenever this changes
  //

  // Names for indexes
  struct FullKeyIndex {};
  struct LogIndex {};
  struct LogShardIndex {};
  struct ClientIndex {};

  using client_id_member = boost::multi_index::
      member<ServerReadStream, ClientID, &ServerReadStream::client_id_>;
  using log_id_member = boost::multi_index::
      member<ServerReadStream, logid_t, &ServerReadStream::log_id_>;
  using read_stream_id_member = boost::multi_index::
      member<ServerReadStream, read_stream_id_t, &ServerReadStream::id_>;
  using shard_member = boost::multi_index::
      member<ServerReadStream, shard_index_t, &ServerReadStream::shard_>;

  boost::multi_index::multi_index_container<
      ServerReadStream,
      boost::multi_index::indexed_by<
          // index by (log ID, client ID, read stream ID, shard)
          boost::multi_index::hashed_unique<
              boost::multi_index::tag<FullKeyIndex>,
              boost::multi_index::composite_key<ServerReadStream,
                                                log_id_member,
                                                client_id_member,
                                                read_stream_id_member,
                                                shard_member>,
              boost::multi_index::composite_key_hash<logid_t::Hash,
                                                     ClientID::Hash,
                                                     read_stream_id_t::Hash,
                                                     std::hash<shard_index_t>>>,

          // index by log ID
          boost::multi_index::hashed_non_unique<
              boost::multi_index::tag<LogIndex>,
              log_id_member,
              logid_t::Hash>,

          // index by (log ID, shard)
          boost::multi_index::hashed_non_unique<
              boost::multi_index::tag<LogShardIndex>,
              boost::multi_index::
                  composite_key<ServerReadStream, log_id_member, shard_member>,
              boost::multi_index::composite_key_hash<logid_t::Hash,
                                                     std::hash<shard_index_t>>>,

          // index by client ID
          boost::multi_index::hashed_non_unique<
              boost::multi_index::tag<ClientIndex>,
              client_id_member,
              ClientID::Hash>>>
      streams_;

  RealTimeRecordBuffer real_time_record_buffer_;

  /**
   * Retrieve a ServerReadStream behind an iterator. boost::multi_index does not
   * allow retrieving a non const ServerReadStream because modifying its
   * components that are part of the index keys is invalid. However, we know
   * what we are doing and will never modify ServerReadStream::log_id_,
   * ServerReadStream::client_id_, and ServerReadStream::id_.
   */
  template <typename I>
  static constexpr ServerReadStream& deref(I& it) {
    return const_cast<ServerReadStream&>(*it);
  }

  /**
   * Evicts buffered real time reads for a single log.
   */
  void evictRealTimeLog(logid_t);

  /**
   * Wake up all the read streams for which `pred` returns true in the specified
   * range.
   *
   * @param begin Iterator to the initial position in a sequence of streams.
   * @param end   Iterator to the final position in a sequence of streams.
   * @param pred  Unary function that accepts a ServerReadStream that belongs to
   *              the specified range and whose return value indicates if that
   *              stream is to be woken up.
   */
  template <typename I, typename Pred>
  void catchupIf(const I& begin,
                 const I& end,
                 Pred pred,
                 CatchupEventTrigger reason = CatchupEventTrigger::OTHER) {
    // Because scheduleForCatchup() may invalidate iterators, we must not call
    // it while iterating through streams_.  Instead, first go through streams
    // for the log, mark them for catchup and later call
    // scheduleForCatchup() for any that need it.
    folly::small_vector<WeakRef<ServerReadStream>> to_push;
    for (I it = begin; it != end; ++it) {
      if (pred(deref(it)) && canScheduleForCatchup(deref(it))) {
        to_push.emplace_back(deref(it).createRef());
      }
    }
    // NOTE: each scheduleForCatchup() call may erase some streams for the
    // client, including streams appearing later in `to_push'.  This is why we
    // took weak references above and check them here.
    for (WeakRef<ServerReadStream>& ref : to_push) {
      if (ref) {
        scheduleForCatchup(*ref, /* allow_delay */ true, reason);
      }
    }
  }

  /**
   * For every Client that we see, we register a callback on the client's
   * Socket so that we get notified when the client disconnects and can free
   * resources.
   */
  struct ClientDisconnectedCallback : public SocketCallback {
    void operator()(Status st, const Address& name) override;
    AllServerReadStreams* owner = nullptr;
  };

  /**
   * We keep one of these for every client that did some reading and is still
   * connected.
   */
  struct ClientState {
    // This will manage all reading for the client
    std::unique_ptr<CatchupQueue> catchup_queue;

    ClientDisconnectedCallback disconnect_callback;

    // Timer used to retry sending a SHARD_STATUS_UPDATE_Message to this client.
    std::unique_ptr<BackoffTimer> timer_;
  };

  typedef std::unordered_map<ClientID, ClientState, ClientID::Hash>
      ClientStateMap;

  ClientStateMap client_states_;

  ServerProcessor* const processor_;

  StatsHolder* stats_;

  UpdateableSettings<Settings> settings_;

  ResourceBudget memory_budget_;

  // Current number of ReadStorageTasks in flight.
  // Used for assertions.
  size_t read_storage_tasks_in_flight_{0};

  // Queue of ReadStorageTasks waiting to be queued for execution. A
  // ReadStorageTask resides in this queue if at the time it was put by
  // CatchupOneStream there was not enough memory available to process it.
  struct QueuedTask {
    std::unique_ptr<ReadStorageTask> task;
    shard_index_t shard;
  };
  std::queue<QueuedTask> delayed_read_storage_tasks_;

  // A zero-delay timer to post tasks from delayed_read_storage_tasks_ to
  // storage threads after some tasks were dropped. We can't post them right
  // away because it's not nice to post more tasks from onDropped() callback.
  Timer send_delayed_storage_tasks_timer_;

  // Worker ID we are on, used to manage subscriptions for RELEASE messages.
  // In production, this is always equal to Worker::onThisThread()->idx_.  In
  // unit tests where there is no Worker, the test supplies a fake value.
  const worker_id_t worker_id_;

  // LogStorageStateMap instance to talk to to subscribe or unsubscribe from
  // RELEASE messages for a log.  Unowned.
  LogStorageStateMap* const log_storage_state_map_;

  // Is this object used from a worker thread? May be false in unit tests.
  bool on_worker_thread_;

  /**
   * Helper method called whenever streams_ changes.  Depending on whether
   * there are any more streams for the log, this calls into
   * log_storage_state_map_ to subscribe to or unsubscribe from RELEASE messages
   * for the log on the given shard.
   *
   * @return On success, returns 0.  On failure (maximum number of logs
   *         reached), returns -1.
   */
  int updateSubscription(logid_t log_id, shard_index_t shard);

  /**
   * Called by notifyNeedsCatchup to figure out if a stream should be scheduled
   * for catchup.
   *
   * @return True if the ServerReadStream can be enqueued in CatchupQueue for
   *         scheduling because there is new data to be read and the stream has
   *         not reached the client provided window.
   */
  bool canScheduleForCatchup(ServerReadStream& stream);

  /**
   * Try to budget some memory for this task.
   * @return True if we could budget some memory for this task, in thaat case
   * the task can be sent. Return false otherwise and the task will be queued
   * for a later retry once another storage task releases memory.
   */
  bool tryAcquireMemoryForTask(std::unique_ptr<ReadStorageTask>& task);

  /**
   * Called when a ReadStorageTask comes back to the worker thread and releases
   * the memory it acquired. Try to send as many tasks enqueued in
   * `delayed_read_storage_tasks_` as possible.
   */
  void sendDelayedReadStorageTasks();

  /**
   * Send a SHARD_STATUS_UPDATE_Message to a client.
   * If the message cannot be sent, a retry timer will be activated.
   *
   * @param cid Id of the client.
   */
  void sendShardStatusToClient(ClientID cid);

  /**
   * Activate a timer to retry sending a SHARD_STATUS_UPDATE_Message to a client
   * later.
   *
   * @param cid Id of the client.
   */
  void scheduleShardStatusUpdateRetry(ClientID cid);

  friend class CatchupQueueTest;
};

class EvictRealTimeRequest : public Request {
 public:
  EvictRealTimeRequest() : Request(RequestType::EVICT_REAL_TIME) {}

  Request::Execution execute() override;
};
}} // namespace facebook::logdevice
