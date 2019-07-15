/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/server/read_path/AllServerReadStreams.h"

#include <functional>
#include <utility>

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/protocol/SHARD_STATUS_UPDATE_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/IteratorCache.h"
#include "logdevice/server/storage_tasks/EpochOffsetStorageTask.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

AllServerReadStreams::AllServerReadStreams(
    UpdateableSettings<Settings> settings,
    size_t max_read_storage_tasks_mem,
    worker_id_t worker_id,
    LogStorageStateMap* log_storage_state_map,
    ServerProcessor* processor,
    StatsHolder* stats,
    bool on_worker_thread)
    : real_time_record_buffer_(
          settings->real_time_max_bytes / settings->num_workers,
          settings->real_time_eviction_threshold_bytes / settings->num_workers,
          stats),
      processor_(processor),
      stats_(stats),
      settings_(settings),
      memory_budget_(max_read_storage_tasks_mem),
      worker_id_(worker_id),
      log_storage_state_map_(log_storage_state_map),
      on_worker_thread_(on_worker_thread) {}

AllServerReadStreams::~AllServerReadStreams() {}

void AllServerReadStreams::onSettingsUpdate() {
  if (!settings_->real_time_reads_enabled) {
    for (;;) {
      distributeNewlyReleasedRecords();

      logid_t logid = real_time_record_buffer_.toEvict();
      if (logid == LOGID_INVALID) {
        break;
      }
      evictRealTimeLog(logid);
    }
  }
}

std::pair<ServerReadStream*, bool>
AllServerReadStreams::insertOrGet(ClientID client_id,
                                  logid_t log_id,
                                  shard_index_t shard,
                                  read_stream_id_t read_stream_id) {
  std::shared_ptr<std::string> log_group_path;
  if (processor_) { // can be null in tests
    if (auto log_path = processor_->config_->get()->getLogGroupPath(log_id)) {
      log_group_path = std::make_shared<std::string>(log_path.value());
    }
  }
  const auto insert_result = streams_.emplace(
      read_stream_id, client_id, log_id, shard, stats_, log_group_path);

  if (insert_result.second) {
    // We actually inserted ...

    // Check that we aren't over capacity
    if (streams_.size() > settings_->max_server_read_streams) {
      streams_.erase(insert_result.first);
      err = E::TEMPLIMIT;
      return std::make_pair(nullptr, false);
    }

    // Subscribe to RELEASE messages for this log
    if (updateSubscription(log_id, shard) != 0) {
      // This can fail if it is the first time we are seeing this log ID (so
      // we need to create a new LogStorageStateMap entry) and there are too
      // many logs in the server's LogStorageStateMap.  This is pretty bad - the
      // server cannot accept read traffic for new logs.
      streams_.erase(insert_result.first);
      err = E::PERMLIMIT;
      return std::make_pair(nullptr, false);
    }

    STAT_INCR(stats_, server_read_streams_created);

    // This may be the first time we are seeing this client.  If so, create a
    // ClientState object and register a callback to clean up when the client
    // disconnects.
    auto client_state_result =
        client_states_.emplace(std::piecewise_construct,
                               std::forward_as_tuple(client_id),
                               std::forward_as_tuple());
    if (client_state_result.second) {
      // Newly inserted ClientState
      auto it = client_state_result.first;
      if (on_worker_thread_) { // may be false in unit tests
        it->second.disconnect_callback.owner = this;
        Worker* worker = Worker::onThisThread();
        int rv = worker->sender().registerOnSocketClosed(
            Address(client_id), it->second.disconnect_callback);
        ld_check(rv == 0);
        // Since this is a new client, let's send it our view of the rebuilding
        // set.
        sendShardStatusToClient(client_id);
      }
    }

    if (on_worker_thread_) {
      // initialize the iterator cache
      deref(insert_result.first).iterator_cache_ =
          std::make_shared<IteratorCache>(
              &processor_->sharded_storage_thread_pool_->getByIndex(shard)
                   .getLocalLogStore(),
              log_id,
              false /* created_for_rebuilding */);
    }
  } else {
    deref(insert_result.first).log_group_path_ = log_group_path;
  }

  return std::make_pair(&deref(insert_result.first), insert_result.second);
}

ServerReadStream* AllServerReadStreams::get(ClientID client_id,
                                            logid_t log_id,
                                            read_stream_id_t read_stream_id,
                                            shard_index_t shard) {
  auto key = boost::make_tuple(log_id, client_id, read_stream_id, shard);
  const auto& index = streams_.get<FullKeyIndex>();
  auto it = index.find(key);
  return it != index.end() ? &deref(it) : nullptr;
}

void AllServerReadStreams::erase(ClientID client_id,
                                 logid_t log_id,
                                 read_stream_id_t read_stream_id,
                                 shard_index_t shard) {
  auto& index = streams_.get<FullKeyIndex>();
  auto key = boost::make_tuple(log_id, client_id, read_stream_id, shard);
  auto it = index.find(key);
  if (it != index.end()) {
    index.erase(it);
    int rv = updateSubscription(log_id, shard);
    // If insertOrGet() had succeeded, there is a LogStorageStateMap entry for
    // this log and there is no reason to fail here.
    ld_check(rv == 0);
  }
}

void AllServerReadStreams::eraseAllForClient(ClientID client_id) {
  auto& client_index = streams_.get<ClientIndex>();

  // First collect all logs that the client is subscribed to
  std::vector<std::pair<logid_t, shard_index_t>> erased;
  auto range = client_index.equal_range(client_id);
  for (auto it = range.first; it != range.second; ++it) {
    erased.push_back(std::make_pair(it->log_id_, it->shard_));
  }

  // Destroy the ServerReadStream instances
  client_index.erase(client_id);

  // Destroy the CatchupQueue
  client_states_.erase(client_id);

  // Update subscriptions
  for (const auto& e : erased) {
    int rv = updateSubscription(e.first, e.second);
    // If insertOrGet() had succeeded, there is a LogStorageStateMap entry for
    // this log and there is no reason to fail here.
    ld_check(rv == 0);
  }
}

void AllServerReadStreams::notifyNeedsCatchup(ServerReadStream& stream,
                                              bool allow_delay) {
  if (canScheduleForCatchup(stream)) {
    scheduleForCatchup(stream, allow_delay);
  }
}

bool AllServerReadStreams::canScheduleForCatchup(ServerReadStream& stream) {
  if (stream.storage_task_in_flight_) {
    ld_check(stream.isCatchingUp());
    // There is a storage task in flight for this stream.
    // Bump the version to avoid race conditions with storage threads.  See
    // docblock in ServerReadStream.
    ++stream.version_.val_;
  }

  // We can schedule the stream for catchup if the stream is not already
  // catching up and has not reached the client provided window or needs
  // to generate a STARTED response. Streams stalled because they have
  // reached the end of the current window will be rescheduled in
  // response to a window update from the client.
  return !stream.isCatchingUp() &&
      (!stream.isPastWindow() || stream.needs_started_message_);
}

void AllServerReadStreams::scheduleForCatchup(
    ServerReadStream& stream,
    bool allow_delay,
    CatchupEventTrigger catchup_reason) {
  // This should be called after insertOrGet() so there should be an
  // entry in client_states_.
  auto it = client_states_.find(stream.client_id_);
  ld_check(it != client_states_.end());

  std::unique_ptr<CatchupQueue>& queue_ptr = it->second.catchup_queue;

  // If we don't already have one, create a new CatchupQueue instance for
  // this client.
  if (!queue_ptr) {
    // tests set it->second.catchup_queue directly, so it's safe to assume this
    // will always run on a worker thread
    ld_check(on_worker_thread_);

    auto deps =
        std::make_unique<CatchupQueueDependencies>(this, Worker::stats());

    queue_ptr.reset(new CatchupQueue(std::move(deps), stream.client_id_));
  }

  // Add this read stream to the CatchupQueue. If we got here because more
  // records became available for delivery (i.e. released), do a delayed push
  // to allow CatchupQueue to process the stream when it's ready to read new
  // records.
  CatchupQueue::PushMode mode = allow_delay ? CatchupQueue::PushMode::DELAYED
                                            : CatchupQueue::PushMode::IMMEDIATE;

  queue_ptr->add(stream, mode);
  queue_ptr->pushRecords(catchup_reason);
}

void AllServerReadStreams::invalidateIterators(ClientID client_id) {
  auto range = streams_.get<ClientIndex>().equal_range(client_id);
  auto now = std::chrono::steady_clock::now();
  auto ttl = Worker::settings().iterator_cache_ttl;

  for (auto it = range.first; it != range.second; ++it) {
    ld_check(it->iterator_cache_ && "IteratorCache not set");

    it->iterator_cache_->invalidateIfUnused(now, ttl);
  }
}

void AllServerReadStreams::onGapSent(ClientID client_id,
                                     const GAP_Message& msg,
                                     const SteadyTimestamp enqueue_time) {
  auto it = client_states_.find(client_id);
  if (it != client_states_.end()) {
    ld_check(it->second.catchup_queue);
    auto* stream = get(client_id,
                       msg.header_.log_id,
                       msg.header_.read_stream_id,
                       msg.header_.shard);
    it->second.catchup_queue->onGapSent(msg, stream, enqueue_time);
  } else {
    // Client disconnected, nothing to do.
  }
}

void AllServerReadStreams::onRecordSent(ClientID client_id,
                                        const RECORD_Message& msg,
                                        const SteadyTimestamp enqueue_time) {
  auto it = client_states_.find(client_id);
  if (it != client_states_.end()) {
    ld_check(it->second.catchup_queue);
    auto* stream = get(client_id,
                       msg.header_.log_id,
                       msg.header_.read_stream_id,
                       msg.header_.shard);
    it->second.catchup_queue->onRecordSent(msg, stream, enqueue_time);
  } else {
    // Client disconnected, nothing to do.
  }
}

void AllServerReadStreams::onStartedSent(ClientID client_id,
                                         const STARTED_Message& msg,
                                         const SteadyTimestamp enqueue_time) {
  auto it = client_states_.find(client_id);
  if (it != client_states_.end()) {
    ld_check(it->second.catchup_queue);
    auto* stream = get(client_id,
                       msg.header_.log_id,
                       msg.header_.read_stream_id,
                       msg.header_.shard);
    it->second.catchup_queue->onStartedSent(msg, stream, enqueue_time);
  } else {
    // Client disconnected, nothing to do.
  }
}

void AllServerReadStreams::onEpochOffsetTask(EpochOffsetStorageTask& task) {
  if (!task.stream_) {
    // The ServerReadStreams was erased while the storage task was in flight.
    return;
  }

  ServerReadStream* stream = task.stream_.get();
  ld_check(stream);
  // only one in flight task allowed.
  ld_check(stream->epoch_task_in_flight == true);
  stream->epoch_task_in_flight = false;

  if (task.status_ == E::OK) {
    task.stream_.get()->epoch_offsets_ =
        std::make_pair(task.epoch_, task.result_offsets_);
  } else {
    ld_error("Got error while executing EpochOffsetStorageTask for epoch %u "
             "in log %ld with status=%s",
             task.epoch_.val_,
             task.log_id_.val_,
             error_description(task.status_));
  }
}

void AllServerReadStreams::onReadTaskDone(ReadStorageTask& task) {
  ld_check(read_storage_tasks_in_flight_ > 0);
  read_storage_tasks_in_flight_--;
  if (task.catchup_queue_) {
    task.catchup_queue_->onReadTaskDone(task);
  } else {
    // Client disconnected.
  }
  task.releaseRecords();
  sendDelayedReadStorageTasks();
}

void AllServerReadStreams::onReadTaskDropped(ReadStorageTask& task) {
  ld_check(read_storage_tasks_in_flight_ > 0);
  read_storage_tasks_in_flight_--;
  if (task.catchup_queue_) {
    task.catchup_queue_->onStorageTaskDropped(task.stream_.get());
  } else {
    // Client disconnected.
  }
  task.releaseRecords();
  scheduleSendDelayedStorageTasks();
}

void AllServerReadStreams::scheduleSendDelayedStorageTasks() {
  if (!send_delayed_storage_tasks_timer_.isAssigned()) {
    send_delayed_storage_tasks_timer_.assign(
        [this] { sendDelayedReadStorageTasks(); });
  }
  send_delayed_storage_tasks_timer_.activate(std::chrono::microseconds(0));
}

Message::Disposition
AllServerReadStreams::onWindowMessage(WINDOW_Message* msg,
                                      const Address& from) {
  if (!from.isClientAddress()) {
    ld_error("got WINDOW message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ServerWorker* w = ServerWorker::onThisThread();

  if (!w->isAcceptingWork()) {
    ld_debug("Ignoring WINDOW message: not accepting more work");
    return Message::Disposition::NORMAL;
  }

  if (!w->processor_->runningOnStorageNode()) {
    // This should never happen.  The client only sends WINDOW messages to
    // storage nodes that sent an OK reply to the START message, which we
    // would not have done because we are not a storage node.
    ld_error("got WINDOW message from client %s but not a storage node",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  const WINDOW_Header& header = msg->header_;

  // TODO validate log ID and send back error

  // The size of the sliding window should be positive.
  if (header.sliding_window.high < header.sliding_window.low) {
    ld_warning("Client %s sent a malformed WINDOW message: "
               "log_id %lu, read_stream_id %lu "
               "sliding_window.high %lu, "
               "sliding_window.low %lu. "
               "Ignoring.",
               Sender::describeConnection(from).c_str(),
               header.log_id.val_,
               uint64_t(header.read_stream_id),
               header.sliding_window.high,
               header.sliding_window.low);
    return Message::Disposition::NORMAL;
  }

  ld_spew("Client %s updated window for log %lu (rsid %ld) to [%s, %s]",
          Sender::describeConnection(from).c_str(),
          header.log_id.val_,
          header.read_stream_id.val_,
          lsn_to_string(header.sliding_window.low).c_str(),
          lsn_to_string(header.sliding_window.high).c_str());

  w->serverReadStreams().onWindowMessage(from.asClientID(), header);
  return Message::Disposition::NORMAL;
}

void AllServerReadStreams::onWindowMessage(ClientID from,
                                           const WINDOW_Header& msg_header) {
  shard_index_t shard = msg_header.shard;
  ServerReadStream* stream =
      this->get(from, msg_header.log_id, msg_header.read_stream_id, shard);

  if (stream == nullptr) {
    // Stream does not exist, likely reached the end and got deleted.
    return;
  }

  // No-op WINDOW messages are normal.  The client sends a WINDOW message
  // immediately after completing the handshake.  The message often has the
  // same window as the START message had had.
  if (msg_header.sliding_window.high == stream->getWindowHigh()) {
    return;
  }

  // WINDOW message should not slide the window backward (by set window_high_
  // to a smaller value. If that happens, we ignore that WINDOW message and
  // log a warning message.
  if (msg_header.sliding_window.high < stream->getWindowHigh()) {
    ld_warning("Client %s tried to send a WINDOW message without "
               "sliding window forward. "
               "log_id %" PRIu64 ", read_stream_id %" PRIu64
               ", WINDOW message sliding_window.high %" PRIu64
               ", ServerReadStream window_high_ %" PRIu64 ". "
               "Ignoring the WINDOW message.",
               from.toString().c_str(),
               msg_header.log_id.val_,
               uint64_t(msg_header.read_stream_id),
               msg_header.sliding_window.high,
               stream->getWindowHigh());
    return;
  }

  // WINDOW message should not set window_high_ beyond
  // until_lsn. If that happends, we ignore that WINDOW message and log
  // a warning message.
  if (msg_header.sliding_window.high > stream->until_lsn_) {
    ld_warning("Client %s tried to set window_high_ beyond until_lsn. "
               "log_id %" PRIu64 ", read_stream_id %" PRIu64
               ", WINDOW message sliding_window.high %" PRIu64
               ", ServerReadStream until_lsn_ %s. "
               "Ignoring the WINDOW message.",
               from.toString().c_str(),
               msg_header.log_id.val_,
               uint64_t(msg_header.read_stream_id),
               msg_header.sliding_window.high,
               lsn_to_string(stream->until_lsn_).c_str());
    return;
  }

  // Update flow-control window and fast-forward lagging storage nodes
  if (stream->getReadPtr().lsn < msg_header.sliding_window.low) {
    stream->setReadPtr(msg_header.sliding_window.low);
  }

  stream->setWindowHigh(msg_header.sliding_window.high);
  if (stream->isPastWindow()) {
    // read_ptr_ is already past the new window high, this means we already
    // sent a gap to the client with higher bound >= window high. We do not
    // need to notify the catchup queue.
    return;
  }

  if (stream->rebuilding_) {
    // We will wake up this stream once the shard finishes rebuilding.
    return;
  }

  this->notifyNeedsCatchup(*stream, /* allow_delay */ false);
}

void AllServerReadStreams::onShardRebuilt(uint32_t shard) {
  // Lambda that returns true if the given stream is currently stalled
  // because it is waiting for rebuilding of the shard to complete.
  auto unstall_stream = [&](ServerReadStream& stream) {
    if (!stream.rebuilding_ || stream.shard_ != shard) {
      return false;
    }
    stream.rebuilding_ = false;
    return true;
  };

  catchupIf(streams_.begin(), streams_.end(), unstall_stream);
}

void AllServerReadStreams::onRelease(RecordID rid,
                                     shard_index_t shard,
                                     bool force) {
  const lsn_t lsn = compose_lsn(rid.epoch, rid.esn);

  auto should_catchup = [&](ServerReadStream& stream) {
    // It is possible (even likely) that, for a log that is being written to,
    // this record was already delivered when a previous record was released.
    // Example sequence of events:
    // (1) Record 101 is stored.
    // (2) Record 102 is stored.
    // (3) RELEASE message comes in for record 101.  LogStorageStateMap is
    //     updated and a ReleaseRequest is posted to the worker.
    // (4) RELEASE message comes in for record 102.  LogStorageStateMap is
    //     updated and a ReleaseRequest is posted to the worker.
    // (5) The ReleaseRequest with LSN 101 (from (3)) is processed by the
    //     worker serving the reader.  Because the LogStorageStateMap already
    //     reflects that LSN 102 is released, both records are immediately
    //     shipped to the client.
    // (6) The ReleaseRequest with LSN 102 (from (4)) is processed.
    //
    // This check ensures that in step (6) we avoid reading from the log store
    // since we know we'd already delivered the record.
    return force || lsn >= stream.getReadPtr().lsn;
  };

  auto key = boost::make_tuple(rid.logid, shard);
  auto range = streams_.get<LogShardIndex>().equal_range(key);
  catchupIf(
      range.first, range.second, should_catchup, CatchupEventTrigger::RELEASE);
}

int AllServerReadStreams::updateSubscription(logid_t log_id,
                                             shard_index_t shard) {
  LogStorageState* log_state =
      log_storage_state_map_->insertOrGet(log_id, shard);
  if (log_state == nullptr) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to subscribe to log %" PRIu64 " because "
                    "LogStorageStateMap of shard %u is full",
                    log_id.val_,
                    shard);
    return -1;
  }

  auto key = boost::make_tuple(log_id, shard);
  const auto& log_index = streams_.get<LogShardIndex>();

  // Depending on whether there are still any active streams for this log,
  // subscribe to or unsubscribe from RELEASE messages for the log.
  if (log_index.find(key) != log_index.end()) {
    log_state->subscribeWorker(worker_id_);
  } else {
    log_state->unsubscribeWorker(worker_id_);
  }
  return 0;
}

void AllServerReadStreams::getCatchupQueuesDebugInfo(
    InfoCatchupQueuesTable& table) {
  for (auto& c : client_states_) {
    if (c.second.catchup_queue) {
      c.second.catchup_queue->getDebugInfo(table);
    }
  }
}

void AllServerReadStreams::getReadStreamsDebugInfo(
    ClientID client_id,
    InfoReadersTable& table) const {
  const auto& client_index = streams_.get<ClientIndex>();
  auto range = client_index.equal_range(client_id);
  std::string out;
  for (auto it = range.first; it != range.second; ++it) {
    it->getDebugInfo(table);
  }
}

void AllServerReadStreams::getReadStreamsDebugInfo(
    logid_t log_id,
    InfoReadersTable& table) const {
  const auto& log_index = streams_.get<LogIndex>();
  auto range = log_index.equal_range(log_id);
  std::string out;
  for (auto it = range.first; it != range.second; ++it) {
    it->getDebugInfo(table);
  }
}

void AllServerReadStreams::getReadStreamsDebugInfo(
    InfoReadersTable& table) const {
  for (const ServerReadStream& stream : streams_) {
    stream.getDebugInfo(table);
  }
}

void AllServerReadStreams::blockUnblockClient(ClientID cid, bool block) {
  auto it = client_states_.find(cid);
  if (it == client_states_.end()) {
    return;
  }
  if (!it->second.catchup_queue) {
    return;
  }

  it->second.catchup_queue->blockUnBlock(block);
}

void AllServerReadStreams::ClientDisconnectedCallback::
operator()(Status /*st*/, const Address& name) {
  ld_check(name.isClientAddress());
  ld_check(owner != nullptr);
  owner->eraseAllForClient(name.id_.client_);
}

bool AllServerReadStreams::tryAcquireMemoryForTask(
    std::unique_ptr<ReadStorageTask>& task) {
  ld_check(task);
  // Acquire a big chunk of memory from the memory budget. We don't want to
  // enqueue the task if there is not enough memory for it to read a whole
  // batch.
  // ReadStorageTask will release memory for what it did not read when it
  // executes.
  const uint64_t mem = task->read_ctx_.max_bytes_to_deliver_;
  ResourceBudget::Token token = memory_budget_.acquireToken(mem);
  if (!token.valid()) {
    return false;
  }
  task->setMemoryToken(std::move(token));
  return true;
}

void AllServerReadStreams::sendDelayedReadStorageTasks() {
  while (!delayed_read_storage_tasks_.empty()) {
    auto& queued = delayed_read_storage_tasks_.front();
    if (!tryAcquireMemoryForTask(queued.task)) {
      // Still not enough memory available. We'll try again next time a storage
      // task comes back.
      ld_check(read_storage_tasks_in_flight_ > 0);
      return;
    }
    read_storage_tasks_in_flight_++;
    sendStorageTask(std::move(queued.task), queued.shard);
    delayed_read_storage_tasks_.pop();
    STAT_DECR(stats_, read_storage_tasks_delayed);
  }
}

void AllServerReadStreams::putStorageTask(
    std::unique_ptr<ReadStorageTask>&& task,
    shard_index_t shard) {
  ld_check(task);
  if (!delayed_read_storage_tasks_.empty() || !tryAcquireMemoryForTask(task)) {
    // We cannot send this task immediately because there are already delayed
    // tasks or there is not enough memory left.
    // Enqueue it in `delayed_read_storage_tasks_`.  We will try to send tasks
    // in that queue each time another task already in flight comes back.
    delayed_read_storage_tasks_.push(QueuedTask{std::move(task), shard});
    STAT_INCR(stats_, read_storage_tasks_delayed);
  } else {
    read_storage_tasks_in_flight_++;
    sendStorageTask(std::move(task), shard);
  }
}

void AllServerReadStreams::sendStorageTask(
    std::unique_ptr<ReadStorageTask>&& task,
    shard_index_t shard) {
  ServerWorker* worker = ServerWorker::onThisThread();
  ld_check(worker);
  auto task_queue = worker->getStorageTaskQueueForShard(shard);
  task_queue->putTask(std::move(task));
}

ResourceBudget& AllServerReadStreams::getMemoryBudget() {
  return memory_budget_;
}

void AllServerReadStreams::appendReleasedRecords(
    std::unique_ptr<ReleasedRecords> records) {
  bool over_mem_budget =
      real_time_record_buffer_.appendReleasedRecords(std::move(records));
  if (over_mem_budget) {
    // TODO: If allocating & enqueing the request here is a performance
    // bottleneck, we could do it outside of EpochRecordCache's rw_lock_.  Would
    // require some ugly plumbing, e.g. having
    // RecordCacheDisposal::onRecordsReleased() returning a bitset of workers to
    // follow up on.
    std::unique_ptr<Request> request = std::make_unique<EvictRealTimeRequest>();
    processor_->postRequest(request, WorkerType::GENERAL, worker_id_.val());
  }
}

void AllServerReadStreams::evictRealTime() {
  while (real_time_record_buffer_.overEvictionThreshold()) {
    distributeNewlyReleasedRecords();

    evictRealTimeLog(real_time_record_buffer_.toEvict());
  }
}

void AllServerReadStreams::evictRealTimeLog(logid_t logid) {
  ld_check(logid != LOGID_INVALID);
  ld_check(logid != LOGID_INVALID2);

  auto range = streams_.get<LogIndex>().equal_range(logid);
  // Items can only ever come off on this thread, so nothing should have
  // removed entries / read streams between the "toEvict()' call above and the
  // "equal_range' call.
  ld_check(range.first != range.second);
  for (auto it = range.first; it != range.second; ++it) {
    auto recs = deref(it).giveReleasedRecords();
    STAT_ADD(stats_, real_time_record_buffer_eviction, recs.size());
    // The shared_ptr to ReleasedRecords will be destroyed here.
  }
}

void AllServerReadStreams::onShardStatusChanged() {
  for (const auto& client_state : client_states_) {
    sendShardStatusToClient(client_state.first);
  }
}

void AllServerReadStreams::sendShardStatusToClient(ClientID cid) {
  if (!on_worker_thread_) { // may be false in unit tests
    return;
  }

  Worker* worker = Worker::onThisThread();

  if (worker->sender().getNodeID(Address(cid)).isNodeID()) {
    // This client is another node in the tier. It is reading the event log and
    // thus does not need us to send an update.
    return;
  }

  SHARD_STATUS_UPDATE_Header hdr;
  auto shard_status_map =
      worker->shardStatusManager().getShardAuthoritativeStatusMap();
  hdr.version = shard_status_map.getVersion();

  // TODO(T15517759): setting num_shards_deprecated can be removed once all
  // clients have the code that understands the "num_shards" property in the
  // node configuration. Leaving this here for backward compatibility until that
  // code is deployed everywhere.
  auto my_node_id = Worker::onThisThread()->processor_->getMyNodeID();
  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();

  auto storage_config =
      nodes_configuration->getNodeStorageAttribute(my_node_id.index());
  ld_check(storage_config);
  hdr.num_shards_deprecated = storage_config->num_shards;

  auto msg =
      std::make_unique<SHARD_STATUS_UPDATE_Message>(hdr, shard_status_map);

  const int rv = worker->sender().sendMessage(std::move(msg), cid);
  if (rv == 0) {
    return;
  }

  switch (err) {
    case E::PROTONOSUPPORT:
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          1,
          "Cannot send SHARD_STATUS_UPDATE_Message to %s because this "
          "client uses an old protocol version.",
          cid.toString().c_str());
      break;
    case E::SHUTDOWN:
      break;
    case E::NOBUFS:
      scheduleShardStatusUpdateRetry(cid);
      break;
    case E::NOTCONN:
    case E::UNREACHABLE:
      // could happen if client was disconnected in the middle of START
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          1,
          "Cannot send SHARD_STATUS_UPDATE_Message to %s because this "
          "client is not connected.",
          cid.toString().c_str());
      break;
    default:
      // None of the other error conditions are expected or recoverable since
      // the target is a ClientID:
      // - some of the codes apply only when the target is a NodeID (ours is a
      //   ClientID);
      // - the rest are unrecoverable.
      ld_error("Got unexpected error from Sender::sendMessage(): %s",
               error_description(err));
      ld_check(false);
  }
}

void AllServerReadStreams::scheduleShardStatusUpdateRetry(ClientID cid) {
  // We should not be here in tests.
  ld_check(on_worker_thread_);

  auto it = client_states_.find(cid);
  ld_check(it != client_states_.end());

  if (!it->second.timer_) {
    auto timer = std::make_unique<ExponentialBackoffTimer>(
        [this, cid]() { sendShardStatusToClient(cid); },
        std::chrono::milliseconds(1),
        std::chrono::seconds(10));
    it->second.timer_ = std::move(timer);
  }

  it->second.timer_->activate();
}

void AllServerReadStreams::onShardStatusUpdateMessageSent(ClientID cid,
                                                          Status st) {
  // We should not be here in tests.
  ld_check(on_worker_thread_);

  if (st == E::PROTONOSUPPORT) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "Cannot send SHARD_STATUS_UPDATE_Message to %s because this "
        "client uses an old protocol version.",
        cid.toString().c_str());
    return;
  }

  auto it = client_states_.find(cid);
  if (it == client_states_.end()) {
    return;
  }

  if (st == E::OK) {
    if (it->second.timer_) {
      // If we had been retrying, resets the delay.
      it->second.timer_->reset();
    }
  } else if (st == E::NOBUFS) {
    scheduleShardStatusUpdateRetry(cid);
  } else {
    // Other errors suggest the socket is gone so no point in scheduling a
    // retry.
  }
}

void AllServerReadStreams::distributeNewlyReleasedRecords() {
  real_time_record_buffer_.sweep(
      [this](std::unique_ptr<ReleasedRecords> records) {
        records->buffer_ = &real_time_record_buffer_;
        real_time_record_buffer_.addToLRU(records->logid_);
        std::shared_ptr<ReleasedRecords> ptr{records.release()};
        auto range = streams_.get<LogIndex>().equal_range(ptr->logid_);
        for (auto it = range.first; it != range.second; ++it) {
          deref(it).addReleasedRecords(ptr);
        }
      });
}

Request::Execution EvictRealTimeRequest::execute() {
  ServerWorker::onThisThread()->serverReadStreams().evictRealTime();
  return Request::Execution::COMPLETE;
}
}} // namespace facebook::logdevice
