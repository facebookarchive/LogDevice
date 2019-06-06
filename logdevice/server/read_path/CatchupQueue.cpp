/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/CatchupQueue.h"

#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/CatchupOneStream.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/read_path/ServerReadStream.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

#define catchup_queue_ld_debug(_name_, ...) \
  ld_debug(                                 \
      _name_ " - client_id=%s", ##__VA_ARGS__, client_id_.toString().c_str());

namespace facebook { namespace logdevice {

CatchupQueueDependencies::CatchupQueueDependencies(
    AllServerReadStreams* all_server_read_streams,
    StatsHolder* stats_holder)
    : sender_(std::make_unique<SenderProxy>()),
      all_server_read_streams_(all_server_read_streams),
      stats_holder_(stats_holder) {}

CatchupQueueDependencies::~CatchupQueueDependencies() = default;

CatchupQueue::CatchupQueue(std::unique_ptr<CatchupQueueDependencies>&& deps,
                           ClientID client_id)
    : client_id_(client_id),
      ref_holder_(this),
      resume_cb_(this),
      deps_(std::move(deps)),
      ping_timer_(deps_->createPingTimer([this]() { pushRecords(); })),
      iterator_invalidation_timer_(deps_->createIteratorTimer(nullptr)) {
  iterator_invalidation_timer_->setCallback(
      [this, timer = iterator_invalidation_timer_.get()] {
        deps_->invalidateIterators(client_id_);
        // Keep the timer active until this CatchupQueue is destroyed.  We
        // refetch the TTL so that changes to the TTL setting apply at
        // runtime.
        timer->activate(deps_->iteratorTimerTTL());
      });
  iterator_invalidation_timer_->activate(deps_->iteratorTimerTTL());
}

CatchupQueue::~CatchupQueue() = default;

void CatchupQueue::add(ServerReadStream& stream, CatchupQueue::PushMode mode) {
  using std::chrono::steady_clock;
  stream.last_enqueued_time_ = steady_clock::now();

  ld_check(!stream.isCatchingUp());

  switch (mode) {
    case PushMode::DELAYED: {
      auto latency_optional = deps_->getDeliveryLatency(stream.log_id_);
      if (latency_optional.hasValue() && latency_optional.value().count() > 0) {
        // put in the separate queue so pushRecords() skips it until it's ready
        // to read more
        auto delivery_latency = latency_optional.value();
        steady_clock::time_point next_read_time =
            stream.last_enqueued_time_ + delivery_latency;

        // Round next_read_time down to a multiple of delivery_latency. This
        // way, CatchupQueue will process all streams that were added in the
        // same delivery-latency-wide time window. We get better batching in
        // the communication layer as records for more streams will be
        // transmitted together, leading to better performance.
        // Note that by rounding down (and therefore grouping read streams),
        // the expected latency to deliver a newly released record becomes
        // 0.5 * delivery_latency.

        auto t = std::chrono::duration_cast<std::chrono::milliseconds>(
                     next_read_time.time_since_epoch())
                     .count();

        stream.next_read_time_ =
            steady_clock::time_point(std::chrono::milliseconds(
                t / delivery_latency.count() * delivery_latency.count()));

        stream_ld_debug(stream, "Enqueue stream in DELAYED mode");
        auto pair = queue_delayed_.insert(stream);
        ld_check(pair.second);
        STAT_INCR(deps_->getStatsHolder(), catchup_queue_push_delayed);
        break;
      } else {
        // delivery latency not set, fall-through and add to queue_ directly
      }
    }
    case PushMode::IMMEDIATE:
      stream_ld_debug(stream, "Enqueue stream in IMMEDIATE mode");
      queue_.push_back(stream);
      STAT_INCR(deps_->getStatsHolder(), catchup_queue_push_immediate);
      break;
  }

  ld_check(stream.isCatchingUp());
  // ReadIoShapingCallback holds a weak reference to CatchupQueue
  stream.addCQRef(ref_holder_.ref());
  stream.adjustStatWhenCatchingUpChanged();
}

void CatchupQueue::processDelayedQueue() {
  if (queue_delayed_.empty()) {
    return;
  }

  auto now = std::chrono::steady_clock::now();
  while (!queue_delayed_.empty()) {
    // Note that if a stream had just been added to queue_ before pushRecords()
    // was called, it will be processed before any streams already queued in
    // queue_delayed_; it's probably not worth adding extra complexity to handle
    // this special case.
    auto stream = queue_delayed_.begin();
    if (stream->next_read_time_ > now) {
      break;
    }

    stream_ld_debug(*stream, "Stream with artificial latency is ready");
    queue_.push_back(*stream);
    queue_delayed_.erase(stream);
    ld_check(stream->isCatchingUp());
  }
}

void CatchupQueue::pushRecords(CatchupEventTrigger catchup_reason) {
  catchup_queue_ld_debug("Pushing records");
  // The current implementation goes through active read streams for the
  // client in round-robin fashion and tries to push a batch of records from
  // each into the socket.  This proceeds until we hit a limit on the number
  // of RECORD bytes we are willing to buffer in the output evbuffer.
  //
  // When the socket fills up and there is no room for the next record in
  // line, we yield until there is enough space for it in a subsequent call.
  // This way, a large record won't get blocked by many read streams
  // consisting of small records.

  if (blocked_) {
    catchup_queue_ld_debug("This catchup queue has been blocked by the `block "
                           "catchup_queues` admin command.");
    return;
  }

  if (resume_cb_.active()) {
    catchup_queue_ld_debug("Waiting for bandwidth");
    return;
  }

  processDelayedQueue();

  if (queue_.empty()) {
    // use ping timer to make sure that we eventually process read streams in
    // queue_delayed_
    adjustPingTimer();

    // Nothing else to do
    return;
  }

  size_t max_record_bytes_queued = deps_->getMaxRecordBytesQueued(client_id_);

  // We limit the number of iterations in that loop in order to yield in the
  // extremely unlikely case where all batches we read keep returning zero or a
  // very small amount of records because most records are filtered. We don't
  // expect to ever hit this because nonblocking reads cannot go on
  // indefinitely.
  static constexpr size_t max_iterations = 30;

  auto next_from_queue = queue_.begin();
  size_t storage_task_count = 0;
  for (size_t i = 0; i < max_iterations && next_from_queue != queue_.end() &&
       record_bytes_queued_ < max_record_bytes_queued;
       ++i) {
    auto stream = next_from_queue;
    ++next_from_queue;

    if (stream->storage_task_in_flight_) {
      // If this stream has a storage task in flight, the catchup queue should
      // also be aware of it.
      if (!storage_task_in_flight_) {
        ld_critical("Although stream %" PRIu64 " of client %s for log:%lu has "
                    "a storage task in flight, the catchup queue doesn't know "
                    "it.",
                    stream->id_.val(),
                    Sender::describeConnection(client_id_).c_str(),
                    stream->log_id_.val());
        ld_check(storage_task_in_flight_);
      }
      storage_task_count++;
      continue;
    }

    const logid_t log_id = stream->log_id_;
    const read_stream_id_t read_stream_id = stream->id_;

    // If traffic shaping is throttling transmissions to this client,
    // there's no value in reading data just to have the message for
    // that data denied.
    if (!deps_->sender_->canSendTo(
            client_id_, stream->trafficClass(), resume_cb_)) {
      // We're most likely waiting for bandwidth. If we aren't, treat
      // it as a transient failure.
      if (err != E::CBREGISTERED) {
        // Ping timer will trigger a retry for this stream.
        STAT_INCR(deps_->getStatsHolder(), read_streams_transient_errors);
      }
      break;
    }

    // Compute how long the read stream has been waiting in the queue, for
    // stats.
    using std::chrono::steady_clock;
    stream->last_batch_started_time_ = steady_clock::now();
    stream->last_batch_status_ = "(not assigned)";
    uint64_t t =
        std::chrono::duration_cast<std::chrono::microseconds>(
            stream->last_batch_started_time_ - stream->last_enqueued_time_)
            .count();
    STAT_ADD(deps_->getStatsHolder(), read_streams_batch_queue_microsec, t);

    ld_check_lt(record_bytes_queued_, max_record_bytes_queued);

    CatchupOneStream::Action act;
    size_t n_bytes_queued;
    bool try_non_blocking_read =
        try_non_blocking_read_ && deps_->getSettings().allow_reads_on_workers;
    std::tie(act, n_bytes_queued) =
        CatchupOneStream::read(*deps_,
                               &*stream,
                               ref_holder_.ref(),
                               try_non_blocking_read,
                               max_record_bytes_queued - record_bytes_queued_,
                               record_bytes_queued_ == 0,
                               !storage_task_in_flight_,
                               catchup_reason);
    record_bytes_queued_ += n_bytes_queued;

    // Note: storage_task_in_flight_ is NOT updated in the above call to
    // CatchupOneStream::read(), but stream->storage_task_in_flight_ is.  Also,
    // storage_task_in_flight_ is true if ANY stream managed by this CatcupQueue
    // has a task in flight.
    if (storage_task_in_flight_) {
      if (n_bytes_queued > 0) {
        STAT_ADD(deps_->getStatsHolder(),
                 bytes_queued_during_storage_task,
                 n_bytes_queued);
      }
    }

    // The stream should have a storage task in flight iff we returned a
    // WAIT_FOR_*.
    if (act == CatchupOneStream::Action::WAIT_FOR_STORAGE_TASK ||
        act == CatchupOneStream::Action::WAIT_FOR_LNG) {
      ld_check(stream->storage_task_in_flight_);
      ld_check(!storage_task_in_flight_);
      storage_task_in_flight_ = true;
      storage_task_count++;
    } else {
      ld_check(!stream->storage_task_in_flight_);
    }

    if (act != CatchupOneStream::Action::WAIT_FOR_STORAGE_TASK) {
      onBatchComplete(&*stream);
    } else {
      // onReadTaskDone() will call onBatchComplete().
    }

    stream_ld_debug(*stream,
                    "Read on worker thread completes with %s",
                    CatchupOneStream::action_names[act].c_str());

    if (act == CatchupOneStream::Action::TRANSIENT_ERROR) {
      // Ping timer will trigger a retry for this stream.
      STAT_INCR(deps_->getStatsHolder(), read_streams_transient_errors);
      break;
    } else if (act == CatchupOneStream::Action::WAIT_FOR_BANDWIDTH) {
      // We ran out of bandwidth trying to send out a record to the client.
      // Wait for bandwidth available callback to fire.
      break;
    } else if (act == CatchupOneStream::Action::WAIT_FOR_READ_BANDWIDTH) {
      // We ran out of configured I/O bandwidth while attempting a
      // read storage task.
      ld_check(err == E::CBREGISTERED);
      break;
    } else if (act == CatchupOneStream::Action::WAIT_FOR_STORAGE_TASK) {
      // We will be woken up when the storage task that was posted comes back.
    } else if (act == CatchupOneStream::Action::WAIT_FOR_LNG) {
      // We need the last known good of the epoch to make progress. The
      // ReadLngTask will call onReadLngTaskDone() when it's done.
    } else if (act == CatchupOneStream::Action::DEQUEUE_AND_CONTINUE) {
      // There are no more records to deliver right now. The stream may be added
      // back to the queue later when we determine there is more to send.
      queue_.erase(stream);
      ld_check(!stream->isCatchingUp());
      stream->adjustStatWhenCatchingUpChanged();
    } else if (act == CatchupOneStream::Action::ERASE_AND_CONTINUE) {
      // All done, or reached an unrecoverable error.  Erase the read stream.
      // This also removes the stream from the queue since we are using
      // folly::IntrusiveList.
      deps_->eraseStream(client_id_, log_id, read_stream_id, stream->shard_);
      stream = queue_.end();
    } else if (act == CatchupOneStream::Action::PERMANENT_ERROR) {
      if (notifyShardError(&*stream) == 0) {
        deps_->eraseStream(client_id_, log_id, read_stream_id, stream->shard_);
        stream = queue_.end();
      }
    } else if (act == CatchupOneStream::Action::WOULDBLOCK) {
      // We can only get here if we disallowed blocking I/O, which we only do
      // when we already have a storage task in flight.
      ld_check(storage_task_in_flight_);
      // To make progress on this stream, we'd have to do a blocking read.  So,
      // just continue to the next stream.
    } else {
      ld_check(act == CatchupOneStream::Action::REQUEUE_AND_DRAIN ||
               act == CatchupOneStream::Action::REQUEUE_AND_CONTINUE);
      // Move the stream to the end of the queue.
      queue_.erase(stream);
      queue_.push_back(*stream);
      ld_check(stream->isCatchingUp());

      if (act == CatchupOneStream::Action::REQUEUE_AND_DRAIN) {
        // Unlikely we'll be able to fit much more into the byte limit, wait
        // until already queued records drain (onRecordSent will wake us up).
        break;
      }
    }
  }

  if (storage_task_count > 1) {
    ld_critical("The catchup queue of client %s started more than one storage "
                "task.",
                Sender::describeConnection(client_id_).c_str());
    ld_check_le(storage_task_count, 1);
  }

  // Depending on the outcome of the above loop, under certain error
  // conditions we need to schedule a timer to try again later.  Or if we
  // recovered from such an error condition, now is the time to cancel the
  // timer.
  adjustPingTimer();
}

int CatchupQueue::notifyShardError(ServerReadStream* stream) {
  // Marking the permanent error state into the log storage state so that it is
  // remembered next time someone tries to read from that log.
  LogStorageState& log_state =
      deps_->getLogStorageStateMap().get(stream->log_id_, stream->shard_);
  log_state.notePermanentError("Catchup");

  STARTED_Header hdr;
  hdr.status = E::FAILED;
  hdr.log_id = stream->log_id_;
  hdr.read_stream_id = stream->id_;
  hdr.filter_version = stream->filter_version_;
  hdr.shard = stream->shard_;
  auto msg = std::make_unique<STARTED_Message>(hdr, stream->trafficClass());
  Worker* worker = Worker::onThisThread();

  ld_debug("Sending %s to %s for log:%lu, stream id:%lu",
           errorStrings()[hdr.status].name,
           stream->client_id_.toString().c_str(),
           hdr.log_id.val(),
           hdr.read_stream_id.val());

  const int rv =
      worker->sender().sendMessage(std::move(msg), stream->client_id_);
  if (rv != 0) {
    RATELIMIT_LEVEL(
        err == E::CBREGISTERED ? dbg::Level::SPEW : dbg::Level::ERROR,
        std::chrono::seconds(10),
        10,
        "Error in sending STARTED message for %s to client %s "
        "for log:%lu and read stream:%" PRIu64 ": %s",
        trafficClasses()[stream->trafficClass()].c_str(),
        Sender::describeConnection(stream->client_id_).c_str(),
        stream->log_id_.val_,
        stream->id_.val_,
        error_name(err));

    if (err != E::CBREGISTERED) {
      adjustPingTimer();
    }
    return -1;
  }

  return 0;
}

void CatchupQueue::onRecordSent(const RECORD_Message& msg,
                                ServerReadStream* stream,
                                const SteadyTimestamp enqueue_time) {
  const auto msg_size = msg.size();
  ld_check(record_bytes_queued_ >= msg_size);
  record_bytes_queued_ -= msg_size;
  ld_spew("record drained, record_bytes_queued_ = %zu", record_bytes_queued_);

  // Try to make progress on the queue after validations are completed.
  // We do this after all validations because pushRecords() can destroy
  // the stream.
  SCOPE_EXIT {
    if (record_bytes_queued_ == 0) {
      catchup_queue_ld_debug("Output evbuffer drained");
      // Only trigger more work when we have flushed all RECORD messages out of
      // the output evbuffer
      pushRecords();
    }
  };

  if (stream == nullptr) {
    // Stream has been reaped. Nothing to validate.
    return;
  }

  // Handle the case of a stream rewind after we hit until lsn and
  // destroyed the ServerReadStream object.
  if (enqueue_time < stream->created_) {
    if (!stream->sent_state.empty() && stream->sent_state.front().started) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_record_violations);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         1,
                         "Sent stale RECORD: RECORD(%s), Stream(%s)",
                         msg.identify().c_str(),
                         toString(*stream).c_str());
    }
    return;
  }

  if (stream->sent_state.empty()) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_record_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "No sending state: RECORD(%s), Stream(%s)",
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }

  auto& head_state = stream->sent_state.front();
  if (!head_state.started) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_record_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "RECORD shipped before STARTED: RECORD(%s), Stream(%s)",
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }

  if (msg.header_.lsn < head_state.min_next_lsn) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_record_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "Stream went backwards: "
                       "min_next_lsn(%s), RECORD(%s), Stream(%s)",
                       lsn_to_string(head_state.min_next_lsn).c_str(),
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }
  head_state.last_lsn = msg.header_.lsn;
  head_state.last_record_lsn = msg.header_.lsn;
  head_state.min_next_lsn = msg.header_.lsn + 1;
}

void CatchupQueue::onGapSent(const GAP_Message& msg,
                             ServerReadStream* stream,
                             const SteadyTimestamp enqueue_time) {
  if (stream == nullptr) {
    // Stream has been reaped. Nothing to validate.
    return;
  }

  // Handle the case of a stream rewind after we hit until_lsn and
  // destroyed the ServerReadStream object.
  if (enqueue_time < stream->created_) {
    if (!stream->sent_state.empty() && stream->sent_state.front().started) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_gap_violations);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         1,
                         "Sent stale GAP: GAP(%s), Stream(%s)",
                         msg.identify().c_str(),
                         toString(*stream).c_str());
    }
    return;
  }

  if (stream->sent_state.empty()) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_gap_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "No sending state: GAP(%s), Stream(%s)",
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }

  auto& head_state = stream->sent_state.front();
  if (!head_state.started) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_gap_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "GAP shipped before STARTED: GAP(%s), Stream(%s)",
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }

  if (msg.header_.start_lsn < head_state.min_next_lsn) {
    STAT_INCR(deps_->getStatsHolder(), read_stream_gap_violations);
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "Stream went backwards: "
                       "min_next_lsn(%s), GAP(%s), Stream(%s)",
                       toString(head_state.min_next_lsn).c_str(),
                       msg.identify().c_str(),
                       toString(*stream).c_str());
    return;
  }
  head_state.last_lsn = msg.header_.end_lsn;
  head_state.min_next_lsn = msg.header_.end_lsn + 1;
}

void CatchupQueue::onStartedSent(const STARTED_Message& msg,
                                 ServerReadStream* stream,
                                 const SteadyTimestamp enqueue_time) {
  if (msg.header_.status != E::OK && msg.header_.status != E::REBUILDING) {
    // Sent state is only recorded for successfully started streams
    // (including streams that will be started once rebuilding completes).
    return;
  }

  if (stream == nullptr) {
    // Stream has been reaped. Nothing to validate.
    return;
  }

  // Handle the case of a stream rewind after we hit until lsn and
  // destroyed the ServerReadStream object.
  if (enqueue_time < stream->created_) {
    if (!stream->sent_state.empty() && stream->sent_state.front().started) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_started_violations);
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Sent stale STARTED: STARTED(%s), Stream(%s)",
                      toString(msg).c_str(),
                      toString(*stream).c_str());
    }
    return;
  }

  // A sent state entry is added on every successful STARTED reply.
  // Because the client may send duplicate START messages, we need
  // to cull send state entries for the current filter version that have
  // been used.
  for (;;) {
    if (stream->sent_state.empty()) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_started_violations);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         1,
                         "No sending state: STARTED(%s), Stream(%s)",
                         toString(msg).c_str(),
                         toString(*stream).c_str());
      break;
    }

    auto& head_state = stream->sent_state.front();
    if (msg.header_.filter_version < head_state.filter_version) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_started_violations);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         1,
                         "Stream went backwards: "
                         "min_expected_fv(%s), STARTED(%s), Stream(%s)",
                         toString(head_state.filter_version).c_str(),
                         toString(msg).c_str(),
                         toString(*stream).c_str());
      break;
    }

    if (head_state.started) {
      ld_debug("Culling Read Stream: SentState(%s), STARTED(%s), Stream(%s)",
               toString(head_state).c_str(),
               toString(msg).c_str(),
               toString(*stream).c_str());
      // State for a previous message.
      stream->sent_state.pop_front();
      continue;
    }

    if (msg.header_.filter_version > head_state.filter_version) {
      STAT_INCR(deps_->getStatsHolder(), read_stream_started_violations);
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         1,
                         "No sending state: STARTED(%s), Stream(%s)",
                         toString(msg).c_str(),
                         toString(*stream).c_str());

      // State for a previous message.
      stream->sent_state.pop_front();
      continue;
    }
    head_state.min_next_lsn = head_state.start_lsn;
    head_state.started = true;
    break;
  }
}

void CatchupQueueDependencies::eraseStream(ClientID client_id,
                                           logid_t log_id,
                                           read_stream_id_t read_stream_id,
                                           shard_index_t shard) {
  all_server_read_streams_->erase(client_id, log_id, read_stream_id, shard);
}

bool CatchupQueueDependencies::canIssueReadIO(
    ReadIoShapingCallback& on_bw_avail,
    ServerReadStream* stream) {
  logid_t logid = stream->log_id_;
  Priority rp = stream->getReadPriority();

  if (on_bw_avail.active()) {
    err = E::CBREGISTERED;
    ld_spew(
        "log:%lu, on_bw_avail:%p was already active", logid.val_, &on_bw_avail);
    return false;
  }

  auto w = Worker::onThisThread(false);
  if (!w) {
    return true;
  }

  // Lock to prevent race between registering for bandwidth and
  // a bandwidth deposit from the TrafficShaper.
  ShapingContainer& read_container = w->readShapingContainer();
  auto& flow_group = read_container.getFlowGroup(NodeLocationScope::NODE);

  STAT_INCR(getStatsHolder(), read_throttling_num_throttle_checks);
  std::unique_lock<std::mutex> lock(read_container.flow_meters_mutex_);
  if (!flow_group.drain(on_bw_avail.cost(), rp)) {
    flow_group.push(on_bw_avail, rp);
    lock.unlock();
    err = E::CBREGISTERED;
    stream->markThrottled(true);
    STAT_INCR(getStatsHolder(), read_throttling_num_reads_throttled);
    ld_spew("Throttled: log:%lu, cb:%p", logid.val_, &on_bw_avail);
    return false;
  }

  STAT_INCR(getStatsHolder(), read_throttling_num_reads_allowed);
  return true;
}

void CatchupQueueDependencies::distributeNewlyReleasedRecords() {
  all_server_read_streams_->distributeNewlyReleasedRecords();
}

void CatchupQueueDependencies::used(logid_t logid) {
  all_server_read_streams_->used(logid);
}

void CatchupQueueDependencies::invalidateIterators(ClientID client_id) {
  all_server_read_streams_->invalidateIterators(client_id);
}

folly::Optional<std::chrono::milliseconds>
CatchupQueueDependencies::getDeliveryLatency(logid_t log_id) {
  auto config = Worker::getConfig();
  auto log = config->getLogGroupByIDShared(log_id);
  return log ? log->attrs().deliveryLatency().value()
             : folly::Optional<std::chrono::milliseconds>();
}

static std::unique_ptr<BackoffTimer>
create_timer_common(std::chrono::milliseconds initial_delay,
                    std::chrono::milliseconds max_delay,
                    std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      callback, initial_delay, max_delay);

  return std::move(timer);
}

std::unique_ptr<BackoffTimer>
CatchupQueueDependencies::createPingTimer(std::function<void()> callback) {
  return create_timer_common(
      std::chrono::milliseconds(1), std::chrono::milliseconds(1000), callback);
}

std::chrono::milliseconds CatchupQueueDependencies::iteratorTimerTTL() const {
  return Worker::settings().iterator_cache_ttl;
}

std::unique_ptr<Timer>
CatchupQueueDependencies::createIteratorTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(std::move(callback));
}

void CatchupQueueDependencies::putStorageTask(
    std::unique_ptr<ReadStorageTask>&& task,
    shard_index_t shard) {
  all_server_read_streams_->putStorageTask(std::move(task), shard);
}

Status
CatchupQueueDependencies::read(LocalLogStore::ReadIterator* read_iterator,
                               LocalLogStoreReader::Callback& callback,
                               LocalLogStoreReader::ReadContext* read_ctx) {
  ld_check(read_iterator != nullptr);
  return LocalLogStoreReader::read(
      *read_iterator, callback, read_ctx, getStatsHolder(), Worker::settings());
}

LogStorageStateMap& CatchupQueueDependencies::getLogStorageStateMap() {
  return ServerWorker::onThisThread()->processor_->getLogStorageStateMap();
}

int CatchupQueueDependencies::recoverLogState(logid_t log_id,
                                              shard_index_t shard_idx,
                                              bool force_ask_sequencer) {
  ServerProcessor* const processor = ServerWorker::onThisThread()->processor_;
  return processor->getLogStorageStateMap().recoverLogState(
      log_id,
      shard_idx,
      LogStorageState::RecoverContext::CATCHUP_QUEUE,
      force_ask_sequencer);
}

NodeID CatchupQueueDependencies::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

size_t CatchupQueueDependencies::getMaxRecordBytesQueued(ClientID client) {
  Worker* w = Worker::onThisThread();
  // Unless overridden by the `output_max_records_kb' setting, we consult the
  // socket's sendbuf size.  The rationale is to keep the TCP connection
  // sufficiently busy while not reading faster than necessary and buffering
  // excessively.
  ssize_t max_record_bytes_queued = Worker::settings().output_max_records_kb > 0
      ? Worker::settings().output_max_records_kb * 1024
      : w->sender().getTcpSendBufSizeForClient(client);
  if (max_record_bytes_queued <= 0) {
    // This ought to have tripped an assert in
    // Sender::getTcpSendBufSizeForClient() but no reason to crash production
    max_record_bytes_queued = 128 * 1024;
  }
  return max_record_bytes_queued;
}

const Settings& CatchupQueueDependencies::getSettings() const {
  return Worker::settings();
}

void CatchupQueue::readThrottlingOnReadTaskDone(const ReadStorageTask& task) {
  // Reconcile our cost estimate with the actual cost of performing this I/O
  size_t cost_estimate = task.getThrottlingEstimate();
  if (!cost_estimate) {
    return;
  }

  STAT_ADD(deps_->getStatsHolder(),
           read_throttling_num_bytes_read,
           task.total_bytes_);

  auto w = Worker::onThisThread(false);
  if (!w) {
    return;
  }

  ShapingContainer& read_container = w->readShapingContainer();
  auto& fg = read_container.getFlowGroup(NodeLocationScope::NODE);
  Priority p = task.getReadPriority();

  if (cost_estimate > task.total_bytes_) {
    // extra credits were debited, give them back
    size_t excess = cost_estimate - task.total_bytes_;
    bool can_run_fg = false;
    size_t overflow_credits = 0;

    auto lock = read_container.lock();
    bool was_pq_blocked = fg.isPriorityQueueBlocked(p);
    overflow_credits = fg.returnCredits(p, excess);
    // similar to FlowGroup::applyUpdate()
    if (was_pq_blocked && fg.canDrainMeter(p)) {
      can_run_fg = true;
    }
    lock.unlock();

    if (can_run_fg) {
      w->readShapingContainer().runFlowGroups(
          ShapingContainer::RunType::REPLENISH);
    }

    STAT_ADD(deps_->getStatsHolder(),
             read_throttling_excess_bytes_debited_from_meter,
             excess);
    STAT_ADD(deps_->getStatsHolder(),
             read_throttling_overflow_bytes,
             overflow_credits);
  } else if (cost_estimate < task.total_bytes_) {
    // more credits were used than initially requested, debit the excess
    auto lock = read_container.lock();
    fg.debitMeter(p, task.total_bytes_ - cost_estimate);
    lock.unlock();
    STAT_ADD(deps_->getStatsHolder(),
             read_throttling_excess_bytes_read_from_rocksdb,
             task.total_bytes_ - cost_estimate);
  } else {
    STAT_INCR(deps_->getStatsHolder(), read_throttling_num_exact_debits);
  }
}

void CatchupQueue::onReadTaskDone(const ReadStorageTask& task) {
  ld_spew("Got %zu records, status=%s",
          task.records_.size(),
          error_description(task.status_));
  STAT_ADD(
      deps_->getStatsHolder(), num_bytes_read_via_read_task, task.total_bytes_);

  // Call to readThrottlingOnReadTaskDone() should remain at the top to give
  // preference to read streams that were already queued for read i/o
  // Until onStorageTaskStopped() is called, no more read storage tasks can
  // be issued because storage_task_in_flight_ will remain true.
  readThrottlingOnReadTaskDone(task);

  // We may be waiting for bandwidth due to attempts to process another
  // stream. Cancel the resume callback so that any records/gaps generated
  // from processing this now completed read task will be transmitted before
  // any future bandwidth callback is invoked. Otherwise the actions of the
  // callback may inject messages based on a later stream state before the
  // messages queued here based on the current stream state.
  //
  // After queuing messages related to this task, a check is made to see
  // if additional work can be completed, including a check to see if we
  // are waiting for bandwidth. If required, the callback will be
  // registered again at that time. (See pushRecords() call below).
  resume_cb_.deactivate();

  ServerReadStream* stream = task.stream_.get();
  onStorageTaskStopped(stream);
  if (!stream) {
    // The ServerReadStreams was erased while the storage task was in flight.
    pushRecords();
    return;
  }

  ld_check(stream == &queue_.front());

  size_t n_bytes_queued;
  CatchupOneStream::Action act;
  std::tie(act, n_bytes_queued) =
      CatchupOneStream::onReadTaskDone(*deps_, stream, task);
  record_bytes_queued_ += n_bytes_queued;

  onBatchComplete(stream);

  stream_ld_debug(*stream,
                  "Read on storage thread completes with %s",
                  CatchupOneStream::action_names[act].c_str());

  if (act == CatchupOneStream::Action::TRANSIENT_ERROR) {
    // We hit an error trying to send out a record to the client.
    adjustPingTimer();
    return;
  }

  if (act == CatchupOneStream::Action::WAIT_FOR_BANDWIDTH) {
    // We ran out of bandwidth trying to send out a record to the client.
    // Wait for our callback to fire.
    //
    // NOTE: We use Sender's deferred message queuing feature when processing
    //       records from tasks, this status should never be returned.
    ld_check(false);
    return;
  }

  if (act == CatchupOneStream::Action::DEQUEUE_AND_CONTINUE) {
    queue_.pop_front();
    ld_check(!stream->isCatchingUp());
    stream->adjustStatWhenCatchingUpChanged();
  } else if (act == CatchupOneStream::Action::ERASE_AND_CONTINUE) {
    // All done, or reached an unrecoverable error.  Erase the read stream.
    // This also removes the stream from the queue since we are using
    // folly::IntrusiveList.
    deps_->eraseStream(
        client_id_, stream->log_id_, stream->id_, stream->shard_);
    stream = nullptr;
  } else if (act == CatchupOneStream::Action::PERMANENT_ERROR) {
    if (notifyShardError(stream) == 0) {
      deps_->eraseStream(
          client_id_, stream->log_id_, stream->id_, stream->shard_);
      stream = nullptr;
    }
  } else {
    ld_check(act == CatchupOneStream::Action::REQUEUE_AND_DRAIN ||
             act == CatchupOneStream::Action::REQUEUE_AND_CONTINUE);
    // Move to the end of the queue.
    queue_.pop_front();
    queue_.push_back(*stream);
    ld_check(stream->isCatchingUp());
  }

  if (act != CatchupOneStream::Action::REQUEUE_AND_DRAIN ||
      record_bytes_queued_ == 0) {
    pushRecords();
  }
}

void CatchupQueue::onStorageTaskStopped(const ServerReadStream* stream) {
  ld_check(storage_task_in_flight_);
  storage_task_in_flight_ = false;

  if (stream != nullptr) {
    // If the ServerReadStream still exists, it should be at the top of the
    // queue.
    ld_check(!queue_.empty());
    ServerReadStream* queue_stream = &queue_.front();
    ld_check_eq(stream, queue_stream);
    ld_check(queue_stream->storage_task_in_flight_);
    queue_stream->storage_task_in_flight_ = false;
  } else {
    catchup_queue_ld_debug("Stream was erased while task was in flight");
  }
}

void CatchupQueue::adjustPingTimer() {
  // If this class cannot get invoked again (because there are no RECORD
  // messages queued in the output evbuffer or outstanding storage tasks) but
  // there is still work to do, then we need to activate the timer to retry
  // later.
  if (record_bytes_queued_ == 0 && !resume_cb_.active() &&
      !storage_task_in_flight_ &&
      (!queue_.empty() || !queue_delayed_.empty())) {
    STAT_INCR(deps_->getStatsHolder(), read_streams_transient_errors);
    catchup_queue_ld_debug("Activate ping timer with timeout=%lu",
                           ping_timer_->getNextDelay().count());
    ping_timer_->activate();
  } else {
    catchup_queue_ld_debug("Reset ping timer");
    ping_timer_->reset();
  }
}

void CatchupQueue::onReadLngTaskDone(ServerReadStream* stream) {
  catchup_queue_ld_debug("ReadLngTask done");
  onStorageTaskStopped(stream);

  // Now that we know the last known good of the stream's current epoch, we may
  // be able to push some records.
  pushRecords();
}

void CatchupQueue::onStorageTaskDropped(ServerReadStream* stream) {
  catchup_queue_ld_debug("Storage task dropped");
  onStorageTaskStopped(stream);

  // NOTE: not calling pushRecords() here to keep the call lightweight.
  // Instead, adjustPingTimer() will check if there are RECORD messages queued
  // and set up a timer if necessary.
  adjustPingTimer();
}

void CatchupQueue::onBatchComplete(ServerReadStream* stream) {
  using std::chrono::steady_clock;
  uint64_t t = std::chrono::duration_cast<std::chrono::microseconds>(
                   steady_clock::now() - stream->last_batch_started_time_)
                   .count();
  STAT_ADD(deps_->getStatsHolder(), read_streams_batch_processing_microsec, t);
  STAT_INCR(deps_->getStatsHolder(), read_streams_batch_complete);
}

void CatchupQueue::getDebugInfo(InfoCatchupQueuesTable& table) {
  table.next()
      .set<0>(client_id_)
      .set<1>(queue_.size() + queue_delayed_.size())
      .set<2>(queue_.size())
      .set<3>(queue_delayed_.size())
      .set<4>(record_bytes_queued_)
      .set<5>(storage_task_in_flight_)
      .set<6>(ping_timer_->isActive())
      .set<7>(blocked_);
}

void CatchupQueue::blockUnBlock(bool block) {
  const bool prev = blocked_;
  blocked_ = block;
  if (prev && !block) {
    pushRecords();
  }
}

void CatchupQueue::ResumeStreamsCallback::operator()(FlowGroup&, std::mutex&) {
  catchup_queue_->pushRecords();
}

}} // namespace facebook::logdevice
