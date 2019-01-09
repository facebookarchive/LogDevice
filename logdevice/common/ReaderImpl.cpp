/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ReaderImpl.h"

#include <chrono>
#include <thread>

#include <folly/Memory.h>
#include <sys/time.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderProgressRequest.h"
#include "logdevice/common/StartReadingRequest.h"
#include "logdevice/common/StopReadingRequest.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/configuration/UpdateableConfig.h"

namespace facebook { namespace logdevice {

/**
 * Used by ClientReadStream to send us records.  Most of these calls happen on
 * worker threads while the Reader runs on an application thread so this class
 * needs to be thread-safe.
 */
class ReaderBridgeImpl : public ReaderBridge {
 public:
  explicit ReaderBridgeImpl(ReaderImpl* owner) : owner_(owner) {}
  int onDataRecord(read_stream_id_t,
                   std::unique_ptr<DataRecordOwnsPayload>&&,
                   bool notify_when_consumed) override;
  int onGapRecord(read_stream_id_t,
                  GapRecord,
                  bool notify_when_consumed) override;

 private:
  void maybeWakeConsumer();
  // Helper method, handles interactions with ReaderImpl for one QueueEntry
  // whether data or gap
  int onEntry(ReaderImpl::QueueEntry&& entry, bool notify_when_consumed);
  ReaderImpl* owner_;
};

static size_t calculate_queue_capacity(size_t max_logs,
                                       size_t client_read_buffer_size,
                                       double flow_control_threshold) {
  // We size the queue so that producer writes never fail.  Suppose that the
  // client buffer size is 100 and the flow control threshold is 0.7.  The
  // worker will ship 100 records, the 70th of them having the
  // notify_when_consumed bit set.  The consumer gobbles 70 records, notifies
  // the worker, then may pause.  The worker gets the notification, slides the
  // window, gets another 100 records from storage nodes and pushes them into
  // the queue.  So the queue needs to have enough items to hold the ((1.0 -
  // flow_control_threshold) * buffer_size) entries from the previous batch,
  // plus the whole current batch.
  //
  // If the space overhead for the queue becomes an issue, with some work this
  // could be changed so the queue size is limited by a client setting.
  // ClientReadStream would need to be changed not to assume that it can
  // always write into the queue, which would also require refactoring to be
  // able to buffer the first record.  Some mechanism of retrying failed
  // writes would also be needed.
  size_t slots_per_log =
      (2.0 - flow_control_threshold) * client_read_buffer_size + 1;
  return max_logs * slots_per_log;
}

ReaderImpl::ReaderImpl(size_t max_logs,
                       ssize_t buffer_size,
                       Processor* processor,
                       EpochMetaDataCache* epoch_metadata_cache,
                       std::shared_ptr<Client> client_shared,
                       std::string csid)
    : // Extract settings and delegate to protected constructor
      ReaderImpl(max_logs,
                 processor,
                 epoch_metadata_cache,
                 std::move(client_shared),
                 std::move(csid),
                 buffer_size < 0
                     ? processor->settings()->client_read_buffer_size
                     : static_cast<size_t>(buffer_size),
                 processor->settings()->client_read_flow_control_threshold) {}

ReaderImpl::ReaderImpl(size_t max_logs,
                       Processor* processor,
                       EpochMetaDataCache* epoch_metadata_cache,
                       std::shared_ptr<Client> client_shared,
                       std::string(csid),
                       size_t client_read_buffer_size,
                       double flow_control_threshold)
    : max_logs_(max_logs),
      read_buffer_size_(client_read_buffer_size),
      bridge_(new ReaderBridgeImpl(this)),
      processor_(processor),
      epoch_metadata_cache_(epoch_metadata_cache),
      client_shared_(std::move(client_shared)),
      csid_(std::move(csid)),
      queue_(calculate_queue_capacity(max_logs,
                                      client_read_buffer_size,
                                      flow_control_threshold)) {}

ReaderImpl::~ReaderImpl() {
  // The destructor needs to ensure that all reading is stopped, otherwise
  // workers may be left with dangling references to this object (through
  // ReaderBridge which they used to send records to us).

  if (!destructor_stops_reading_) {
    // Tests bypass this code to avoid non-virtual calls to
    // postStopReadingRequest() in the destructor
    return;
  }

  std::chrono::milliseconds delay(1);
  const std::chrono::milliseconds MAX_DELAY(1000);
  auto& index = log_states_.get<LogIndex>();

  Semaphore sem;
  int nlogs = index.size();
  while (!index.empty()) {
    // Try to post a StopReadingRequest for each log.  Some may fail if
    // workers' Request queues are full.
    for (auto it = index.begin(); it != index.end();) {
      auto cur = it++;
      if (ThreadID::isWorker()) {
        Worker* w = Worker::onThisThread();
        if (w->idx_ == cur->handle.worker_id) {
          StopReadingRequest req(cur->handle, [] {});
          req.execute();
          index.erase(cur);
          nlogs--;
          continue;
        }
      }
      // NOTE: postStopReadingRequest() call not virtual
      int rv = postStopReadingRequest(cur->handle, [&]() { sem.post(); });
      if (rv == 0) {
        index.erase(cur);
      }
    }
    if (index.empty()) {
      // All of the StopReadingRequests's have been posted.
      break;
    }

    // Some logs failed; wait a bit and try again.
    std::this_thread::sleep_for(delay);
    delay = std::min(delay * 2, MAX_DELAY);
  }

  // Now wait for all StopReadingRequests's to execute.  After that, it is
  // guaranteed that all ClientReadStream instances inside workers have been
  // destroyed.  There are no more references to this object inside workers.
  // It is safe to quit.
  for (int i = 0; i < nlogs; ++i) {
    sem.wait();
  }
}

int ReaderImpl::startReading(logid_t log_id,
                             lsn_t from,
                             lsn_t until,
                             const ReadStreamAttributes* attrs) {
  if (from > until) {
    ld_error("called with from > until for log_id %lu", log_id.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  if (lsn_to_epoch(from) > EPOCH_MAX) {
    ld_error("reading from an invalid epoch > EPOCH_MAX for log_id %lu",
             log_id.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  const auto& index = log_states_.get<LogIndex>();
  auto it = index.find(log_id);
  // If we're already reading from this log, first stop that.
  if (it != index.end() && stopReading(log_id) != 0) {
    ld_check(err != E::NOTFOUND);
    // err set by stopReading()
    return -1;
  }

  if (log_states_.size() >= max_logs_) {
    ld_check(log_states_.size() == max_logs_);
    ld_error("reading too many logs");
    err = E::TOOMANY;
    return -1;
  }

  LogState state;
  state.log_id = log_id;
  state.front_lsn = from;
  state.until_lsn = until;

  int rv = startReadingImpl(log_id, from, until, &state.handle, attrs);
  if (rv != 0) {
    // err set by startReadingImpl()
    return rv;
  }

  auto insert_result = log_states_.insert(state);
  ld_check(insert_result.second);
  return 0;
}

int ReaderImpl::stopReading(logid_t log_id) {
  auto& index = log_states_.get<LogIndex>();
  auto it = index.find(log_id);
  if (it == index.end()) {
    ld_error("not reading log %lu, nothing to stop", log_id.val_);
    err = E::NOTFOUND;
    return -1;
  }

  const LogState& state = *it;
  if (state.front_lsn <= state.until_lsn) {
    Semaphore sem;
    int rv = postStopReadingRequest(state.handle, [&]() { sem.post(); });
    if (rv != 0) {
      return rv;
    }
    // Wait for the worker to process our request; without this we risk the
    // following sequence of events:
    // 1) Client calls this method to stop reading a log, which returns
    //    quickly before the ClientReadStream instance is destroyed on the
    //    worker.
    // 2) Client destroys the Reader instance.
    // 3) ClientReadStream tries to talk to the Reader because the worker
    //    hasn't processed the StopReadingRequest yet.
    // This is of course avoidable but deemed not worth the effort just to
    // make this method nonblocking.
    sem.wait();
  } else {
    // Reading is already done (front_lsn > until_lsn) in which case
    // ClientReadStream self-destructed; there is no need to post a Request to
    // the worker.
  }

  index.erase(it);
  {
    std::lock_guard<std::mutex> guard(health_map_lock_);
    health_map_.erase(log_id);
  }
  return 0;
}

bool ReaderImpl::isReading(logid_t log_id) const {
  auto& index = log_states_.get<LogIndex>();
  return index.find(log_id) != index.end();
}

bool ReaderImpl::isReadingAny() const {
  return !log_states_.empty();
}

// Production implementation of startReadingImpl(): tries to send a
// StartReadingRequest to the Processor.  Tests are expected to stub out to
// always succeed and not require an actual Processor to be running.
int ReaderImpl::startReadingImpl(logid_t log_id,
                                 lsn_t from,
                                 lsn_t until,
                                 ReadingHandle* handle_out,
                                 const ReadStreamAttributes* attrs) {
  auto config = processor_->config_->get();
  ld_check(config);
  if (!config->logsConfig()->logExists(log_id)) {
    ld_error("called with invalid log_id: %lu", log_id.val_);
    err = E::NOTFOUND;
    return -1;
  }

  auto settings = processor_->settings();

  read_stream_id_t rsid = processor_->issueReadStreamID();

  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid,
      log_id,
      csid_,
      // no callbacks, using ReaderBridge
      std::function<bool(std::unique_ptr<DataRecord>&)>(),
      std::function<bool(const GapRecord&)>(),
      std::function<void(logid_t)>(),
      epoch_metadata_cache_,
      // NOTE: The callback keeps a pointer to `this`.  No danger of the pointer
      // dangling because the destructor destroys all ClientReadStream
      // instances.
      [this, log_id](bool healthy) {
        std::lock_guard<std::mutex> guard(health_map_lock_);
        health_map_[log_id] = healthy;
      },
      nullptr);

  auto read_stream = std::make_unique<ClientReadStream>(
      rsid,
      log_id,
      from,
      until,
      settings->client_read_flow_control_threshold,
      buffer_type_,
      read_buffer_size_,
      std::move(deps),
      processor_->config_,
      bridge_.get(),
      attrs);

  if (without_payload_) {
    read_stream->setNoPayload();
  }
  if (payload_hash_only_) {
    read_stream->addStartFlags(START_Header::PAYLOAD_HASH_ONLY);
  }
  if (ship_pseudorecords_) {
    read_stream->shipPseudorecords();
  }
  if (require_full_read_set_) {
    read_stream->requireFullReadSet();
  }
  if (ignore_released_status_) {
    read_stream->ignoreReleasedStatus();
  }

  if (force_no_scd_) {
    read_stream->forceNoSingleCopyDelivery();
  }
  if (do_not_skip_partially_trimmed_sections_) {
    read_stream->doNotSkipPartiallyTrimmedSections();
  }
  if (include_byte_offset_) {
    read_stream->includeByteOffset();
  }

  read_stream->addStartFlags(additional_start_flags_);

  // Select a worker thread to route the StartReadingRequest to.  We need to
  // remember it so that we can later route a StopReadingRequest to the same
  // thread.
  //
  // Use load-aware worker assignment to avoid pathological cases like
  // #7621815.
  worker_id_t worker_id = processor_->selectWorkerLoadAware();
  *handle_out = ReadingHandle{worker_id, rsid};
  std::unique_ptr<Request> req = std::make_unique<StartReadingRequest>(
      worker_id, log_id, std::move(read_stream));
  return processor_->postRequest(req);
}

// Production implementation of postStopReadingRequest(): tries to send a
// StopReadingRequest to the Processor.  Tests are expected to stub out to
// always succeed and not require an actual Processor to be running.
int ReaderImpl::postStopReadingRequest(ReadingHandle handle,
                                       std::function<void()> cb) {
  std::unique_ptr<Request> req =
      std::make_unique<StopReadingRequest>(handle, std::move(cb));
  return processor_->postRequest(req);
}

int ReaderImpl::setTimeout(std::chrono::milliseconds timeout) {
  if (timeout.count() < -1) {
    return -1;
  }
  timeout_ = std::min(timeout, MAX_TIMEOUT);
  return 0;
}

void ReaderImpl::waitOnlyWhenNoData() {
  wait_only_when_no_data_ = true;
}

void ReaderImpl::withoutPayload() {
  without_payload_ = true;
}

void ReaderImpl::payloadHashOnly() {
  payload_hash_only_ = true;
}

void ReaderImpl::doNotSkipPartiallyTrimmedSections() {
  do_not_skip_partially_trimmed_sections_ = true;
}

void ReaderImpl::includeByteOffset() {
  include_byte_offset_ = true;
}

int ReaderImpl::isConnectionHealthy(logid_t log_id) const {
  // This call is made on the application thread so it is fine to access
  // log_states_
  if (log_states_.find(log_id) == log_states_.end()) {
    err = E::NOTFOUND;
    return -1;
  }

  std::lock_guard<std::mutex> guard(health_map_lock_);
  auto it = health_map_.find(log_id);
  return it != health_map_.end() && it->second ? 1 : 0;
}

ssize_t ReaderImpl::read(size_t nrecords,
                         std::vector<std::unique_ptr<DataRecord>>* data_out,
                         GapRecord* gap_out) {
  // This is the workhorse method.  Each iteration of the loop consumes one
  // entry from the queue.  If we clear the queue, we wait (except in the case
  // of non-blocking reads when we immediately return).  The waiting protocol
  // is described in the header file.

  ld_check(data_out != nullptr);
  ld_check(gap_out != nullptr);

  if (nrecords == 0) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "called with nrecords == 0. Note that this call will "
                      "not return any records or gaps.");
  }

  if (log_states_.empty()) {
    // Not reading any logs, can return quickly.
    return 0;
  }

  read_initWaitParams();

  nrecords_ = nrecords;
  nread_ = 0;
  while (nread_ < nrecords_) {
    QueueEntry head;
    int rv = read_popQueue(head);
    if (rv != 0) {
      break;
    }

    bool should_notify_worker = false;
    if (head.shouldNotifyWhenConsumed()) {
      // Worker asked us to notify when we have consumed this record.
      should_notify_worker = true;

      // NOTE: Important to do this even if we soon discard the entry because
      // the LogState instance no longer exists.  Otherwise notify_count_ can
      // go out of sync with what is actually in the queue.
      auto prev = notify_count_.fetch_sub(1);
      ld_check(prev > 0);

      // Make sure we don't notify twice if this record gets buffered
      head.setNotifyWhenConsumed(false);
    }

    // Find the LogState for this log
    auto& index = log_states_.get<ReadStreamIDIndex>();
    auto it = index.find(head.getReadStreamID());
    if (it == index.end()) {
      // Log state not found.  This can happen if the queue entry was for a
      // read stream that no longer exists, for example if the application
      // just stopped it or seeked to a different LSN.  Ignore the queue
      // entry.
      continue;
    }

    // boost::multi_index is conservative in only giving us a const-ref
    // because we must not modify keys; cast away the const.
    LogState* state = const_cast<LogState*>(&*it);

    if (should_notify_worker) {
      notifyWorker(*state);
    }

    // handleData() and handleGap() may invalidate the iterator if they delete
    // the log; safer to invalidate it right away.
    it = index.end();

    if (head.getType() == QueueEntry::Type::DATA) {
      if ((head.getData().flags_ & RECORD_Header::BUFFERED_WRITER_BLOB) &&
          decode_buffered_writes_ && !without_payload_) {
        // The record contains a blob composed by BufferedWriter that we
        // should decode into records originally provided by the client.  This
        // call will do so and populate `pre_queue_' with entries that we'll
        // pick up on subsequent iterations of the loop.
        read_decodeBuffered(head);
      } else {
        read_handleData(head, state, data_out);
      }
    } else if (head.getType() == QueueEntry::Type::GAP) {
      bool break_loop;
      read_handleGap(head, state, gap_out, &break_loop);
      if (break_loop) {
        break;
      }
    } else {
      ld_check(false); // invalid queue entry
    }
  }
  return nread_;
}

void ReaderImpl::read_initWaitParams() {
  if (timeout_.count() == -1) {
    may_wait_ = true;
  } else if (timeout_.count() > 0) {
    may_wait_ = true;
    until_ = std::chrono::steady_clock::now() + timeout_;
  } else {
    may_wait_ = false;
  }
}

int ReaderImpl::read_popQueue(QueueEntry& entry_out) {
  if (!pre_queue_.empty()) {
    entry_out = std::move(pre_queue_.front());
    pre_queue_.pop_front();
    return 0;
  }

  while (!queue_.read(entry_out)) {
    if (!may_wait_) {
      return -1;
    }
    read_wait();
    // read_wait() may have set may_wait_ to false in which case the loop
    // will terminate next time around
  }
  // Decrement record_count_ as a result of removing an entry from queue_. Note
  // that record_count_ may temporarily become negative if this executes before
  // ReaderBridgeImpl::onEntry() updates the counter.
  record_count_ -= entry_out.getRecordCount();
  if (entry_out.getType() == QueueEntry::Type::GAP) {
    --gap_count_;
  }
  return 0;
}

void ReaderImpl::read_wait() {
  ld_check(may_wait_);
  ld_check(nrecords_ > nread_);

  // If waitOnlyWhenNoData() was called, 1 data record is enough to wake us.
  // Otherwise, set the watermark to however many we still need to satisfy the
  // client's request.
  const int64_t watermark = wait_only_when_no_data_
      ? 1
      : std::min(queue_.capacity(), nrecords_ - nread_);

  auto wait_predicate = [=]() {
    return
        // Wake if the number of data records in the queue has reached the
        // watermark
        record_count_.load() >= watermark ||
        // Wake if there is a gap in the queue, since that should cause read()
        // to return quickly per the API
        gap_count_.load() > 0 ||
        // Wake if ClientReadStream posted an entry it wants a notification for,
        // failure to do so could cause a deadlock as we would be preventing it
        // from sliding its window
        notify_count_.load() > 0;
  };

  std::unique_lock<std::mutex> lock(cv_mutex_);
  wait_watermark_.store(watermark);
  if (timeout_.count() == -1) {
    cv_.wait(lock, wait_predicate);
  } else {
    bool timed_out = !cv_.wait_until(lock, until_, wait_predicate);
    if (timed_out) {
      // If the semaphore wait timed out, disallow further waiting.  We'll
      // process whatever is on the queue then stop.
      may_wait_ = false;
      wait_watermark_.store(-1);
    }
  }
}

void ReaderImpl::read_handleData(
    QueueEntry& entry,
    LogState* state,
    std::vector<std::unique_ptr<DataRecord>>* data_out) {
  const lsn_t lsn = entry.getData().attrs.lsn;
  state->front_lsn = lsn + 1;

  if (state->front_lsn > state->until_lsn) {
    // We reached the client-supplied `until` LSN.  We don't want this
    // read() call to block just because the client asked for a bigger
    // chunk of data than was left in the log.
    may_wait_ = false;
    if (entry.getAllowEndReading()) {
      // Try to tear down the ClientReadStream instance.  This is
      // best-effort, may fail.
      stopReading(state->log_id);
    }
    // If we did call stopReading() above, it erased the LogState instance,
    // invalidating `state`.
    state = nullptr;
  }

  if (wait_only_when_no_data_) {
    // Caller asked for read() to wait only when there is no data to return.
    // We have a data record, therefore prevent the current read() call from
    // waiting.
    may_wait_ = false;
  }

  // Here we upcast the std::unique_ptr<DataRecordOwnsPayload> to a
  // std::unique_ptr<DataRecord>.  DataRecord has a virtual destructor so
  // the payload will get freed when the application deletes the DataRecord.
  data_out->push_back(entry.releaseData());
  ++nread_;
}

void ReaderImpl::read_handleGap(QueueEntry& entry,
                                LogState* state,
                                GapRecord* gap_out,
                                bool* break_loop_out) {
  // If caller asked to skip gaps and this is not a gap at the end of a
  // log, ignore it.
  if (skip_gaps_ && entry.getGap().hi < state->until_lsn) {
    state->front_lsn = entry.getGap().hi + 1;
    *break_loop_out = false;
    return;
  }

  // If we consumed any data records, break the read() loop to return them
  // now.  The next call to read() will yield the gap.
  if (nread_ > 0) {
    pre_queue_.push_front(std::move(entry));
    *break_loop_out = true;
    return;
  }

  // Otherwise, deliver the gap immediately.

  state->front_lsn = entry.getGap().hi + 1;

  if (state->front_lsn > state->until_lsn) {
    // We reached the client-supplied `until` LSN.  Clean up some state,
    // similar to data record handling above.
    stopReading(state->log_id);
    state = nullptr;
  }

  nread_ = -1;
  err = E::GAP;
  *gap_out = entry.getGap();
  *break_loop_out = true;
}

void ReaderImpl::read_decodeBuffered(QueueEntry& entry) {
  // We only expect to see a buffered write come off the main queue.  Check
  // this so that it doesn't matter if we push to the front or the back of
  // pre_queue_.
  ld_check(pre_queue_.empty());

  // Make a copy of attributes since we'll need them after we pass ownership
  // of `entry.getData()'
  logid_t log_id = entry.getData().logid;
  DataRecordAttributes attrs = entry.getData().attrs;
  RECORD_flags_t flags = entry.getData().flags_;
  // We shouldn't be decoding buffered writes while rebuilding
  ld_check(!entry.getData().extra_metadata_);

  auto decoder = std::make_shared<BufferedWriteDecoderImpl>();
  std::vector<Payload> payloads;
  int rv = decoder->decodeOne(entry.releaseData(), payloads);
  if (rv != 0) {
    // Whoops, decoding failed.  This is tragic and unlikely with checksums
    // but let's generate a DATALOSS gap to inform the client.
    pre_queue_.emplace_back( // creating a QueueEntry
        entry.getReadStreamID(),
        std::make_unique<GapRecord>(
            log_id, GapType::DATALOSS, attrs.lsn, attrs.lsn));
    return;
  }

  // Decoding succeeded.  Now we need to create a DataRecordOwnsPayload for
  // each original record, push them onto `pre_queue_' and let the main
  // read() loop consume them.
  int batch_offset = 0;
  for (Payload& payload : payloads) {
    auto record = std::make_unique<DataRecordOwnsPayload>(
        log_id,
        std::move(payload),
        attrs.lsn,
        attrs.timestamp,
        flags & ~RECORD_Header::BUFFERED_WRITER_BLOB,
        nullptr, // no rebuilding metadata
        decoder, // shared ownership of the decoder
        batch_offset++);
    pre_queue_.emplace_back( // creating a QueueEntry
        entry.getReadStreamID(),
        std::move(record),
        1);
    // Only allow read() to stop reading the log after consuming the last
    // record
    pre_queue_.back().setAllowEndReading(&payload == &payloads.back());
  }
}

void ReaderImpl::notifyWorker(LogState& state) {
  std::unique_ptr<Request> req =
      std::make_unique<ReaderProgressRequest>(state.handle);
  // We need the Request to make it through to the worker eventually, so this
  // goes through Processor::postWithRetrying(). If this used plain
  // Processor::postRequest() and the worker's request pipe was full,
  // reading would halt since the worker would not be notified of our progress.
  if (processor_->postWithRetrying(req) != 0) {
    ld_error("Posting ReaderProgressRequest failed with error %s.  "
             "Reading will halt for log %lu.",
             error_description(err),
             state.log_id.val_);
    return;
  }
}

// These methods run on worker threads

int ReaderBridgeImpl::onEntry(ReaderImpl::QueueEntry&& entry,
                              bool notify_when_consumed) {
  if (notify_when_consumed) {
    entry.setNotifyWhenConsumed(true);
    ++owner_->notify_count_;
  }
  const int64_t nrecords = entry.getRecordCount();
  const bool is_gap = entry.getType() == ReaderImpl::QueueEntry::Type::GAP;
  if (!owner_->queue_.write(std::move(entry))) {
    // Queue write failed.  We'll push back on ClientReadStream.  It will try
    // again later, by when the consumer will have hopefully made some space
    // in the queue.
    if (notify_when_consumed) {
      // Revert the notify_count_ bump since we failed to write to the queue.
      // The counter was too high for a short while but that is not an issue
      // as it can only cause a spurious wakeup of the consumer.
      --owner_->notify_count_;
    }
    return -1;
  }
  owner_->record_count_ += nrecords;
  if (is_gap) {
    ++owner_->gap_count_;
  }
  maybeWakeConsumer();
  return 0;
}

int ReaderBridgeImpl::onDataRecord(
    read_stream_id_t rsid,
    std::unique_ptr<DataRecordOwnsPayload>&& record,
    bool notify_when_consumed) {
  uint32_t nrecords = 1;
  if (record->flags_ & RECORD_Header::BUFFERED_WRITER_BLOB &&
      owner_->decode_buffered_writes_ && !owner_->without_payload_) {
    // This record's payload was composed by BufferedWriter. Extract the number
    // of individual records inside the batch in order to correctly determine
    // whether to wake up the consumer (e.g. if a single batch with 100 records
    // is received, read(100) should return immediately).
    size_t batch_size;
    if (BufferedWriteDecoderImpl::getBatchSize(*record, &batch_size) == 0) {
      nrecords =
          std::min<size_t>(batch_size, std::numeric_limits<uint32_t>::max());
    }
  }

  ReaderImpl::QueueEntry entry(rsid, std::move(record), nrecords);
  int rv = onEntry(std::move(entry), notify_when_consumed);
  if (rv != 0) {
    record = entry.releaseData();
  }
  return rv;
}

int ReaderBridgeImpl::onGapRecord(read_stream_id_t rsid,
                                  GapRecord gap,
                                  bool notify_when_consumed) {
  ReaderImpl::QueueEntry entry(rsid, std::make_unique<GapRecord>(gap));
  return onEntry(std::move(entry), notify_when_consumed);
}

void ReaderBridgeImpl::maybeWakeConsumer() {
  std::atomic<int64_t>& atom = owner_->wait_watermark_;
  while (1) {
    int64_t watermark = atom.load();
    if (watermark == -1) { // consumer not waiting
      break;
    }

    bool should_wake = owner_->record_count_.load() >= watermark ||
        owner_->gap_count_.load() > 0 || owner_->notify_count_.load() > 0;
    if (!should_wake) {
      break;
    }

    if (atom.compare_exchange_strong(watermark, -1)) {
      // Wait until consumer is inside the monitor (lock is free), then notify
      std::unique_lock<std::mutex> lock(owner_->cv_mutex_);
      owner_->cv_.notify_one();
      break;
    }
  }
}

}} // namespace facebook::logdevice
