/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/StreamWriterAppendSink.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

StreamWriterAppendSink::StreamWriterAppendSink(
    std::shared_ptr<Processor> processor,
    std::unique_ptr<ClientBridge> bridge,
    std::chrono::milliseconds append_retry_timeout,
    chrono_expbackoff_t<std::chrono::milliseconds> expbackoff_settings)
    : processor_(std::move(processor)),
      bridge_(std::move(bridge)),
      append_retry_timeout_(append_retry_timeout),
      holder_(this),
      expbackoff_settings_(expbackoff_settings) {}

StreamWriterAppendSink::~StreamWriterAppendSink() {
  if (!processor_) {
    // processor_ can be nullptr only in tests.
    return;
  }

  // Must not be on a worker otherwise CancelInflightAppendsRequest posted on
  // that worker will not be executed ever and will enter a deadlock.
  ld_check(!Worker::onThisThread(false));

  // Close the sink atomically to prevent new appends while being destroyed.
  if (shutting_down_.exchange(true)) {
    // already called
    return;
  }

  ld_info("Shutting down StreamWriterAppendSink.");

  // Post requests to cancel all inflight requests before dying.

  Semaphore blocking_sem;
  const int nworkers = processor_->getWorkerCount(WorkerType::GENERAL);
  for (worker_id_t widx(0); widx.val_ < nworkers; ++widx.val_) {
    std::unique_ptr<Request> req =
        std::make_unique<CancelInflightAppendsRequest>(streams_, widx);
    req->setClientBlockedSemaphore(&blocking_sem);
    int rv = processor_->postWithRetrying(req);
    ld_check(rv == 0);
  }

  for (int i = 0; i < nworkers; ++i) {
    blocking_sem.wait();
  }

  // An alternate implementation that seems easier is to post a cancel request
  // for each stream on its target_worker. However that may violate correctness
  // if appendBuffered and shutdown are called concurrently. For instance, we
  // may post such a request for stream 1, 2 and 3 with target workers 1, 2
  // and 3. Meanwhile worker 4 for stream 4 can post a new StreamAppendRequest.
  // Note `shutting_down` flag does not prevent this. In the current
  // implementation, however, this does not happen since all appendBuffered
  // callers are detroyed before shutdown is called.
}

bool StreamWriterAppendSink::checkAppend(logid_t logid,
                                         size_t payload_size,
                                         bool allow_extra) {
  // Check if logid is valid.
  if (logid == LOGID_INVALID) {
    err = E::INVALID_PARAM;
    return false;
  }

  // Check if payload size is within bounds. Also sets err to E::TOOBIG if
  // size is too big.
  return AppendRequest::checkPayloadSize(
      payload_size, getMaxPayloadSize(), allow_extra);
}

StreamWriterAppendSink::Stream*
StreamWriterAppendSink::getStream(logid_t log_id) {
  auto it = streams_.find(log_id);
  if (it == streams_.end()) {
    // We assign a random write stream id. Since the id space is large (64 bits)
    // and uniqueness property for write streams is limited only to individual
    // logs, we hope that this won't clash across BufferedWriters (even across
    // clients). Each BufferedWriter object has at most one write stream mapped
    // to a log.
    write_stream_id_t stream_id(folly::Random::rand64());
    auto result = streams_.insert(
        std::make_pair(log_id, std::make_unique<Stream>(log_id, stream_id)));
    ld_check(result.second);
    auto stream = result.first->second.get();
    stream->setRetryTimer(createBackoffTimer(*stream));
    return stream;
  } else {
    return it->second.get();
  }
}

size_t StreamWriterAppendSink::getMaxPayloadSize() noexcept {
  return processor_->updateableSettings()->max_payload_size;
}

std::chrono::milliseconds
StreamWriterAppendSink::getAppendRetryTimeout() noexcept {
  return processor_->updateableSettings()->append_timeout.value_or(
      append_retry_timeout_);
}

std::pair<Status, NodeID> StreamWriterAppendSink::appendBuffered(
    logid_t logid,
    const BufferedWriter::AppendCallback::ContextSet& contexts,
    AppendAttributes attrs,
    PayloadHolder&& payload,
    AppendRequestCallback callback,
    worker_id_t target_worker,
    int checksum_bits) {
  // appendBuffered callers will be destroyed before shutting down begins. So
  // there should be no calls to appendBuffered after shutting_down_ flag is
  // set.
  ld_check(!shutting_down_.load());

  // We don't expect BufferedWriter to prepend the checksum in a client
  // context.  Instead it will be prepended by
  // APPEND_Message::serialize() as with normal appends.
  ld_check(checksum_bits == 0);

  if (!checkAppend(logid, payload.size(), true)) {
    // following ClientImpl::appendBuffered. Shouldn't this return some non-
    // E::OK value to differentiate from a posted append request?
    return std::make_pair(E::OK, NodeID());
  }

  // obtain the stream using logid
  auto& stream = *(getStream(logid));
  // each stream is associated with a target_worker. once assigned we cannot
  // change it, ever, to ensure epoch monotonicity.
  if (stream.target_worker_ == WORKER_ID_INVALID) {
    stream.target_worker_ = target_worker;
  }
  // for safety, do an ld_check
  ld_check(stream.target_worker_ == target_worker);

  // stream request id for the append request. once the request is posted
  // successfully this cannot change forever.
  write_stream_request_id_t stream_req_id = {
      stream.stream_id_, stream.next_seq_num_};

  // Increment stream sequence number.
  increment_seq_num(stream.next_seq_num_);

  // Create appropriate request state in inflight map before sending async
  // call.
  auto result = stream.pending_stream_requests_.emplace(
      std::piecewise_construct,
      // Sequence number as key
      std::forward_as_tuple(stream_req_id.seq_num),
      // Parameters to create StreamAppendRequestState
      std::forward_as_tuple(logid,
                            contexts,
                            attrs,
                            std::move(payload),
                            callback,
                            target_worker,
                            checksum_bits,
                            stream_req_id));
  ld_check(result.second);

  // If the message is next one to be posted, then post immediately.
  if (stream_req_id.seq_num ==
      next_seq_num(stream.max_inflight_window_seq_num_)) {
    postNextReadyRequestsIfExists(stream, 1);
  }

  return std::make_pair(E::OK, NodeID());
}

std::unique_ptr<StreamAppendRequest>
StreamWriterAppendSink::createAppendRequest(
    Stream& stream,
    const StreamWriterAppendSink::StreamAppendRequestState& req_state) {
  auto sink_ref = holder_.ref();
  auto stream_ref = stream.holder_.ref();
  auto stream_reqid = req_state.stream_req_id;
  auto wrapped_callback = [sink_ref, stream_ref, stream_reqid](
                              Status status, const DataRecord& record) {
    // Log this as a warning since this is not as critical, especially when
    // APPEND requests are delayed over the network.
    if (!sink_ref) {
      ld_warning("StreamWriterAppendSink destroyed while APPEND request was "
                 "in flight.");
      return;
    }
    if (!stream_ref) {
      ld_warning("Stream destroyed while APPEND request was in flight.");
      return;
    }
    auto sink = sink_ref.get();
    auto stream = stream_ref.get();
    sink->onCallback(*stream, stream_reqid, status, record);
  };
  return std::make_unique<StreamAppendRequest>(
      bridge_.get(),
      req_state.logid,
      req_state.attrs, // cannot std::move since we need this later
      req_state.payload.clone(),
      getAppendRetryTimeout(),
      wrapped_callback,
      req_state.stream_req_id,
      req_state.stream_req_id.seq_num ==
          next_seq_num(stream.max_prefix_acked_seq_num_));
}

void StreamWriterAppendSink::onCallback(Stream& stream,
                                        write_stream_request_id_t stream_reqid,
                                        Status status,
                                        const DataRecord& record) {
  auto req_seq_num = stream_reqid.seq_num;
  auto it = stream.pending_stream_requests_.find(req_seq_num);
  ld_check(it != stream.pending_stream_requests_.end());
  auto& req_state = it->second;

  // Update epoch by default. Must be done based on status (subsequent diffs).
  stream.updateSeenEpoch(getSeenEpoch(stream.target_worker_, stream.logid_));

  // Update last status, inflight_request.
  req_state.last_status = status;
  req_state.inflight_request = nullptr;

  if (status == Status::OK) {
    // Check that LSN returned does not violate monotonicity for sequence
    // numbers until req_seq_num.
    if (checkLsnMonotonicityUntil(stream, req_seq_num, record.attrs.lsn)) {
      // Ensure that LSN monotonicity can be maintained after LSN is accepted
      // for req_seq_num, by rewinding stream until the first instance of
      // violation in the stream (if there exists any).
      //
      // Note that ensureMonotonicityAfter can rewind at most until (req_seq_num
      // + 1). There are two cases possible:
      // (1) req_seq_num == (max_prefix_acked_seq_num_ + 1), then we know we are
      // going to post some requests and hence no need to worry about progress.
      // (2) req_seq_num > (max_prefix_acked_seq_num_ + 1), then by definition
      // leftmost is inflight. So progress is guaranteed.
      ensureLsnMonotonicityAfter(stream, req_seq_num, record.attrs.lsn);

      // Accept the ACK.
      req_state.record_attrs = record.attrs;
      // Update the max acked sequence number.
      if (req_seq_num > stream.max_acked_seq_num_) {
        stream.max_acked_seq_num_ = req_seq_num;
      }

      // If message is left most in the window, we trigger callbacks and send
      // more stream requests that are ready. Currently, we are sending twice
      // the number of prefix ACKs.
      //
      // If we send only 1 pending message per prefix ACK, that will resemble
      // ONE_AT_A_TIME mode. We chose 2 so that the client can systematically
      // improve the input throughput by having many inflight, as a proper write
      // stream connection is established with a sequencer.
      // TODO: Try out more than 2 triggers for each sequencer prefix ACK.
      if (req_seq_num == next_seq_num(stream.max_prefix_acked_seq_num_)) {
        auto num_called_back = stream.triggerPrefixCallbacks();
        postNextReadyRequestsIfExists(stream, 2 * num_called_back);
      }
    } else {
      rewindStreamUntil(stream, req_seq_num);
      postNextReadyRequestsIfExists(stream, 2);
    }
  } else {
    // Rewinding a stream until req_seq_num discards any previous successful
    // appends with higher sequence number and cancels any inflight appends with
    // higher sequence number. Note that this is fine because we have to retry
    // req_seq_num message anyway and that will definitely get a larger LSN
    // than all these. To maintain LSN monotonicity property, we will retry all
    // of them anyway!
    rewindStreamUntil(stream, req_seq_num);
    ensureEarliestPendingRequestInflight(stream);
  }
}

epoch_t StreamWriterAppendSink::getSeenEpoch(worker_id_t worker_id,
                                             logid_t logid) {
  // seen epoch on the worker is updated before callback.
  auto worker = Worker::onThisThread();
  ld_check_eq(worker, &processor_->getWorker(worker_id, WorkerType::GENERAL));
  auto& map = worker->appendRequestEpochMap().map;
  auto it = map.find(logid);
  return it != map.end() ? it->second : EPOCH_INVALID;
}

std::unique_ptr<BackoffTimer>
StreamWriterAppendSink::createBackoffTimer(Stream& stream) {
  auto wrapped_callback = [this, &stream]() {
    postNextReadyRequestsIfExists(stream, 1);
  };
  std::unique_ptr<BackoffTimer> retry_timer =
      std::make_unique<ExponentialBackoffTimer>(
          wrapped_callback, expbackoff_settings_);
  return retry_timer;
}

void StreamWriterAppendSink::postAppend(Stream& stream,
                                        StreamAppendRequestState& req_state) {
  auto req_append = createAppendRequest(stream, req_state);
  ld_check_ne(stream.target_worker_, WORKER_ID_INVALID);
  req_append->setTargetWorker(stream.target_worker_);
  req_append->setBufferedWriterBlobFlag();
  req_append->setAppendProbeController(&processor_->appendProbeController());
  // Store pointer to request in inflight_request before posting.
  ld_check(!req_state.inflight_request);
  req_state.inflight_request = req_append.get();
  std::unique_ptr<Request> req(std::move(req_append));
  int rv = processor_->postImportant(req);
  if (rv) {
    ld_critical("Processor::postImportant failed with error code: %hd",
                (std::uint16_t)err);
  }
}

void StreamWriterAppendSink::postNextReadyRequestsIfExists(
    Stream& stream,
    size_t suggested_count) {
  for (size_t i = 0; i < suggested_count; i++) {
    auto next_ready_seq_num = next_seq_num(stream.max_inflight_window_seq_num_);
    auto it = stream.pending_stream_requests_.find(next_ready_seq_num);
    if (it != stream.pending_stream_requests_.end()) {
      auto& req_state = it->second;
      // request must not be in flight.
      ld_check(!req_state.inflight_request);
      postAppend(stream, req_state);
      increment_seq_num(stream.max_inflight_window_seq_num_);
    } else {
      break;
    }
  }
}

void StreamWriterAppendSink::ensureEarliestPendingRequestInflight(
    Stream& stream) {
  auto earliest_seq_num = next_seq_num(stream.max_prefix_acked_seq_num_);
  auto it = stream.pending_stream_requests_.find(earliest_seq_num);
  ld_check(it != stream.pending_stream_requests_.end());
  auto& req_state = it->second;
  // Either there must be an inflight request corresponding to the
  // earliest_seq_num or the corresponding retry_timer must be active so that it
  // will eventually be retried again.
  if (!req_state.inflight_request) {
    // Check that max_inflight_window_seq_num_ is also same as
    // max_prefix_acked_seq_num_ i.e. are no inflight messages.
    ld_check_eq(
        stream.max_prefix_acked_seq_num_, stream.max_inflight_window_seq_num_);
    // If timer is already active, does nothing.
    stream.retry_timer_->activate();
  }
}

bool StreamWriterAppendSink::checkLsnMonotonicityUntil(
    Stream& stream,
    write_stream_seq_num_t seq_num,
    lsn_t assigned_lsn) {
  // assigned_lsn > lsn_of_max_prefix_acked_seq_num_ gives us the guarantee that
  // assigned_lsn is greater than all of the messages that are part of the
  // persistent prefix.
  bool monotonic = (stream.lsn_of_max_prefix_acked_seq_num_ < assigned_lsn);

  // For messages from (max_prefix_acked_seq_num_, seq_num), we must check that
  // all the available LSNs are smaller than assigned_lsn. We do this by tracing
  // back from (seq_num - 1) in reverse and comparing the LSN of the first ACK
  // we see with assigned_lsn. Since, LSN monotonicity for existing ACKs is an
  // invariant, this guarantees our monotonic property for currently available
  // ACKs and assigned_lsn at seq_num.
  if (monotonic) {
    write_stream_seq_num_t prev(seq_num.val_ - 1);
    for (auto temp = prev; temp > stream.max_prefix_acked_seq_num_;
         --temp.val_) {
      auto it = stream.pending_stream_requests_.find(temp);
      ld_check(it != stream.pending_stream_requests_.end());
      auto& req_state = it->second;
      if (req_state.last_status == E::OK) {
        monotonic = (req_state.record_attrs.lsn < assigned_lsn);
        break;
      }
    }
  }
  return monotonic;
}

void StreamWriterAppendSink::ensureLsnMonotonicityAfter(
    Stream& stream,
    write_stream_seq_num_t seq_num,
    lsn_t assigned_lsn) {
  for (auto temp = next_seq_num(seq_num); temp <= stream.max_acked_seq_num_;
       increment_seq_num(temp)) {
    auto it = stream.pending_stream_requests_.find(temp);
    ld_check(it != stream.pending_stream_requests_.end());
    auto& req_state = it->second;
    if (req_state.last_status == E::OK) {
      if (req_state.record_attrs.lsn < assigned_lsn) {
        // Uh oh, we have a violation. Rewind the stream until here so that we
        // can repost and eliminate the violation.
        rewindStreamUntil(stream, req_state.stream_req_id.seq_num);
      }
      // If the first ACK is not violated, later ones are not because they are
      // larger than req_state.record_attrs.lsn. More power to induction!
      break;
    }
  }
}

void StreamWriterAppendSink::rewindStreamUntil(Stream& stream,
                                               write_stream_seq_num_t seq_num) {
  // Reset all requests in the range [seq_num, max_inflight_window_seq_num_]
  for (auto temp = seq_num; temp <= stream.max_inflight_window_seq_num_;
       increment_seq_num(temp)) {
    auto it = stream.pending_stream_requests_.find(temp);
    ld_check(it != stream.pending_stream_requests_.end());
    resetPreviousAttempts(it->second);
  }

  // We have discarded all ACKs after seq_num. We need to find the largest
  // sequence number that has been ACKed before seq_num.
  stream.max_acked_seq_num_ = stream.max_prefix_acked_seq_num_;
  // Go in the reverse direction, maybe quicker!
  write_stream_seq_num_t prev(seq_num.val_ - 1);
  for (auto temp = prev; temp > stream.max_prefix_acked_seq_num_; --temp.val_) {
    auto it = stream.pending_stream_requests_.find(temp);
    ld_check(it != stream.pending_stream_requests_.end());
    auto& req_state = it->second;
    if (req_state.last_status == E::OK) {
      stream.max_acked_seq_num_ = temp;
      break;
    }
  }

  // Since we rewound all requests including and after seq_num, the
  // max_inflight_window_seq_num should be one less than seq_num.
  stream.max_inflight_window_seq_num_.val_ = (seq_num.val_ - 1);
}

}} // namespace facebook::logdevice
