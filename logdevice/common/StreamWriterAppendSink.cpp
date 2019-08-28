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
    // Using log id as stream id for now. Definitely needs to change soon.
    // TODO: Make stream id unique for every session, socket, log.
    write_stream_id_t stream_id(log_id.val());
    auto result = streams_.insert(
        std::make_pair(log_id, std::make_unique<Stream>(log_id, stream_id)));
    return result.first->second.get();
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
    const Payload& payload,
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
  auto result = stream.inflight_stream_requests_.emplace(
      std::piecewise_construct,
      // Sequence number as key
      std::forward_as_tuple(stream_req_id.seq_num),
      // Parameters to create StreamAppendRequestState
      std::forward_as_tuple(logid,
                            contexts,
                            attrs,
                            payload,
                            callback,
                            target_worker,
                            checksum_bits,
                            stream_req_id));
  ld_check(result.second);
  postAppend(stream, result.first->second);
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
      req_state.payload,
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
  auto it = stream.inflight_stream_requests_.find(stream_reqid.seq_num);
  ld_check(it != stream.inflight_stream_requests_.end());
  auto& req_state = it->second;

  // Update epoch by default. Must be done based on status (subsequent diffs).
  stream.updateSeenEpoch(getSeenEpoch(stream.target_worker_, stream.logid_));

  // Update last status, inflight_request.
  req_state.last_status = status;
  req_state.inflight_request = nullptr;

  if (status == Status::OK) {
    // Copy record attributes into req_state to create DataRecord for callback.
    req_state.record_attrs = record.attrs;

    // If seq_num is one more than max_prefix_acked_seq_num, then trigger prefix
    // callback on the stream. The callback is invoked for the entire prefix of
    // sequence numbers that have been ACKed so far.
    if (stream_reqid.seq_num ==
        next_seq_num(stream.max_prefix_acked_seq_num_)) {
      stream.triggerPrefixCallbacks();
    }
  } else {
    // Activate the retry_timer to post another request in the future.
    if (!req_state.retry_timer) {
      req_state.retry_timer = createBackoffTimer(stream, req_state);
    }
    req_state.retry_timer->activate();
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

std::unique_ptr<BackoffTimer> StreamWriterAppendSink::createBackoffTimer(
    Stream& stream,
    StreamAppendRequestState& req_state) {
  auto wrapped_callback = [this, &stream, &req_state]() {
    postAppend(stream, req_state);
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
}} // namespace facebook::logdevice
