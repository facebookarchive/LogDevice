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
    ClientBridge* bridge,
    std::chrono::milliseconds append_retry_timeout)
    : processor_(processor),
      bridge_(bridge),
      append_retry_timeout_(append_retry_timeout),
      holder_(this) {}

bool StreamWriterAppendSink::checkAppend(logid_t logid,
                                         size_t payload_size,
                                         bool allow_extra) {
  // Check if logid is valid.
  if (logid == LOGID_INVALID) {
    err = E::INVALID_PARAM;
    return false;
  }

  // Check if payload size is within bounds. Also sets err to E::TOOBIG if size
  // is too big.
  return AppendRequest::checkPayloadSize(
      payload_size, getMaxPayloadSize(), allow_extra);
}

StreamWriterAppendSink::Stream*
StreamWriterAppendSink::getStream(logid_t log_id) {
  auto it = streams_.find(log_id);
  if (it == streams_.end()) {
    auto result = streams_.insert(std::make_pair(
        log_id, std::make_unique<Stream>(write_stream_id_t(log_id.val()))));
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
  auto stream = getStream(logid);

  // stream request id for the append request. once the request is posted
  // successfully this cannot change forever.
  write_stream_request_id_t stream_req_id = {
      stream->stream_id_, stream->next_seq_num_};

  // Increment stream sequence number.
  stream->next_seq_num_.val_++;

  // Create appropriate request state in inflight map before sending async call.
  auto result = stream->inflight_stream_requests_.emplace(
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

  // Create append request.
  auto req = createAppendRequest(stream, result.first->second);
  req->setTargetWorker(target_worker);
  req->setBufferedWriterBlobFlag();
  postAppend(std::move(req));
  return std::make_pair(E::OK, NodeID());
}

std::unique_ptr<StreamAppendRequest>
StreamWriterAppendSink::createAppendRequest(
    Stream* stream,
    const StreamWriterAppendSink::StreamAppendRequestState& req_state,
    bool stream_resume) {
  return std::make_unique<StreamAppendRequest>(
      bridge_,
      req_state.logid,
      req_state.attrs, // cannot std::move since we need this later
      req_state.payload,
      getAppendRetryTimeout(),
      createRetryCallback(this, stream, req_state.stream_req_id),
      req_state.stream_req_id,
      stream_resume);
}

append_callback_t StreamWriterAppendSink::createRetryCallback(
    StreamWriterAppendSink* sink,
    Stream* stream,
    write_stream_request_id_t stream_reqid) {
  WeakRef<StreamWriterAppendSink> ref1 = sink->holder_.ref();
  WeakRef<Stream> ref2 = stream->holder_.ref();
  return [ref1, ref2, stream_reqid](Status status, const DataRecord& record) {
    // Log this as a warning since this is not as critical, especially when
    // APPEND requests are delayed over the network.
    if (!ref1) {
      ld_warning("StreamWriterAppendSink destroyed while APPEND request was in "
                 "flight.");
      return;
    }
    if (!ref2) {
      ld_warning("Stream destroyed while APPEND request was in flight.");
      return;
    }

    // Obtain pointer to sink.
    auto sink = ref1.get();
    auto stream = ref2.get();
    ld_check(sink);
    ld_check(stream);
    auto it = stream->inflight_stream_requests_.find(stream_reqid.seq_num);
    ld_check(it != stream->inflight_stream_requests_.end());
    auto& req_state = it->second;

    if (status == Status::OK) {
      // Update max acked seq num, callback and erase.
      stream->max_acked_seq_num_ =
          std::max(stream_reqid.seq_num, stream->max_acked_seq_num_);
      req_state.callback(status, record, NodeID());
      stream->inflight_stream_requests_.erase(it);
    } else {
      // Update last status and post another request.
      req_state.last_status = status;
      auto req = sink->createAppendRequest(stream, req_state);
      req->setTargetWorker(req_state.target_worker);
      req->setBufferedWriterBlobFlag();
      sink->postAppend(std::move(req));
    }
  };
}

void StreamWriterAppendSink::postAppend(
    std::unique_ptr<StreamAppendRequest> req_append) {
  ld_check(req_append != nullptr);

  req_append->setAppendProbeController(&processor_->appendProbeController());
  std::unique_ptr<Request> req(std::move(req_append));
  int rv = processor_->postImportant(req);
  if (rv) {
    ld_critical("Processor::postImportant failed with error code: %hd",
                (std::uint16_t)err);
  }
}

}} // namespace facebook::logdevice
