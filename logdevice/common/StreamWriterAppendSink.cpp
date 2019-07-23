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

StreamWriterAppendSink::StreamWriterAppendSink(BufferedWriterAppendSink* sink)
    : sink_(sink), streams_() {
  ld_check(sink_);
}

bool StreamWriterAppendSink::checkAppend(logid_t logid,
                                         size_t payload_size,
                                         bool allow_extra) {
  return sink_->checkAppend(logid, payload_size, allow_extra);
}

Status StreamWriterAppendSink::canSendToWorker() {
  return sink_->canSendToWorker();
}

void StreamWriterAppendSink::onBytesSentToWorker(ssize_t num_bytes) {
  sink_->onBytesSentToWorker(num_bytes);
}

void StreamWriterAppendSink::onBytesFreedByWorker(size_t num_bytes) {
  sink_->onBytesFreedByWorker(num_bytes);
}

StreamWriterAppendSink::Stream&
StreamWriterAppendSink::getStream(logid_t log_id) {
  auto it = streams_.find(log_id);
  if (it == streams_.end()) {
    streams_.insert(std::make_pair(log_id, std::make_unique<Stream>()));
    it = streams_.find(log_id);
  }
  ld_check(it != streams_.end());
  return *(it->second.get());
}

std::pair<Status, NodeID> StreamWriterAppendSink::appendBuffered(
    logid_t logid,
    const BufferedWriter::AppendCallback::ContextSet& contexts,
    AppendAttributes attrs,
    const Payload& payload,
    AppendRequestCallback callback,
    worker_id_t target_worker,
    int checksum_bits) {
  // create params to use for callbacks later.
  std::unique_ptr<AppendBufferedRequestState> params =
      std::make_unique<AppendBufferedRequestState>(logid,
                                                   contexts,
                                                   attrs,
                                                   payload,
                                                   callback,
                                                   target_worker,
                                                   checksum_bits);

  // get the next stream id and increment now.
  auto& stream = getStream(logid);
  stream_request_id_t req_id = stream.next_stream_request_id_++;

  // insert request into inflight map before sending async call
  ld_check(stream.inflight_stream_requests_.find(req_id) ==
           stream.inflight_stream_requests_.end());
  stream.inflight_stream_requests_[req_id] = std::move(params);

  // invoke appendBuffered on sink_
  std::pair<Status, NodeID> result =
      sink_->appendBuffered(logid,
                            contexts,
                            attrs,
                            payload,
                            createRetryCallback(req_id),
                            target_worker,
                            checksum_bits);

  // handle case when request is not accepted. rewind and remove request from
  // internal inflight map.
  if (result.first != Status::OK) {
    stream.next_stream_request_id_--;
    auto it = stream.inflight_stream_requests_.find(req_id);
    ld_check(it != stream.inflight_stream_requests_.end());
    stream.inflight_stream_requests_.erase(it);
  }
  // If result.first is not E::OK, the buffered writer immediately schedules a
  // retry -- refer `BufferedWriterSingleLog::appendBatch`. So this happens in
  // order.
  return result;
}

StreamWriterAppendSink::AppendRequestCallback
StreamWriterAppendSink::createRetryCallback(stream_request_id_t req_id) {
  return
      [this, req_id](Status status, const DataRecord& record, NodeID node_id) {
        auto logid = record.logid;
        auto& stream = getStream(logid);
        auto it = stream.inflight_stream_requests_.find(req_id);
        ld_check(it != stream.inflight_stream_requests_.end());
        auto request = it->second.get();
        if (status == Status::OK) {
          request->callback(status, record, node_id);
        } else {
          // update last status for request
          request->last_status = status;
          // retry request
          std::pair<Status, NodeID> result =
              sink_->appendBuffered(request->logid,
                                    request->contexts,
                                    request->attrs,
                                    request->payload,
                                    createRetryCallback(req_id),
                                    request->target_worker,
                                    request->checksum_bits);
          if (result.first != Status::OK) {
            ld_critical("Could not retry an accepted appendBuffered request");
          }
        }
      };
}

}} // namespace facebook::logdevice
