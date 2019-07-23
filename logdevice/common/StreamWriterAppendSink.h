/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"

namespace facebook { namespace logdevice {

class StreamWriterAppendSink : public BufferedWriterAppendSink {
 public:
  // Contains information required to retry an append request.
  struct AppendBufferedRequestState {
    // parameters in the appendBuffered invocation
    logid_t logid;
    const BufferedWriter::AppendCallback::ContextSet& contexts;
    AppendAttributes attrs;
    Payload payload;
    AppendRequestCallback callback;
    worker_id_t target_worker;
    int checksum_bits;
    // status of the last invocation, if failed.
    Status last_status;

    AppendBufferedRequestState(
        logid_t _logid,
        const BufferedWriter::AppendCallback::ContextSet& _contexts,
        AppendAttributes _attrs,
        const Payload& _payload,
        AppendRequestCallback _callback,
        worker_id_t _target_worker,
        int _checksum_bits)
        : logid(_logid),
          contexts(_contexts),
          attrs(_attrs),
          payload(_payload),
          callback(_callback),
          target_worker(_target_worker),
          checksum_bits(_checksum_bits),
          last_status(Status::UNKNOWN) {}
  };
  using stream_request_id_t = uint64_t;
  using StreamRequestsMap =
      folly::F14FastMap<stream_request_id_t,
                        std::unique_ptr<AppendBufferedRequestState>>;

  struct Stream {
    // internal map of inflight stream requests from the stream writer.
    StreamRequestsMap inflight_stream_requests_;
    // stream request id for the next append request.
    stream_request_id_t next_stream_request_id_ = 0UL;
  };
  using StreamsMap = folly::ConcurrentHashMap<logid_t, std::unique_ptr<Stream>>;

  explicit StreamWriterAppendSink(BufferedWriterAppendSink* sink);

  bool checkAppend(logid_t logid,
                   size_t payload_size,
                   bool allow_extra) override;

  Status canSendToWorker() override;

  void onBytesSentToWorker(ssize_t /*bytes*/) override;

  void onBytesFreedByWorker(size_t /*bytes*/) override;

  /**
   * Calls appendBuffered of the append_sink_ but does not allow the a write
   * to fail asynchronously.
   */
  std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet& contexts,
                 AppendAttributes attrs,
                 const Payload& payload,
                 AppendRequestCallback callback,
                 worker_id_t target_worker,
                 int checksum_bits) override;

 private:
  AppendRequestCallback createRetryCallback(stream_request_id_t req_id);

  Stream& getStream(logid_t log_id);

  // sink to which StreamWriterAppendSink sends appendBuffered requests.
  BufferedWriterAppendSink* sink_;

  // map from logid_t to stream. A sink can have at most one stream per log.
  StreamsMap streams_;
};

}} // namespace facebook::logdevice
