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

#include "logdevice/common/ClientBridge.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/StreamAppendRequest.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"

namespace facebook { namespace logdevice {

class StreamWriterAppendSink : public BufferedWriterAppendSink {
 public:
  // Contains information required to retry a stream append request.
  struct StreamAppendRequestState {
    // Parameters required to resend the stream append request.
    logid_t logid;
    const BufferedWriter::AppendCallback::ContextSet& contexts;
    AppendAttributes attrs;
    Payload payload;
    AppendRequestCallback callback;
    worker_id_t target_worker;
    int checksum_bits;
    write_stream_request_id_t stream_req_id;
    // Status of the last invocation, if failed.
    Status last_status;

    StreamAppendRequestState(
        logid_t _logid,
        const BufferedWriter::AppendCallback::ContextSet& _contexts,
        AppendAttributes _attrs,
        const Payload& _payload,
        AppendRequestCallback _callback,
        worker_id_t _target_worker,
        int _checksum_bits,
        write_stream_request_id_t _stream_req_id)
        : logid(_logid),
          contexts(_contexts),
          attrs(_attrs),
          payload(_payload),
          callback(_callback),
          target_worker(_target_worker),
          checksum_bits(_checksum_bits),
          stream_req_id(_stream_req_id),
          last_status(Status::UNKNOWN) {}
  };
  using StreamRequestsMap = folly::F14NodeMap<write_stream_seq_num_t,
                                              StreamAppendRequestState,
                                              write_stream_seq_num_t::Hash>;

  struct Stream {
    write_stream_id_t stream_id_;
    // Internal map of inflight stream requests from the stream writer.
    StreamRequestsMap inflight_stream_requests_;
    // Stream sequence number for the next append request. starts from 1.
    write_stream_seq_num_t next_seq_num_;
    // Maximum sequence number that has been acked by any sequencer.
    write_stream_seq_num_t max_acked_seq_num_;
    // Weak ref holder for Stream
    WeakRefHolder<Stream> holder_;
    explicit Stream(
        write_stream_id_t stream_id,
        write_stream_seq_num_t next_seq_num = write_stream_seq_num_t(1UL))
        : stream_id_(stream_id),
          inflight_stream_requests_(),
          next_seq_num_(next_seq_num),
          max_acked_seq_num_(write_stream_seq_num_t(0UL)),
          holder_(this) {}
  };
  using StreamsMap = folly::ConcurrentHashMap<logid_t, std::unique_ptr<Stream>>;

  /**
   * Creates a stream writer append sink that posts StreamAppendRequests to
   * the processor. The client bridge exposes some functions belonging to the
   * client (for instance tracing, write tokens. Refer 'ClientBridge.h').
   * 'append_retry_timeout' is used to determine when an inflight APPEND request
   * has failed so it they can retried.
   */
  StreamWriterAppendSink(std::shared_ptr<Processor> processor,
                         ClientBridge* bridge,
                         std::chrono::milliseconds append_retry_timeout);

  bool checkAppend(logid_t logid,
                   size_t payload_size,
                   bool allow_extra) override;

  std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet& contexts,
                 AppendAttributes attrs,
                 const Payload& payload,
                 AppendRequestCallback callback,
                 worker_id_t target_worker,
                 int checksum_bits) override;

  static append_callback_t
  createRetryCallback(StreamWriterAppendSink* sink,
                      Stream* stream,
                      write_stream_request_id_t stream_reqid);

 protected:
  // can override in tests
  virtual void postAppend(std::unique_ptr<StreamAppendRequest> req_append);

  virtual size_t getMaxPayloadSize() noexcept;

  virtual std::chrono::milliseconds getAppendRetryTimeout() noexcept;

  Stream* getStream(logid_t log_id);

 private:
  std::unique_ptr<StreamAppendRequest> createAppendRequest(
      Stream* stream,
      const StreamWriterAppendSink::StreamAppendRequestState& req_state,
      bool stream_resume = false);

  // Processor to which append requests are posted.
  std::shared_ptr<Processor> processor_;

  // Bridge for the client that exposes a few non-essential parts of the
  // ClientImpl class such as tracing, write token checking. We do not want to
  // include ClientImpl directly to allow running StreamWriterAppendSink on
  // servers.
  ClientBridge* bridge_;

  // Map from logid_t to stream. A sink can have at most one stream per log.
  StreamsMap streams_;

  // Timeout used to determine if an inflight append request has failed so that
  // it can be retried. Currently we use client's append_timeout from
  // UpdateableSettings by default and if not present, we use this value. This
  // may need to change in the future.
  std::chrono::milliseconds append_retry_timeout_;

  // Weak reference holder to prevent an append request from calling back after
  // StreamWriterAppendSink has been destroyed.
  WeakRefHolder<StreamWriterAppendSink> holder_;
};

}} // namespace facebook::logdevice
