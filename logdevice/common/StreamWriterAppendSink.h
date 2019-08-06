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
    // StreamAppendRequest that is currently inflight. Value is nullptr
    // when no such request is inflight which happens in two scenarios:
    // 1. Before the first request.
    // 2. All previously posted requests have called back and we have scheduled
    // to post a new request.
    StreamAppendRequest* inflight_request;
    // Retry timer used to post failed append requests. We use
    // ExponentialBackoffTimer for this purpose.  Created on demand since we
    // expect most requests to succeed in first attempt. This is not
    // thread-safe. The callback sent to retry_timer is executed on the same
    // thread as `appendBuffered` was invoked from, which is also the same
    // thread on which `onCallback` is called after the `AppendRequest` has been
    // processed.
    std::unique_ptr<BackoffTimer> retry_timer;

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
          last_status(Status::UNKNOWN),
          inflight_request(nullptr),
          retry_timer(nullptr) {}
  };
  using StreamRequestsMap = folly::F14NodeMap<write_stream_seq_num_t,
                                              StreamAppendRequestState,
                                              write_stream_seq_num_t::Hash>;

  struct Stream {
    // log id corresponding to the stream
    logid_t logid_;
    // write stream id
    write_stream_id_t stream_id_;
    // Internal map of inflight stream requests from the stream writer.
    StreamRequestsMap inflight_stream_requests_;
    // Stream sequence number for the next append request. starts from 1.
    write_stream_seq_num_t next_seq_num_;
    // Maximum sequence number that has been acked by any sequencer.
    write_stream_seq_num_t max_acked_seq_num_;
    // All requests in a stream must be posted on the same target worker.
    worker_id_t target_worker_;
    // Latest seen epoch by the write stream - it maybe used to determine how to
    // react on a WRITE_STREAM_UNKNOWN status. For instance, if we get
    // stream unknown status from a stale sequencer, we may want to retry just
    // the request to reach the latest sequencer. On the other hand, if a newer
    // sequencer says stream unknown, we have to retry all requests from
    // (max_acked_seq_num_ + 1). Currently, we do not use this.
    epoch_t seen_epoch_;
    // Weak reference holder for stream object that is used by callback to check
    // if it can safely execute.
    WeakRefHolder<Stream> holder_;
    Stream(logid_t logid,
           write_stream_id_t stream_id,
           write_stream_seq_num_t next_seq_num = write_stream_seq_num_t(1UL))
        : logid_(logid),
          stream_id_(stream_id),
          inflight_stream_requests_(),
          next_seq_num_(next_seq_num),
          max_acked_seq_num_(WRITE_STREAM_SEQ_NUM_INVALID),
          target_worker_(WORKER_ID_INVALID),
          seen_epoch_(EPOCH_INVALID),
          holder_(this) {}

    // Updates the seen epoch for the stream and returns true if updated.
    bool updateSeenEpoch(epoch_t epoch) {
      if (epoch != EPOCH_INVALID && epoch > seen_epoch_) {
        seen_epoch_ = epoch;
        return true;
      }
      return false;
    }
  };
  using StreamsMap = folly::ConcurrentHashMap<logid_t, std::unique_ptr<Stream>>;

  // Cancels inflight append requests that were posted to a particular worker.
  class CancelInflightAppendsRequest : public Request {
   public:
    CancelInflightAppendsRequest(StreamsMap& streams, worker_id_t worker)
        : Request(RequestType::STREAM_WRITER_CANCEL_INFLIGHT_APPENDS),
          worker_(worker),
          streams_(streams) {}

    int getThreadAffinity(int /*nthreads*/) override {
      return worker_.val();
    }

    Execution execute() override {
      for (auto& stream_value : streams_) {
        auto& stream = stream_value.second;
        if (stream->target_worker_ == worker_) {
          for (auto& req_state_value : stream->inflight_stream_requests_) {
            auto& req_state = req_state_value.second;
            auto req = req_state.inflight_request;
            ld_check(req); // there must be a non-null inflight request.
            req->cancel();
          }
        }
      }
      return Execution::COMPLETE;
    }

   private:
    worker_id_t worker_;
    StreamsMap& streams_;
  };
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
  virtual ~StreamWriterAppendSink() override;

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

  void onCallback(Stream& stream,
                  write_stream_request_id_t stream_reqid,
                  Status status,
                  const DataRecord& record);

 protected:
  // can override in tests
  virtual void postAppend(Stream& stream, StreamAppendRequestState& req_state);

  virtual size_t getMaxPayloadSize() noexcept;

  virtual std::chrono::milliseconds getAppendRetryTimeout() noexcept;

  // Obtain the seen_epoch for log from specified worker. This MUST be called
  // from within the corresponding worker for thread safety. Currently it is
  // called by the callback that is executed by the worker when
  // AppendRequest is being destroyed.
  virtual epoch_t getSeenEpoch(worker_id_t worker_id, logid_t logid);

  // Creates an exponential backoff timer with a callback that invokes
  // postAppend on stream and req_state. Overrided in test to create
  // MockSyncBackoffTimer.
  virtual std::unique_ptr<BackoffTimer>
  createBackoffTimer(Stream& stream, StreamAppendRequestState& req_state);

  Stream* getStream(logid_t log_id);

  std::unique_ptr<StreamAppendRequest>
  createAppendRequest(Stream& stream,
                      const StreamAppendRequestState& req_state);

 private:
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

  // Denotes if the sink has been closed for any new appendBuffered requests.
  std::atomic<bool> shutting_down_{false};

  // Settings for the exponential backoff retry timer that is used to retry
  // failed stream append requests.
  chrono_expbackoff_t<std::chrono::milliseconds> expbackoff_settings_;
};

}} // namespace facebook::logdevice
