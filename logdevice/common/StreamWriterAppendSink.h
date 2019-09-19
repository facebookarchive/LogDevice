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
    // Data record attributes sent by the AppendRequest callback is stored so
    // that it can be used to create the DataRecord when calling back
    // BufferedWriter in order.
    DataRecordAttributes record_attrs;
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
          inflight_request(nullptr) {}
  };
  using StreamRequestsMap = folly::F14NodeMap<write_stream_seq_num_t,
                                              StreamAppendRequestState,
                                              write_stream_seq_num_t::Hash>;

  // At any given time the window of pending requests looks something like this:
  //
  // ? - AppendRequest in flight
  // o - acked (LSN known)
  // - - waiting
  //
  //  --+---+---+---+---+---+---+---+---+---+---+---+--
  //    | ? | ? | o | ? | o | o | ? | ? | - | - | - |
  //  --+---+---+---+---+---+---+---+---+---+---+---+--
  //  ^                       ^       ^               ^ next_seq_num_
  //  |                       |       max_inflight_window_seq_num_
  //  |                       max_acked_seq_num_
  //  max_prefix_acked_seq_num_
  //  (lsn_of_max_prefix_acked_seq_num_)
  //
  // Invariants:
  // * Sequence numbers in pending_stream_requests_ form a contiguous range
  //   [max_prefix_acked_seq_num_ + 1, next_seq_num_ - 1].
  // * Each sequence number in
  //   [max_prefix_acked_seq_num_ + 1, max_inflight_window_seq_num_]
  //   either has an AppendRequest in flight or has already been appended
  //   successfully (last_status = E::OK, and LSN known).
  // * Sequence numbers above max_inflight_window_seq_num_ have
  //   last_status = E::UNKNOWN, no LSN, and no AppendRequest in flight.
  //   (They might have had append attempts and even successes previously, but
  //   their results were discarded.)
  // * The append at max_prefix_acked_seq_num_ + 1 is not complete yet
  //   (otherwise it would have been evicted from the window already).
  // * LSNs of acked appends are strictly increasing with increasing sequence
  //   number. If a newly acked append breaks the monotonicity, we reset some
  //   acked appends back to unacked state to restore monotonicity.
  struct Stream {
    // log id corresponding to the stream
    logid_t logid_;
    // write stream id
    write_stream_id_t stream_id_;
    // Internal map of pending stream requests from the stream writer. Some of
    // these requests have not been posted, some maybe inflight and some may
    // have been ACKed but are not part of the continuous prefix.
    StreamRequestsMap pending_stream_requests_;
    // Stream sequence number for the next append request. starts from 1.
    write_stream_seq_num_t next_seq_num_ = WRITE_STREAM_SEQ_NUM_MIN;
    // Maximum sequence number that has been acked by any sequencer such that
    // all sequence numbers before that has been ACKed.
    write_stream_seq_num_t max_prefix_acked_seq_num_ =
        WRITE_STREAM_SEQ_NUM_INVALID;
    // LSN for the last message that is part of the ACKed continuous prefix.
    lsn_t lsn_of_max_prefix_acked_seq_num_ = LSN_INVALID;
    // Maximum sequence number with an accepted ACK.
    write_stream_seq_num_t max_acked_seq_num_ = WRITE_STREAM_SEQ_NUM_INVALID;
    // max_inflight_window_seq_num_ is the maximum seq num in the inflight
    // window. Inflight window is the sub-sequence of sequence numbers after
    // max_prefix_acked_seq_num_ that are
    // (1) either currently inflight and we are expecting a reply from or
    // (2) those requests that we have already accepted a reply for.
    write_stream_seq_num_t max_inflight_window_seq_num_ =
        WRITE_STREAM_SEQ_NUM_INVALID;
    // All requests in a stream must be posted on the same target worker.
    worker_id_t target_worker_ = WORKER_ID_INVALID;
    // Latest seen epoch by the write stream - it maybe used to determine how to
    // react on a WRITE_STREAM_UNKNOWN status. For instance, if we get
    // stream unknown status from a stale sequencer, we may want to retry just
    // the request to reach the latest sequencer. On the other hand, if a newer
    // sequencer says stream unknown, we have to retry all requests from
    // (max_acked_seq_num_ + 1). Currently, we do not use this.
    epoch_t seen_epoch_ = EPOCH_INVALID;
    // Weak reference holder for stream object that is used by callback to check
    // if it can safely execute.
    WeakRefHolder<Stream> holder_;
    // Retry timer used to post the earliest pending message in the write
    // stream. Every time we triggerPrefixCallback() the timer is reset. We use
    // ExponentialBackoffTimer for this purpose. This is not thread-safe. The
    // callback sent to retry_timer is executed on the same thread as
    // `appendBuffered` was invoked from, which is also the same thread on which
    // `onCallback` is called after the `AppendRequest` has been processed.
    std::unique_ptr<BackoffTimer> retry_timer_;
    Stream(logid_t logid, write_stream_id_t stream_id)
        : logid_(logid),
          stream_id_(stream_id),
          holder_(this),
          retry_timer_(nullptr) {}

    // Sets the retry timer for the stream.
    void setRetryTimer(std::unique_ptr<BackoffTimer> retry_timer) {
      ld_check(!retry_timer_);
      retry_timer_ = std::move(retry_timer);
    }

    // Updates the seen epoch for the stream and returns true if updated.
    bool updateSeenEpoch(epoch_t epoch) {
      if (epoch != EPOCH_INVALID && epoch > seen_epoch_) {
        seen_epoch_ = epoch;
        return true;
      }
      return false;
    }

    // Calls back requests after max_prefix_acked_seq_num_ that have been ACKed
    // by the sequencer, in order. Returns the number of requests that were
    // called back.
    size_t triggerPrefixCallbacks() {
      size_t num_callbacks = 0;
      while (true) {
        auto it = pending_stream_requests_.find(
            next_seq_num(max_prefix_acked_seq_num_));
        if (it != pending_stream_requests_.end() &&
            it->second.last_status == Status::OK) {
          // Invoke callback and erase all state corresponding to seq_num.
          DataRecord record(it->second.logid,
                            it->second.payload,
                            it->second.record_attrs.lsn,
                            it->second.record_attrs.timestamp,
                            it->second.record_attrs.batch_offset,
                            it->second.record_attrs.offsets);
          it->second.callback(Status::OK, record, NodeID());
          pending_stream_requests_.erase(it);
          // Move on to the next sequence number.
          increment_seq_num(max_prefix_acked_seq_num_);
          ld_check(record.attrs.lsn > lsn_of_max_prefix_acked_seq_num_);
          lsn_of_max_prefix_acked_seq_num_ = record.attrs.lsn;
          ++num_callbacks;
        } else {
          break;
        }
      }

      if (num_callbacks > 0) {
        // If we did callback the leftmost message in the window, then we can
        // reset the retry_timer. It is maybe safe to check that it is indeed
        // inactive.
        ld_check(retry_timer_);
        retry_timer_->reset();
      }

      return num_callbacks;
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
          for (auto& req_state_value : stream->pending_stream_requests_) {
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
  StreamWriterAppendSink(
      std::shared_ptr<Processor> processor,
      std::unique_ptr<ClientBridge> bridge,
      std::chrono::milliseconds append_retry_timeout,
      chrono_expbackoff_t<std::chrono::milliseconds> expbackoff_settings);
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

  // Posts suggested_count number of  stream requests that are ready but not
  // inflight, that corresponds to the requests after sequence number
  // stream.max_inflight_window_seq_num_.
  void postNextReadyRequestsIfExists(Stream& stream, size_t suggested_count);

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
  // postNextReadyRequest() on stream with a suggested_count of 1.
  virtual std::unique_ptr<BackoffTimer> createBackoffTimer(Stream& stream);

  Stream* getStream(logid_t log_id);

  std::unique_ptr<StreamAppendRequest>
  createAppendRequest(Stream& stream,
                      const StreamAppendRequestState& req_state);

  virtual void resetPreviousAttempts(StreamAppendRequestState& req_state) {
    if (req_state.inflight_request) {
      req_state.inflight_request->cancel();
      req_state.inflight_request = nullptr;
      req_state.last_status = E::UNKNOWN;
    } else {
      req_state.last_status = E::UNKNOWN;
      req_state.record_attrs.lsn = LSN_INVALID;
    }
  }

 private:
  // Processor to which append requests are posted.
  std::shared_ptr<Processor> processor_;

  // Bridge for the client that exposes a few non-essential parts of the
  // ClientImpl class such as tracing, write token checking. We do not want to
  // include ClientImpl directly to allow running StreamWriterAppendSink on
  // servers.
  std::unique_ptr<ClientBridge> bridge_;

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

  // Checks if the earliest pending request, (also the first one in the inflight
  // window), which corresponds to sequence number
  // (stream.max_prefix_acked_seq_num_ + 1) in the stream is in flight. If not,
  // posts it.
  void ensureEarliestPendingRequestInflight(Stream& stream);

  // Checks if the LSN monotonicity property is violated by the LSN allotted to
  // message at seq_num, assigned_lsn. If a message with smaller sequence number
  // than seq_num has a larger LSN than assigned_lsn, returns false. Else, the
  // LSN monotonicity property is maintained until seq_num and hence returns
  // true.
  bool checkLsnMonotonicityUntil(Stream& stream,
                                 write_stream_seq_num_t seq_num,
                                 lsn_t assigned_lsn);

  // Checks if the LSN monotonicity property is violated by any sequence number
  // strictly after seq_num when seq_num is allotted assigned_lsn. If so,
  // rewinds the stream until that sequence number to be retried again.
  void ensureLsnMonotonicityAfter(Stream& stream,
                                  write_stream_seq_num_t seq_num,
                                  lsn_t assigned_lsn);

  // Cancel all inflight requests in stream that have a sequence number larger
  // than or equal to seq_num. For inflight requests, the requests are
  // abandoned. For already ACKed requests, we forget the ACK.
  // When we do that seq_num will be the next ready request to be posted, and
  // max_acked_seq_num will be a sequence number less than seq_num.
  void rewindStreamUntil(Stream& stream, write_stream_seq_num_t seq_num);
};

}} // namespace facebook::logdevice
