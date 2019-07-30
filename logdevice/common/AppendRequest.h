/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/AppendRequestBase.h"
#include "logdevice/common/ClientAppendTracer.h"
#include "logdevice/common/ClientBridge.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace opentracing { inline namespace v2 {
class Tracer;
}} // namespace opentracing::v2

namespace facebook { namespace logdevice {

struct APPENDED_Header;
struct APPEND_PROBE_REPLY_Header;
class AppendProbeController;
class SenderBase;
struct Settings;
class StatsHolder;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct AppendRequestEpochMap {
  std::unordered_map<logid_t, epoch_t, logid_t::Hash> map;
};

/**
 *  @file AppendRequest is a Request that attempts to append a record to a
 *  log, sending an APPEND message over the network to the sequencer.
 */

class AppendRequest : public AppendRequestBase,
                      public SequencerRouter::Handler {
 public:
  /**
   * Constructor used by clients to submit an original AppendRequest to a
   * Processor.
   *
   * @param client  ClientBridge to make write token checks through, may be
   *                nullptr if bypassWriteTokenCheck() is called
   * @param logid   log to append the record to
   * @param payload record payload
   * @param timeout cancel the request and report E::TIMEDOUT to client
   *                if a reply is not received for this many milliseconds
   * @param callback functor to call when a reply is received or on timeout
   */
  AppendRequest(ClientBridge* client,
                logid_t logid,
                AppendAttributes attrs,
                const Payload& payload,
                std::chrono::milliseconds timeout,
                append_callback_t callback)
      : AppendRequest(client,
                      logid,
                      std::move(attrs),
                      payload,
                      timeout,
                      std::move(callback),
                      std::make_unique<SequencerRouter>(logid, this)) {}
  /**
   * Constructor used by clients to submit an original AppendRequest to a
   * Processor.
   *
   * @param client  ClientBridge to make write token checks through, may be
   *                nullptr if bypassWriteTokenCheck() is called
   * @param logid   log to append the record to
   * @param payload record payload which becomes owned by append request and
   *                will automatically be released after calling callback.
   * @param timeout cancel the request and report E::TIMEDOUT to client
   *                if a reply is not received for this many milliseconds
   * @param callback functor to call when a reply is received or on timeout
   */
  AppendRequest(ClientBridge* client,
                logid_t logid,
                AppendAttributes attrs,
                std::string payload,
                std::chrono::milliseconds timeout,
                append_callback_t callback)
      : AppendRequest(client,
                      logid,
                      std::move(attrs),
                      Payload(),
                      timeout,
                      std::move(callback),
                      std::make_unique<SequencerRouter>(logid, this)) {
    string_payload_ = std::move(payload);
    record_.payload = Payload(string_payload_.data(), string_payload_.size());
  }

  ~AppendRequest() override;

  // see Request.h
  Request::Execution execute() override;

  // see Request.h
  int getThreadAffinity(int nthreads) override {
    if (target_worker_.val_ < 0) {
      target_worker_.val_ =
          folly::hash::hash_combine(
              AppendRequest::clientThreadId, record_.logid.val_) %
          nthreads;
    }
    return target_worker_.val_;
  }

  /**
   * Called when Configuration::getLogByIDAsync() returns with the log config.
   */
  void onLogConfigAvailable(std::shared_ptr<LogsConfig::LogGroupNode> cfg);

  // implementation of the SequencerRouter::Handler interface
  void onSequencerKnown(NodeID dest, SequencerRouter::flags_t flags) override;
  void onSequencerRoutingFailure(Status status) override;

  void onProbeSendError(Status st, NodeID to) override;
  void onProbeReply(const APPEND_PROBE_REPLY_Header& reply,
                    const Address& from) override;

  // The source_type argument is used in e2e tracing to know the
  // source of the reply
  void onReplyReceived(const APPENDED_Header& reply,
                       const Address& from,
                       ReplySource source_type = ReplySource::APPEND) override;

  /**
   * Called by (1) APPEND_Message::onSent() if an attempt to send an
   * APPEND message for this request fails, and (2) on_socket_close_
   * callback if the socket through which APPEND was sent is closed
   * before a reply is received.
   *
   * @param st  error code passed to onSent() or Socket::close()
   *            (can't be E::OK)
   * @param to  destination to which we sent or attempted to send an APPEND
   * @param request_sent    true if the APPEND message has been passed to TCP
   *                        (and may have been received and executed by the
   *                        appender), false if APPEND has not yet been passed
   *                        to TCP
   */
  void noReply(Status st, const Address& from, bool request_sent);

  /**
   * Forces the append to run on a specific Worker.  If not called or called
   * with an ID < 0, a target Worker will be selected according to the
   * threading policy; see getThreadAffinity().
   */
  void setTargetWorker(worker_id_t id) {
    target_worker_ = id;
  }

  void setBufferedWriterBlobFlag() {
    buffered_writer_blob_flag_ = true;
  }

  bool getBufferedWriterBlobFlag() const {
    return buffered_writer_blob_flag_;
  }

  void setFailedToPost() {
    failed_to_post_ = true;
  }

  void bypassWriteTokenCheck() {
    bypass_write_token_check_ = true;
  }

  // Specify write token for specific request. Append will go through if
  // either per_request_token or per client token in ClientBridge will match
  // token specified in config. See Client.h addWriteToken() doc for more.
  void setPerRequestToken(std::unique_ptr<std::string> token) {
    per_request_token_ = std::move(token);
  }

  /**
   * Helper method to bump the correct counter after an append completes
   * (e.g. Stats::client::append_failed_TIMEDOUT).  See client_stats.inc for
   * the counter names.
   */
  static void bumpStatForOutcome(StatsHolder* stats, Status status);

  void setAppendProbeController(AppendProbeController* ptr) {
    append_probe_controller_ = ptr;
  }

  // Two convenience methods to be used with traffic shadowing
  logid_t getRecordLogID() const {
    return record_.logid;
  }

  std::pair<Payload, AppendAttributes> getShadowData() const {
    return std::make_pair(record_.payload, attrs_);
  }

  // Enable tracing for this request. Tracing setting should be modified
  // according to configuration
  void setTracingContext() {
    is_traced_ = true;

    if (e2e_tracer_) {
      // now that tracing is enable we should create the corresponding span
      // for this request
      request_span_ = e2e_tracer_->StartSpan("AppendRequest_initiated");
      request_span_->SetTag("destination_log_id", record_.logid.val_);
      request_span_->Finish();
    }
  }

  bool isE2ETracingOn() {
    return is_traced_;
  }

  bool hasTracerObject() {
    return e2e_tracer_ == nullptr ? false : true;
  }

  // Checks if the payload size is within bounds. If not, sets err to E::TOOBIG
  // and returns false.
  static bool checkPayloadSize(size_t payload_size,
                               size_t max_payload_size,
                               bool allow_extra);

  // see on_socket_close_ below
  class SocketClosedCallback : public SocketCallback {
   public:
    explicit SocketClosedCallback(request_id_t append_id) : rqid_(append_id) {}

    void operator()(Status st, const Address& name) override;

   private:
    request_id_t rqid_; // id of containing AppendRequest
  };

 protected: // tests can override
  AppendRequest(ClientBridge* client,
                logid_t logid,
                AppendAttributes attrs,
                const Payload& payload,
                std::chrono::milliseconds timeout,
                append_callback_t callback,
                std::unique_ptr<SequencerRouter> router);
  AppendRequest(AppendRequest&& req) noexcept;

  std::unique_ptr<SenderBase> sender_;

  ClientBridge* client_;

  // Did this AppendRequest fail to get posted to a Worker?  If so, we should
  // not invoke the callback.
  bool failed_to_post_ = false;

  // status code to pass to callback_ when request is destroyed
  Status status_;

  // Initialize a timer that'll abort this request if it takes too long to
  // complete.
  virtual void setupTimer();

  // Request the config for the log being appended to, currently just to check
  // if the write should be allowed
  virtual void fetchLogConfig();

  virtual const Settings& getSettings() const;

  // Returns the highest epoch across all successful appends for the given log.
  virtual epoch_t getSeenEpoch(logid_t log) const;

  // Called on a successful append to maintain the maximum seen epoch for the
  // log.
  virtual void updateSeenEpoch(logid_t log_id, epoch_t seen_epoch);

  // Proxy for AppendRequestBase::destroy() that, if `status' is anything
  // besides E::UNKNOWN, also sets the status that'll be reported to the user.
  virtual void destroyWithStatus(Status status) {
    if (status != E::UNKNOWN) {
      status_ = status;
    }
    destroy(); // destructor invokes callback_
  }

  // Called when a reply is received with E:REDIRECTED/E:PREEMPTED
  virtual void resetServerSocketConnectThrottle(NodeID node_id);

  // We found the sequencer and, if there was a probe, the server gave us the
  // go-ahead.  Time to send the APPEND message and wait for onReplyReceived()
  // to be invoked.
  virtual void sendAppendMessage();

  // creates the append message to be sent on the network.
  virtual std::unique_ptr<APPEND_Message> createAppendMessage();

  // Returns the set of flags that included in APPEND messages.
  virtual APPEND_flags_t getAppendFlags();

  /*
   * Including protected accessors to expose read/write access to private fields
   * from child class (mainly StreamAppendRequest). In cases where copy
   * constructors are disabled, the fields are made protected. Once we have a
   * prototype for stream appends, we should re-examine this.
   */

  lsn_t getPreviousLsn() const {
    return previous_lsn_;
  }

  const AppendAttributes& getAppendAttributes() const {
    return attrs_;
  }

  std::chrono::milliseconds getTimeout() const {
    return timeout_;
  }

  bool isTraced() const {
    return is_traced_;
  }

  const std::string& getTracingContext() const {
    return tracing_context_;
  }

  SequencerRouter::flags_t getSequencerRouterFlags() const {
    return sequencer_router_flags_;
  }

  // This field contains the target log id, request creation time, and
  // payload supplied by a client. It is passed back to the client through
  // append_callback_t when the request completes. This field is used only if
  // this AppendRequest object was constructed on a client thread.
  DataRecord record_;

 private:
  // Request creation time.  Latency is reported (for successful appends) from
  // this point up to just before we invoke the client callback.
  const std::chrono::steady_clock::time_point creation_time_;

  // request timeout
  const std::chrono::milliseconds timeout_;

  // Client callback to call when a reply is received or request times out.
  append_callback_t callback_;

  // Additional append attributes
  AppendAttributes attrs_;

  std::unique_ptr<std::string> per_request_token_;

  // If the ownership of the payload is transferred to LogDevice, this buffer
  // contains that payload (and record_.payload points to it).
  std::string string_payload_;

  // Here we cache the id of the worker on which this request must be running.
  // This is either set by setTargetWorker() or if setTargetWorker() was never
  // called, this is set by getThreadAffinity() the first time it is called.
  worker_id_t target_worker_{-1};

  // If this is a redirection of an append previously sent to another sequencer,
  // this is the LSN generated there.  Used for removing silent duplicates.
  lsn_t previous_lsn_{LSN_INVALID};

  // timer used to detect when the append timeout expires
  Timer timer_;

  // timer to trigger a cluster state refresh if it expires
  Timer cluster_state_refresh_timer_;

  // this functor is called when Socket through which we sent our
  // APPEND message closes before a reply is received
  SocketClosedCallback on_socket_close_;

  // Router object used to find a sequencer node to send an APPEND message to.
  std::unique_ptr<SequencerRouter> router_;
  // Result of sequencer routing; this is the node we send an APPEND message
  // to.
  NodeID sequencer_node_;
  SequencerRouter::flags_t sequencer_router_flags_;

  AppendProbeController* append_probe_controller_ = nullptr;

  // Client-side append tracer
  ClientAppendTracer tracer_;

  // OpenTracing tracer object used for distributed tracing.
  std::shared_ptr<opentracing::Tracer> e2e_tracer_;

  // e2e distrubuted tracing span that is created when an append request is
  // created
  std::unique_ptr<opentracing::Span> request_span_;

  // e2e distributed tracing span that is used to trace the actual execution
  // of the request
  std::unique_ptr<opentracing::Span> request_execution_span_;

  // encoding of the tracing context associated to the request execution span
  std::string tracing_context_;

  // Appends coming from BufferedWriter should have the BUFFERED_WRITER_BLOB
  // flag set in APPEND_Header.
  bool buffered_writer_blob_flag_ = false;

  bool bypass_write_token_check_ = false;

  // keeps track of whether the append response had the REDIRECT_NOT_ALIVE flag
  bool append_redirected_to_dead_node_ = false;

  // Control whether e2e tracing is on
  bool is_traced_ = false;

  // This thread-local is set on *client* threads. Processor::postRequest()
  // executing on a client thread will pass this request object to Worker
  // whose index is a function of clientThreadId and the log id. This
  // guarantees that all records created by consecutive non-blocking append()
  // calls for the same log on the same thread end up sequenced in the order
  // of those append() calls.
  static __thread unsigned clientThreadId;

  // value of clientThreadId to use for next thread to call Client::append()
  static std::atomic<unsigned> nextThreadId;

  void onWriteTokenCheckDone();

  // We found the sequencer, consulted AppendProbeController and it instructed
  // us to send a probe.  This method sends an APPEND_PROBE message to the
  // sequencer; the state machine waits for onProbeReply() to be invoked.
  void sendProbe();
  // There was an error sending an APPEND or APPEND_PROBE message to the
  // sequencer.  This method handles the error with two possible outcomes:
  // 1) The state machine rewinds, retries sequencer routing.
  // 2) The error is considered unrecoverable and destroyWithStatus() is
  // invoked, which also invokes the client callback and destroys the object.
  //
  // Due to 2) it is *not* safe to use the object after calling this method.
  void handleMessageSendError(MessageType, Status, NodeID dest);

  // Called when an append timeout expires.
  //
  // onTimeout is called when the AppendRequest timed out. This is different
  // from a connection timeout, that is failure to connect to the sequencer and
  // may trigger connecting to a different node to send the append.
  // the AppendRequest timeout is reported as an error to the caller. the
  // append may or may not have reached the sequencer and succeeded, but in any
  // case the response did not come back in time.
  void onTimeout();

  // Converts internal error codes into those exposed to the client.
  Status translateInternalError(Status status);

  // used to bump the per-node stats for the sequencer node, given the status
  void bumpPerNodeStatForOutcome(StatsHolder* holder, Status status);

  /**
   * these functions will return true if the corresponding stat should
   * have an element added to it in PerNodeTimeSeriesStats
   */
  bool shouldAddAppendSuccess(Status status);
  bool shouldAddAppendFail(Status status);

  friend class AppendRequestTest;
  friend class TestStreamWriterAppendSink;
};

}} // namespace facebook::logdevice
