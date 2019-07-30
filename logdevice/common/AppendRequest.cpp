/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/common/AppendRequest.h"

#include <memory>

#include <folly/stats/BucketedTimeSeries.h>
#include <folly/synchronization/Baton.h>
#include <opentracing/tracer.h>

#include "logdevice/common/AppendProbeController.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/common/protocol/APPEND_PROBE_Message.h"
#include "logdevice/common/protocol/APPEND_PROBE_REPLY_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/ClientHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

static_assert(
    MAX_PAYLOAD_SIZE_INTERNAL == MAX_PAYLOAD_SIZE_PUBLIC + 32,
    "MAX_PAYLOAD_SIZE_INTERNAL should be just over MAX_PAYLOAD_SIZE_PUBLIC");

std::atomic<unsigned> AppendRequest::nextThreadId;
__thread unsigned AppendRequest::clientThreadId;

AppendRequest::AppendRequest(ClientBridge* client,
                             logid_t logid,
                             AppendAttributes attrs,
                             const Payload& payload,
                             std::chrono::milliseconds timeout,
                             append_callback_t callback,
                             std::unique_ptr<SequencerRouter> router)
    : AppendRequestBase(RequestType::APPEND),
      sender_(std::make_unique<SenderProxy>()),
      client_(client),
      status_(E::UNKNOWN),
      record_(logid,
              payload,
              LSN_INVALID,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())),
      creation_time_(std::chrono::steady_clock::now()),
      timeout_(timeout),
      callback_(std::move(callback)),
      attrs_(std::move(attrs)),
      on_socket_close_(id_),
      router_(std::move(router)),
      tracer_(client ? client->getTraceLogger() : nullptr),
      e2e_tracer_(client ? client->getOTTracer() : nullptr) {
  if (!AppendRequest::clientThreadId) {
    AppendRequest::clientThreadId =
        std::max<unsigned>(1, ++AppendRequest::nextThreadId);
  }

  if (client && client_->shouldE2ETrace()) {
    setTracingContext();
  }

  setRequestType(SRRequestType::APPEND_REQ_TYPE);

  // if it is decided that e2e tracing is on for this request, then create
  // the corresponding span
  if (is_traced_ && e2e_tracer_) {
    request_span_ = e2e_tracer_->StartSpan("AppendRequest_initiated");
    request_span_->SetTag("destination_log_id", logid.val_);
    request_span_->Finish();
  }
}

AppendRequest::AppendRequest(AppendRequest&& other) noexcept
    : AppendRequestBase(RequestType::APPEND),
      sender_(std::move(other.sender_)),
      client_(std::move(other.client_)),
      failed_to_post_(std::move(other.failed_to_post_)),
      status_(std::move(other.status_)),
      record_(other.record_.logid,
              std::move(other.record_.payload),
              LSN_INVALID,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())),
      creation_time_(other.creation_time_),
      timeout_(other.timeout_),
      callback_(std::move(other.callback_)),
      attrs_(std::move(other.attrs_)),
      per_request_token_(std::move(other.per_request_token_)),
      string_payload_(std::move(other.string_payload_)),
      target_worker_(std::move(other.target_worker_)),
      previous_lsn_(std::move(other.previous_lsn_)),
      on_socket_close_(id_),
      router_(std::make_unique<SequencerRouter>(record_.logid, this)),
      sequencer_node_(std::move(other.sequencer_node_)),
      sequencer_router_flags_(std::move(other.sequencer_router_flags_)),
      append_probe_controller_(std::move(other.append_probe_controller_)),
      tracer_(std::move(other.tracer_)),
      buffered_writer_blob_flag_(std::move(other.buffered_writer_blob_flag_)),
      bypass_write_token_check_(std::move(other.bypass_write_token_check_)),
      append_redirected_to_dead_node_(
          std::move(other.append_redirected_to_dead_node_)) {
  if (!AppendRequest::clientThreadId) {
    AppendRequest::clientThreadId =
        std::max<unsigned>(1, ++AppendRequest::nextThreadId);
  }
  setRequestType(SRRequestType::APPEND_REQ_TYPE);

  // make sure the moved-from request does not call callback
  other.setFailedToPost();
}

AppendRequest::~AppendRequest() {
  if (failed_to_post_) {
    // the request has not made it to a Worker. Do not call the callback.
    return;
  }

  const Worker* w = Worker::onThisThread();
  if (status_ == E::UNKNOWN) {
    ld_check(w->shuttingDown());
    status_ = E::SHUTDOWN;
  }

  Status client_status = translateInternalError(status_);

  StatsHolder* stats = w->stats();
  bumpStatForOutcome(stats, client_status);
  bumpPerNodeStatForOutcome(stats, client_status);

  auto latency_usec = usec_since(creation_time_);
  if (stats) {
    if (client_status == E::OK) {
      CLIENT_HISTOGRAM_ADD(stats, append_latency, latency_usec);
    }

    // bump stats to track whether REDIRECT_NOT_ALIVE flag is effective
    if (append_redirected_to_dead_node_) {
      if (client_status == E::OK) {
        STAT_INCR(stats, client.append_redirected_not_alive_success);
      } else {
        STAT_INCR(stats, client.append_redirected_not_alive_failed);
      }
    }
  }

  tracer_.traceAppend(record_.logid,
                      record_.payload.size(),
                      timeout_.count(),
                      client_status,
                      status_,
                      record_.attrs.lsn,
                      latency_usec,
                      previous_lsn_,
                      sequencer_node_);

  // finish the corresponding e2e tracing span
  if (request_execution_span_) {
    request_execution_span_->Finish();
  }

  callback_(client_status, record_);
}

Request::Execution AppendRequest::execute() {
  // transfer ownership of `this' to Worker's runningAppends map
  registerRequest();

  // initialize a timer that'll abort this request if more than timeout_
  // milliseconds pass
  setupTimer();

  if (request_span_) {
    // start span for tracing the execution flow of the append
    request_execution_span_ = e2e_tracer_->StartSpan(
        "APPEND_execution", {FollowsFrom(&request_span_->context())});
  }

  // kick off the state machine
  if (bypass_write_token_check_) {
    // No need to fetch log config in this case
    onWriteTokenCheckDone();
  } else {
    fetchLogConfig();
  }

  // ownership was already transferred to runningAppends
  return Execution::CONTINUE;
}

void AppendRequest::setupTimer() {
  if (timeout_ < std::chrono::milliseconds::max()) {
    timer_.assign([this] { onTimeout(); });
    timer_.activate(timeout_);
  }
}

void AppendRequest::fetchLogConfig() {
  ld_check(client_ != nullptr);
  request_id_t rqid = id_;
  Worker::onThisThread()->getConfig()->getLogGroupByIDAsync(
      record_.logid, [rqid](std::shared_ptr<LogsConfig::LogGroupNode> logcfg) {
        // Callback must not bind to `this', need to go through
        // `Worker::runningAppends()' in case the AppendRequest timed out while
        // waiting for the config.
        auto& runningAppends = Worker::onThisThread()->runningAppends().map;
        auto it = runningAppends.find(rqid);
        if (it != runningAppends.end()) {
          checked_downcast<AppendRequest*>(it->second.get())
              ->onLogConfigAvailable(std::move(logcfg));
        }
      });
}

void AppendRequest::onLogConfigAvailable(
    std::shared_ptr<LogsConfig::LogGroupNode> cfg) {
  if (!cfg) {
    destroyWithStatus(E::NOTFOUND);
    return;
  }
  auto attrs = cfg->attrs();

  if ((attrs.writeToken().hasValue() &&
       attrs.writeToken().value().hasValue()) &&
      !client_->hasWriteToken(attrs.writeToken().value().value()) &&
      (!per_request_token_ ||
       *per_request_token_ != attrs.writeToken().value().value())) {
    ld_error(
        "Attempting to append into log %lu which is configured to require a "
        "write token.  Are you writing into the correct log?  If so, call "
        "Client::addWriteToken() to supply the write token.",
        record_.logid.val_);
    destroyWithStatus(E::ACCESS);
    return;
  }

  onWriteTokenCheckDone();
}

void AppendRequest::onWriteTokenCheckDone() {
  router_->start();
}

void AppendRequest::onSequencerKnown(NodeID dest,
                                     SequencerRouter::flags_t flags) {
  ld_check(dest.isNodeID());
  sequencer_node_ = dest;
  sequencer_router_flags_ = flags;

  // Check if we should send a probe
  if (append_probe_controller_ != nullptr &&
      append_probe_controller_->shouldProbe(dest, record_.logid)) {
    sendProbe();
  } else {
    sendAppendMessage();
  }
}

void AppendRequest::onSequencerRoutingFailure(Status status) {
  destroyWithStatus(status);
}

void AppendRequest::sendProbe() {
  APPEND_PROBE_Header header{
      id_,
      record_.logid,
      record_.payload.size(),
      // TODO set `unbatched_size' when coming from BufferedWriter
      -1,
      APPEND_PROBE_flags_t(0)};
  auto msg = std::make_unique<APPEND_PROBE_Message>(header);
  // make sure that on_socket_close_ is not active in case we're resending this
  // message
  on_socket_close_.deactivate();

  std::unique_ptr<opentracing::Span> probe_message_send_span;

  // create span for probe message
  if (request_execution_span_) {
    probe_message_send_span = e2e_tracer_->StartSpan(
        "PROBE_Message_send", {ChildOf(&request_execution_span_->context())});
    probe_message_send_span->SetTag("log_id", record_.logid.val_);
  }

  auto set_status_span_tag =
      [&probe_message_send_span](std::string status) -> void {
    probe_message_send_span->SetTag("status", status);
    probe_message_send_span->Finish();
  };

  int rv =
      sender_->sendMessage(std::move(msg), sequencer_node_, &on_socket_close_);
  if (rv != 0) {
    onProbeSendError(err, sequencer_node_);

    if (probe_message_send_span) {
      probe_message_send_span->SetTag("error", error_name(err));
      set_status_span_tag("error_sending");
    }

  } else {
    if (probe_message_send_span) {
      set_status_span_tag("success");
    }
  }
}

void AppendRequest::onProbeSendError(Status st, NodeID to) {
  if (err == E::PROTONOSUPPORT) {
    // The server just doesn't support probes yet, proceed as
    // if we got an OK reply.
    sendAppendMessage();
  } else {
    WORKER_STAT_ADD(client.append_probes_bytes_unsent_probe_send_error,
                    record_.payload.size());
    handleMessageSendError(MessageType::APPEND_PROBE, st, to);
    // `this' may be destroyed now, must return
    return;
  }
}

void AppendRequest::onProbeReply(const APPEND_PROBE_REPLY_Header& reply,
                                 const Address& from) {
  ld_check(!from.isClientAddress());
  // NOTE: We currently don't expect to ever see stale replies because the
  // state machine does not rewind while messages are in flight (only if we
  // get a PREEMPTED reply).  If this changes (e.g. we add retries) this would
  // need changing to handle stale replies and avoid ABA issues, possibly by
  // versioning the state machine and including the version in every message.
  ld_check(from.asNodeID() == sequencer_node_);
  if (reply.status == E::OK) {
    sendAppendMessage();
  } else {
    // Record that we saved some bandwidth.  Impact!
    WORKER_STAT_ADD(client.append_probes_bytes_saved, record_.payload.size());

    // Pretend we received an APPENDED error reply and call the normal error
    // reply handler
    APPENDED_Header hdr{reply.rqid,
                        LSN_INVALID,
                        RecordTimestamp::zero(),
                        reply.redirect,
                        reply.status,
                        APPENDED_flags_t(0)};
    onReplyReceived(hdr, from, ReplySource::PROBE);
  }
}

std::unique_ptr<APPEND_Message> AppendRequest::createAppendMessage() {
  APPEND_flags_t append_flags = getAppendFlags();
  APPEND_Header header = {
      id_,
      record_.logid,
      getSeenEpoch(record_.logid),
      uint32_t(std::min<decltype(timeout_)::rep>(timeout_.count(), UINT_MAX)),
      append_flags};

  return std::make_unique<APPEND_Message>(
      header,
      previous_lsn_,
      // The key and payload may point into user-owned memory or be backed by a
      // std::string contained in this class.  Do not transfer ownership to
      // APPEND_Message.
      attrs_,
      PayloadHolder(record_.payload, PayloadHolder::UNOWNED),
      tracing_context_);
}

void AppendRequest::sendAppendMessage() {
  const NodeID dest = sequencer_node_;

  // The tracing span associated with this request
  // Should be instantiated if this request is traced
  std::unique_ptr<opentracing::Span> append_message_sent_span;

  // If there is a request_span associated with this request, it means that
  // e2e tracing is on for this request and so we can create tracing spans
  if (request_execution_span_) {
    // create tracing span for this append message
    append_message_sent_span = e2e_tracer_->StartSpan(
        "APPEND_Message_send", {ChildOf(&request_execution_span_->context())});

    // Add append information to span
    append_message_sent_span->SetTag("log_id", record_.logid.val_);
    append_message_sent_span->SetTag("dest_node", dest.toString());
  }

  // Obtain tracing information
  if (request_execution_span_) {
    // inject the request_execution_span_ context to be propagated
    // we want further spans (on the server side) to be childof this span
    std::stringstream in_stream;

    e2e_tracer_->Inject(request_execution_span_->context(), in_stream);
    tracing_context_ = in_stream.str();
  }

  auto msg = createAppendMessage();

  // make sure that on_socket_close_ is not active in case we're resending this
  // message
  on_socket_close_.deactivate();

  ld_spew("Sending an append for log:%lu to node %s",
          record_.logid.val_,
          dest.toString().c_str());

  // We want to set the status of the sending operation as tag to the span
  auto set_status_span_tag =
      [&append_message_sent_span](std::string status) -> void {
    append_message_sent_span->SetTag("status", status);
    append_message_sent_span->Finish();
  };

  int rv = sender_->sendMessage(std::move(msg), dest, &on_socket_close_);
  if (rv != 0) {
    handleMessageSendError(MessageType::APPEND, err, dest);
    // Object may be destroyed, must return

    if (append_message_sent_span) {
      // There was an error while sending the append message, note this to
      // corresponding span
      append_message_sent_span->SetTag("error", error_name(err));
      set_status_span_tag("error_sending");
    }

    return;
  }

  if (append_message_sent_span) {
    set_status_span_tag("sent");
  }
}

void AppendRequest::handleMessageSendError(MessageType type,
                                           Status st,
                                           const NodeID dest) {
  ld_check(type == MessageType::APPEND || type == MessageType::APPEND_PROBE);
  RATELIMIT_ERROR(std::chrono::seconds(1),
                  5,
                  "Failed to queue %s "
                  "message for log %lu of size %zu for sending to %s: %s",
                  messageTypeNames()[int(type)].c_str(),
                  record_.logid.val_,
                  record_.payload.size(),
                  Sender::describeConnection(Address(dest)).c_str(),
                  error_description(st));

  // Need to handle error codes from `Sender::send()' (synchronous failure)
  // and `Message::onSent()' (async failure)
  switch (st) {
    case E::CANCELLED:
    case E::PEER_UNAVAILABLE:
    case E::DESTINATION_MISMATCH:
    case E::INVALID_CLUSTER:
    case E::PEER_CLOSED:
    case E::UNROUTABLE:
    case E::DISABLED:
    case E::NOTINCONFIG:
    case E::TIMEDOUT: // connection timeout
    case E::CONNFAILED:
      // Let noReply() handle these errors
      noReply(st, Address(dest), /* request_sent = */ false);
      return;

    case E::NOMEM:
      st = E::SYSLIMIT;
      break;

    case E::ACCESS:
    case E::TOOBIG:
    case E::SYSLIMIT:
    case E::INTERNAL:
    case E::NOBUFS:
    case E::SHUTDOWN:
      // Fail the append in all of the remaining cases.
      break;
    default:
      ld_error("Got an unexpected error code %s from Sender::sendMessage() or "
               "Message::onSent() to %s",
               error_name(st),
               Sender::describeConnection(Address(dest)).c_str());
      ld_check(false);
      st = E::INTERNAL;
      break;
  }

  destroyWithStatus(st);
}

void AppendRequest::onReplyReceived(const APPENDED_Header& reply,
                                    const Address& from,
                                    ReplySource source_type) {
  ld_check(!from.isClientAddress());
  // NOTE: We currently don't expect to ever see stale replies because the
  // state machine does not rewind while messages are in flight (only if we
  // get a PREEMPTED reply).  If this changes (e.g. we add retries) this would
  // need changing to handle stale replies and avoid ABA issues, possibly by
  // versioning the state machine and including the version in every message.
  ld_check(from.asNodeID() == sequencer_node_);
  ld_check(reply.rqid == id_);

  if (append_probe_controller_) {
    append_probe_controller_->onAppendReply(
        from.asNodeID(), record_.logid, reply.status);
  }

  status_ = reply.status;

  auto create_reply_span = [this, from](std::string description) -> void {
    auto reply_span = e2e_tracer_->StartSpan(
        description, {ChildOf(&request_execution_span_->context())});
    reply_span->SetTag("source_node_id", from.asNodeID().toString());
    reply_span->SetTag("reply_status", error_name(status_));
    reply_span->Finish();
  };

  // if we have a tracing span associated to the request,
  // create tracing span for this APPENDED_message
  if (request_execution_span_ && (source_type == ReplySource::APPEND)) {
    create_reply_span("APPENDED_Message_receive");
  }

  if (request_execution_span_ && (source_type == ReplySource::PROBE)) {
    create_reply_span("PROBE_Reply_receive");
  }

  switch (status_) {
    case E::OK:
      record_.attrs.lsn = reply.lsn;
      // If the sequencer supports returning the timestamp it applied and
      // stored with the record, update/override the timestamp set by
      // the client when it initiated the append.
      //
      // NOTE: Even servers that support returning timestamps will return a
      //       zero timestamp in the case that a previously attempted but
      //       preempted store from this AppendRequest was determined to
      //       be successful.  In this case, the client must rely on the
      //       timestamp provided in the preemption response. See E::PREEMPTED
      //       handling below.
      if (reply.timestamp != RecordTimestamp::zero()) {
        record_.attrs.timestamp = reply.timestamp.toMilliseconds();
      }
      updateSeenEpoch(record_.logid, lsn_to_epoch(reply.lsn));
      FOLLY_FALLTHROUGH;
    case E::TOOBIG:
    case E::BADPAYLOAD:
    case E::NOSPC:
    case E::OVERLOADED:
    case E::SEQNOBUFS:
    case E::SEQSYSLIMIT:
    case E::DISABLED:
    case E::CANCELLED:
    case E::NOSEQUENCER:
    case E::NOTINSERVERCONFIG:
      // Append was either successful or not, but in any case we reached the
      // right node (didn't get redirected)
      break;
    case E::ISOLATED:
    case E::NOTREADY:
      // Nodes reply with E::NOTREADY when they're just starting up and are not
      // yet ready to execute appends. This is a signal to clients to select a
      // different node (same as below).
    case E::SHUTDOWN:
    case E::REBUILDING:
      // Node replied, but is not capable of running sequencers at the moment.
      // Try to pick some other node. Note that if we got redirected here by
      // some other node, it's fine to fail an append (which onNodeUnvailable
      // will indeed do), as it indicates that other nodes haven't picked up
      // that this one is not available; however, that should be rare in a
      // healthy cluster.
      router_->onNodeUnavailable(from.id_.node_, status_);
      return;
    case E::PREEMPTED:
      if (!(reply.flags & APPENDED_Header::NOT_REPLICATED)) {
        // The server lets us know if a record was not replicated anywhere
        // before getting preempted. if that flag is not set, a copy may or may
        // not have been stored on a storage node. In that case, pass the
        // previous LSN in the next attempt to let the new sequencer go through
        // the silent duplicate check.
        previous_lsn_ = reply.lsn;

        // Update to use the timestamp from the preempted append. If the new
        // sequencer confirms that this store was successful, this is the
        // timestamp that will match the record at previous_lsn_.
        if (reply.timestamp != RecordTimestamp::zero()) {
          record_.attrs.timestamp = reply.timestamp.toMilliseconds();
        }
      }
    // fallthrough.
    case E::REDIRECTED: {
      ld_check(reply.redirect.isNodeID());
      NodeID redirect;
      if (reply.flags & APPENDED_Header::REDIRECT_NOT_ALIVE) {
        append_redirected_to_dead_node_ = true;
        // if the server is telling us that the node it is redirecting to
        // appears to be dead, we will ignore the redirection and retry the
        // append to the same server with force flags. This may happen if
        // the server cannot handle the situation itself and reactivate the
        // sequencer.
        // we also mark the node as dead in the cluster state.
        router_->onDeadNode(reply.redirect, E::DISABLED);
      } else {
        // Request was redirected to or preempted by a seq running on
        // "reply.redirect" node. Client could have tried connecting
        // unsuccessfully to redirected node in previous attempts causing
        // the future attempts to be throttled, even if the node came back
        // up. So when a request is redirected, trust the server and reset
        // the throttle so that we attempt to establish connection.
        resetServerSocketConnectThrottle(reply.redirect);
        redirect = reply.redirect;
      }
      router_->onRedirected(from.id_.node_, redirect, status_);
      return;
    }
    case E::ACCESS:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      3,
                      "ACCESS ERROR From server %s. Client does not have "
                      "required permissions to perform an append to %lu.",
                      (from.valid() ? Sender::describeConnection(from).c_str()
                                    : "[LOCAL SEQUENCER]"),
                      static_cast<uint64_t>(record_.logid));
      break;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "PROTOCOL ERROR: got an unexpected error code %s from "
                      "server %s",
                      error_name(reply.status),
                      (from.valid() ? Sender::describeConnection(from).c_str()
                                    : "[LOCAL SEQUENCER]"));
      status_ = E::INTERNAL;
      break;
  }

  // destroys *this, destructor calls callback_
  destroy();
}

void AppendRequest::noReply(Status st, const Address& to, bool request_sent) {
  if (!request_sent && request_execution_span_) {
    // the request_sent being false means that the append_message was not sent
    // if we have e2e tracing on for this request then we should add a new span
    auto append_message_sent_error_span =
        e2e_tracer_->StartSpan("APPEND_Message_sending_failure",
                               {ChildOf(&request_execution_span_->context())});
    append_message_sent_error_span->SetTag("destination", to.toString());
    append_message_sent_error_span->Finish();
  }

  switch (st) {
    case E::PEER_UNAVAILABLE:
    case E::CANCELLED:
      if (!request_sent) {
        // let's try a different node if possible
        router_->onNodeUnavailable(to.asNodeID(), st);
        return;
      }
      status_ = st;
      break;
    case E::SHUTDOWN:
      // We know for sure that request was never accepted by server,
      // let's see if it can be retried.
      router_->onNodeUnavailable(to.asNodeID(), st);
      return;
    case E::ACCESS:
      status_ = st;
      break;
    case E::PROTONOSUPPORT:
      ld_check(!request_sent);
      status_ = E::CONNFAILED;
      break;
    case E::DESTINATION_MISMATCH:
    case E::INVALID_CLUSTER:
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Failed to establish connection to %s due to %s. "
                     "The error will be translated to E::CONNFAILED and sent"
                     "back to the client.",
                     Sender::describeConnection(to).c_str(),
                     error_name(st));
      status_ = E::CONNFAILED;
      break;
    case E::BADMSG:
    case E::PROTO:
    case E::PEER_CLOSED:
      status_ = request_sent ? E::PEER_CLOSED : E::CONNFAILED;
      break;
    case E::UNROUTABLE:
    case E::DISABLED:
      // Convert DISABLED and UNROUTABLE to CONNFAILED before calling
      // onNodeUnavailable();
      // DISABLED has a different meaning (see include/Client.h) when reported
      // to clients.
      st = E::CONNFAILED;
    case E::NOTINCONFIG:
    case E::TIMEDOUT:
      // This is a connection timeout. Server timeouts are handled differently
      // by onTimeout()
    case E::CONNFAILED:
      // We were unable to reach the node. Call onNodeUnavailable() to decide
      // whether we should try a different node or just fail an append.
      ld_check(!to.isClientAddress());
      router_->onNodeUnavailable(to.asNodeID(), st);
      return;
    case E::SSLREQUIRED:
      // Connection is being upgraded to SSL. Call onShouldRetry() to retry
      // sending to the same node.
      router_->onShouldRetry(to.asNodeID(), st);
      return;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Unexpected status %s in failure to send notification "
                      "while sending an APPEND to %s",
                      error_name(st),
                      Sender::describeConnection(to).c_str());
      ld_check(false);
      status_ = E::INTERNAL;
      break;
  }

  // mark the node as dead
  router_->onDeadNode(to.asNodeID(), status_);

  destroy();
}

Status AppendRequest::translateInternalError(Status status) {
  switch (status) {
    case E::UNROUTABLE:
    case E::PROTONOSUPPORT:
    case E::DESTINATION_MISMATCH:
    case E::INVALID_CLUSTER:
      // These connection-related errors are reported as CONNFAILED.
      return E::CONNFAILED;
    case E::NOTINCONFIG:
    case E::NOTREADY:
    case E::REBUILDING:
      // If these ever make it to the client, return NOSEQUENCER instead.
      return E::NOSEQUENCER;
    default:
      break;
  }

  return status;
}

void AppendRequest::onTimeout() {
  RATELIMIT_INFO(std::chrono::seconds(1),
                 10,
                 "AppendRequest rqid:%lu log:%lu, %lums timeout expired",
                 id_.val_,
                 record_.logid.val_,
                 timeout_.count());

  if (request_execution_span_) {
    auto append_request_timeout_span =
        e2e_tracer_->StartSpan("APPEND_Request_timeout",
                               {ChildOf(&request_execution_span_->context())});
    append_request_timeout_span->SetTag("dest_log_id", record_.logid.val_);
    append_request_timeout_span->Finish();
  }

  if (sequencer_node_.isNodeID()) {
    // If the node we sent an APPEND to is not responding (for example, it
    // might be overloaded, or the connection to it broken), let's ask other
    // nodes about the health of the nodes of the cluster and update the
    // ClusterState object. This may mark `sequencer_node_' node as
    // dead, and potentially other nodes as well (if for instance a whole rack
    // went down)
    auto cs = Worker::getClusterState();
    if (cs) {
      cs->refreshClusterStateAsync();
    }

    if (append_probe_controller_) {
      append_probe_controller_->onAppendReply(
          sequencer_node_, record_.logid, E::TIMEDOUT);
    }
  }

  destroyWithStatus(E::TIMEDOUT);
}

void AppendRequest::resetServerSocketConnectThrottle(NodeID node_id) {
  Worker::onThisThread()->sender().resetServerSocketConnectThrottle(node_id);
}

bool AppendRequest::checkPayloadSize(size_t payload_size,
                                     size_t max_payload_size,
                                     bool allow_extra) {
  size_t max_size = max_payload_size;
  if (allow_extra) {
    max_size += MAX_PAYLOAD_EXTRA_SIZE;
  }
  ld_check(max_payload_size <= MAX_PAYLOAD_SIZE_PUBLIC);
  ld_check(max_size <= MAX_PAYLOAD_SIZE_INTERNAL);
  if (payload_size > max_size) {
    err = E::TOOBIG;
    return false;
  }
  return true;
}

APPEND_flags_t AppendRequest::getAppendFlags() {
  APPEND_flags_t append_flags = 0;

  append_flags |= appendFlagsForChecksum(getSettings().checksum_bits);
  if (buffered_writer_blob_flag_) {
    append_flags |= APPEND_Header::BUFFERED_WRITER_BLOB;
  }
  if (sequencer_router_flags_ & SequencerRouter::REDIRECT_CYCLE) {
    // `sequencer_node_' is part of a redirection cycle. Include the NO_REDIRECT
    // flags to break it.
    append_flags |= APPEND_Header::NO_REDIRECT;
  }
  if (sequencer_router_flags_ & SequencerRouter::PREEMPTED) {
    // `sequencer_node_' is known to be preempted for the epoch this record
    // belongs to. By setting this flag we're asking the node to reactivate the
    // sequencer regardless.
    append_flags |= APPEND_Header::REACTIVATE_IF_PREEMPTED;
  }
  if (!attrs_.optional_keys.empty()) {
    append_flags |= APPEND_Header::CUSTOM_KEY;
  }
  if (attrs_.counters.hasValue()) {
    append_flags |= APPEND_Header::CUSTOM_COUNTERS;
  }
  if (previous_lsn_ != LSN_INVALID) {
    append_flags |= APPEND_Header::LSN_BEFORE_REDIRECT;
  }
  if (is_traced_) {
    append_flags |= APPEND_Header::E2E_TRACING_ON;
  }
  return append_flags;
}

const Settings& AppendRequest::getSettings() const {
  return Worker::settings();
}

epoch_t AppendRequest::getSeenEpoch(logid_t log_id) const {
  auto& map = Worker::onThisThread()->appendRequestEpochMap().map;

  auto it = map.find(log_id);
  return it != map.end() ? it->second : EPOCH_INVALID;
}

void AppendRequest::updateSeenEpoch(logid_t log_id, epoch_t seen_epoch) {
  auto& map = Worker::onThisThread()->appendRequestEpochMap().map;

  auto it = map.find(log_id);
  if (it != map.end()) {
    it->second = std::max(it->second, seen_epoch);
  } else {
    map.insert(std::make_pair(log_id, seen_epoch));
  }
}

void AppendRequest::SocketClosedCallback::operator()(Status st,
                                                     const Address& name) {
  // notify AppendRequest of failure so that it can quickly unblock the client

  Worker* w = Worker::onThisThread();
  ld_check(w);

  ld_debug("AppendRequest::SocketClosedCallback called for socket %s "
           "with status %s ",
           Sender::describeConnection(name).c_str(),
           error_name(st));

  auto pos = w->runningAppends().map.find(rqid_);
  if (pos != w->runningAppends().map.end()) {
    ld_check(pos->second);
    checked_downcast<AppendRequest*>(pos->second.get())
        ->noReply(st, name, /*request_sent=*/true);
  } else {
    // This must never happen. We are in a method of a subobject of
    // AppendRequest object we were looking for. That AppendRequest
    // must never be removed from runningAppends map without getting
    // destroyed, which would remove this callback from its Socket's
    // callback list.
    ld_critical("INTERNAL ERROR: 'socket closed' callback did not find "
                "request id %" PRIu64 " in the map of running Append "
                "requests. Send status was %s.",
                uint64_t(rqid_),
                error_description(st));
    ld_check(false);
  }
}

void AppendRequest::bumpStatForOutcome(StatsHolder* stats, Status status) {
  switch (status) {
    case E::OK:
      STAT_INCR(stats, client.append_success);
      break;
    case E::SHUTDOWN:
      // Exempt SHUTDOWN since it is not really an error and we don't want to
      // track it.
      break;

#define HANDLE_CODE(code)                          \
  case E::code:                                    \
    STAT_INCR(stats, client.append_failed_##code); \
    break;

      HANDLE_CODE(TIMEDOUT)
      HANDLE_CODE(NOTFOUND)
      HANDLE_CODE(NOSEQUENCER)
      HANDLE_CODE(NOTINSERVERCONFIG)
      HANDLE_CODE(CONNFAILED)
      HANDLE_CODE(PEER_CLOSED)
      HANDLE_CODE(TOOBIG)
      HANDLE_CODE(NOBUFS)
      HANDLE_CODE(SYSLIMIT)
      HANDLE_CODE(SEQNOBUFS)
      HANDLE_CODE(SEQSYSLIMIT)
      HANDLE_CODE(NOSPC)
      HANDLE_CODE(OVERLOADED)
      HANDLE_CODE(DISABLED)
      HANDLE_CODE(ACCESS)
      HANDLE_CODE(INTERNAL)
      HANDLE_CODE(INVALID_PARAM)
      HANDLE_CODE(BADPAYLOAD)
      HANDLE_CODE(ISOLATED)
      HANDLE_CODE(CANCELLED)
      HANDLE_CODE(PEER_UNAVAILABLE)

#undef HANDLE_CODE

    default:
      // Bucket any other errors into one counter.  This shouldn't happen if
      // we keep the above list in sync with return values from
      // Client::appendSync().
      RATELIMIT_WARNING(std::chrono::hours(1),
                        1,
                        "no counter for append error %s",
                        error_name(status));
      STAT_INCR(stats, client.append_failed_other);
  }
}

void AppendRequest::bumpPerNodeStatForOutcome(StatsHolder* stats,
                                              Status status) {
  if (!sequencer_node_.isNodeID()) {
    // no sequencer node to register stats for
    return;
  }
  if (shouldAddAppendSuccess(status)) {
    PER_NODE_STAT_ADD(stats, sequencer_node_, AppendSuccess);
  } else if (shouldAddAppendFail(status)) {
    PER_NODE_STAT_ADD(stats, sequencer_node_, AppendFail);
  }
}

bool AppendRequest::shouldAddAppendSuccess(Status status) {
  return status == Status::OK;
}

bool AppendRequest::shouldAddAppendFail(Status status) {
  switch (status) {
    case Status::TIMEDOUT:
    case Status::PEER_CLOSED:
    case Status::SEQNOBUFS:
    case Status::CONNFAILED:
    case Status::DISABLED:
    case Status::SEQSYSLIMIT:
      return true;
    default:
      return false;
  }
}
}} // namespace facebook::logdevice
