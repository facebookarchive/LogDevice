/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>

#include <folly/IntrusiveList.h>
#include <folly/Preprocessor.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/InternalAppendRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/protocol/APPEND_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file Wrapper for a BufferedWriter instance that runs on sequencers,
 * allowing them to batch incoming appends together.  The benefit is fewer,
 * larger, records stored in the system, which usually improves efficiency.
 * The downsides are: different clients will get back the same LSN when
 * appending, lesser ordering guarantees (some writes may get batched, others
 * may not), and the buffering introduces a (configurable) latency penalty.
 *
 * The implementation is somewhat complex due to threading and integration
 * requirements.  The rough flow is:
 *
 * - APPEND messages come in from clients.
 *
 * - The processing path for an APPEND message consults SequencerBatching,
 *   offering to buffer the append for batching.  If SequencerBatching decides
 *   to buffer (based on settings and its discretion e.g. the payload size),
 *   it claims ownership of the append; processing of the APPEND message stops
 *   there.
 *
 * - If the incoming APPEND had itself come from a client using
 *   BufferedWriter, that is decompressed and unpacked before handing to
 *   BufferedWriter.  (BufferedWriter will later recompress when flushing.
 *   This is an extra compression round trip which costs CPU, however there is
 *   typically improved compression because the batches are larger.  It also
 *   avoids the need to handle batches-of-batches on the read path.)
 *
 * - The SequencerBatching::AppendMessageState struct tracks the state for one
 *   such incoming APPEND message, which may be one or more records now in
 *   BufferedWriter (more if we unpacked in the previous step).
 *
 * - All of the above happens on the worker thread that received the APPEND
 *   message.  However, BufferedWriter is single-threaded per log so it
 *   internally needs to hand the batch of records to the correct worker
 *   thread via a Request.
 *
 * - Once BufferedWriter decides to flush a batch for a log, it calls back
 *   into SequencerBatching::appendBuffered(), on the one worker thread
 *   handling buffering for that log within BufferedWriter.  That creates an
 *   Appender and has the Sequencer process it.  It uses most of the same code
 *   used to process incoming APPEND messages (currently in the class
 *   AppenderPrep).
 *
 * - Unlike the normal (unbatched) append path, that Appender does not
 *   directly send an APPENDED reply but invokes a callback,
 *   BatchedAppendRequest::onReplyReceived().  This callback just forwards to
 *   BufferedWriter's generic append callback.  The BufferedWriter generic
 *   append callback finds the relevant contexts (which are really pointers to
 *   AppendMessageState instances) and calls SequencerBatching::onReply() or
 *   onFailure().
 *
 * - SequencerBatching::onReply/onFailure receive as input a set of
 *   AppendMessageState instances that were in the buffered write sent to
 *   storage nodes.  These contain appends that came in on **different**
 *   workers so it needs to distribute them to the right workers via Requests.
 *
 * - Once on the correct worker, these Requests finally send APPENDED replies
 *   to the client and destroy the right AppendMessageState instances.
 *
 * - Phew!
 */

class Appender;
class Socket;
class SocketProxy;

class SequencerBatching : public BufferedWriterImpl::AppendCallbackInternal,
                          public BufferedWriterAppendSink {
 public:
  explicit SequencerBatching(Processor* processor);
  ~SequencerBatching() override;

  /**
   * Offers an incoming APPEND message for batching.  This class can decide to
   * absorb the append for batching or decline (based on settings, the
   * specific append etc).
   *
   * Returns true if SequencerBatching assumed responsibility for the append
   * (destroying the Appender), false if the normal append path should
   * proceed.
   */
  bool buffer(logid_t, std::unique_ptr<Appender>&);

  /**
   * Requests shutdown.  Must not be called on a worker thread because it
   * requires communication with workers.
   *
   * The class will reject further work; buffer() will always return false.
   * Ongoing buffer() calls will complete.
   */
  void shutDown();

  bool checkAppend(logid_t /*logid*/,
                   size_t /*payload_size*/,
                   bool /*allow_extra*/) override {
    // ld_debug("checkAppend(log=%lu, payload_size=%zu, allow_extra=%d)",
    //           logid.val_,
    //           payload_size,
    //           int(allow_extra));
    // TODO impl me maybe?
    return true;
  }

  /**
   * Called by BufferedWriter::append() to check that we're not buffering too
   * much, and appendProbe() to help clients avoid sending batches that won't
   * be accepted.
   *
   * NOTE: the return status code can get shipped to the client verbatim in an
   * APPENDED and APPEND_PROBE_REPLY message.
   */
  Status canSendToWorker() override;

  // Inherited from BufferedWriterAppendSink. "shard" here means Worker index.
  void onBytesSentToWorker(ssize_t bytes) override;

  // Inherited from BufferedWriterAppendSink. "shard" here means Worker index.
  void onBytesFreedByWorker(size_t bytes) override;

  std::pair<Status, NodeID>
  appendBuffered(logid_t,
                 const BufferedWriter::AppendCallback::ContextSet& contexts,
                 AppendAttributes attrs,
                 const Payload&,
                 BufferedWriterAppendSink::AppendRequestCallback,
                 worker_id_t target_worker,
                 int checksum_bits) override;

  void onSuccess(logid_t log_id,
                 ContextSet contexts,
                 const DataRecordAttributes& attrs) override {
    onResult(log_id,
             std::move(contexts),
             E::OK,
             NodeID(),
             attrs.lsn,
             RecordTimestamp(attrs.timestamp));
  }
  void onFailureInternal(logid_t log_id,
                         ContextSet contexts,
                         Status status,
                         NodeID redirect) override {
    onResult(log_id, std::move(contexts), status, redirect);
  }
  RetryDecision onRetry(logid_t, const ContextSet&, Status) override {
    // Don't expect to be using retries on the sequencer
    ld_check(false);
    return RetryDecision::DENY;
  }

  /**
   * We received an APPEND_PROBE message on the client.  If sequencer batching
   * is on, is there enough buffer space to take an append?
   */
  Status appendProbe();

 protected:
  virtual folly::Optional<APPENDED_Header>
  runBufferedAppend(logid_t logid,
                    AppendAttributes attrs,
                    const Payload& payload,
                    InternalAppendRequest::Callback callback,
                    APPEND_flags_t flags,
                    int checksum_bits,
                    uint32_t timeout_ms,
                    uint32_t append_message_count);

  std::unique_ptr<SenderBase> sender_;

 private:
  Processor* processor_;
  std::atomic<bool> shutting_down_{false};
  // Processes one incoming APPEND message / buffer() call
  struct AppendMessageState {
    // Worker that received the original APPEND message over the wire, on
    // which buffer() was called and on which we need to send the reply.
    // Atomic so that the BufferedWriter callbacks can read and direct to the
    // right thread.
    std::atomic<int> owner_worker;
    ClientID reply_to;
    // Socket proxy instance which ensures that clientID does not get reused
    // even if the socket closes.
    std::unique_ptr<SocketProxy> socket_proxy;
    logid_t log_id;
    request_id_t append_request_id;
    folly::IntrusiveListHook list_hook;
  };
  struct StateMachineList {
    char FB_ANONYMOUS_VARIABLE(padding)[128];
    using ListType = folly::IntrusiveList<AppendMessageState,
                                          &AppendMessageState::list_hook>;
    ListType list;
    char FB_ANONYMOUS_VARIABLE(padding)[128 - sizeof(ListType)];
  };
  // State machines grouped by owner worker.
  std::vector<StateMachineList> worker_state_machines_;

  // Needs to be destroyed first to disarm callbacks before state machines are
  // destroyed
  BufferedWriterImpl buffered_writer_;

  // Total size of uncompressed payloads buffered by SequencerBatching in the
  // sequencers.
  std::atomic<size_t> totalBufferedAppendSize_{0};

  bool shouldPassthru(const Appender& appender) const;

  // Common handling of BufferedWriter success and failure
  class DispatchResultsRequest;
  void onResult(logid_t,
                ContextSet,
                Status,
                NodeID redirect = NodeID(),
                lsn_t = LSN_INVALID,
                RecordTimestamp = RecordTimestamp::zero());

  void sendReply(const AppendMessageState& state,
                 Status status,
                 NodeID redirect = NodeID(),
                 lsn_t lsn = LSN_INVALID,
                 RecordTimestamp timestamp = RecordTimestamp::zero(),
                 uint32_t offset = 0);
};

}} // namespace facebook::logdevice
