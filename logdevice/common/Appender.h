/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>
#include <chrono>
#include <memory>
#include <string>
#include <type_traits>

#include <boost/intrusive/unordered_set.hpp>
#include <folly/Optional.h>
#include <folly/small_vector.h>
#include <opentracing/tracer.h>

#include "logdevice/common/AppenderTracer.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/IntrusiveUnorderedMap.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/RecipientSet.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file an Appender is a state machine for appending a single record to a log.
 *      An Appender is responsible for sending STORE messages with copies of the
 *      record being appended to a set of storage nodes, called the "copyset".
 *      The size of the copyset is equal to the value of the replication factor
 *      plus the number of extras.
 *
 *      The Appender resides in the EpochSequencer's SlidingWindow until it is
 *      reaped.
 *
 *      Once the Appender fully replicates the record, it "retires", meaning it
 *      indicates the EpochSequencer that it finished replicating (or that it
 *      was preempted). Retiring an Appender may cause one or more Appenders to
 *      be "reaped", meaning they were remove from the EpochSequencer's sliding
 *      window and the corresponding LSNs are released for delivery. At this
 *      point the Appender is allowed to send releases.
 *
 *      When it is reaped, the Appender was removed from the SlidingWindow but
 *      is still in the Worker's activeAppenders() map. When the Appender
 *      completes its state machine, it removes itself from this map so that
 *      future stale replies from storage nodes get discarded.
 *      When the Appender completed its state machine *and* is reaped it deletes
 *      itself. Note that currently Appender always completes before it is
 *      reaped, but the code is architected in a way that makes it possible to
 *      call complete() after onReaped() is called if Appender needs to do more
 *      clean-up work.
 *
 *      Waves and store timeout
 *      =======================
 *
 *      A wave is the actual process of selecting a copyset, sending STORE
 *      messages to the recipients and waiting for replies until we get to the
 *      end of the last stage.
 *
 *      At the beginning of a wave, a timer "the store timer" is started. If it
 *      triggers before we got a chance to fully replicate the record (before
 *      we are done with the replication stage), we start again with a new
 *      wave.
 */

class EpochSequencer;
class PayloadHolder;
class SenderBase;
class Sequencer;
class SocketProxy;
class STORE_Message;
class TailRecord;
class TraceLogger;
class Worker;
enum class ReleaseType : uint8_t;
struct Address;
struct APPENDED_Header;
struct Settings;

/**
 * The return type of runAppender() methods in various classes (e.g. Sequencer,
 * EpochSequencer, AppenderPrep, MetaDataLogWriter).
 *
 * SUCCESS_*: runAppender() hasn't encountered any errors so far.  runAppender()
 * will ensure a reply is sent, the caller doesn't need to.
 *
 * ERROR_*: runAppender() encountered an error.  It's the caller's responsibilty
 * to call appender->sendError().
 *
 * *_KEEP: runAppender() has taken ownership of the Appender; don't delete it!
 *
 * *_DELETE: runAppender() is done with the Appender, caller should delete it.
 */
enum class RunAppenderStatus { SUCCESS_KEEP, SUCCESS_DELETE, ERROR_DELETE };

class Appender : public IntrusiveUnorderedMapHook {
 public:
  /**
   * This constructor is used by APPEND_Message::onReceived() for executing
   * an append() request on a shared log.
   *
   * @param lsn_before_redirect If this append was previously sent to another
   * sequencer and then redirected, this is the LSN from that previous
   * sequencer.  It is LSN_INVALID if either it wasn't redirected, or the
   * previous sequencer didn't give it an LSN.
   */
  Appender(Worker* worker,
           std::shared_ptr<TraceLogger> trace_logger,
           std::chrono::milliseconds client_timeout,
           request_id_t append_request_id,
           STORE_flags_t passthru_flags,
           logid_t log_id,
           AppendAttributes attrs,
           PayloadHolder payload,
           ClientID return_address,
           epoch_t seen_epoch,
           size_t full_appender_size,
           lsn_t lsn_before_redirect,
           std::shared_ptr<opentracing::Tracer> e2e_tracer = nullptr,
           std::shared_ptr<opentracing::Span> appender_span = nullptr);

  virtual ~Appender();

  /**
   * Reason for the retirement of the Appender. Used to determine actions when
   * the appender retires.
   */
  enum class RetireReason {
    // Appender retires because it finishes its replication
    STORED = 0,
    // preempted by another sequencer running in higher epoch
    PREEMPTED,
    // Worker running the Appender is shutting down
    SHUTDOWN,
    // Appender is aborted (e.g., its epoch has been evicted and all appenders
    // in the epoch need to be aborted)
    ABORTED
  };

  /**
   * If Appender is fully replicated on reaped. This tag is passed to
   * sequencers to determine if LNG can be advanced and release can be sent.
   */
  enum class FullyReplicated { YES, NO };

  /**
   * Instances of this functor class are passed to SlidingWindow::retire() after
   * an appender stored `replication_` copies of the record, and received
   * successful STORED acknowledgements from sync_leaders_ leftmost nodes in the
   * copyset.
   */
  class Reaper {
   public:
    void operator()(Appender* a) {
      a->onReaped();
    }
  };

  // Used by AppenderMap of IntrusiveUnorderedMap
  class KeyExtractor {
   public:
    const RecordID& operator()(const Appender& a) const {
      return a.store_hdr_.rid;
    }
  };

  // Used by AppenderMap of IntrusiveUnorderedMap, called on map destruction
  class Disposer {
   public:
    void operator()(Appender* a);
  };

  /**
   * @return true iff start() has been successfully called on this appender
   */
  bool started() {
    return started_;
  }

  /**
   * Start this appender state machine.
   *
   * @param   epoch_sequencer    EpochSequencer for managing the epoch in which
   *                             the appender runs. It is starting this Appender
   *                             object. The appender will hold a reference to
   *                             the EpochSequencer once started.
   *
   * @param   lsn                LSN assigned to appender's record by
   *                             _epoch_sequencer_
   *
   * @return 0 on success, -1 if the state machine could not be started,
   *         sets err to
   *      SYSLIMIT     if a system-wide resource limit, such as the number
   *                   of ephemeral ports has been reached
   *      INPROGRESS   if Appender has already been started.
   *                   Debug build asserts.
   *      INTERNAL     if another Appender is currently active for the same
   *                   RecordID as this one, or if some other internal error
   *                   occurs. Logs the error. Debug build asserts.
   */
  virtual int start(std::shared_ptr<EpochSequencer> epoch_sequencer, lsn_t lsn);

  /**
   * Mark this Appender as retired in its EpochSequencer's SlidingWindow. This
   * may cause this and other Appenders in that SlidingWindow to be reaped.
   *
   * Note on implementation: although retire() could cause other Appenders in
   * the sliding window to be reaped and destroyed. retire() should *never*
   * directly destroy `this' Appender object, despite that onReaped() might be
   * synchonronously called *inside* retire(). The destruction of Appender
   * should happen on the subsequent onComplete() calls.
   *
   * Must follow start().
   */
  void retire(RetireReason reason);

  /**
   * Abort the Appender object regardless of its completion status. Used in
   * situations like epoch eviction and cancel long-expired appenders because
   * of domain isolation.
   *
   * @param linked   defaults to true. false if the Appender is already
   *                 unlinked (e.g., in Worker destruction)
   */
  void abort(bool linked = true);

  /**
   * Use/Return to client the provided timestamp instead of the one
   * calculated during Appender startup.
   *
   * Used when, after recovery, it is determined that the Appender represents
   * a retry for a record that was successfully stored in the previous epoch.
   * No additional store processing is performed and metadata about the
   * previously successful store is returned instead.
   */
  void overrideTimestamp(RecordTimestamp ts) {
    getStoreHeader().timestamp = ts.toMilliseconds().count();
  }

  /**
   * Send a single STORE message to the specified destination. Log and triage
   * errors.
   *
   * Pre-condition: Sender::checkConnection() was called on this worker thread
   * in this event loop iteration, and returned AVAILABLE or AVAILABLE_NOCHAIN.
   *
   * @param  copyset  copyset to put in message header. Its size is in
   *                  store_hdr_.copyset_size. If the message is
   *                  being sent through a chain (CHAIN flag is set in
   *                  store_hdr_.flags), this argument also defines the chain
   *                  of recipients.
   * @param  copyset_offset  offset of recipient in copyset, designates message
   *                         destination and copied into message header
   * @param  block_starting_lsn  the lsn where the block starts. See docblock
   *                             for CopySetManager::getCopySet()
   * @param  flags    a bitset of flags to OR into message header flags
   *
   * @return 1 if a STORE message was successfully sent
   *         0 if the message was not sent because of a transient error.
   *           An immediate redelivery to another destination is advised.
   *           -1  if the message was not sent because of a more serious error,
   *               The caller is not advised to immediately resend to another
   *               destination. It should either wait until STORE timeout or
   *               shut down the sequencer alltogether. err is set to:
   *           NOBUFS      if we hit the output evbuffer space limit
   *           SYSLIMIT    if the box is out of ephemeral ports or some other
   *                       system resource
   *           INTERNAL    if an internal error occurred (debug build asserts)
   */
  int sendSTORE(const StoreChainLink copyset[],
                copyset_off_t copyset_offset,
                folly::Optional<lsn_t> block_starting_lsn,
                STORE_flags_t flags,
                std::shared_ptr<opentracing::Span> store_span = nullptr);

  /**
   * Called by a Recipient whose initial attempt to send a STORE message was
   * declined by traffic shaping once bandwidth is available.
   */
  void sendDeferredSTORE(std::unique_ptr<STORE_Message>, ShardID);

  /**
   * Called by a Recipient with a deferred store outstanding when
   * its bandwidth available callback is cancelled. This indicates
   * that the STORE will never succeed (e.g. because the socket has
   * been closed).
   */
  void onDeferredSTORECancelled(std::unique_ptr<STORE_Message> msg,
                                ShardID dest,
                                Status st);

  /**
   * Attempt to send a "wave" of STORE messages containing copies of the
   * record to R+X storage nodes randomly selected from the log's nodeset.
   * Update recipients_ as copies get passed to the messaging layer.
   *
   * @return   0 if no fatal errors were encountered. This does not guarantee
   *           that a complete wave has been successfully sent.
   *           replies_expected_ will have the number of STORE messages
   *           successfully sent in this wave. On error -1 is returned.
   *           err is set to:
   *           ENOBUFS    the Worker-wide limit on bytes buffered in output
   *                      evbuffers of all Sockets combined has been reached
   *           SYSLIMIT   if the box is out of ephemeral ports or some other
   *                      system resource.
   *           INTERNAL   if an internal error occurred (debug build asserts)
   */
  int sendWave();

  /**
   * Translate an error code that caused an APPEND request to fail into a
   * status code that we want to report to the client. Increment the error
   * counter corresponding to @param st, send reply with a translated status.
   *
   * @param st   error code indicating why an APPEND request failed. One of
   *             the following is expected:
   */
  void sendError(Status st);

  /**
   * Sends a redirect reply to the client.  This is either an explicit redirect
   * (E::REDIRECTED) indicating that another node should take care of the
   * append, or E::PREEMPTED, which means that a sequencer for a higher epoch
   * exists somewhere else.
   *
   * @param to      node to redirect clients to
   * @param status  either E::REDIRECTED or E::PREEMPTED
   * @param lsn     (optional) include this LSN in the reply; useful if one was
   *                already assigned to the appender before preemption was
   *                detected
   */
  void sendRedirect(NodeID to, Status status, lsn_t lsn = LSN_INVALID);

  /**
   * Called when we failed to forward the STORE message to node at position
   * `index` in a chain. This calls onRecipientFailed() for each node at
   * position >= `index` in the chain.
   *
   * This can happen if:
   * - we fail to send STORE to the first node in the chain (onCopySent called
   *   with st != E::OK), in which case index is set to 0;
   * - A storage node sends STORED with st == E::FORWARD in order to notify that
   *   it failed to forward the STORE message to the next node in the chain.
   *
   * @param index Position of the next node in the chain to which we could not
   *              forward the STORE message.
   */
  void onChainForwardingFailure(unsigned int index);

  /**
   * STORE_Message::onSent() calls this to inform the Appender that one
   * of its STORE messages has been passed to TCP for delivery, or that
   * the Socket has failed to pass the message to TCP.
   *
   * @param st   status passed to STORE_Message::onSent() (see Message.h)
   * @param to   id of shard to which the message was sent
   * @param mhdr message header
   */
  void onCopySent(Status st, ShardID to, const STORE_Header& mhdr);

  /**
   * This handler is called when a Socket through which this Appender
   * expects to receive a reply to a STORE message from the most
   * recent wave gets closed.
   *
   * @param shard shard id of the storage node the Socket to which was closed
   * @param cb    callback functor that called this handler
   */
  void onSocketClosed(Status st,
                      ShardID shard,
                      Recipient::SocketClosedCallback* cb);

  /**
   * This handler is called when a STORED reply is received for a
   * STORE message previously sent by this Appender.
   *
   * @param header              STORED message's header
   * @param from                storage shard that sent the reply
   * @param rebuildingRecipient If header.status == E::REBULDING, recipient in
   *                            the copyset that is rebuilding.
   *
   * @return 0 on success, -1 if reply is invalid, sets err to E::PROTO.
   */
  int onReply(const STORED_Header& header,
              ShardID from,
              ShardID rebuildingRecipient = ShardID());

  const PayloadHolder* getPayload() const {
    return payload_.get();
  }

  const AppendAttributes& getAppendAttributes() {
    return attrs_;
  }

  logid_t getLogID() const {
    return log_id_;
  }

  /**
   * @return  the LSN of record this Appender is appending. The value
   *          is undefined until start().
   */
  lsn_t getLSN() const {
    return store_hdr_.rid.lsn();
  }

  /**
   * @param sequencer_epoch  current epoch of the sequencer
   *
   * @return true if this appender expects a higher epoch than sequencer's
   *         current one
   */
  bool isStale(epoch_t sequencer_epoch) const {
    return seen_ > sequencer_epoch;
  }

  STORE_flags_t getPassthruFlags() const {
    return passthru_flags_;
  }

  size_t getChecksumBytes() const {
    if (passthru_flags_ & STORE_Header::CHECKSUM) {
      return passthru_flags_ & STORE_Header::CHECKSUM_64BIT ? 8 : 4;
    }
    // no checksum
    return 0;
  }

  ClientID getReplyTo() const {
    return reply_to_;
  }

  std::unique_ptr<SocketProxy> getClientSocketProxy() const;

  request_id_t getClientRequestID() const {
    return append_request_id_;
  }

  lsn_t getLSNBeforeRedirect() const {
    return lsn_before_redirect_;
  }

  virtual bool maxAppendersHardLimitReached() const;

  folly::Optional<epoch_t> getAcceptableEpoch() const {
    return acceptable_epoch_;
  }

  /**
   * Specify map of byte offsets within epoch until (and include) the current
   * record.
   *
   * @param offset_within_epoch   How many bytes were written in epoch (to which
   *                              this Appender belongs) until and include
   *                              record of this Appender.
   */
  void setLogOffset(OffsetMap offset_within_epoch);

  void setAppendMessageCount(uint32_t count) {
    append_message_count_ = count;
  }

  void setAcceptableEpoch(folly::Optional<epoch_t> epoch) {
    if (epoch.hasValue() && !MetaDataLog::isMetaDataLog(log_id_)) {
      // conditional appends are only supported for metadata log records
      ld_critical(
          "attempting to set acceptable epoch for data log %lu", log_id_.val());
      ld_check(false);
      return;
    }
    acceptable_epoch_ = epoch;
  }

  /**
   * If this Appender was created in response to an incoming APPEND_Message,
   * send an APPENDED message back to the client running the corrsponding
   * AppendRequest. If this Appender was created by an AppendRequest
   * directly in the client address space, call a method of that AppendRequest
   * object to inform it of the outcome. In the latter case the call must be
   * made on the Worker thread running the AppendRequest.
   *
   * Note that the request may not have been started yet, because an earlier
   * storage attempt succeeded.
   *
   * This can happen if:
   *
   * - The append was earlier sent to another sequencer, which partially stored
   *   it before being pre-empted.
   *
   * - The append was then redirected to this node via a preemption redirect.
   *
   * - The LSN that got partially stored was later replicated by log recovery
   *   and will be delivered to the reader.
   *
   * To avoid silent duplicates, the Appender should not be assigned with a new
   * LSN, instead, we just reply with the LSN sent to us by the earlier node.
   *
   * @param lsn  lsn assigned to the record on success, or LSN_INVALID if
   *             request failed
   * @param st   E::OK if request succeeded, otherwise the error code to send
   * @param redirect  if st = E::PREEMPTED, node id of the sequencer to which
   *                  to resend this append
   */
  void sendReply(lsn_t lsn, Status st, NodeID redirect = NodeID());

  /**
   * Select store timeout based on node response time statistics.
   */
  std::chrono::milliseconds selectStoreTimeout(const StoreChainLink copyset[],
                                               int size) const;

  /**
   * Select store timeout in case we cannot select copy set and
   * there is no way to select histogram based timeouts.
   * Also, it's used when adaptive stores timeouts are disabled.
   * It is simply initial_delay * 2 ^ {wave number - 1}.
   */
  std::chrono::milliseconds exponentialStoreTimeout() const;

  int64_t getStoreTimeoutMultiplier() const;

  /**
   * Only used in tests.  See Settings::hold_store_replies.
   */
  copyset_size_t repliesExpected() const {
    return replies_expected_;
  }

  /**
   * Only used in tests.  See Settings::hold_store_replies.
   */
  size_t repliesHeld() const {
    return held_store_replies_.size();
  }

  /**
   * Only used in tests.  See Settings::hold_store_replies.
   */
  void holdReply(const STORED_Header& header,
                 ShardID from,
                 ShardID rebuildingRecipient) {
    if (header.wave == store_hdr_.wave) {
      HeldReply reply;
      reply.hdr = header;
      reply.from = from;
      reply.rebuildingRecipient = rebuildingRecipient;
      held_store_replies_.push_back(reply);
    }
  }

  struct HeldReply {
    STORED_Header hdr;
    ShardID from;
    ShardID rebuildingRecipient;
  };

  /**
   * Only used in tests.  See Settings::hold_store_replies.
   */
  std::vector<HeldReply> takeHeldReplies() {
    return std::move(held_store_replies_);
  }

  /**
   * Verify the checksum of the payload that this appender holds, if a checksum
   * was prepended to the payload by the client.
   *
   * @param payload_checksum    The checksum the client provided
   * @param expected_checksum   Calculated checksum on the current payload
   *
   * @return True if checksum is correct for this payload or was not provided
   */
  bool verifyChecksum(uint64_t* payload_checksum, uint64_t* expected_checksum);

  /**
   * Used only in tests. See Settings::test_sequencer_corrupt_stores
   */
  void TEST_corruptPayload() {
    payload_->TEST_corruptPayload();
  }

  void setPrevTracingSpan(std::shared_ptr<opentracing::Span> previous_span) {
    previous_span_ = std::move(previous_span);
  }

 protected:
  // protected: members are for use by test subclasses. This class is not
  // intended to be subclassed other than for testing.

  /**
   * This constructor is used by tests only.
   */
  Appender(Worker* worker,
           std::shared_ptr<TraceLogger> trace_logger,
           std::chrono::milliseconds client_timeout,
           request_id_t append_request_id,
           STORE_flags_t passthru_flags,
           logid_t log_id,
           PayloadHolder payload,
           epoch_t seen_epoch,
           size_t full_appender_size,
           std::shared_ptr<opentracing::Tracer> e2e_tracer = nullptr,
           std::shared_ptr<opentracing::Span> appender_span = nullptr);

  // I want to keep track of all stores sent, identifying a store message sent
  // by pair (wave, dest)
  std::unordered_map<std::pair<uint32_t, ShardID>,
                     std::shared_ptr<opentracing::Span>>
      all_store_spans_;

  // EpochSequencer that accepted and assigned LSN for this Appender. A shared
  // reference is held here to ensure that EpochSequencer is destroyed only
  // after all Appender it owns are destroyed. This should only be accessed
  // through virtual methods of this class so that tests can work without
  // having a real EpochSequencer (epoch_sequencer_ may be nullptr in tests).
  std::shared_ptr<EpochSequencer> epoch_sequencer_;

  // Access to Sender.
  std::unique_ptr<SenderBase> sender_;

  // Set of ShardIDs that sent a STORED reply with status OK.
  // Preserved across waves.  Any subsequent waves sent to these nodes
  // can just amend the copyset and avoid sending the payload again.
  //
  // The choice of data structure here is not obvious.  It's fundamentally a
  // set, however:
  // - In the success case (single wave), the set only grows as large as the
  //   replication factor, typically 3.
  // - With sporadic failures (2-3 waves), it will likely be under 10 elements.
  // - With persistent failures (many waves), it can grow as big as the
  //   nodeset size (typically 10-20).
  //
  // Given the above, we keep a sorted vector with a few elements inline.
  // Inserting is O(n) on paper but the limited size means the cost of the
  // copying shouldn't outweigh the constant factors of heavier data
  // structures.
  copyset_custsz_t<4> nodes_stored_amendable_;

  // expose the store_hdr_ to test subclasses
  STORE_Header& getStoreHeader() {
    return store_hdr_;
  }

  virtual int link();

  // Initialize/activate/cancel timers used by this Appender.
  virtual void initStoreTimer();
  virtual void initRetryTimer();
  virtual void cancelStoreTimer();
  virtual void activateStoreTimer(std::chrono::milliseconds delay);
  virtual void fireStoreTimer();
  virtual bool storeTimerIsActive();
  virtual void cancelRetryTimer();
  virtual void activateRetryTimer();
  virtual bool retryTimerIsActive();
  virtual bool isNodeAlive(NodeID node);

 private:
  using ReleaseTypeRaw = std::underlying_type<ReleaseType>::type;

  // AppenderTracer for tracing append operations
  AppenderTracer tracer_;
  // Worker on whose thread this Appender was created. May be null in tests so
  // Appender should access this through virtual methods of this class so that
  // tests can override them.
  Worker* created_on_;

  // Use this field to assert that we do not call retire() twice.
  bool retired_ = false;

  // Size in bytes of Appender + payload.
  size_t full_appender_size_;

  // Keep track if this Appender was started (start() was called).
  bool started_ = false;

  // Backlog duration (in seconds) configured for this log (used for tracing)
  folly::Optional<std::chrono::seconds> backlog_duration_;

  // Indicate if we already successfully sent `replication_` copies of the
  // record during the current wave or a previous wave, and sent the APPENDED
  // reply to the client.
  bool reply_sent_ = false;

  folly::Optional<std::chrono::milliseconds> timeout_;

  // header to use when sending STORE messages to storage nodes
  // Includes among other things:
  //        * esn, epoch, logid, and timestamp of this record.
  STORE_Header store_hdr_ __attribute__((__aligned__(8)));

  // Maximum epoch a client has seen a record for this log in. Used to reject
  // an append if epoch_sequencer_ has an older epoch (this prevents breaking
  // ordering guarantees).
  epoch_t seen_;

  // Append will be rejected with E::ABORTED if it is different from the value
  // below. Used for internal metadata log appends to prevent ordering issues.
  // Can only be used on metadata logs
  folly::Optional<epoch_t> acceptable_epoch_;

  // copyset manager for selecting nodes to store copies of the record
  std::shared_ptr<CopySetManager> copyset_manager_;

  // state of the copyset manager, used to perform stateful selection
  std::unique_ptr<CopySetManager::State> csm_state_;

  // the number of STORE messages that are waiting for bandwidth to
  // become available before being transmitted.
  copyset_size_t deferred_stores_;

  // the number of STORE messages that have been successfully sent in the
  // latest wave and to whose recipients we still have intact connections
  // so that we can expect to get replies from them.
  copyset_size_t replies_expected_;

  // the following members keep track of the status of STOREs accross waves:
  // number of successful STORE messages
  bool stored_{false};
  // the number of failed STORE messages with E::PREEMPTED
  bool preempted_{false};
  // the number of outstanding STORE responses
  int outstanding_{0};

  // the number of replies in the most recent wave that we need to have
  // SYNCED set in .flags, indicating that the copy was synced to disk
  // before reply was sent
  int synced_replies_remaining_;

  // The biggest replication scope with replication factor greater than one.
  // Used by DomainIsolationChecker to ping this appender when our domain
  // (of this scope) rejoins the cluster.
  NodeLocationScope biggest_replication_scope_;

  // a set of recipients to which we sent the most recent wave of STORE
  // messages.
  RecipientSet recipients_;

  // a set of ShardIDs of recipient nodes from the most recent wave that have
  // successfully stored their copy and must receive a RELEASE message when this
  // Appender is reaped.
  copyset_custsz_t<4> release_;

  // If this Appender was created by an APPEND_Message::onReceived(), this is
  // the id of client Socket into which to send an APPENDED reply. If this
  // Appender was created by an AppendRequest, this is an invalid ClientID.
  const ClientID reply_to_;

  // log id where the append is going
  logid_t log_id_;

  // Additional append attributes.
  AppendAttributes attrs_;

  // payload of record we are appending.
  std::shared_ptr<PayloadHolder> payload_;

  // tail record for this append, will pass to sequencer when this appender
  // is reaped
  std::shared_ptr<TailRecord> tail_record_;

  // time when the appender was created, used to calculate the latency
  std::chrono::steady_clock::time_point creation_time_;

  // deadline after which the client is presumed to have timed out. If the
  // epoch to which this Appender belongs (store_hdr_.epoch) is shut down
  // after this deadline, the appender may abort the request without sending
  // a reply to client. The sequencer responsible for next epoch of the log
  // will clean up any log irregularities that may result.
  std::chrono::steady_clock::time_point client_deadline_;

  // id of the AppendRequest (local or remote) that created the record being
  // appended. If Appender successfully appends the record before
  // client_deadline_, it notifies the AppendRequest either through a method
  // call (if AppendRequest is local), or by sending an APPENDED message.
  request_id_t append_request_id_;

  // Flags copied from APPEND header
  STORE_flags_t passthru_flags_ = 0;

  // timer for the STORE timeout
  // Note: in tests, this is left uninitialized.
  Timer store_timer_;

  // special timer, set up with a zero timeout, used to trigger another wave
  // of STOREs to be sent on the next iteration of the event loop
  // Note: in tests, this is left uninitialized.
  Timer retry_timer_;

  // If the append was created by SequencerBatching, this contains the number
  // of constituent appends (APPEND messages that came over the wire).  Used
  // to bump stats (success/failure counters) accurately.
  uint32_t append_message_count_ = 1;

  // boolean flag indicating that the Appender started a timer
  // with non-zero STORE timeout, used for diagnosing append failures
  bool store_timeout_set_{false};

  // Set to NONE or PER_EPOCH if at some point it is decided that this Appender
  // should not send a global RELEASE message. This can happen if the Appender
  // is aborted because we called retire() before the record was fully
  // replicated, or if noteAppenderReaped returns false, meaning the Sequencer
  // was preempted (or reactivated).
  //
  // This will prevent onReaped() and deleteIfDone() from sending RELEASE
  // messages, which is unsafe in those situations.
  std::atomic<ReleaseTypeRaw> release_type_;

  STORE_Extra extra_;

  // If this append was earlier sent to another Sequencer which
  // redirected/preempted it here, this is the LSN that the previous Sequencer
  // assigned the append, if any.
  lsn_t lsn_before_redirect_;

  // In tests, we may want to hold STORED replies until all have come in, to be
  // sure that storage nodes have finished processing the associated STORE
  // messages.
  std::vector<HeldReply> held_store_replies_;

  // Used by deleteIfDone to determine when it is safe to delete ourselves.
  // It is safe to delete ourselves when both:
  // - We were reaped (onReaped() called);
  // - We completed the state machine and unlinked ourselves from
  //   Worker::activeAppenders(). (onComplete() called)
  std::atomic_uchar state_{0};
  static const uint8_t FINISH = 1u << 0u;
  static const uint8_t REAPED = 1u << 1u;

  // OpenTracing tracer object used in distributed e2e tracing
  std::shared_ptr<opentracing::Tracer> e2e_tracer_;
  std::shared_ptr<opentracing::Span> appender_span_;

  // e2e tracing span that is received from the AppenderPrep side
  std::shared_ptr<opentracing::Span> previous_span_;
  // tracing span corresponding to the last wave that was attempted to be sent
  // If another wave is to be sent, then in e2e tracing we will
  // create a span for the new wave in a FollowsFrom relationship with previous
  std::unique_ptr<opentracing::Span> prev_wave_send_span_;
  std::unique_ptr<opentracing::Span> wave_send_span_;

  /**
   * Called when the state machine completed (record is fully replicated) or we
   * gave up early for some reason.
   * Unlink ourselves from Worker::activeAppenders() map and free up as much
   * heap allocated memory as possible.
   *
   * @param linked        defaults to true. false if the Appender is already
   *                      unlinked (e.g., in Worker destruction)
   *
   * Delete this Appender if onReaped() was already called.
   */
  void onComplete(bool linked = true);

  /**
   * Called when this Appender is reaped, ie it was removed from the
   * EpochSequencer's sliding window.
   *
   * Delete this Appender if onComplete() was already called.
   *
   * Note: this may be called from another thread than the worker thread that
   * owns this Appender.
   *
   * Made it virtual for testing purpose only.
   */
  virtual void onReaped();

  /**
   * Called by onComplete() and onReaped(). Delete this Appender once both have
   * called it. This uses the `state_` atomic to handle that logic
   * while onComplete() and onReaped() may be called on a different thread.
   *
   * @param flag set to FINISH or REAPED depending on the caller.
   */
  void deleteIfDone(uint8_t flag);

  /**
   * Called when we managed to store a copy on a recipient.
   *
   * Determine what to do next:
   * - send a reply and retire this Appender if we now have `replication_`
   *   copies of the record;
   * - wait for more recipients to complete if we don't have `replication_`
   *   copies yet.
   *
   * @param recipient Recipient for which the outcome is known.
   */
  void onRecipientSucceeded(Recipient* recipient);

  /**
   * Called when we know that we failed to store on a recipient because either:
   * - The recipient replied with a STORED message having status different
   *   than E::OK;
   * - The STORE message could not be sent to the recipient;
   * - The socket to the recipient was closed before we could get a reply.
   *
   * Activates the store timer or retry timer if it is impossible to make
   * progress for this wave.
   *
   * @param recipient Recipient in which we could not store a copy;
   * @param reason    Reason why we could not store the copy.
   *
   * @return True if we may wait for more recipients to successfully complete
   *         this wave, False if we decided to abort or complete the wave.
   */
  bool onRecipientFailed(Recipient* recipient, Recipient::State reason);

  /**
   * Called when the node running this appender has been found to corrupt
   * stores. This has been observed due to hardware gone bad, and those nodes
   * have continued to frequently corrupt payloads. Thus we attempt to abort
   * the node to keep it from corrupting more things, unless too many nodes are
   * dead.
   */
  void onMemoryCorruption();

  /**
   * Send DELETE messages to all nodes in recipients_ for which we do
   * not have a positive acknowledgement of the record being stored.
   *
   * This method must be called only after we receive r positive STORE
   * acknowledgements from recipients. Because DELETEs are just a space
   * optimization they are sent best-effort and will not be redelivered
   * after a failure.
   */
  void deleteExtras();

  /**
   * Send RELEASE messages to the specified nodes.
   */
  virtual void sendReleases(const ShardID* dests,
                            size_t ndests,
                            const RecordID& rid,
                            ReleaseType release_type);

  /**
   * Build the TailRecord using the info in STORE header after Appender starts.
   *
   * @param include_payload    if true, include payload in the tail record; used
   *                           in tail optimized logs
   */
  void prepareTailRecord(bool include_payload);

  /**
   * This callback is called by timer_ event if:
   * (1) we don't get enough STORED replies to the last wave of STORE requests
   *     before the STORE timeout expires;
   * (2) the number of connections from which we may expect a positive reply to
   *     a last-wave STORE drops below the replication factor for our log or we
   *     know one of the sync leaders is not available and we need to try
   *     sending a new wave.
   */
  void onTimeout();

  /**
   * Mark a storage shard as not available in the NodeSetState so that it will
   * not be selected for sending STORE messages for certain amount of time.
   *
   * @param shard             the storage shard to mark
   * @param retry_interval    time interval to before the Appender retries to
   *                          store records on the node
   *
   * @param reason            reason that the node is considered not available
   */
  void setNotAvailableShard(ShardID shard,
                            std::chrono::seconds retry_interval,
                            NodeSetState::NotAvailableReason reason);

  // For a wave, decides which of the nodes in the copyset should receive
  // amends (STORE message without the payload).  May also decide to disable
  // chain sending for the wave, if only 0 or 1 nodes need the payload (amends
  // are little so chaining will not save much bandwidth in those cases).
  void decideAmends(const StoreChainLink copyset[],
                    copyset_size_t copyset_size,
                    bool* chain_in_out,
                    std::bitset<COPYSET_SIZE_MAX>* amendable_set_out,
                    copyset_size_t* first_amendable_offset_out) const;

  // try sending waves of STOREs without resetting csm_state_
  // until a complete wave is sent, or copyset selector is unable to select
  // a copyset
  int trySendingWavesOfStores(const copyset_size_t cfg_synced,
                              const copyset_size_t cfg_extras,
                              const CopySetManager::AppendContext& append_ctx);

  void forgetThePreviousWave(const copyset_size_t cfg_synced);

  // The following methods access this class' dependencies and may be overridden
  // by tests.

  // Check that the current worker thread is the worker thread this Appender was
  // created on. Tests which do not use a worker thread should make this a no
  // op.
  virtual void checkWorkerThread();

  virtual lsn_t getLastKnownGood() const;
  virtual copyset_size_t getExtras() const;
  virtual copyset_size_t getSynced() const;
  virtual std::shared_ptr<CopySetManager> getCopySetManager() const;
  virtual NodeLocationScope getBiggestReplicationScope() const;
  virtual NodeLocationScope getCurrentBiggestReplicationScope() const;

  virtual const Settings& getSettings() const;
  virtual const std::shared_ptr<Configuration> getClusterConfig() const;
  virtual NodeID getMyNodeID() const;
  virtual std::string describeConnection(const Address& addr) const;
  virtual bool bytesPendingLimitReached() const;
  virtual bool isAcceptingWork() const;
  virtual NodeID checkIfPreempted(epoch_t epoch);
  virtual void retireAppender(Status st, lsn_t lsn, Reaper& reaper);
  virtual void noteAppenderPreempted(epoch_t epoch, NodeID preempted_by);
  virtual bool checkNodeSet() const;
  virtual bool noteAppenderReaped(FullyReplicated replicated,
                                  lsn_t reaped_lsn,
                                  std::shared_ptr<TailRecord> tail_record,
                                  epoch_t* last_released_epoch_out,
                                  bool* lng_changed_out);
  virtual bool epochMetaDataAvailable(epoch_t epoch) const;

  virtual void
  setNotAvailableUntil(ShardID shard,
                       std::chrono::steady_clock::time_point until_time,
                       NodeSetState::NotAvailableReason reason);
  virtual StatsHolder* getStats();
  virtual int registerOnSocketClosed(NodeID nid, SocketCallback& cb);
  virtual void replyToAppendRequest(APPENDED_Header& replyhdr);
  virtual void schedulePeriodicReleases();

  // check if the appender is the one of the appenders that the sequencer
  // would like to drain during graceful reactivation/migration
  virtual bool isDraining() const;

  friend class Recipient;
  friend class RecipientSet;
  friend class AppenderTest;
  friend class DomainIsolationUpdatedRequest;
} __attribute__((__aligned__(4))); // minimal alignment required for
                                   // SlidingWindow elements. It's very
                                   // unlikely that the struct will ever be
                                   // less than 4b-aligned even without this
                                   // attribute.

// Intrusive hash map of appenders.  Wrapper instead of typedef to allow
// forward-declaring in Worker.h.
struct AppenderMap {
  explicit AppenderMap(size_t nbuckets) : map(nbuckets) {}
  IntrusiveUnorderedMap<Appender,
                        RecordID,
                        Appender::KeyExtractor,
                        RecordID::Hasher,
                        std::equal_to<RecordID>,
                        Appender::Disposer>
      map;
};

}} // namespace facebook::logdevice
