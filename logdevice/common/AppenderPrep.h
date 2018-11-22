/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/protocol/APPEND_Message.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

/**
 * @file Short-lived container for server-side logic to process an incoming
 * append.  Contains most of the code that gets executed after we receive an
 * APPEND message until we kick off an Appender.
 */

namespace facebook { namespace logdevice {

class Appender;
class Sequencer;
class StatsHolder;
class SequencerLocator;

class AppenderPrep : public std::enable_shared_from_this<AppenderPrep> {
 public:
  explicit AppenderPrep(PayloadHolder payload) : payload_(std::move(payload)) {}

  virtual ~AppenderPrep() {
    if (appender_span_) {
      appender_span_->Finish();
    }
  }

  AppenderPrep& setAppendMessage(
      const APPEND_Header& header,
      lsn_t lsn_before_redirect,
      ClientID from,
      AppendAttributes attrs,
      std::shared_ptr<opentracing::Tracer> e2e_tracer = nullptr,
      std::unique_ptr<opentracing::Span> append_msg_recv_span = nullptr) {
    header_ = header;
    lsn_before_redirect_ = lsn_before_redirect;
    attrs_ = std::move(attrs);
    from_ = from;
    e2e_tracer_ = std::move(e2e_tracer);

    if (append_msg_recv_span) {
      // receiving a span corresponding to the append message being received
      // means we have e2e tracing on, so we continue creating spans
      append_msg_recv_span->Finish();

      ld_check(e2e_tracer_);
      appender_span_ = e2e_tracer_->StartSpan(
          "APPENDER", {FollowsFrom(&append_msg_recv_span->context())});
    }
    return *this;
  }

  AppenderPrep& setAppendMessageCount(uint32_t count) {
    append_message_count_ = count;
    return *this;
  }

  AppenderPrep& setAcceptableEpoch(folly::Optional<epoch_t> epoch) {
    acceptable_epoch_ = epoch;
    return *this;
  }

  AppenderPrep& disallowBatching() {
    allow_batching_ = false;
    return *this;
  }

  void execute();

  // Called directly in tests
  void execute(std::unique_ptr<Appender>);

  epoch_t getSeen() const {
    return header_.seen;
  }

 protected: // can be overridden in tests
  // e2e tracing span corresponding to the life-time of the appender
  std::shared_ptr<opentracing::Span> appender_span_;

  // Returns a pointer to the Sequencer object for a given log, if one exists.
  // See AllSequencers::findSequencer() for a description of err codes.
  virtual std::shared_ptr<Sequencer> findSequencer(logid_t log_id) const;

  // Checks if the given node is considered to be available.
  virtual bool isAlive(NodeID node) const;

  // Checks if the given node is boycotted or not
  virtual bool isBoycotted(NodeID node) const;

  // Checks if we are isolated from the rest of the cluster
  virtual bool isIsolated() const;

  // Checks if the given node is a sequencer and is present in the cluster
  // configuration.
  virtual bool nodeInConfig(NodeID node) const;

  // Returns the id of this node.
  virtual NodeID getMyNodeID() const;

  // Verifies that there are enough nodes available to handle the append.
  virtual bool checkNodeSet(const Sequencer& sequencer) const;

  // Check if some appenders were already buffered.
  virtual bool hasBufferedAppenders(logid_t log_id) const;

  // Proxy for Sequencer::runAppender()
  virtual RunAppenderStatus runAppender(Sequencer& sequencer,
                                        Appender& appender);

  // Conditionally reactivates the sequencer into a new epoch. If the
  // sequencer doesn't exist yet, `sequencer' will be updated to point to the
  // newly appointed one.
  // See AllSequencer::activateSequencer() for a description of err codes.
  virtual int activateSequencer(logid_t log_id,
                                std::shared_ptr<Sequencer>& sequencer,
                                Sequencer::ActivationPred condition);

  // Adds the appender to a special queue of appenders which will get processed
  // once the sequencers gets activated.  Returns 0 on success, otherwise
  // returns -1 and sets err to E::PENDING_FULL.
  virtual int bufferAppender(logid_t log_id,
                             std::unique_ptr<Appender>& appender);

  // Called when an append with NO_REDIRECT flag set is executed. Updates
  // sequencer's no_redirect_until_ to prevent it from sending further redirects
  // for some time.
  virtual void updateNoRedirectUntil(Sequencer& sequencer);

  // Is this node allowed to bring up sequencers on demand?
  virtual bool canActivateSequencers() const;

  // Returns true if this node is willing to do more work (i.e. it's not
  // shutting down).
  virtual bool isAcceptingWork() const;

  // Reply to the client with a given error status.
  virtual void sendError(Appender*, Status) const;

  // Reply to the client with a redirect to `target'.
  virtual void sendRedirect(Appender*, NodeID target, Status) const;

  // Returns a pointer to the object containing stats.
  virtual StatsHolder* stats() const;

  // Return the settings used by the APPEND_Message object. May be invalid in
  // tests, should be accessed through a virtual method so that tests can
  // overrride them.
  virtual const Settings& getSettings() const;

  // proxy for sender::getPrincipal()
  virtual const PrincipalIdentity* getPrincipal();

  // Calls the PermissionChecker owned by the processor to determine if the
  // client is allowed to perform an append to the specified logid
  virtual void isAllowed(std::shared_ptr<PermissionChecker> permission_checker,
                         const PrincipalIdentity& principal,
                         callback_func_t cb);

  virtual std::shared_ptr<PermissionChecker> getPermissionChecker();

  // Returns processor's sequencer locator
  virtual SequencerLocator& getSequencerLocator();

 private:
  PayloadHolder payload_;
  // TODO factor away
  APPEND_Header header_;
  // If this append was previously sent to another sequencer, then
  // redirected/preempted here, this is the previous LSN if any.
  lsn_t lsn_before_redirect_{LSN_INVALID};
  // Additional append attributes
  AppendAttributes attrs_;
  // Client that sent the APPEND message when received over network, otherwise
  // ClientID::INVALID
  ClientID from_;
  // If the append was created by SequencerBatching, this contains the number
  // of constituent appends (APPEND messages that came over the wire).  Used
  // to bump stats (success/failure counters) accurately.
  uint32_t append_message_count_ = 1;
  // Allow the write to go through SequencerBatching?  This flag enables
  // SequencerBatching to prevent further batching of already batched appends,
  // avoiding batching recursion.
  // NOTE: This is still subject to batching settings; e.g. if Settings say
  // not to batch this doesn't matter.
  bool allow_batching_ = true;
  // only allow the append to go through on the following epoch, if set
  folly::Optional<epoch_t> acceptable_epoch_;

  // tracer object used in e2e distributed tracing
  std::shared_ptr<opentracing::Tracer> e2e_tracer_;

  // tracing spans used for e2e tracing
  std::shared_ptr<opentracing::Span> permission_checking_span_;
  std::shared_ptr<opentracing::Span> sequencer_locating_span_;

  // Constructs an Appender after the message is received
  std::unique_ptr<Appender> constructAppender();

  // see shouldRedirect() below
  enum class Decision {
    REDIRECT = 0,
    CORRECT_NODE,
    FLAG_NO_REDIRECT,
    FLAG_REACTIVATE_IF_PREEMPTED,
    NO_REDIRECT_UNTIL,
    PREEMPTED_DEAD,
  };

  /**
   * Checks if a redirect message should be sent to the client instead of
   * executing an append. `preempted' indicates that a sequencer on `seq_node'
   * preempted this one.
   * @return   value of APPEND_Message::Decision, can be one of:
   *             REDIRECT       redirect is needed
   *
   *           should not perform redirect for the following cases:
   *             CORRECT_NODE       @param seq_node is this node itself
   *             FLAG_NO_REDIRECT   client sets NO_REDIRECT flag
   *             FLAG_REACTIVATE_IF_PREEMPTED
   *                  sequencer is preempted but the client sets the
   *                  REACTIVATE_IF_PREEMPTED flag
   *             NO_REDIRECT_UNTIL  sequencer is active and
   *                                now() < no_redirect_until_
   *             PREEMPTED_DEAD     sequencer is preempted but the preempted
   *                                node is considered dead
   */
  Decision shouldRedirect(NodeID seq_node,
                          const Sequencer* sequencer,
                          bool preempted = false) const;

  /**
   * Buffers the Appender so it gets processed once a sequencer for `log_id'
   * is active. Assumes that `sequencer' is either nullptr or inactive.
   *
   * @param sequencer  (in/out) May be updated due to (re)activation.
   * @param force      if set, skips the canActivateSequencers() check
   * @param condition  prerequisite for reactivation, evaluated under a lock
   *
   * @return  0 on success and -1 otherwise, with err set to:
   *   PENDING_FULL  - buffer of pending appenders is full
   *   EXISTS        - a sequencer was already reactivated by another thread
   *                   (`sequencer' is updated to point to it)
   *   ABORTED       - `condition' not satisfied
   *   NOSEQUENCER   - activating a sequencer failed
   *   NOTFOUND      - log_id is not in the config of this sequencer node
   */
  int activateAndBuffer(logid_t log_id,
                        std::unique_ptr<Appender>& appender,
                        std::shared_ptr<Sequencer>& sequencer,
                        bool force,
                        Sequencer::ActivationPred condition);

  /**
   * Starts an appender. Returns 0 on success; on failure, logs and returns -1
   * (see Sequencer::runAppender() for a description of error codes).
   *
   * @param sequencer  (in/out) May be updated due to reactivation.
   */
  RunAppenderStatus append(std::shared_ptr<Sequencer>& sequencer,
                           std::unique_ptr<Appender>& appender,
                           bool can_retry = true);

  /**
   * Callback invoked by SequencerLocator. A status of E::OK indicates that a
   * sequencer has been found. See SequencerLocator.h for more details.
   */
  void onSequencerNodeFound(Status status,
                            logid_t datalog_id,
                            NodeID seq_node,
                            std::unique_ptr<Appender> appender,
                            std::shared_ptr<Sequencer> sequencer);

  void sendReply(std::unique_ptr<Appender> appender,
                 PermissionCheckStatus permission_status);
};
}} // namespace facebook::logdevice
