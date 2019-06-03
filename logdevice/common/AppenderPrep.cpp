/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppenderPrep.h"

#include <opentracing/tracer.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/AppenderBuffer.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerBatching.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

void AppenderPrep::sendReply(std::unique_ptr<Appender> appender,
                             PermissionCheckStatus permission_status) {
  Status st = PermissionChecker::toStatus(permission_status);

  if (st != E::OK) {
    RATELIMIT_LEVEL(st == E::ACCESS ? dbg::Level::WARNING : dbg::Level::INFO,
                    std::chrono::seconds(2),
                    1,
                    "APPEND request from %s for log %lu failed with %s",
                    Sender::describeConnection(from_).c_str(),
                    header_.logid.val_,
                    error_description(st));
    sendError(appender.get(), st);
    return;
  }

  const logid_t datalog_id = MetaDataLog::dataLogID(header_.logid);
  std::shared_ptr<Sequencer> sequencer = findSequencer(datalog_id);
  ld_check(sequencer || err == E::NOSEQUENCER);

  if (permission_checking_span_) {
    sequencer_locating_span_ = e2e_tracer_->StartSpan(
        "Sequencer_locating",
        {FollowsFrom(&permission_checking_span_->context())});
  }

  auto appender_ptr = appender.get();
  auto cb = [=](Status status, logid_t log_id, NodeID node_id) {
    if (sequencer_locating_span_) {
      sequencer_locating_span_->SetTag("status", error_name(status));
      sequencer_locating_span_->SetTag("log_id", log_id.val());
      sequencer_locating_span_->SetTag("node_id", node_id.toString());

      sequencer_locating_span_->Finish();
      appender_ptr->setPrevTracingSpan(sequencer_locating_span_);
    }
    onSequencerNodeFound(status,
                         log_id,
                         node_id,
                         std::unique_ptr<Appender>(appender_ptr),
                         sequencer);
  };

  auto& locator = getSequencerLocator();
  if (locator.locateSequencer(datalog_id, cb) == 0) {
    appender.release();
  } else {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "The sequencer locator failed for log %lu: %s, "
                      "failing the append with E::NOSEQUENCER.",
                      datalog_id.val_,
                      error_description(err));
    sendError(appender.get(), E::NOSEQUENCER);
  }
}

void AppenderPrep::execute() {
  execute(constructAppender());
}

void AppenderPrep::execute(std::unique_ptr<Appender> appender) {
  // Appends for a metadata log is handled by the corresponding data
  // log sequencer of the log, so here we mask the MetaDataLog::ID_SHIFT
  // bit of the logid to find the data log sequencer
  const logid_t datalog_id = MetaDataLog::dataLogID(header_.logid);

  std::shared_ptr<Sequencer> sequencer = findSequencer(datalog_id);
  ld_check(sequencer || err == E::NOSEQUENCER);

  auto my_nodeid = getMyNodeID();

  if (!isAlive(my_nodeid) || !nodeInConfig(my_nodeid)) {
    // This node is not up (so claims the failure detector) or is not even a
    // sequencer node. This means that the node is still coming up (in which
    // case it isn't ready to run any sequencers yet), is partitioned from the
    // rest of the cluster, or had its weight set to zero (in which case,
    // failure detector can't be relied on since nodes with weight zero don't
    // normally receive gossip messages). Anyhow, tell the client to be kind
    // and try another node.
    sendError(appender.get(), E::NOTREADY);
    return;
  }

  if (isIsolated()) {
    // More than half of the nodes in the cluster appear to be dead. This node
    // is isolated and can't accept append operations.
    sendError(appender.get(), E::ISOLATED);
    return;
  }

  // Verify the integrity of the checksum bits: CHECKSUM_PARITY should be the
  // XNOR of the other two.
  bool expected_parity = bool(header_.flags & APPEND_Header::CHECKSUM) ==
      bool(header_.flags & APPEND_Header::CHECKSUM_64BIT);
  if (expected_parity != bool(header_.flags & APPEND_Header::CHECKSUM_PARITY)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "APPEND request from %s failed for log %lu because the "
                    "checksum flag parity was invalid",
                    Sender::describeConnection(from_).c_str(),
                    header_.logid.val_);
    sendError(appender.get(), E::BADPAYLOAD);
    return;
  }

  // If the verify-checkum-before-storage setting is used and the message came
  // with a checksum, verify it.
  if (getSettings().verify_checksum_before_replicating &&
      header_.flags & APPEND_Header::CHECKSUM) {
    uint64_t payload_checksum = 0;
    uint64_t expected_checksum = 0;
    if (!appender->verifyChecksum(&payload_checksum, &expected_checksum)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "APPEND request from %s failed for log %lu because the "
                      "checksum verification failed. "
                      "payload checksum %lx, calculated checksum %lx",
                      Sender::describeConnection(from_).c_str(),
                      header_.logid.val_,
                      payload_checksum,
                      expected_checksum);
      sendError(appender.get(), E::BADPAYLOAD);
      return;
    }
  }

  auto permission_checker = getPermissionChecker();
  // permissions are not checked for internal appends
  if (!permission_checker || !from_.valid()) {
    sendReply(std::move(appender), PermissionCheckStatus::NONE);
    return;
  }

  const PrincipalIdentity* principal = getPrincipal();
  if (principal == nullptr) {
    ld_critical("APPEND Request from %s for log %lu failed because "
                "there is no Principal associated with %s",
                Sender::describeConnection(from_).c_str(),
                header_.logid.val_,
                Sender::describeConnection(from_).c_str());
    // This should never happen. We are invoking onReceived from the socket
    // that has already performed the hello/ack handshake.
    ld_check(false);
    sendError(appender.get(), E::ACCESS);
    return;
  }

  if (appender_span_) {
    permission_checking_span_ = e2e_tracer_->StartSpan(
        "Permission_checking", {ChildOf(&appender_span_->context())});
  }

  std::shared_ptr<AppenderPrep> appender_prep = shared_from_this();
  isAllowed(permission_checker,
            *principal,
            [appender = std::move(appender),
             appender_prep = std::move(appender_prep),
             permission_checking_span_ = permission_checking_span_](
                PermissionCheckStatus permission_status) mutable {
              if (permission_checking_span_) {
                permission_checking_span_->Finish();
              }

              appender_prep->sendReply(std::move(appender), permission_status);
            });

  return;
}

void AppenderPrep::onSequencerNodeFound(Status st,
                                        logid_t datalog_id,
                                        NodeID seq_node,
                                        std::unique_ptr<Appender> appender,
                                        std::shared_ptr<Sequencer> sequencer) {
  if (!canActivateSequencers()) {
    // on-demand sequencer activation is disabled. pick self as sequencer node
    // in that case to preserve the behavior of previous implementation.
    // TODO: this should perhaps be handled by implementing a different
    // SequencerLocator for no routing and static routing, with a setting to
    // select which method to use, independent of wether the failure
    // detector is running or not.
    seq_node = getMyNodeID();
  } else if (st != E::OK) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "No sequencer node found for log %lu, failing the "
                      "append with E::NOSEQUENCER.",
                      datalog_id.val_);
    sendError(appender.get(), E::NOSEQUENCER);
    return;
  }

  Decision redirect_decision = shouldRedirect(seq_node, sequencer.get());
  if (redirect_decision == Decision::REDIRECT) {
    // `seq_node' should handle writes for this log instead
    sendRedirect(appender.get(), seq_node, E::REDIRECTED);
    return;
  }
  if (!isAcceptingWork()) {
    sendError(appender.get(), E::SHUTDOWN);
    return;
  }

  auto bump_activation_stats = [this, datalog_id](
                                   Decision redir_decision, bool preempted) {
    switch (redir_decision) {
      case Decision::REDIRECT:
        // shouldn't activate sequencer if we need redirect
        ld_check(false);
        break;
      case Decision::CORRECT_NODE:
        if (preempted) {
          // sequencer is preempted by itself, unlikely to happen
          STAT_INCR(stats(), sequencer_activations_preempted_self);
        } else {
          // common case: on-demand sequencer bring-up
          STAT_INCR(stats(), sequencer_activations_no_seq_correct_node);
        }
        break;
      case Decision::FLAG_NO_REDIRECT:
        ld_check(!preempted);
        // sequencer is brought up on-demand
        // because no_redirect flag is set by the client
        STAT_INCR(stats(), sequencer_activations_no_seq_flag_no_redirect);
        break;
      case Decision::FLAG_REACTIVATE_IF_PREEMPTED:
        ld_check(preempted);
        // sequencer was preempted but is reactivated since
        // the REACTIVATE_IF_PREEMPTED flag is set by the client
        STAT_INCR(stats(), sequencer_activations_preempted_flag_reactivate);
        break;
      case Decision::NO_REDIRECT_UNTIL:
        // this shouldn't cause sequencer to (re)activate since the sequencer
        // is not preempted and in ACTIVE state
        ld_critical("Sequencer activation for log %lu from %s caused by "
                    "NO_REDIRECT_UNTIL. This shouldn't happen.",
                    datalog_id.val_,
                    Sender::describeConnection(from_).c_str());
        ld_check(false);
        break;
      case Decision::PREEMPTED_DEAD:
        ld_check(preempted);
        // sequencer was preempted but preemptor is considered dead
        STAT_INCR(stats(), sequencer_activations_preempted_dead);
        break;
    };
  };

  // Note: we use predicate functions to determine whether a sequencer can be
  // activated. The pattern is that we check the predicate and decide whether or
  // not to (re)activate sequencer, then pass the predicate to the activation
  // function again so that they can be evaluated under sequencer's internal
  // mutex so that to prevent concurrent activations on the same condition.

  auto seq_unavailable_pred = [](const Sequencer& seq) {
    // sequencer has not yet gotten a valid epoch

    // Note: the Sequencer will attempt to run the append if the it is
    // in ACTIVATING state. Sequencer::runAppender() will fail with
    // E::INPROGRESS, making the Appender buffered it AppenderBuffer until
    // activation is completed
    return seq.getState() == Sequencer::State::UNAVAILABLE;
  };

  if (!sequencer || seq_unavailable_pred(*sequencer)) {
    // Activate the sequencer if it doesn't exist yet or the sequencer is
    // unavailable.
    // In the meantime, let the Appender sit in a queue.
    int rv = activateAndBuffer(header_.logid,
                               appender,
                               sequencer,
                               /*force=*/false,
                               seq_unavailable_pred);
    if (rv == 0) {
      bump_activation_stats(redirect_decision, /* preempted = */ false);
      return;
    } else if (err != E::EXISTS && err != E::ABORTED) {
      // E::ABORT means that the pred evaluated false and the sequencer is no
      // longer in unavailable state, we should try the appender instead.
      sendError(appender.get(), err);
      return;
    }
  }

  ld_check(sequencer);

  auto seq_preempted_pred = [](const Sequencer& seq) {
    return seq.isPreempted();
  };

  auto preempted_by = sequencer->checkIfPreempted(sequencer->getCurrentEpoch());
  if (preempted_by.isNodeID()) {
    // This sequencer was detected to be preempted by another one (either due
    // to recovery or a STORE failing with E::PREEMPTED). Check if we should
    // send a redirect to its preemptor instead of reactivating the sequencer.
    redirect_decision = shouldRedirect(preempted_by,
                                       sequencer.get(),
                                       /* preempted = */ true);
    if (redirect_decision == Decision::REDIRECT) {
      sendRedirect(appender.get(), preempted_by, E::PREEMPTED);
      return;
    }

    if (canActivateSequencers()) {
      int rv = activateAndBuffer(header_.logid,
                                 appender,
                                 sequencer,
                                 /*force=*/false,
                                 seq_preempted_pred);
      if (rv == 0) {
        bump_activation_stats(redirect_decision, /* preempted = */ true);
        return;
      } else if (err != E::EXISTS && err != E::ABORTED) {
        // E::ABORT means that the pred evaluated false and the sequencer is no
        // longer preempted. It's likely that sequencer has been reactivate to a
        // new epoch, we should try the appender instead.
        sendError(appender.get(), err);
        return;
      }
    }
  }

  if (!MetaDataLog::isMetaDataLog(header_.logid)) {
    if (!checkNodeSet(*sequencer) && err != E::INPROGRESS) {
      // More than n - r storage nodes are not available, which means
      // that the APPEND request cannot be fulfilled at this time.
      // Abort early and return E::NOSPC, E::OVERLOADED or E::SYSLIMIT
      // to the client
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "APPEND request from %s for log %lu failed because "
                      "there are too many storage nodes not available "
                      "to complete the request: %s",
                      Sender::describeConnection(from_).c_str(),
                      header_.logid.val_,
                      error_name(err));
      sendError(appender.get(), err);
      return;
    }

    auto state = sequencer->getState();
    if ((state == Sequencer::State::ACTIVE ||
         state == Sequencer::State::PREEMPTED) &&
        hasBufferedAppenders(header_.logid)) {
      // In the rare case that sequencer is up, but there are still buffered
      // Appenders not yet processed, we have to buffer this Appender too so
      // that ordering is preserved
      if (bufferAppender(header_.logid, appender) == 0) {
        return;
      }
      sendError(appender.get(), err);
      return;
    }
  }

  // See if this append should be buffered for batching.  NOTE: Not the same
  // as AppenderBuffer above.
  Worker* w = Worker::onThisThread(false);
  if (w) {
    Processor* processor = w->processor_;
    if (allow_batching_ &&
        processor->sequencerBatching().buffer(header_.logid, appender)) {
      return;
    }
  }

  RunAppenderStatus status = append(sequencer, appender);
  switch (status) {
    case RunAppenderStatus::ERROR_DELETE:
      // Appender could not be started. Report back to AppendRequest.
      sendError(appender.get(), err);
      break;
    case RunAppenderStatus::SUCCESS_KEEP:
      // The record has been assigned an LSN and an Appender is running.  Once
      // it's done that Appender will send a reply and delete itself.
      appender.release();
      FOLLY_FALLTHROUGH;
    case RunAppenderStatus::SUCCESS_DELETE:
      if (header_.flags & APPEND_Header::FORCE) {
        // To avoid redirecting excessively, once an append with NO_REDIRECT or
        // REACTIVATE_IF_PREEMPTED is processed, we'll keep this node sequencing
        // a log for at least a little longer.
        updateNoRedirectUntil(*sequencer);
      }
  }
}

std::unique_ptr<Appender> AppenderPrep::constructAppender() {
  // Assert that the checksum flag bits match between APPEND and STORE
  // headers, then copy them from the APPEND header
  static_assert(
      APPEND_Header::CHECKSUM == STORE_Header::CHECKSUM &&
          APPEND_Header::CHECKSUM_64BIT == STORE_Header::CHECKSUM_64BIT &&
          APPEND_Header::CHECKSUM_PARITY == STORE_Header::CHECKSUM_PARITY &&
          APPEND_Header::BUFFERED_WRITER_BLOB ==
              STORE_Header::BUFFERED_WRITER_BLOB,
      "");
  STORE_flags_t passthru_flags = header_.flags &
      (APPEND_Header::CHECKSUM | APPEND_Header::CHECKSUM_64BIT |
       APPEND_Header::CHECKSUM_PARITY | APPEND_Header::BUFFERED_WRITER_BLOB);

  // TODO: This does not account for Appender's
  // std::shared_ptr<PayloadHolder>'s shared segment, or evbuffer overhead if
  // the PayloadHolder wraps one.  There is no longer a reason to calculate
  // this externally and pass into Appender; Appender could just calculate it
  // itself.
  size_t full_size = sizeof(Appender) + payload_.size();

  auto appender =
      std::make_unique<Appender>(Worker::onThisThread(),
                                 Worker::onThisThread()->getTraceLogger(),
                                 std::chrono::milliseconds(header_.timeout_ms),
                                 header_.rqid,
                                 passthru_flags,
                                 header_.logid,
                                 std::move(attrs_),
                                 std::move(payload_),
                                 from_,
                                 header_.seen,
                                 full_size,
                                 lsn_before_redirect_,
                                 e2e_tracer_);
  appender->setAppendMessageCount(append_message_count_);
  appender->setAcceptableEpoch(acceptable_epoch_);
  return appender;
}

std::shared_ptr<Sequencer> AppenderPrep::findSequencer(logid_t log_id) const {
  return Worker::onThisThread()->processor_->allSequencers().findSequencer(
      log_id);
}

bool AppenderPrep::isAlive(NodeID node) const {
  bool result = Worker::onThisThread()->processor_->isNodeAlive(node.index());
  if (!result) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        5,
        "Node %s is not alive according to Failure Detector. Log:%lu",
        node.toString().c_str(),
        header_.logid.val_);
  }

  return result;
}

bool AppenderPrep::isBoycotted(NodeID node) const {
  bool result =
      Worker::onThisThread()->processor_->isNodeBoycotted(node.index());
  if (result) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        5,
        "Node %s is boycotted according to Failure Detector. Log:%lu",
        node.toString().c_str(),
        header_.logid.val_);
  }

  return result;
}

bool AppenderPrep::isIsolated() const {
  bool result = Worker::onThisThread()->processor_->isNodeIsolated();
  if (result) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   5,
                   "Node is isolated according to Failure Detector. Log:%lu",
                   header_.logid.val_);
  }

  return result;
}

bool AppenderPrep::canActivateSequencers() const {
  // Allow lazy sequencer activation only if the gossip-based failure detector
  // is used.
  return Worker::onThisThread()->processor_->isFailureDetectorRunning();
}

bool AppenderPrep::nodeInConfig(NodeID node) const {
  const auto nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto& seq_membership = nodes_configuration->getSequencerMembership();

  if (!seq_membership->isSequencingEnabled(node.index())) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Node %s is not in sequencer membership or does not have "
                      "a positive sequencer weight",
                      node.toString().c_str());
    return false;
  }

  return true;
}

NodeID AppenderPrep::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

AppenderPrep::Decision AppenderPrep::shouldRedirect(NodeID seq_node,
                                                    const Sequencer* sequencer,
                                                    bool preempted) const {
  ld_check(seq_node.isNodeID());
  if (seq_node.index() == getMyNodeID().index()) {
    // `seq_node' is this node, we should execute the append
    return Decision::CORRECT_NODE;
  }

  ld_assert(seq_node != getMyNodeID());
  if (!canActivateSequencers()) {
    // Send a redirect since we're not allowed to bring up sequencers on
    // this node.
    return Decision::REDIRECT;
  }

  if (!preempted) {
    if (header_.flags & APPEND_Header::NO_REDIRECT) {
      // NO_REDIRECT is a signal to the node to bring up a sequencer even if
      // it would normally redirect to another node (because `log_id' hashes
      // to a different node).
      // it normally wouldn't do so (e.g. because it determined that some other
      // node should run a sequencer for the log). Clients normally set it when
      // they're unable to follow a redirect or hit redirect loops.
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Received NO_REDIRECT flag for log:%lu, from %s, rqid:%lu",
                     header_.logid.val_,
                     Sender::describeConnection(from_).c_str(),
                     header_.rqid.val());
      STAT_INCR(stats(), append_no_redirect);
      return Decision::FLAG_NO_REDIRECT;
    }
  } else {
    if (header_.flags & APPEND_Header::REACTIVATE_IF_PREEMPTED) {
      // By setting REACTIVATE_IF_PREEMPTED, clients can force a node to bring
      // up a sequencer even if it was previously preempted. Clients use it when
      // they detect redirect cycles; it helps to ensure that eventually each
      // log is handled by a sequencer the log id hashes to (consider the
      // scenario where N0 has an active sequencer for a log, but h(log_id)=N1,
      // and sequencer for `log_id' on N1 got preempted by N0; client will
      // follow this path of redirects: N0 -> N1 -> N0 (due to preemption) ->
      // N1 (cycle; REACTIVATE_IF_PREEMPTED flag set); as a result, N1 will end
      // up running a sequencer for the log, as it should).
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Received REACTIVATE_IF_PREEMPTED flag for log:%lu,"
                     " from %s, rqid:%lu",
                     header_.logid.val_,
                     Sender::describeConnection(from_).c_str(),
                     header_.rqid.val());
      STAT_INCR(stats(), append_reactivate_if_preempted);
      return Decision::FLAG_REACTIVATE_IF_PREEMPTED;
    }
  }

  if (!sequencer || !isAcceptingWork()) {
    return Decision::REDIRECT;
  }

  // Don't redirect if the sequencer is still active (hasn't been preempted) and
  // now() < no_redirect_until_. This is used to prevent excessive sequencer
  // reactivations: if an APPEND with NO_REDIRECT is received and a sequencer is
  // activated on this box, it'll keep executing appends for a short time
  // without redirecting.
  if (!preempted && sequencer->getState() == Sequencer::State::ACTIVE) {
    if (sequencer->checkNoRedirectUntil()) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Not redirecting to Node %s for log:%lu because"
                     " no_redirect_until hasn't expired.",
                     seq_node.toString().c_str(),
                     header_.logid.val_);
      return Decision::NO_REDIRECT_UNTIL;
    }
  }

  // If `seq_node' is known to be alive and not boycotted, redirect to it (note
  // that we err on the side of caution and consider all nodes alive if the
  // failure detector is not used).
  if (isAlive(seq_node) && !isBoycotted(seq_node)) {
    return Decision::REDIRECT;
  }

  if (preempted) {
    // If we were preempted by `seq_node', but that node's not in our config,
    // we may be dealing with stale config. Send a redirect to the client,
    // hoping it'll know what to do. If `seq_node' is missing from client's
    // config as well, we expect to get a follow-up with REACTIVATE_IF_PREEMPTED
    // anyway.
    if (!nodeInConfig(seq_node)) {
      STAT_INCR(stats(), append_redir_not_in_config);
      return Decision::REDIRECT;
    }
  }

  STAT_INCR(stats(), append_preempted_dead);
  return Decision::PREEMPTED_DEAD;
}

int AppenderPrep::activateSequencer(logid_t log_id,
                                    std::shared_ptr<Sequencer>& sequencer,
                                    Sequencer::ActivationPred pred) {
  auto& seqmap = Worker::onThisThread()->processor_->allSequencers();
  int rv;

  auto state =
      sequencer ? sequencer->getState() : Sequencer::State::UNAVAILABLE;
  epoch_t cur_epoch = sequencer ? sequencer->getCurrentEpoch() : EPOCH_INVALID;
  RATELIMIT_INFO(std::chrono::seconds(10),
                 100,
                 "Attempting sequencer %sactivation for log:%lu, rqid:%lu"
                 ", state=%s, cur_epoch=%u, client:%s",
                 sequencer ? "re-" : "",
                 log_id.val_,
                 header_.rqid.val(),
                 Sequencer::stateString(state),
                 cur_epoch.val_,
                 Sender::describeConnection(from_).c_str());
  if (sequencer) {
    rv = seqmap.reactivateIf(log_id, "append (reactivation)", pred);
  } else {
    rv = seqmap.activateSequencer(log_id, "append", pred);
  }

  E saved_err = err;
  if (err == E::TOOMANY) {
    // too many recent reactivations
    STAT_INCR(stats(), append_rejected_reactivation_limit);
  }

  sequencer = seqmap.findSequencer(log_id);
  err = saved_err;
  return rv;
}

int AppenderPrep::bufferAppender(logid_t log_id,
                                 std::unique_ptr<Appender>& appender) {
  ld_check(appender != nullptr);
  Worker* w = Worker::onThisThread();
  // If sequencer batching is on, prefer to hand the Appender to it instead of
  // AppenderBuffer, so that it can get batched like all incoming appends.
  // `allow_batching_' assures that this is not an Appender that already went
  // through sequencer batching.
  Processor* processor = w->processor_;
  if (allow_batching_ &&
      processor->sequencerBatching().buffer(log_id, appender)) {
    return 0;
  }

  return w->appenderBuffer().bufferAppender(log_id, appender)
      ? 0
      : (err = E::PENDING_FULL, -1);
}

int AppenderPrep::activateAndBuffer(logid_t log_id,
                                    std::unique_ptr<Appender>& appender,
                                    std::shared_ptr<Sequencer>& sequencer,
                                    bool force,
                                    Sequencer::ActivationPred pred) {
  // always activate the data log sequencer
  const logid_t datalog_id = MetaDataLog::dataLogID(log_id);
  ld_check(!sequencer || sequencer->getLogID() == datalog_id);

  if (!force && !canActivateSequencers()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Did not attempt to activate a sequencer for log:%lu since "
                    "Failure Detector is not running and 'force' is not set, "
                    "returning E::NOSEQUENCER, client:%s, rqid:%lu",
                    datalog_id.val_,
                    Sender::describeConnection(from_).c_str(),
                    header_.rqid.val());
    err = E::NOSEQUENCER;
    return -1;
  }

  if (!force && (header_.flags & APPEND_Header::NO_ACTIVATION)) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "Did not attempt to activate a sequencer for append to log %lu since "
        "append received with NO_ACTIVATION flag. Returning E::NOSEQUENCER",
        log_id.val_);
    err = E::NOSEQUENCER;
    return -1;
  }

  if (!pred) {
    // reactivate unconditionally
    pred = [](const Sequencer&) { return true; };
  }
  int rv = activateSequencer(datalog_id, sequencer, pred);
  if (rv == 0) {
    return bufferAppender(log_id, appender);
  }

  if (err == E::EXISTS || err == E::INPROGRESS) {
    // *activateSequencer() methods could fail with one of the error codes above
    // if another thread managed to activate the sequencer in the meantime. In
    // this case, let's proceed as if the sequencer was already active (and
    // possibly buffer the appender later if the sequencer is not ready yet).
    err = E::EXISTS;
    return -1;
  } else if (err == E::ABORTED) {
    // Let the caller know that conditional reactivation failed.
    return -1;
  }

  RATELIMIT_ERROR(std::chrono::seconds(1),
                  1,
                  "Failed to %sactivate a sequencer for log %lu: %s",
                  sequencer != nullptr ? "re" : "",
                  datalog_id.val_,
                  error_description(err));
  err = (err == E::NOTFOUND ? E::NOTFOUND : E::NOSEQUENCER);
  return -1;
}

bool AppenderPrep::checkNodeSet(const Sequencer& sequencer) const {
  return sequencer.checkNodeSet();
}

bool AppenderPrep::hasBufferedAppenders(logid_t log_id) const {
  return Worker::onThisThread()->appenderBuffer().hasBufferedAppenders(log_id);
}

RunAppenderStatus AppenderPrep::runAppender(Sequencer& sequencer,
                                            Appender& appender) {
  if (MetaDataLog::isMetaDataLog(header_.logid)) {
    // appender is for metadata log, redirect it to the MetaDataLogWriter
    // proxy
    auto meta_writer = sequencer.getMetaDataLogWriter();
    ld_check(meta_writer != nullptr);
    return meta_writer->runAppender(&appender);
  }

  return sequencer.runAppender(&appender);
}

RunAppenderStatus AppenderPrep::append(std::shared_ptr<Sequencer>& sequencer,
                                       std::unique_ptr<Appender>& appender,
                                       bool can_retry) {
  ld_check(sequencer);
  ld_check(appender != nullptr);

  auto status = runAppender(*sequencer, *appender);
  if (status != RunAppenderStatus::ERROR_DELETE) {
    // Success!
    return status;
  }

  ld_check(err != E::OK);
  switch (err) {
    case E::BADPAYLOAD:
      ld_check(MetaDataLog::isMetaDataLog(header_.logid));
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "APPEND request from %s failed for log %lu because the "
                      "payload does not contain valid epoch metadata.",
                      Sender::describeConnection(from_).c_str(),
                      header_.logid.val_);
      break;

    case E::NOSEQUENCER:
      ld_info("APPEND request from %s failed to find an active sequencer "
              "for log %lu. Reporting NOSEQUENCER to client.",
              Sender::describeConnection(from_).c_str(),
              header_.logid.val_);
      break;

    case E::CANCELLED:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "APPEND request from %s failed for log %lu because it "
                      "was redirected from a previous epoch (previous lsn: %s) "
                      "whose recovery status is unknown.",
                      Sender::describeConnection(from_).c_str(),
                      header_.logid.val_,
                      lsn_to_string(appender->getLSNBeforeRedirect()).c_str());
      break;

    case E::STALE: {
      // The sequencer is supposed to execute an append, but the client
      // reported that it had already successfully appended a record with
      // an epoch higher than this sequencer's. We'll reactivate the sequencer
      // into a higher epoch here (note that we could also get here if this
      // sequencer was preempted, but doesn't know it yet -- in which case we
      // would reactivate it anyway, we'll just do it sooner). Alternatively,
      // we could reject an append and potentially cause all further writes
      // from the client to fail (unlikely; see t6545771), or just execute it
      // and risk breaking the ordering.

      auto seen_epoch = header_.seen;
      auto pred = [&](const Sequencer& seq) {
        // To prevent multiple reactivations, check again if the sequencer
        // really needs to be reactivated.
        if (MetaDataLog::isMetaDataLog(header_.logid)) {
          auto* meta_writer = seq.getMetaDataLogWriter();
          ld_check(meta_writer);
          int rv = meta_writer->checkAppenderPayload(
              appender.get(), seq.getCurrentEpoch());
          // the parent epoch is still stale
          return rv != 0 && err == E::STALE;
        }

        return seen_epoch > seq.getCurrentEpoch();
      };

      if (activateAndBuffer(header_.logid,
                            appender,
                            sequencer,
                            // if the logid is a metadata logid, force the
                            // activation even if lazy reactivation is disabled
                            MetaDataLog::isMetaDataLog(header_.logid),
                            pred) == 0) {
        STAT_INCR(stats(), stale_reactivation);
        return RunAppenderStatus::SUCCESS_KEEP;
      } else if (can_retry && (err == E::EXISTS || err == E::ABORTED)) {
        // The sequencer was already reactivated by another thread. Try to run
        // the appender one more time.
        return append(sequencer, appender, /* can_retry */ false);
      }

      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Rejecting an append from %s for log %lu because "
                        "this sequencer's epoch (%u) is too old (client "
                        "expects at least %u). Reporting NOSEQUENCER.",
                        Sender::describeConnection(from_).c_str(),
                        header_.logid.val_,
                        sequencer->getCurrentEpoch().val_,
                        seen_epoch.val_);
      // err was set by a failed call to activateAndBuffer() above
      err = (err == E::NOTFOUND ? E::NOTFOUND : E::NOSEQUENCER);
      break;
    }
    case E::TOOBIG: {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "APPEND request from %s (rqid:%lu) for log %lu failed "
                     "because ESN space for epoch %u has been exhaused. "
                     "Reactivating the Sequencer to a higher Epoch.",
                     Sender::describeConnection(from_).c_str(),
                     header_.rqid.val(),
                     header_.logid.val_,
                     sequencer->getCurrentEpoch().val_);

      auto pred = [](const Sequencer& seq) { return !seq.hasAvailableLsns(); };

      if (activateAndBuffer(header_.logid,
                            appender,
                            sequencer,
                            true, // do it even if lazy reactivation is disabled
                            pred) == 0) {
        STAT_INCR(stats(), epoch_end_reactivation);
        // appender is successfully buffered with transferred ownership
        return RunAppenderStatus::SUCCESS_KEEP;
      } else if (can_retry && (err == E::EXISTS || err == E::ABORTED)) {
        // already reactivated, try again
        return append(sequencer, appender, /* can_retry */ false);
      }
      // err was set by a failed call to activateAndBuffer() above
      err = (err == E::NOTFOUND ? E::NOTFOUND : E::NOSEQUENCER);
      break;
    }
    case E::INPROGRESS:
      // Sequencer exists but is being initialized or reactivated, try
      // buffering the appender before dropping the message
      ld_debug("Buffering the Appender for log %lu from %s because "
               "Sequencer is still initializing.",
               header_.logid.val_,
               Sender::describeConnection(from_).c_str());
      if (bufferAppender(header_.logid, appender) == 0) {
        // appender is successfully buffered with transferred ownership
        return RunAppenderStatus::SUCCESS_KEEP;
      }
      // appender buffer is full, consider this as a transient error,
      // set err to E::NOBUFS which will translate to E::SEQNOBUFS when sending
      // to the client.
      err = E::NOBUFS;
      break;
    case E::NOBUFS:
      RATELIMIT_INFO(std::chrono::seconds(1),
                     2,
                     "APPEND request from %s for log %lu failed because "
                     "maximum append window size %zu has been reached",
                     Sender::describeConnection(from_).c_str(),
                     header_.logid.val_,
                     MetaDataLog::isMetaDataLog(header_.logid)
                         ? SLIDING_WINDOW_MIN_CAPACITY
                         : sequencer->getMaxWindowSize());
      break;
    case E::TEMPLIMIT:
      RATELIMIT_INFO(std::chrono::seconds(1),
                     2,
                     "APPEND request from %s for log %lu failed because "
                     "maximum total size of appenders %zu has been reached",
                     Sender::describeConnection(from_).c_str(),
                     header_.logid.val_,
                     Worker::settings().max_total_appenders_size_hard);
      break;
    case E::SYSLIMIT:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "APPEND request from %s failed with E::SYSLIMIT",
                      Sender::describeConnection(from_).c_str());
      break;
    case E::SHUTDOWN:
      ld_debug("Ignoring APPEND message: not accepting more work");
      break;
    case E::ABORTED:
      ld_check(acceptable_epoch_.hasValue());
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Aborted append from %s for log %lu due to unacceptable "
                     "epoch epoch %u requested)",
                     Sender::describeConnection(from_).c_str(),
                     header_.logid.val(),
                     acceptable_epoch_.value().val());
      break;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Unexpected error %u (%s) while processing an "
                      "APPEND request from %s. Append failed.",
                      (unsigned)err,
                      error_name(err),
                      Sender::describeConnection(from_).c_str());
      ld_check(false);
  }
  return RunAppenderStatus::ERROR_DELETE;
}

void AppenderPrep::updateNoRedirectUntil(Sequencer& sequencer) {
  sequencer.setNoRedirectUntil(Worker::settings().no_redirect_duration);
}

bool AppenderPrep::isAcceptingWork() const {
  return Worker::onThisThread()->isAcceptingWork();
}

void AppenderPrep::sendError(Appender* appender, Status status) const {
  ld_check(appender != nullptr);
  appender->sendError(status);
}

void AppenderPrep::sendRedirect(Appender* appender,
                                NodeID target,
                                Status status) const {
  ld_check(appender != nullptr);

  const logid_t datalog_id = MetaDataLog::dataLogID(header_.logid);
  RATELIMIT_INFO(std::chrono::seconds(1),
                 10,
                 "Sending a redirect to client %s (rqid:%lu) with status[%s]"
                 " for log:%lu. Redirect target is Node %s",
                 Sender::describeConnection(from_).c_str(),
                 header_.rqid.val(),
                 error_name(status),
                 datalog_id.val_,
                 target.toString().c_str());

  appender->sendRedirect(target, status);
}

StatsHolder* AppenderPrep::stats() const {
  return Worker::stats();
}

const Settings& AppenderPrep::getSettings() const {
  return Worker::settings();
}

const PrincipalIdentity* AppenderPrep::getPrincipal() {
  return Worker::onThisThread()->sender().getPrincipal(Address(from_));
}

void AppenderPrep::isAllowed(
    std::shared_ptr<PermissionChecker> permission_checker,
    const PrincipalIdentity& principal,
    callback_func_t cb) {
  ld_check(permission_checker != nullptr);
  permission_checker->isAllowed(
      ACTION::APPEND, principal, header_.logid, std::move(cb));
}

SequencerLocator& AppenderPrep::getSequencerLocator() {
  return *Worker::onThisThread()->processor_->sequencer_locator_;
}

// needed for tests mock
std::shared_ptr<PermissionChecker> AppenderPrep::getPermissionChecker() {
  if (Worker::settings().require_permission_message_types.count(
          MessageType::APPEND) == 0) {
    // Bypass permission checking since APPEND is not listed in message types
    // that require permissions
    // TODO: move permission checking out of AppenderPrep and into
    // APPEND_onReceived on server-side.
    return nullptr;
  }
  return Worker::onThisThread()
      ->processor_->security_info_->getPermissionChecker();
}

}} // namespace facebook::logdevice
