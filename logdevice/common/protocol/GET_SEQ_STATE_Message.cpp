/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_REPLY_Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

MessageReadResult GET_SEQ_STATE_Message::deserialize(ProtocolReader& reader) {
  logid_t log_id;
  request_id_t request_id(-1);
  GET_SEQ_STATE_flags_t flags = 0;
  GetSeqStateRequest::Context calling_ctx{GetSeqStateRequest::Context::UNKNOWN};
  folly::Optional<epoch_t> min_epoch;
  reader.read(&log_id);
  reader.read(&request_id);

  if (reader.proto() < Compatibility::GSS_32BIT_FLAG) {
    uint8_t flags_LEGACY = 0;
    reader.read(&flags_LEGACY);
    flags = flags_LEGACY;
  } else {
    reader.read(&flags);
  }

  reader.read(&calling_ctx);

  if (flags & GET_SEQ_STATE_Message::MIN_EPOCH) {
    epoch_t read_min_epoch;
    reader.read(&read_min_epoch);
    min_epoch.assign(read_min_epoch);
  }

  if (reader.proto() < Compatibility::IS_LOG_EMPTY_IN_GSS_REPLY) {
    flags &= ~GET_SEQ_STATE_Message::INCLUDE_IS_LOG_EMPTY;
  }

  reader.allowTrailingBytes();
  return reader.result([&] {
    return new GET_SEQ_STATE_Message(
        log_id, request_id, flags, calling_ctx, min_epoch);
  });
}

uint16_t GET_SEQ_STATE_Message::getMinProtocolVersion() const {
  if (flags_ & GET_SEQ_STATE_Message::INCLUDE_IS_LOG_EMPTY) {
    return Compatibility::IS_LOG_EMPTY_IN_GSS_REPLY;
  } else {
    return Compatibility::MIN_PROTOCOL_SUPPORTED;
  }
}

void GET_SEQ_STATE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(log_id_);
  writer.write(request_id_);

  if (writer.proto() >= Compatibility::GSS_32BIT_FLAG) {
    writer.write(flags_);
  } else {
    // Wipe all but last 8 bits, and convert to the legacy 8-bit field
    uint8_t flags_LEGACY = flags_ & 0xff;
    writer.write(flags_LEGACY);
  }

  writer.write(calling_ctx_);

  if (flags_ & GET_SEQ_STATE_Message::MIN_EPOCH) {
    ld_check(min_epoch_.hasValue());
    writer.write(min_epoch_.value());
  }
}

CopySetSelector::Result
GET_SEQ_STATE_Message::getCopySet(std::shared_ptr<Sequencer> seq,
                                  std::vector<ShardID>& copyset_out) {
  ld_check(seq);
  copyset_manager_ = seq->getCurrentCopySetManager();

  logid_t datalog_id = seq->getLogID();

  if (!copyset_manager_) {
    WORKER_STAT_INCR(check_seal_req_copysetmanager_invalid);
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "There is no copyset manager to pick a copyset for log:%lu"
                    ", GSS rqid:%lu, sequencer state=%s, epoch=%u",
                    datalog_id.val_,
                    request_id_.val(),
                    Sequencer::stateString(seq->getState()),
                    seq->getCurrentEpoch().val_);
    return CopySetSelector::Result::FAILED;
  }

  const copyset_size_t replication =
      copyset_manager_->getCopySetSelector()->getReplicationFactor();
  StoreChainLink* copyset =
      (StoreChainLink*)alloca(replication * sizeof(StoreChainLink));
  copyset_size_t ndest = 0;
  ld_spew("Calling getCopysetUsingUnderlyingSelector() for log:%lu, rqid:%lu",
          datalog_id.val_,
          request_id_.val());
  auto result = copyset_manager_->getCopysetUsingUnderlyingSelector(datalog_id,
                                                                    0, // extras
                                                                    copyset,
                                                                    &ndest);

  ld_spew("Copyset selection result for log:%lu, rqid:%lu, result:%d",
          datalog_id.val_,
          request_id_.val(),
          (int)result);
  switch (result) {
    case CopySetSelector::Result::PARTIAL:
    case CopySetSelector::Result::SUCCESS:
      ld_check(ndest == replication);
      break;
    case CopySetSelector::Result::FAILED:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "There are not ENOUGH nodes to send CHECK_SEAL request"
                      " for log:%lu (GSS rqid:%lu), needed:%d, got:%d",
                      datalog_id.val_,
                      request_id_.val(),
                      replication,
                      ndest);
      return result;
    default:
      ld_critical("Unexpected error code %d returned from copy set selection"
                  " for log:%lu (GSS rqid:%lu)",
                  (int)result,
                  datalog_id.val_,
                  request_id_.val());
      ld_check(false);
      return CopySetSelector::Result::FAILED;
  }

  copyset_out.resize(replication);
  std::string copyset_nodes_str;
  for (int i = 0; i < ndest; i++) {
    copyset_out[i] = copyset[i].destination;
    copyset_nodes_str += copyset_out[i].toString() + ",";
  }

  ld_spew("Nodes selected in copyset for log:%lu (GSS rqid:%lu): [%s]",
          datalog_id.val_,
          request_id_.val(),
          copyset_nodes_str.c_str());
  return CopySetSelector::Result::SUCCESS;
}

Message::Disposition
GET_SEQ_STATE_Message::checkSeals(Address src, std::shared_ptr<Sequencer> seq) {
  logid_t datalog_id = seq->getLogID();
  if (!seq) {
    ld_spew("Sequencer not present for log:%lu, "
            "not creating a CheckSealRequest, GSS rqid:%lu",
            datalog_id.val_,
            request_id_.val());
    ld_check(false);
  }

  std::vector<ShardID> copyset;
  if (getCopySet(seq, copyset) != CopySetSelector::Result::SUCCESS) {
    ld_debug("Can't pick a copyset for log:%lu, GSS rqid:%lu",
             datalog_id.val_,
             request_id_.val());
    return Disposition::NORMAL;
  }

  auto rq = std::make_unique<CheckSealRequest>(
      std::unique_ptr<GET_SEQ_STATE_Message>(this),
      src,
      datalog_id,
      copyset,
      seq->getCurrentEpoch());

  rq->execute();
  rq.release(); // ownership of GET_SEQ_STATE message transferred

  return Disposition::KEEP;
}

Status
GET_SEQ_STATE_Message::getSequencer(logid_t datalog_id,
                                    std::shared_ptr<Sequencer>& sequencer_out,
                                    Address const& from,
                                    NodeID& preempted_by) {
  Worker* w = Worker::onThisThread();
  auto& seqmap = w->processor_->allSequencers();
  Status status = E::OK;
  int rv = 0;

  sequencer_out = seqmap.findSequencer(datalog_id);

  auto seq_activation_pred = [this](const Sequencer& seq) {
    // activate if:
    // 1) sequencer does not have a valid epoch; OR
    // 2) current epoch is less than min_epoch_
    return seq.getState() == Sequencer::State::UNAVAILABLE ||
        (min_epoch_.hasValue() && seq.getCurrentEpoch() < min_epoch_.value());
  };

  epoch_t cur_epoch =
      sequencer_out ? sequencer_out->getCurrentEpoch() : EPOCH_INVALID;
  auto state =
      sequencer_out ? sequencer_out->getState() : Sequencer::State::UNAVAILABLE;
  if (!sequencer_out || seq_activation_pred(*sequencer_out)) {
    WORKER_STAT_INCR(get_seq_state_nosequencer);
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        100,
        "Sequencer for log:%lu is %s, state=%s, cur_epoch=%u,"
        " attempting to activate it upon receiving GSS (rqid:%lu, ctx:%s, "
        "min_epoch:%s) from %s",
        datalog_id.val_,
        sequencer_out ? "present" : "absent",
        Sequencer::stateString(state),
        cur_epoch.val_,
        request_id_.val(),
        getContextString(calling_ctx_).c_str(),
        min_epoch_.hasValue() ? std::to_string(min_epoch_.value().val()).c_str()
                              : "none",
        Sender::describeConnection(from).c_str());

    if (!sequencer_out) {
      rv = seqmap.activateSequencer(
          datalog_id, "GET_SEQ_STATE", seq_activation_pred);
    } else {
      rv = seqmap.reactivateIf(
          datalog_id, "GET_SEQ_STATE (reactivation)", seq_activation_pred);
    }
  } else {
    cur_epoch = sequencer_out->getCurrentEpoch();
    preempted_by = sequencer_out->checkIfPreempted(cur_epoch);
    state = sequencer_out->getState();

    if (preempted_by.isNodeID()) {
      if (facebook::logdevice::dbg::currentLevel <
          facebook::logdevice::dbg::Level::DEBUG) {
        RATELIMIT_INFO(
            std::chrono::seconds(1),
            1,
            "Sequencer for log:%lu is in %s state and was preempted by %s",
            datalog_id.val_,
            Sequencer::stateString(state),
            preempted_by.toString().c_str());
      } else {
        ld_debug("Sequencer for log:%lu is in %s state "
                 "and was preempted by %s",
                 datalog_id.val_,
                 Sequencer::stateString(state),
                 preempted_by.toString().c_str());
      }

      auto p = w->processor_;
      bool preemptor_dead = (p->isFailureDetectorRunning() &&
                             !p->isNodeAlive(preempted_by.index()));
      bool preemptor_boycotted = (p->isFailureDetectorRunning() &&
                                  p->isNodeBoycotted(preempted_by.index()));
      if ((flags_ & GET_SEQ_STATE_Message::REACTIVATE_IF_PREEMPTED) ||
          preemptor_dead || preemptor_boycotted) {
        if (preemptor_dead) {
          ld_info("Sequencer for log:%lu was preempted by %s, "
                  "current sequencer's state (%s), "
                  "but the preemptor is dead according to the failure "
                  "detector, reactivating sequencer.",
                  datalog_id.val_,
                  preempted_by.toString().c_str(),
                  Sequencer::stateString(state));
          STAT_INCR(stats(), get_seq_state_reactivate_preemptor_dead);
        } else if (preemptor_boycotted) {
          ld_info("Sequencer for log:%lu was preempted by %s, "
                  "current sequencer's state (%s), "
                  "but the preemptor is boycotted according to the failure "
                  "detector, reactivating sequencer.",
                  datalog_id.val_,
                  preempted_by.toString().c_str(),
                  Sequencer::stateString(state));
          STAT_INCR(stats(), get_seq_state_reactivate_preemptor_boycotted);
        } else {
          // client wants to forcefully re-activate(into higher epoch)
          // a sequencer
          ld_info("Sequencer for log:%lu was preempted by %s, "
                  "current sequencer's state (%s), "
                  "received REACTIVATE_IF_PREEMPTED flag from %s, "
                  "reactivating sequencer.",
                  datalog_id.val_,
                  preempted_by.toString().c_str(),
                  Sequencer::stateString(state),
                  Sender::describeConnection(from).c_str());
          STAT_INCR(stats(), get_seq_state_reactivate_if_preempted);
        }
        rv = seqmap.reactivateIf(
            datalog_id, "GET_SEQ_STATE (preempted)", [](const Sequencer& seq) {
              return seq.isPreempted();
            });
      } else {
        status = E::PREEMPTED;
        WORKER_STAT_INCR(get_seq_state_redirect);
        RATELIMIT_INFO(std::chrono::seconds(1),
                       1,
                       "Returning status=PREEMPTED to [%s] for log:%lu. "
                       "Preemption redirect target is Node %s",
                       Sender::describeConnection(from).c_str(),
                       log_id_.val_,
                       preempted_by.toString().c_str());
        return status;
      }
    } else {
      if (flags_ & GET_SEQ_STATE_Message::REACTIVATE_IF_PREEMPTED) {
        ld_debug("Sequencer for log:%lu wasn't preempted, "
                 "current sequencer's state (%s), "
                 "received REACTIVATE_IF_PREEMPTED flag from %s, "
                 "but not reactivating.",
                 datalog_id.val_,
                 Sequencer::stateString(state),
                 Sender::describeConnection(from).c_str());
      }
    }
  }

  // It is ok to do error handling for both activation and reactivation
  // cases together for now, because reactivateSequencer internally calls
  // AllSequencers::activateSequencer, and both return same error codes
  if (rv) {
    switch (err) {
      case E::INPROGRESS:
      case E::AGAIN:
      // E::FAILED means an error in initiating epoch store request, see
      // AllSequencers::activateSequencer
      case E::FAILED:
        // consider this as transient error
        status = E::AGAIN;
        break;
      case E::EXISTS:
      case E::ABORTED:
        // EXISTS and ABORTED treated as E::OK
        break;
      case E::ISOLATED:
      case E::NOTFOUND:
      case E::SYSLIMIT:
      case E::NOBUFS:
      case E::TOOMANY:
        RATELIMIT_WARNING(std::chrono::seconds(1),
                          5,
                          "sequencer activation for log:%lu from %s"
                          ", failed with err %s",
                          log_id_.val_,
                          Sender::describeConnection(from).c_str(),
                          error_name(err));
        status = err == E::NOTFOUND ? E::NOTFOUND : E::FAILED;
        break;
      default:
        RATELIMIT_CRITICAL(
            std::chrono::seconds(1),
            5,
            "INTERNAL ERROR: sequencer activation for log:%lu from %s"
            ", failed with unexpected error %s",
            log_id_.val_,
            Sender::describeConnection(from).c_str(),
            error_name(err));
        status = E::FAILED;
        ld_check(false);
        break;
    }

    if (status != E::OK) {
      return status;
    }
  }

  sequencer_out = seqmap.findSequencer(datalog_id);
  ld_check(sequencer_out != nullptr);

  return status;
}

void GET_SEQ_STATE_Message::updateNoRedirectUntil(
    std::shared_ptr<Sequencer> sequencer) {
  ld_check(sequencer != nullptr);
  sequencer->setNoRedirectUntil(Worker::settings().no_redirect_duration);
}

Message::Disposition GET_SEQ_STATE_Message::onReceived(Address const& from) {
  GET_SEQ_STATE_REPLY_Header reply_hdr = {
      log_id_,
      LSN_INVALID, // last_released_lsn
      LSN_INVALID  // next_lsn
  };
  Status status = E::OK;
  NodeID redirect;

  const logid_t datalog_id = MetaDataLog::dataLogID(log_id_);
  ld_spew("Received a GET_SEQ_STATE message(rqid:%lu, ctx:%s, flags:%u) "
          "for log %lu (datalog:%lu) from %s",
          request_id_.val(),
          getContextString(calling_ctx_).c_str(),
          flags_,
          log_id_.val_,
          datalog_id.val_,
          Sender::describeConnection(from).c_str());

  if (calling_ctx_ == GetSeqStateRequest::Context::UNRELEASED_RECORD) {
    WORKER_LOG_STAT_INCR(
        log_id_, get_seq_state_received_context_unreleased_record);
  }

  // Check if this node can activate a sequencer
  if (!isReady(&status)) {
    sendReply(from, reply_hdr, status, NodeID());
    return Disposition::NORMAL;
  }

  auto cb = [=](Status st, logid_t log_id, NodeID node_id) {
    onSequencerNodeFound(st, log_id, node_id, from);
  };

  auto& locator = getSequencerLocator();
  if (locator.locateSequencer(datalog_id, cb) != 0) {
    sendReply(from, reply_hdr, E::NOSEQUENCER, NodeID());
    return Disposition::NORMAL;
  }

  // GET_SEQ_STATE_Message will be destroyed automatically by
  // onSequencerNodeFound or ownership transferred by checkSeals
  return Disposition::KEEP;
}

void GET_SEQ_STATE_Message::onSequencerNodeFound(Status status,
                                                 logid_t datalog_id,
                                                 NodeID redirect,
                                                 Address const& from) {
  // will destroy this when out of scope
  std::unique_ptr<GET_SEQ_STATE_Message> msg(this);

  GET_SEQ_STATE_REPLY_Header reply_hdr = {
      log_id_,
      LSN_INVALID, // last_released_lsn
      LSN_INVALID  // next_lsn
  };

  // Consult the failure detector to figure out which node is supposed to be
  // running a sequencer for the log. Send a redirect if it's some node other
  // than this one.
  if (shouldRedirectOrFail(datalog_id, status, redirect, from)) {
    sendReply(from, reply_hdr, status, redirect);
    return;
  }

  auto w = Worker::onThisThread();
  auto failure_detector_running = w->processor_->isFailureDetectorRunning();
  if (failure_detector_running && !Worker::settings().disable_check_seals) {
    auto& seqmap = w->processor_->allSequencers();
    std::shared_ptr<Sequencer> sequencer = nullptr;
    sequencer = seqmap.findSequencer(datalog_id);
    if (sequencer) {
      epoch_t cur_epoch = sequencer->getCurrentEpoch();
      auto state = sequencer->getState();
      switch (state) {
        case Sequencer::State::UNAVAILABLE:
          break;
        case Sequencer::State::PREEMPTED:
          if (cur_epoch == EPOCH_INVALID) {
            NodeID preempted_by = sequencer->checkIfPreempted(cur_epoch);
            auto st = preempted_by.isNodeID() ? E::REDIRECTED : E::AGAIN;
            RATELIMIT_INFO(
                std::chrono::seconds(10),
                5,
                "Sequencer for log:%lu is in PREEMPTED state and cur_epoch is "
                "INVALID, this means that the sequencer failed  epoch store "
                "conditional update during first time activation. "
                "(st:%s, preempted_by: %s)",
                datalog_id.val_,
                error_description(st),
                preempted_by.toString().c_str());
            sendReply(from, reply_hdr, st, preempted_by);
            return;
          }

          // In all other cases, we still perform check seals since the
          // preemptor a sequencer got may not be up-to-date. Attempt to
          // figureout the latest sequencer to send more accurate redirects
          /* BOOST_FALLTHROUGH */
        case Sequencer::State::ACTIVE:
          if (checkSeals(from, sequencer) == Disposition::KEEP) {
            // ownership is transferred to CheckSealsRequest
            msg.release();
            return;
          }
          /* BOOST_FALLTHROUGH */
        case Sequencer::State::ACTIVATING:
          sendReply(from, reply_hdr, E::AGAIN, NodeID());
          return;
          break;
        default:
          // let continueExecution() handle it
          break;
      }
    }
  }

  continueExecution(from);
}

void GET_SEQ_STATE_Message::continueExecution(Address const& from) {
  GET_SEQ_STATE_REPLY_Header reply_hdr = {
      log_id_,
      LSN_INVALID, // last_released_lsn
      LSN_INVALID  // next_lsn
  };
  NodeID redirect;

  const logid_t datalog_id = MetaDataLog::dataLogID(log_id_);
  std::shared_ptr<Sequencer> sequencer = nullptr;
  NodeID preempted_by;
  Status status = getSequencer(datalog_id, sequencer, from, preempted_by);
  if (status != E::OK) {
    sendReply(from, reply_hdr, status, preempted_by);
    return;
  }

  const bool wait_for_recovery =
      !(flags_ & GET_SEQ_STATE_Message::DONT_WAIT_FOR_RECOVERY);

  auto state = sequencer->getState();

  if (state != Sequencer::State::ACTIVE ||
      (wait_for_recovery && !sequencer->isRecoveryComplete())) {
    // Client(storage nodes) will give up sending requests to this node, once
    // their backoff timer expires. This ensures that this node doesn't keep
    // on getting the same request over and over again.
    RATELIMIT_INFO(
        std::chrono::seconds(5),
        5,
        "Received a GET_SEQ_STATE message (log:%lu, rqid:%lu, ctx:%s) from %s, "
        "but its data log sequencer's state is %s",
        log_id_.val_,
        request_id_.val(),
        getContextString(calling_ctx_).c_str(),
        Sender::describeConnection(from).c_str(),
        (state != Sequencer::State::ACTIVE) ? Sequencer::stateString(state)
                                            : "Recovery Incomplete");
    status = E::AGAIN;
  }

  if (flags_ & GET_SEQ_STATE_Message::FORCE) {
    // To avoid redirecting excessively, we'll keep this node sequencing
    // for a little longer.
    updateNoRedirectUntil(sequencer);
  }
  folly::Optional<LogTailAttributes> tail_attributes = folly::none;
  folly::Optional<OffsetMap> epoch_offsets = folly::none;
  std::shared_ptr<const EpochMetaDataMap> metadata_map;
  std::shared_ptr<TailRecord> tail_record;
  folly::Optional<bool> is_log_empty = folly::none;

  if (status == E::OK) {
    // If the request is for the metadata log, provide last_released_lsn with
    // our best effort
    if (MetaDataLog::isMetaDataLog(log_id_)) {
      MetaDataLogWriter* meta_writer = sequencer->getMetaDataLogWriter();
      ld_check(meta_writer);
      reply_hdr.last_released_lsn = meta_writer->getLastReleased();
      reply_hdr.next_lsn = compose_lsn(sequencer->getCurrentEpoch(), ESN_MIN);
      if (wait_for_recovery && reply_hdr.last_released_lsn == LSN_INVALID) {
        // Metadata log sequencer is recovering.
        status = E::AGAIN;
      }
    } else {
      reply_hdr.last_released_lsn = sequencer->getLastReleased();
      reply_hdr.next_lsn = sequencer->getNextLSN();
      // if wait_for_recovery is false, last_released_lsn may be invalid. so
      // we only check this assert if wait_for_recovery is true
      if (wait_for_recovery) {
        ld_check(reply_hdr.last_released_lsn != LSN_INVALID);
      }
      if (flags_ &
          (GET_SEQ_STATE_Message::INCLUDE_TAIL_ATTRIBUTES |
           GET_SEQ_STATE_Message::INCLUDE_TAIL_RECORD)) {
        auto tail = sequencer->getTailRecord();
        if (tail) {
          reply_hdr.flags |=
              GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_ATTRIBUTES;
          LogTailAttributes attributes;
          attributes.last_released_real_lsn = tail->header.lsn;
          attributes.last_timestamp =
              std::chrono::milliseconds(tail->header.timestamp);
          attributes.offsets = OffsetMap::toRecord(tail->offsets_map_);
          tail_attributes.assign(attributes);
          if (flags_ & GET_SEQ_STATE_Message::INCLUDE_TAIL_RECORD) {
            reply_hdr.flags |= GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_RECORD;
            // make a copy to get rid of the constness, note that this doesn't
            // copy the actual payload data so it should be cheap
            tail_record = std::make_shared<TailRecord>(*tail);
          }
        } else {
          // sequencer does not yet have tail record / attribute
          status = E::AGAIN;
        }
      }

      if (flags_ & GET_SEQ_STATE_Message::INCLUDE_EPOCH_OFFSET) {
        if (sequencer->isRecoveryComplete()) {
          reply_hdr.flags |= GET_SEQ_STATE_REPLY_Header::INCLUDES_EPOCH_OFFSET;
          epoch_offsets.assign(sequencer->getEpochOffsetMap());
        }
      }

      if (flags_ & GET_SEQ_STATE_Message::INCLUDE_HISTORICAL_METADATA) {
        auto map = sequencer->getMetaDataMap();
        if (map && map->getEffectiveUntil() == sequencer->getCurrentEpoch()) {
          reply_hdr.flags |=
              GET_SEQ_STATE_REPLY_Header::INCLUDES_HISTORICAL_METADATA;
          metadata_map = std::move(map);
        } else {
          // sequencer has not yet acquired historical metadata or its metadata
          status = E::AGAIN;
        }
      }

      if (flags_ & GET_SEQ_STATE_Message::INCLUDE_IS_LOG_EMPTY) {
        reply_hdr.flags |= GET_SEQ_STATE_REPLY_Header::INCLUDES_IS_LOG_EMPTY;
        auto res = sequencer->isLogEmpty();
        status = res.first;
        is_log_empty = res.second;
      }
    }
  }

  sendReply(from,
            reply_hdr,
            status,
            NodeID(),
            tail_attributes,
            std::move(epoch_offsets),
            std::move(metadata_map),
            std::move(tail_record),
            is_log_empty);
}

void GET_SEQ_STATE_Message::onSent(Status status, const Address& to) const {
  ld_check(!to.isClientAddress());

  Message::onSent(status, to);

  if ((status == E::OK) &&
      (calling_ctx_ == GetSeqStateRequest::Context::UNRELEASED_RECORD)) {
    WORKER_LOG_STAT_INCR(log_id_, get_seq_state_sent_context_unreleased_record);
  }

  auto& requestMap = Worker::onThisThread()->runningGetSeqState();
  auto entry = requestMap.getGssEntryFromRequestId(log_id_, request_id_);

  if (entry != nullptr && entry->request != nullptr) {
    entry->request->onSent(to.id_.node_, status);
  } else {
    ld_debug("Request %lu for log %lu is not present in the map",
             request_id_.val(),
             log_id_.val_);
  }
}

void GET_SEQ_STATE_Message::notePreempted(epoch_t e, NodeID sealed_by) {
  ld_spew("PREEMPTED by %s for epoch %u of log %lu",
          sealed_by.toString().c_str(),
          e.val(),
          log_id_.val());
  Worker* w = Worker::onThisThread();
  NodeID my_node_id = w->processor_->getMyNodeID();

  if (my_node_id == sealed_by) {
    return;
  }

  auto& seqmap = w->processor_->allSequencers();
  std::shared_ptr<Sequencer> sequencer = nullptr;
  const logid_t datalog_id = MetaDataLog::dataLogID(log_id_);
  sequencer = seqmap.findSequencer(datalog_id);
  if (!sequencer) {
    return;
  }

  epoch_t current_epoch = sequencer->getCurrentEpoch();
  auto seq_state = sequencer->getState();
  if (e >= current_epoch && seq_state == Sequencer::State::ACTIVE) {
    ld_debug("Log:%lu has a stale sequencer, with epoch:%u,"
             " but there is a seal with epoch:%u and seq_node:%s",
             log_id_.val_,
             current_epoch.val_,
             e.val_,
             sealed_by.toString().c_str());
    STAT_INCR(stats(), get_seq_state_stale_sequencer);
  }

  Seal preempted_by_before = sequencer->getSealRecord();
  sequencer->notePreempted(e, sealed_by);
  Seal preempted_by_after = sequencer->getSealRecord();

  if (preempted_by_after > preempted_by_before) {
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    10,
                    "Sequencer for log:%lu updated preempted_epoch_ because of "
                    "the check-seal result (seal before %s, seal after %s)"
                    " via gss rqid:%lu",
                    log_id_.val_,
                    preempted_by_before.toString().c_str(),
                    preempted_by_after.toString().c_str(),
                    request_id_.val());
    STAT_INCR(stats(), get_seq_state_set_preempted_epoch);
  }
}

bool GET_SEQ_STATE_Message::shouldRedirectOnPreemption(
    std::shared_ptr<Sequencer> sequencer,
    NodeID& preempted_by) {
  ld_check(sequencer != nullptr);
  preempted_by = sequencer->checkIfPreempted(sequencer->getCurrentEpoch());
  if (!preempted_by.isNodeID()) {
    return false;
  }

  // Don't redirect if the sequencer has been activated on this node recently
  if (sequencer->checkNoRedirectUntil()) {
    RATELIMIT_INFO(std::chrono::seconds(5),
                   5,
                   "Not redirecting to Node %s for log:%lu because"
                   " no_redirect_until hasn't expired.",
                   preempted_by.toString().c_str(),
                   log_id_.val_);
    return false;
  }

  return true;
}

bool GET_SEQ_STATE_Message::shouldRedirectOrFail(logid_t datalog_id,
                                                 Status& status_out,
                                                 NodeID seq_node,
                                                 const Address& from) {
  Worker* w = Worker::onThisThread();
  auto failure_detector_running = w->processor_->isFailureDetectorRunning();

  // TODO: clean up: we shouldn't use the fact that failure detector is running
  // to check wether on-demand sequencer activation is enabled.
  //
  // If static routing is in use, skip on-demand sequencer activation
  if (!failure_detector_running) {
    auto& seqmap = w->processor_->allSequencers();
    std::shared_ptr<Sequencer> sequencer = nullptr;
    sequencer = seqmap.findSequencer(datalog_id);
    if (!sequencer) {
      RATELIMIT_INFO(std::chrono::seconds(5),
                     5,
                     "Failure Detector is not running, but no sequencer"
                     " found for log:%lu, returning E::NOSEQUENCER",
                     datalog_id.val_);

      WORKER_STAT_INCR(get_seq_state_nosequencer);
      status_out = E::NOSEQUENCER;
      return true;
    } else {
      ld_debug("Failure Detector is not running,"
               " and sequencer is running for log:%lu",
               datalog_id.val_);
    }

    return false;
  }

  if (flags_ & GET_SEQ_STATE_Message::NO_REDIRECT) {
    RATELIMIT_INFO(std::chrono::seconds(5),
                   5,
                   "Received NO_REDIRECT flag for log:%lu, from %s",
                   datalog_id.val_,
                   Sender::describeConnection(from).c_str());
    STAT_INCR(stats(), get_seq_state_no_redirect);
    return false;
  }

  if (status_out != E::OK) {
    RATELIMIT_WARNING(std::chrono::seconds(5),
                      5,
                      "No sequencer node found for log:%lu, replying with "
                      "E::NOSEQUENCER.",
                      datalog_id.val_);
    status_out = E::NOSEQUENCER;
    return true;
  }

  // does FD think that this node should be running the sequencer
  ld_check(seq_node.isNodeID());
  if (seq_node.index() == w->processor_->getMyNodeID().index()) {
    return false;
  }

  status_out = E::REDIRECTED;
  WORKER_STAT_INCR(get_seq_state_redirect);
  RATELIMIT_INFO(std::chrono::seconds(5),
                 5,
                 "Returning status=REDIRECTED to [%s] for log:%lu. "
                 "Redirect target is Node %s",
                 Sender::describeConnection(from).c_str(),
                 log_id_.val_,
                 seq_node.toString().c_str());
  return true;
}

bool GET_SEQ_STATE_Message::isReady(Status* status_out) {
  ld_check(status_out != nullptr);

  Worker* w = Worker::onThisThread();
  if (!w->isAcceptingWork()) {
    RATELIMIT_INFO(
        std::chrono::seconds(5),
        5,
        "Node is not accepting work, returning E::SHUTDOWN for log:%lu",
        log_id_.val_);
    *status_out = E::SHUTDOWN;
    return false;
  }

  auto processor = w->processor_;
  auto detector_is_running = processor->isFailureDetectorRunning();
  auto node_id = processor->getMyNodeID();
  auto nc = w->getNodesConfiguration();
  ld_check(nc->isNodeInServiceDiscoveryConfig(node_id.index()));

  if (detector_is_running && !processor->isNodeAlive(node_id.index())) {
    // node is coming up
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Node %s is not alive according to Failure Detector"
                      ", returning E::NOTREADY for log:%lu",
                      node_id.toString().c_str(),
                      log_id_.val_);
    *status_out = E::NOTREADY;
    return false;
  }

  if (!nc->getSequencerConfig()->getMembership()->isSequencingEnabled(
          node_id.index())) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Node %s is not a sequencer node, "
                      "returning E::NOTREADY for log:%lu",
                      node_id.toString().c_str(),
                      log_id_.val_);
    *status_out = E::NOTREADY;
    return false;
  }

  return true;
}

void GET_SEQ_STATE_Message::blacklistNodeInNodeset(
    ShardID shard,
    std::chrono::seconds retry_interval,
    NodeSetState::NotAvailableReason reason) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 2,
                 "Blacklisting %s for log:%lu. (rqid:%lu)",
                 shard.toString().c_str(),
                 log_id_.val(),
                 request_id_.val());

  const auto& nc = Worker::onThisThread()->getNodesConfiguration();
  if (nc->isNodeInServiceDiscoveryConfig(shard.node())) {
    ld_check(copyset_manager_ != nullptr);
    copyset_manager_->getNodeSetState()->setNotAvailableUntil(
        shard,
        std::chrono::steady_clock::now() +
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                retry_interval),
        reason);
  }
}

void GET_SEQ_STATE_Message::sendReply(
    Address const& dest,
    GET_SEQ_STATE_REPLY_Header const& header,
    Status status,
    NodeID redirect,
    folly::Optional<LogTailAttributes> tail_attributes,
    folly::Optional<OffsetMap> epoch_offsets,
    std::shared_ptr<const EpochMetaDataMap> metadata_map,
    std::shared_ptr<TailRecord> tail_record,
    folly::Optional<bool> is_log_empty) {
  auto msg = std::make_unique<GET_SEQ_STATE_REPLY_Message>(header);
  msg->request_id_ = request_id_;
  msg->status_ = status;
  msg->redirect_ = redirect;
  if (tail_attributes.hasValue()) {
    msg->tail_attributes_ = tail_attributes.value();
  }
  if (epoch_offsets.hasValue()) {
    msg->epoch_offsets_ = std::move(epoch_offsets.value());
  }
  if (metadata_map) {
    msg->metadata_map_ = std::move(metadata_map);
  }
  if (tail_record) {
    msg->tail_record_ = std::move(tail_record);
  }
  if (is_log_empty.hasValue()) {
    msg->is_log_empty_ = is_log_empty.value();
  }

  ld_spew("Sending GET_SEQ_STATE_REPLY(log:%lu, rqid:%lu, status=%s, "
          "last_released_lsn:%s, next_lsn:%s, ctx:%s) to %s(%s)",
          log_id_.val_,
          request_id_.val(),
          error_name(status),
          lsn_to_string(header.last_released_lsn).c_str(),
          lsn_to_string(header.next_lsn).c_str(),
          getContextString(calling_ctx_).c_str(),
          dest.toString().c_str(),
          Sender::describeConnection(dest).c_str());
  int rv = Worker::onThisThread()->sender().sendMessage(std::move(msg), dest);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    5,
                    "Failed to send GET_SEQ_STATE_REPLY to %s: %s. "
                    "(log:%lu, rqid:%lu, status=%s, last_released_lsn:%s,"
                    " next_lsn:%s, ctx:%s)",
                    Sender::describeConnection(dest).c_str(),
                    error_description(err),
                    log_id_.val_,
                    request_id_.val(),
                    error_name(status),
                    lsn_to_string(header.last_released_lsn).c_str(),
                    lsn_to_string(header.next_lsn).c_str(),
                    getContextString(calling_ctx_).c_str());
  }
}

StatsHolder* GET_SEQ_STATE_Message::stats() const {
  return Worker::stats();
}

SequencerLocator& GET_SEQ_STATE_Message::getSequencerLocator() {
  return *Worker::onThisThread()->processor_->sequencer_locator_;
}

std::vector<std::pair<std::string, folly::dynamic>>
GET_SEQ_STATE_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto flagsToString = [](GET_SEQ_STATE_flags_t flags) {
    folly::small_vector<std::string, 4> strings;
#define FLAG(x)                           \
  if (flags & GET_SEQ_STATE_Message::x) { \
    strings.emplace_back(#x);             \
  }
    FLAG(NO_REDIRECT)
    FLAG(REACTIVATE_IF_PREEMPTED)
    FLAG(DONT_WAIT_FOR_RECOVERY)
    FLAG(INCLUDE_TAIL_ATTRIBUTES)
    FLAG(INCLUDE_EPOCH_OFFSET)
    FLAG(MIN_EPOCH)
    FLAG(INCLUDE_HISTORICAL_METADATA)
    FLAG(INCLUDE_TAIL_RECORD)
#undef FLAG
    return folly::join('|', strings);
  };

  auto add = [&res](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };
  add("log_id", toString(log_id_));
  add("rqid", request_id_.val());
  add("flags", flagsToString(flags_));
  add("calling_ctx", getContextString(calling_ctx_));
  if (min_epoch_.hasValue()) {
    add("min_epoch", min_epoch_.value().val());
  }
  return res;
}

}} // namespace facebook::logdevice
