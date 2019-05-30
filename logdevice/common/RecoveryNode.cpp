/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RecoveryNode.h"

#include <functional>

#include <folly/CppAttributes.h>

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/CLEAN_Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/protocol/STOP_Message.h"

namespace facebook { namespace logdevice {

RecoveryNode::RecoveryNode(ShardID shard, EpochRecovery* recovery)
    : shard_(shard),
      nid_(shard.node()),
      read_stream_id_(READ_STREAM_ID_INVALID),
      expected_read_stream_id_(READ_STREAM_ID_INVALID),
      recovery_(recovery),
      socket_callback_(this),
      resend_(recovery_->getDeps().createBackoffTimer(
          {std::chrono::milliseconds(100), std::chrono::seconds(10)},
          [this] {
            // messages are sent by this class only in DIGESTING and CLEANING
            // state. In MUTATABLE state messages are handled by Mutator
            // instead.
            ld_check(state_ == State::DIGESTING || state_ == State::CLEANING);
            ld_check(!socket_callback_.active());
            transition(state_); // just repeat the transition
          })) {
  ld_check(recovery_ != nullptr);
}

RecoveryNode::~RecoveryNode() {
  // RecoveryNode objects are destroyed along with EpochRecovery state
  // machines, stop the digest read stream if in digesting to free the
  // read stream resource on the storage node
  if (state_ == State::DIGESTING) {
    stopDigesting();
  }
}

bool RecoveryNode::digestingReadStream(read_stream_id_t rsid) const {
  if (state_ != State::DIGESTING) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "RecoveryNode %s of %s is not expecting a digest "
                   "read stream %lu. It is in state %s.",
                   shard_.toString().c_str(),
                   recovery_->identify().c_str(),
                   rsid.val_,
                   stateName(state_));
    return false;
  }

  if (read_stream_id_ != rsid) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "RecoveryNode %s of %s is not expecting a digest "
                   "read stream %lu. It is expecting read stream %lu.",
                   shard_.toString().c_str(),
                   recovery_->identify().c_str(),
                   rsid.val_,
                   read_stream_id_.val_);
    return false;
  }

  return true;
}

void RecoveryNode::resendMessage() {
  switch (state_) {
    case State::DIGESTING:
      ld_check(read_stream_id_ != READ_STREAM_ID_INVALID);
      read_stream_id_ = READ_STREAM_ID_INVALID;
      break;
    case State::CLEANING:
      break;
    default:
      ld_critical("INTERNAL ERROR: attempt to resend a message to %s while "
                  "in an invalid state %s during recovery of %s. "
                  "Expected state_ to be DIGESTING or CLEANING.",
                  shard_.toString().c_str(),
                  stateName(state_),
                  recovery_->identify().c_str());
      ld_check(false);
  }

  ld_check(socket_callback_.active()); // the connection must still be up
  ld_check(!resend_->isActive());

  socket_callback_.deactivate();
  resend_->activate();
}

void RecoveryNode::changeState(State to) {
  ld_check(recovery_ != nullptr);
  state_ = to;
  recovery_->onNodeStateChanged(shard_, to);
}

void RecoveryNode::transition(State to) {
  int rv;
  std::unique_ptr<Message> msg; // if this transition requires sending a
                                // message, this is the message
  switch (to) {
    case State::DIGESTING:
    case State::CLEANING:
      ld_check(state_ == State((uint8_t)to - 1) || // first try
               state_ == to);                      // retry
      ld_check(!resend_->isActive());
      ld_check(!socket_callback_.active());
      ld_check(read_stream_id_ == READ_STREAM_ID_INVALID);

      if (to == State::CLEANING) {
        expected_read_stream_id_ = READ_STREAM_ID_INVALID;

        const StorageSet& absent_shards = recovery_->getAbsentNodes();
        const TailRecord& tail_record = recovery_->getEpochTailRecord();
        const OffsetMap& epoch_size_map = recovery_->getEpochSizeMap();
        ld_check(absent_shards.size() <
                 std::numeric_limits<nodeset_size_t>::max());
        CLEAN_Header header{recovery_->getLogID(),
                            recovery_->epoch_,
                            recovery_->id_,
                            0, /*flags*/
                            recovery_->getDeps().getLogRecoveryNextEpoch(),
                            recovery_->getLastKnownGood(),
                            recovery_->getLastDigestEsn(),
                            static_cast<nodeset_size_t>(absent_shards.size()),
                            BYTE_OFFSET_INVALID, /* unused */
                            BYTE_OFFSET_INVALID, /* unused */
                            shard_.shard()};

        ld_debug("Sending CLEAN message to %s, logid %lu, clean epoch %u, "
                 "recovery id: %lu, sequencer epoch: %u, "
                 "recovery window (%u, %u], absent nodes: [%s], "
                 "epoch size map %s, TailRecord %s.",
                 shard_.toString().c_str(),
                 header.log_id.val_,
                 header.epoch.val_,
                 header.recovery_id.val_,
                 header.sequencer_epoch.val_,
                 header.last_known_good.val_,
                 header.last_digest_esn.val_,
                 toString(absent_shards).c_str(),
                 epoch_size_map.toString().c_str(),
                 tail_record.toString().c_str());
        msg = std::make_unique<CLEAN_Message>(
            header, tail_record, epoch_size_map, absent_shards);
      } else {
        ld_check(to == State::DIGESTING);
        expected_read_stream_id_ = recovery_->getDeps().issueReadStreamID();
        esn_t start_esn = recovery_->getDigestStart();
        ld_check(start_esn != ESN_INVALID);
        msg.reset(new START_Message(START_Header(
            {recovery_->getLogID(),                     // log id
             expected_read_stream_id_,                  // read stream
             compose_lsn(recovery_->epoch_, start_esn), // start lsn
             compose_lsn(recovery_->epoch_, ESN_MAX),   // until lsn
             compose_lsn(recovery_->epoch_, ESN_MAX),   // window high
             START_Header::DIGEST | START_Header::INCLUDE_EXTRA_METADATA |
                 START_Header::IGNORE_RELEASED_STATUS, // flags
             -1,                                       // deprecated
             filter_version_t{1},                      // filter version
             0,                                        // blacklist size
             0,                                        // replication (ignored)
             SCDCopysetReordering::NONE,
             shard_.shard()})));

        ld_debug("Sending START_Message to %s for log:%lu, rsid=%lu",
                 shard_.toString().c_str(),
                 recovery_->getLogID().val(),
                 expected_read_stream_id_.val());
      }

      ld_check(msg);
      rv = recovery_->getDeps().sender_->sendMessage(std::move(msg), nid_);

      if (rv == 0) {
        changeState(to);
        return;
      }

      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Failed to send %s message to %s for %s: %s",
                     messageTypeNames()[msg->type_].c_str(),
                     shard_.toString().c_str(),
                     recovery_->identify().c_str(),
                     error_description(err));

      if (err == E::NOTINCONFIG) {
        // Node was removed from config.
        recovery_->getDeps().onShardRemovedFromConfig(shard_);
      } else {
        ld_check(err == E::UNROUTABLE || err == E::NOBUFS ||
                 err == E::DISABLED || err == E::SYSLIMIT || err == E::NOMEM ||
                 err == E::INTERNAL || err == E::SHUTDOWN);

        // We currently don't distinguish between fatal and transient errors.
        // Just schedule a retry.
        changeState(to);
        resend_->activate();
      }
      break;

    case State::DIGESTED:
      ld_check(read_stream_id_ != READ_STREAM_ID_INVALID);
      read_stream_id_ = READ_STREAM_ID_INVALID;

      ld_check(!resend_->isActive());
      ld_check(socket_callback_.active());

      // fall through
      FOLLY_FALLTHROUGH;
    case State::CLEAN:
      ld_check(state_ == State((uint8_t)to - 1));

      // Since it's possible that this transition was caused by a delayed
      // CLEANED reply (to a CLEAN message sent before this epoch recovery was
      // restarted), the two asserts in DIGESTED above don't apply for this
      // case.

      socket_callback_.deactivate();
      resend_->reset();

      changeState(to);
      break;

    case State::MUTATABLE:
      // DIGESTED -> MUTATABLE
      ld_check(state_ == State((uint8_t)to - 1));

      ld_check(!resend_->isActive());
      ld_check(!socket_callback_.active());
      ld_check(read_stream_id_ == READ_STREAM_ID_INVALID);

      changeState(to);
      break;

    case State::SEALED:
      // we can transition to SEALED either from SEALING after
      // a node has been sealed, or from a state higher than SEALED
      // when we restart the epoch recovery

      if (state_ == State::SEALING) {
        changeState(to);
        break;
      } else if (state_ == State::SEALED) {
        // SEALED->SEALED transitions are only allowed for inactive
        // EpochRecovery machines.
        ld_check(!recovery_->isActive());
        break;
      }
      // for all other values of state_ fall through and reset
      FOLLY_FALLTHROUGH;
    case State::SEALING:
      // note that this case covers the fall-through from SEALED above for
      // all transitions from states higher than SEALED. The actions must
      // be identical for both target states.

      if (state_ == State::DIGESTING) {
        // if the node is previously in DIGESTING, stop the digesting read
        // stream to stop wasting unnecessary resource on the storage node
        stopDigesting();
      }

      socket_callback_.deactivate();
      resend_->reset();
      read_stream_id_ = READ_STREAM_ID_INVALID;
      changeState(to); // this may be SEALING or SEALED as we fall through above
      break;

    case State::Count:
      // this is here to satisfy -Werror=switch
      ld_check(false && "Invalid RecoveryNode::State");
  }

  // read_stream_id_ is set in onMessageSent(), not here.
  ld_check(read_stream_id_ == READ_STREAM_ID_INVALID);
}

void RecoveryNode::onMessageSent(MessageType type,
                                 Status status,
                                 read_stream_id_t rsid) {
  int rv;

  switch (type) {
    case MessageType::START:
      if (state_ != State::DIGESTING) {
        RATELIMIT_WARNING(
            std::chrono::seconds(10),
            10,
            "called for a stale START message to %s during "
            "recovery of %s. RecoveryNode is in state %s. START "
            "had read stream %lu and send disposition %s. Ignored.",
            shard_.toString().c_str(),
            recovery_->identify().c_str(),
            stateName(state_),
            rsid.val_,
            error_name(status));
        return;
      }

      ld_check(rsid == expected_read_stream_id_);
      ld_check(read_stream_id_ == READ_STREAM_ID_INVALID);

      expected_read_stream_id_ = READ_STREAM_ID_INVALID;

      // we defer initializing read_stream_id_ until the START message
      // is passed to TCP in order to avoid tripping the
      // !resend.isActive() assert below in the unlikely scenario where
      // a rogue node sends us a STARTED(status=failed) with a matching
      // log id and read_stream_id_ while this START is still
      // pending. The action when we receive such a STARTED, unlikely as
      // it might be, is to activate resend_. This may trip the assert
      // when START::onSent() is finally called for the matching start.
      if (status == Status::OK) {
        read_stream_id_ = rsid;
      }
      break;

    case MessageType::CLEAN:
      ld_check(rsid == READ_STREAM_ID_INVALID);
      if (state_ != State::CLEANING) {
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          10,
                          "called for a stale CLEAN message to %s during "
                          "recovery of %s. RecoveryNode is in state %s. Send "
                          "disposition was %s. Ignored.",
                          shard_.toString().c_str(),
                          recovery_->identify().c_str(),
                          stateName(state_),
                          error_name(status));
        return;
      }
      break;

    default:
      ld_check(false && "message type is not START or CLEAN");
      return;
  }

  ld_check(!resend_->isActive());

  switch (status) {
    case Status::OK:
      rv = recovery_->getDeps().registerOnSocketClosed(
          Address(nid_), socket_callback_);
      // cannot fail to register an on-close callback if we just successfully
      // sent a message to that Address.
      ld_check(rv == 0);
      return;

    case E::NOTINCONFIG:
      recovery_->getDeps().onShardRemovedFromConfig(shard_);
      break;

    default:
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Failed to send a %s message to %s during recovery of %s: "
                     "%s",
                     messageTypeNames()[type].c_str(),
                     shard_.toString().c_str(),
                     recovery_->identify().c_str(),
                     error_description(status));

      resend_->activate();
  }

  // We get here only if we failed to send the message. Must not have
  // registered the socket callback or set read_stream_id_.
  ld_check(!socket_callback_.active());
  ld_check(read_stream_id_ == READ_STREAM_ID_INVALID);
}

void RecoveryNode::onDisconnect(Status st) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Lost connection to %s while waiting for reply in state %s "
                 "during recovery of %s: %s",
                 shard_.toString().c_str(),
                 stateName(state_),
                 recovery_->identify().c_str(),
                 error_description(st));

  switch (state_) {
    case State::DIGESTING:
      ld_check(read_stream_id_ != READ_STREAM_ID_INVALID);
      read_stream_id_ = READ_STREAM_ID_INVALID;
      break;
    case State::CLEANING:
      break;
    default:
      ld_critical("INTERNAL ERROR: lost connection to %s while waiting for a "
                  "reply in an invalid state %s during recovery of %s: %s. "
                  "Expected state_ to be DIGESTING or CLEANING.",
                  shard_.toString().c_str(),
                  stateName(state_),
                  recovery_->identify().c_str(),
                  error_name(st));
      ld_check(false);
  }

  // No special handling for st==ENOTINCONFIG needed. resend_ indirectly
  // calls Sender::sendMessage(nid_), which will fail with ENOTINCONFIG again
  // and that will be handled.

  resend_->activate();
}

void RecoveryNode::stopDigesting() {
  if (state_ != State::DIGESTING) {
    ld_critical("INTERNAL ERROR: called in state: %s other than DIGESTING.",
                stateName(state_));
    ld_check(false);
    return;
  }

  read_stream_id_t rsid =
      (read_stream_id_ == READ_STREAM_ID_INVALID ? expected_read_stream_id_
                                                 : read_stream_id_);

  if (rsid == READ_STREAM_ID_INVALID) {
    // it is possbile that both read_stream_id_ and expected_read_stream_id_
    // are invalid when we are about to retry sending the START message
    // (e.g., receive STARTED with E::AGAIN) after the resend_ timeout.
    // In such case, resend will be canceled, nothing to do.
    return;
  }

  STOP_Header header({recovery_->getLogID(), // log id
                      rsid,                  // read stream ID
                      shard_.shard()});
  auto msg = std::make_unique<STOP_Message>(header);
  // Do not check success status since it is a best-effort approach
  recovery_->getDeps().sender_->sendMessage(std::move(msg), nid_);
}

const char* RecoveryNode::stateName(State st) {
  static_assert(
      (size_t)State::Count == 7,
      "Please update this method after adding values to RecoveryNode::State");
  static const char* state_names[(size_t)State::Count]{"SEALING",
                                                       "SEALED",
                                                       "DIGESTING",
                                                       "DIGESTED",
                                                       "MUTATABLE",
                                                       "CLEANING",
                                                       "CLEAN"};

  return state_names[(uint8_t)st];
}

}} // namespace facebook::logdevice
