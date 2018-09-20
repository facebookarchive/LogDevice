/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Address.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

class EpochRecovery;

/**
 * @file a RecoveryNode represents a storage node as a participant in the
 *       recovery of a particular epoch of a log. Each RecoveryNode object is
 *       a member of exactly one RecoverySet.
 */

class RecoveryNode {
 public:
  /**
   * @param shard        ID of the shard
   * @param recovery     EpochRecovery to which this RecoveryNode belongs
   */
  RecoveryNode(ShardID shard, EpochRecovery* recovery);

  ~RecoveryNode();

  // the state of this node with respect to a particular epoch recovery
  enum class State : uint8_t {
    // SEAL sent but SEALED has not yet been received for the node
    SEALING = 0,

    // SEALED has been received along with its local tail attribute (e.g., lng)
    SEALED,

    // START has been sent, updating digest with records from node
    DIGESTING,

    // All records in the range ]LNG, (next-epoch,0)[ stored on this shard
    // have been received and applied to the digest.
    DIGESTED,

    // The node has finished digest and is eligible for participating in
    // mutations (e.g., can accept writes). The node will always be in the
    // mutation set in the mutation phase, and epoch recovery may perform
    // mutation on it.
    MUTATABLE,

    // CLEAN has been sent to node
    CLEANING,

    // CLEANED(success) has been received
    CLEAN,

    // the number of states, must be last
    Count
  };

  ShardID getShardID() const {
    return shard_;
  }

  NodeID getNodeID() const {
    return nid_;
  }

  State getState() const {
    return state_;
  }

  /**
   * Perform a state transition into @param state. If state is DIGESTING or
   * CLEAN, this involves sending a START or CLEAN message respectively
   * The parameters of those messages are determined by the state of recovery_.
   */
  void transition(State state);

  /**
   * This function must be called only if this RecoveryNode is in
   * state DIGESTING or CLEANING. Moreover, it must have sent a START
   * (or CLEAN respectively) to nid_ and must be waiting for a
   * reply. The Socket to nid_ must still be connected.
   *
   * The function activates the resend_ timer. Upon expiration the
   * timer will try to resend the START or CLEAN, depending on the
   * current state.
   */
  void resendMessage();

  /**
   * Called by the onSent() of a START or CLEAN message previously sent by this
   * RecoveryNode.
   *
   * @param type    type of message: START or CLEAN
   * @param status  OK if message was passed to TCP, otherwise one of the
   *                error codes documented for Message::onSent().
   * @param rsid    if @param type is START, the stream id in the START header
   */
  void onMessageSent(MessageType type,
                     Status status,
                     read_stream_id_t rsid = READ_STREAM_ID_INVALID);

  /**
   * EpochRecovery uses this in order to check that a STARTED, RECORD or
   * GAP that it got from nid_ is not stale.
   *
   * @param rsid  the read stream to which the STARTED, RECORD, or GAP
   *              belongs
   */
  bool digestingReadStream(read_stream_id_t rsid) const;

  /**
   * If this node is in the DIGESTING state, returns the read stream id included
   * in the START message that is being sent. Otherwise, 0 is returned. This
   * method is used by RecoverySet::onMessageSent() to ensure stale messages
   * are ignored (e.g. a START message that was buffered before EpochRecovery
   * was restarted).
   */
  read_stream_id_t getExpectedReadStreamID() const {
    return expected_read_stream_id_;
  }

  /**
   * @return  a human-readable name for RecoveryNode::State @param st
   */
  static const char* stateName(State st);

 private:
  ShardID shard_;

  // cluster offset of the node that holds this shard.
  NodeID nid_;

  // current state of the node, should only be changed by changState() but not
  // directly
  State state_ = State::SEALING;

  // When this object is in DIGESTING state, this field contains the read
  // stream id in the most recent START sent by this object in order to initiate
  // a read stream for reading digest records and gaps from nid_. At all other
  // times this field is READ_STREAM_ID_INVALID.
  read_stream_id_t read_stream_id_;

  // Set by transition() to the read stream id of the START message that's being
  // sent. Since read_stream_id_ is only set when START message is passed to TCP
  // (see comment in onMessageSent), this is used to ensure that onMessageSent
  // processes the correct START message.
  read_stream_id_t expected_read_stream_id_;

  // backpointer to the EpochRecovery machine that has this
  // RecoveryNode in its RecoverySet
  EpochRecovery* recovery_;

  /**
   * Change the state of the recovery node to @param to.
   * Note: Changing the state_ member variable must be done through this
   * function ONLY.
   */
  void changeState(State to);

  /**
   * Called by socket_callback_ when the Socket to the node represented by
   * this RecoveryNode object closes and.
   *
   * @param reason   reason for disconnection, this is the value passed to
   *                 Socket::close(). See Socket.h.
   */
  void onDisconnect(Status reason);

  /**
   * Called by START_Message::onSent() when a START message sent by
   * this RecoveryNode while transitioning into DIGESTING has been
   * sent or dropped.
   *
   * @param id  read stream id in the SENT. Checked against read_stream_id_.
   * @param st  see Message::onSent(). If this is not Status::OK,
   *            start a retry timer.
   */
  void onStartSent(read_stream_id_t id, Status st);

  /**
   * Called when (1) the resend_ timer expires after an unsuccessful attempt
   * to send a START or CLEAN to this node, or (2) when the connection to the
   * node is closed after a START or CLEAN have been sent, but the RecoveryNode
   * has not yet moved on to the next state. The action is to resend the
   * message to nid_.
   */
  void resend();

  /**
   * Attempts to send a START message with RECOVERY flag set to nid_.
   * Registers socket_callback_ on success.
   *
   */
  int requestDigestRecords();

  /**
   * Attempts to send a STOP message to the storage node to terminate the
   * digest read stream. Called when EpochRecovery restarts or aborts. Must
   * called in the DIGESTING state.
   */
  void stopDigesting();

  class RecoverySocketCallback : public SocketCallback {
   public:
    explicit RecoverySocketCallback(RecoveryNode* owner) : owner_(owner) {
      ld_check(owner);
    }

    void operator()(Status st, const Address& name) override {
      ld_check(!name.isClientAddress() &&
               name.id_.node_.index() == owner_->nid_.index());
      owner_->onDisconnect(st);
    }

   private:
    RecoveryNode* owner_;
  };

  // calls onDisconnect() when a Socket to nid_ into which we have
  // successfully sent a START or CLEAN closes.
  RecoverySocketCallback socket_callback_;

  // resends START and SEAL indefinitely on a failure
  std::unique_ptr<BackoffTimer> resend_;
};

}} // namespace facebook::logdevice
