/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/CHECK_SEAL_Message.h"
#include "logdevice/common/protocol/CHECK_SEAL_REPLY_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Request that Sequencer uses upon receiving a GET_SEQ_STATE Message, to
 * determine if the current sequencer is preempted or not. Sends CHECK_SEAL
 * messages to 'r' storage nodes and checks to see if some of them are sealed
 * to a higher epoch.
 *
 * Storage nodes send the highest seal(soft or normal) in response to the
 * CHECK_SEAL Message.
 *
 * If the sequencer's epoch <= storage nodes' seal,
 * Sequencer::notePreempted() is called.
 */
class CheckSealRequest;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct CheckSealRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<CheckSealRequest>,
                     request_id_t::Hash>
      map;
};

class CheckSealRequest : public Request {
 public:
  explicit CheckSealRequest(std::unique_ptr<GET_SEQ_STATE_Message> message,
                            Address from,
                            logid_t log_id,
                            std::vector<ShardID> copyset,
                            epoch_t local_epoch);

  Request::Execution execute() override;

  /**
   * Called when a CHECK_SEAL_REPLY message is received from a storage node.
   *
   * @param from : Shard which sent the CHECK_SEAL_Reply
   * @param msg  : CHECK_SEAL Reply message received from 'from'
   */
  void onReply(ShardID from, const CHECK_SEAL_REPLY_Message& msg);

  /**
   * @param shard     : Shard to which an attempt was made to send
   *                    CHECK_SEAL_Message
   * @param status :
   *   OK             : no immediate action, wait for node to reply
   *   others         : see Message.h::onSent() for remaining error code
   *                    descriptions. The current handling of these errors is
   *                    to not do anything on server-side. This may lead to
   *                    returning E::AGAIN to the caller of GetSeqState, which
   *                    should in turn retry.
   */
  void onSent(ShardID shard, Status status);

  ~CheckSealRequest() override;

 private:
  /**
   * CheckSealRequest owns the GET_SEQ_STATE_Message that issued the request
   */
  std::unique_ptr<GET_SEQ_STATE_Message> gss_message_;

  /**
   * sender of the GET_SEQ_STATE_Message
   */
  Address from_;

  const logid_t log_id_;

  /**
   * set of nodes picked to whom CHECK_SEAL message will be sent
   */
  std::vector<ShardID> copyset_;

  /**
   * Epoch against which we compare the seal sent by storage nodes
   * via CHECK_SEAL_REPLY message.
   */
  epoch_t local_epoch_;

  /**
   * List of shards from which reply has been received
   */
  std::vector<ShardID> recvd_from_;

  /**
   * Timeout until which a CheckSealRequest will wait for replies from
   * nodes in 'copyset_'.
   *
   * If not all nodes could reply within the timeout specified, the request is
   * destroyed and E::AGAIN sent back to the client of GET_SEQ_STATE Message.
   */
  std::unique_ptr<Timer> request_timer_;

  /**
   * List of nodes from which successful replies have been
   * received, e.g.
   * E::OK
   *
   * The following error code(s) are treated as success:
   * E::NOTSUPPORTED   : for the case when we need to disable this feature,
   *                     and do rolling restart.
   */
  int replies_successful_ = 0;

  // TODO (#10481839): currently unused, but will need
  // in future with server side check-seal retries.
  //
  // Needed to distinguish b/w replies from different
  // waves.
  //
  // Keeping it to avoid proto change.
  uint32_t wave_{0};

  /**
   * Send CHECK_SEAL message to nodes in copyset_
   */
  void sendCheckSeals();

  /**
   * Tells the client of GET_SEQ_STATE to retry by sending E::AGAIN, e.g. when:
   * - Not all storage nodes could reply with a successful CHECK_SEAL_REPLY.
   * - OR the sequencer node couldn't send CHECK_SEAL to all storage nodes.
   * - OR Request timeout expired waiting for replies from nodes in 'copyset_'
   */
  void sendRetryToClient();

  /**
   * Setup the overall CheckSealRequest timer as half of what GET_SEQ_STATE
   * client timer is (i.e. 'seq_state_reply_timeout').
   * The minimum CheckSealRequest timeout will be set to atleast 1 sec.
   */
  virtual void initTimer();

  virtual void onTimeout();

  /**
   * Cancels the timer and destroys the CheckSealRequest
   */
  void finalize();
};

}} // namespace facebook::logdevice
