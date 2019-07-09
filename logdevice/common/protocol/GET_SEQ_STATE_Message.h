/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by a storage node to the sequencer asking it to send
 *       its state for a log -- in particular, the last released LSN and
 *       the sequence number that will be assigned to the next record.
 *
 * NOTE: this is a best-effort facility.  The sequencer may not reply even if
 * it is up and we have a healthy connection to it (e.g. if it doesn't know
 * its next epoch number yet).  At the time of writing, all places where this
 * is needed had retrying so this was not an issue.
 */

class CopySetManager;
struct GET_SEQ_STATE_REPLY_Header;
class Sequencer;
class SequencerLocator;
class TailRecord;

using GET_SEQ_STATE_flags_t = uint32_t;

class GET_SEQ_STATE_Message : public Message {
 public:
  GET_SEQ_STATE_Message(logid_t log_id,
                        request_id_t request_id,
                        GET_SEQ_STATE_flags_t flags,
                        GetSeqStateRequest::Context calling_ctx,
                        folly::Optional<epoch_t> min_epoch = folly::none)
      : Message(MessageType::GET_SEQ_STATE, TrafficClass::RECOVERY),
        log_id_(log_id),
        request_id_(request_id),
        flags_(flags),
        calling_ctx_(calling_ctx),
        min_epoch_(min_epoch) {}

  // If set in flags, a sequencer node that receives the message will make an
  // attempt to process it, possibly (re)activating a sequencer, even if it
  // would otherwise send a redirect.
  static const GET_SEQ_STATE_flags_t NO_REDIRECT = 1u << 0;
  // Indicates that a sequencer node that receives the message should reactivate
  // a sequencer object if the existing one was preempted.
  static const GET_SEQ_STATE_flags_t REACTIVATE_IF_PREEMPTED = 1u << 1;
  static const GET_SEQ_STATE_flags_t FORCE =
      NO_REDIRECT | REACTIVATE_IF_PREEMPTED;

  // If set, the sequencer will not return E::AGAIN if it is still doing
  // recovery. It will return E::OK instead.
  static const GET_SEQ_STATE_flags_t DONT_WAIT_FOR_RECOVERY = 1u << 2;

  // If set, execution will try to get tail attributes (i.e., tail record w/o
  // payload) of the log from sequencer
  static const GET_SEQ_STATE_flags_t INCLUDE_TAIL_ATTRIBUTES = 1u << 3;

  // If set, execution will try to get epoch offset of active epoch.
  static const GET_SEQ_STATE_flags_t INCLUDE_EPOCH_OFFSET = 1u << 4;

  // If set, min_epoch will be appended at the end of the serialized message
  static const GET_SEQ_STATE_flags_t MIN_EPOCH = 1u << 5;

  // If set, get historical epoch metadata from sequencer
  static const GET_SEQ_STATE_flags_t INCLUDE_HISTORICAL_METADATA = 1u << 6;

  // If set, get tail record (with payload for tail optimized logs)
  // from the sequencer
  static const GET_SEQ_STATE_flags_t INCLUDE_TAIL_RECORD = 1u << 7;

  // If set, include is_log_empty in GSS response.
  static const GET_SEQ_STATE_flags_t INCLUDE_IS_LOG_EMPTY = 1u << 8;

  // implementation of the Message interface
  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;
  Disposition onReceived(const Address& from) override;
  void onSent(Status status, const Address& to) const override;

  // Returns a pointer to the object containing stats.
  virtual StatsHolder* stats() const;

  static std::string getContextString(GetSeqStateRequest::Context ctx) {
    return GetSeqStateRequest::getContextString(ctx);
  }

  // Returns processor's sequencer locator
  virtual SequencerLocator& getSequencerLocator();

  // log to get the state for
  logid_t log_id_;

  // id of the GetSeqStateRequest, used to route the reply to it
  request_id_t request_id_{request_id_t(-1)};

  const GET_SEQ_STATE_flags_t flags_;

  // Context in which GetSeqStateRequest with 'request_id_'
  // was issued
  GetSeqStateRequest::Context calling_ctx_;

  // Min epoch in which the sequencer should reactivate
  folly::Optional<epoch_t> min_epoch_;

  // Copyset selector and nodeset. Needed to select nodes to send checkseals to.
  std::shared_ptr<CopySetManager> copyset_manager_;

  uint16_t getMinProtocolVersion() const override;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  // Check if the node is running or able to run sequencers (e.g. it's not
  // rebuilding or partitioned, in which case `status_out' will be updated).
  bool isReady(Status* status_out);

  // If the node brings up a sequencer as a result of GET_SEQ_STATE message,
  // but finds out that it has been preempted, should it send a REDIRECT or not.
  bool shouldRedirectOnPreemption(std::shared_ptr<Sequencer> sequencer,
                                  NodeID& redirect);

  /**
   * A function to check whether this node is the right one to process this
   * request. Otherwise fail the request if no sequencer can be found, or
   * send a redirect to a node which may be able to process this request.
   *
   * @param datalog_id   data-log for which sequencer state is queried
   *
   * @param status_out   error code to return to caller
   *                     E::OK
   *                     E::NOSEQUENCER
   *                     E::REDIRECTED
   *
   * @param seq_node     Node which should handle this request according
   *                     to the sequencer locator.
   *
   * @param from         Address of client which sent this request.
   *                     This is useful for debugging purpose only.
   *
   * @return false: - If this node should be handling this get-seq-state request
   *                - If client explicitly asked us not to REDIRECT
   *                - If lazy sequencer placement is not being used and there
   *                  is already a sequencer present for 'datalog_id'
   *                - If the primary sequencer node is not alive according to
   *                  this node's FailureDetector.
   *
   *          true: - If lazy sequencer placement is not being used and this
   *                  node doesn't already have a sequencer for 'datalog_id'.
   *                - If according to Failure Detector there is no sequencer
   *                  node available for 'datalog_id'
   *                - If according to Failure Detector some other node should
   *                  be handling this request.
   */
  bool shouldRedirectOrFail(logid_t datalog_id,
                            Status& status_out,
                            NodeID seq_node,
                            const Address& from);

  /**
   * Called when CHECK_SEAL_REPLY contains a seal that can
   * potentially preempt the current sequencer.
   *
   * @param e         : epoch got from a storage node via CHECK_SEAL_REPLY
   * @param sealed_by : Node which sealed epoch 'e' on that storage node.
   */
  void notePreempted(epoch_t e, NodeID sealed_by);

  /**
   * This is called when:
   * - There is no sequencer, and GET_SEQ_STATE_Message::onReceived()
   *   doesn't send a check-seal request.
   * - OR, when CheckSealRequest decides that it has done its job of
   *   detecting preemption.
   *
   * @param from  Address of storage node which sent GET_SEQ_STATE message
   *              to this sequencer.
   */
  void continueExecution(Address const& from);

  /**
   * Mark a shard as unavailable for copy-set selection for next
   * 'retry_interval' seconds.
   */
  void blacklistNodeInNodeset(ShardID shard,
                              std::chrono::seconds retry_interval,
                              NodeSetState::NotAvailableReason reason);

  // Called when a GET_SEQ_STATE request is received with NO_REDIRECT or
  // REACTIVATE_IF_PREEMPTED flag. Updates sequencer's no_redirect_until_
  // to prevent it from sending further redirects for some time.
  virtual void updateNoRedirectUntil(std::shared_ptr<Sequencer> sequencer);

  // Find or bringup sequencer for a logid
  // Sequencer activation is required to serve logids which don't
  // get frequent appends
  Status getSequencer(logid_t log_id,
                      std::shared_ptr<Sequencer>& sequencer_out,
                      Address const& from,
                      NodeID& preempted_by);

  /**
   * Pick 'r' nodes from sequencer's underlying copyset selector,
   * to send CHECK_SEAL messages for detecting sequencer preemption.
   *
   * @param seq           pointer to sequencer for 'log_id_'
   * @param copyset_out   list of storage nodes picked for sending
   *                      CHECK_SEAL message
   *
   * @return  CopySetSelector::Result::FAILED
   *          - when copyset can't be picked by underlying copyset selector
   *          - OR an unexpected error was returned from underlying selector
   *          - OR sequencer's copyset manager is not present
   *
   *          CopySetSelector::Result::PARTIAL
   *          - when extras can't be selected, this shouldn't apply in this
   *            case, since we don't specify extras
   *
   *          CopySetSelector::Result::SUCCESS
   *          - when a copyset was selected successfully
   */
  CopySetSelector::Result getCopySet(std::shared_ptr<Sequencer> seq,
                                     std::vector<ShardID>& copyset_out);

  /**
   * This method does the following:
   * - pick a copyset containing 'r' storage nodes,
       by using existing copyset manager present in the sequencer.
   * - create a new CheckSealRequest and transfers ownership of `this'
   * - execute the request which effectively sends the CHECK_SEAL message
   *   to 'r' storage nodes.
   *
   * @param from  Address of storage node which sent GET_SEQ_STATE message
   *              to this sequencer.
   * @param seq   pointer to sequencer for 'log_id_'
   *
   * @return Disposition::NORMAL - There was an error in picking copyset,
   *                               and 'this' was not moved inside a
   *                               CheckSealRequest, therefore the caller
   *                               should delete 'this'
   * @return Disposition::KEEP   - CheckSealRequest owns lifecycle of 'this'
   */
  Message::Disposition checkSeals(Address from, std::shared_ptr<Sequencer> seq);

  void
  sendReply(Address const& dest,
            GET_SEQ_STATE_REPLY_Header const& header,
            Status status,
            NodeID redirect,
            folly::Optional<LogTailAttributes> tail_attributes = folly::none,
            folly::Optional<OffsetMap> epoch_offsets = folly::none,
            std::shared_ptr<const EpochMetaDataMap> metadata_map = nullptr,
            std::shared_ptr<TailRecord> tail_record = nullptr,
            folly::Optional<bool> is_log_empty = folly::none);

  void onSequencerNodeFound(Status status,
                            logid_t datalog_id,
                            NodeID seq_node,
                            Address const& from);
  friend class CheckSealRequest;
};

}} // namespace facebook::logdevice
