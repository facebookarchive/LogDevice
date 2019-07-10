/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <memory>

#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/SimpleMessage.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file GET_EPOCH_RECOVERY_METADATA_REPLY is a message sent by a storage node
 *       in response to a GET_EPOCH_RECOVERY_METADATA_Message sent by another
 *       storage node peforming the purging procedure. For each epoch in the
 *       requested range, the response contains a pair of
 *       <Status, EpochRecoveryMetadata>. The reply also contains a status
 *       in the header which can sometimes be used to deduce the status of
 *       all the epochs in the range. See the comment for different status
 *       codes below
 */

class EpochRecoveryMetadata;

// Status can be one of E::OK, E::EMPTY, E::NOTREADY
// EpochRecoveryMetadata is valid only if the corresponding status
// is E:OK. Invalid otherwise
using EpochRecoveryStateMap =
    std::unordered_map<epoch_t::raw_type,
                       std::pair<Status, EpochRecoveryMetadata>>;

struct GET_EPOCH_RECOVERY_METADATA_REPLY_Header {
  // used to identify the purge state machine
  logid_t log_id;
  epoch_t purge_to;

  epoch_t start;
  uint16_t flags; // currently unused

  // can be one of:
  // E::OK           Request was processed for all epochs in the range. The
  //                 status of individual epochs in the range could be any of
  //                 E:OK, E:EMPTY, E:NOTREADY.
  //
  // E::EMPTY        requested range of epoch is clean and empty (or trimmed)
  //                 for the log, no EpochRecoveryMetadata will be included
  //
  // E::NOTREADY     None of the epochs in request range are clean
  //
  // E::REBUILDING   the node/shard on which the log is stored is rebuilding
  //
  // E::NOTSTORAGE   the node is not a storage node
  //
  // E::FAILED       the node is unable to read the metadata from its local
  //                 logstore, or the metdata is malformed
  //
  // E::AGAIN        the node is overloaded and can be retried
  Status status;

  // Shard from which EpochRecoveryMetadata is coming from.
  shard_index_t shard;

  // Shard being purged that is requesting EpochRecoveryMetadata.
  shard_index_t purging_shard;

  // Last epoch in the range of epochs this reply is for
  epoch_t end;

  // Number of non empty epochs in the range [start, end] for
  // which this reply contains the metadata
  uint64_t num_non_empty_epochs;

  // Request id from GET_EPOCH_RECOVERY_METADATA_Message
  request_id_t id;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(GET_EPOCH_RECOVERY_METADATA_REPLY_Header);
  }
} __attribute__((__packed__));

class GET_EPOCH_RECOVERY_METADATA_REPLY_Message : public Message {
 public:
  GET_EPOCH_RECOVERY_METADATA_REPLY_Message(
      const GET_EPOCH_RECOVERY_METADATA_REPLY_Header& header,
      std::vector<epoch_t> epochs,
      std::vector<Status> status,
      std::vector<std::string> metadata_blobs);

  GET_EPOCH_RECOVERY_METADATA_REPLY_Message(
      const GET_EPOCH_RECOVERY_METADATA_REPLY_Message&) noexcept = delete;
  GET_EPOCH_RECOVERY_METADATA_REPLY_Message(
      GET_EPOCH_RECOVERY_METADATA_REPLY_Message&&) noexcept = delete;
  GET_EPOCH_RECOVERY_METADATA_REPLY_Message&
  operator=(const GET_EPOCH_RECOVERY_METADATA_REPLY_Message&) = delete;
  GET_EPOCH_RECOVERY_METADATA_REPLY_Message&
  operator=(GET_EPOCH_RECOVERY_METADATA_REPLY_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;

  Disposition onReceived(const Address&) override {
    // Receipt handler lives in
    // server/GET_EPOCH_RECOVERY_METADATA_REPLY_onReceived.cpp; this should
    // never get called.
    std::abort();
  }

  const GET_EPOCH_RECOVERY_METADATA_REPLY_Header& getHeader() const {
    return header_;
  }

  static Message::deserializer_t deserialize;

  GET_EPOCH_RECOVERY_METADATA_REPLY_Header header_;

  // TODO: T23822953 Save space by not including empty epochs
  // and decuding emptyness from epoch not being included
  // in the map

  // vector of epochs follows the header. Size of the
  // epochs_ will be end-start+1
  std::vector<epoch_t> epochs_;
  // For each corresponding position in epochs_,
  // a status follows. Size of status_ will be
  // end-start+1
  std::vector<Status> status_;
  // For each E:OK status, a EpochRecoveryMetadata
  // follows. Size of metadata_ will be `num_non_empty_epochs`
  // in the header
  std::vector<std::string> metadata_;
};

namespace GET_EPOCH_RECOVERY_METADATA_REPLY {

// convenience function for sending a reply
void createAndSend(
    const Address& to,
    logid_t log_id,
    shard_index_t purging_shard,
    shard_index_t shard,
    epoch_t purge_to,
    epoch_t start,
    epoch_t end,
    uint16_t flags,
    Status status,
    request_id_t id,
    std::unique_ptr<EpochRecoveryStateMap> epoch_recovery_state_map);
} // namespace GET_EPOCH_RECOVERY_METADATA_REPLY

}} // namespace facebook::logdevice
