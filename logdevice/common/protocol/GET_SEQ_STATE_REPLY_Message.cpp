/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "GET_SEQ_STATE_REPLY_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

MessageReadResult
GET_SEQ_STATE_REPLY_Message::deserialize(ProtocolReader& reader) {
  GET_SEQ_STATE_REPLY_Header header;
  header.flags = 0;
  reader.read(&header, sizeof(header));

  auto msg = std::make_unique<GET_SEQ_STATE_REPLY_Message>(header);

  reader.read(&msg->request_id_);
  reader.read(&msg->status_);
  reader.read(&msg->redirect_);

  if (header.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_ATTRIBUTES) {
    uint64_t timestamp;
    reader.read(&msg->tail_attributes_.last_released_real_lsn);
    reader.read(&timestamp);
    reader.read(&msg->tail_attributes_.byte_offset);
    msg->tail_attributes_.last_timestamp = std::chrono::milliseconds(timestamp);
  }

  if (header.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_EPOCH_OFFSET) {
    reader.read(&msg->epoch_offset_);
  }

  if (header.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_HISTORICAL_METADATA) {
    if (reader.proto() < Compatibility::HISTORICAL_METADATA_IN_GSS_REPLY) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Bad GET_SEQ_STATE_REPLY_Message: "
                      "HISTORICAL_METADATA_IN_GSS_REPLY flag received in "
                      "protocol %d",
                      reader.proto());
      return reader.errorResult(E::BADMSG);
    }

    // TODO(TT15517759): do not need log_id and server config for constructing
    // epoch metadata
    auto server_config = Worker::onThisThread()->getServerConfig();
    msg->metadata_map_ =
        EpochMetaDataMap::deserialize(reader,
                                      /*evbuffer_zero_copy=*/false,
                                      header.log_id,
                                      *server_config);
  }

  if (header.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_RECORD) {
    if (reader.proto() < Compatibility::TAIL_RECORD_IN_GSS_REPLY) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Bad GET_SEQ_STATE_REPLY_Message: "
                      "INCLUDES_TAIL_RECORD flag received in "
                      "protocol %d",
                      reader.proto());
      return reader.errorResult(E::BADMSG);
    }
    msg->tail_record_ = std::make_shared<TailRecord>();
    msg->tail_record_->deserialize(reader, /*zero_copy*/ true);
  }

  return reader.resultMsg(std::move(msg));
}

void GET_SEQ_STATE_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, sizeof(header_));
  writer.write(request_id_);
  writer.write(status_);
  writer.write(redirect_);

  if (header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_ATTRIBUTES) {
    writer.write(tail_attributes_.last_released_real_lsn);
    uint64_t timestamp = tail_attributes_.last_timestamp.count();
    writer.write(timestamp);
    writer.write(tail_attributes_.byte_offset);
  }

  if (header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_EPOCH_OFFSET) {
    writer.write(epoch_offset_);
  }

  if (header_.flags &
      GET_SEQ_STATE_REPLY_Header::INCLUDES_HISTORICAL_METADATA) {
    if (writer.proto() < Compatibility::HISTORICAL_METADATA_IN_GSS_REPLY) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Sending GET_SEQ_STATE_REPLY_Message with "
                         "HISTORICAL_METADATA_IN_GSS_REPLY flag in "
                         "protocol %d.",
                         writer.proto());
      // this implies that we are replying to a connection w/ lower protocol
      // version with the new feature, this is not possible as we clear the
      // GET_SEQ_STATE_Message::INCLUDE_HISTORICAL_METADATA flag on receiving
      // the initial GSS request message
      writer.setError(E::BADMSG);
      ld_check(false);
      return;
    }

    ld_check(metadata_map_ != nullptr);
    metadata_map_->serialize(writer);
  }

  if (header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_RECORD) {
    if (writer.proto() < Compatibility::TAIL_RECORD_IN_GSS_REPLY) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Sending GET_SEQ_STATE_REPLY_Message with "
                         "INCLUDES_TAIL_RECORD flag in protocol %d.",
                         writer.proto());
      // for the same reason above
      writer.setError(E::BADMSG);
      ld_check(false);
      return;
    }

    ld_check(tail_record_ != nullptr);
    tail_record_->serialize(writer);
  }
}

Message::Disposition
GET_SEQ_STATE_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    5,
                    "PROTOCOL ERROR: received a GET_SEQ_STATE_REPLY message "
                    "for log %lu from a client address %s.",
                    header_.log_id.val_,
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  if (status_ == E::REDIRECTED || status_ == E::PREEMPTED) {
    if (!redirect_.isNodeID()) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "PROTOCOL ERROR: got GET_SEQ_STATE_REPLY from %s with "
                      "status %s and invalid redirect",
                      Sender::describeConnection(from).c_str(),
                      error_name(status_));
      err = E::PROTO;
      return Disposition::ERROR;
    }
  }

  if (!epoch_valid_or_unset(lsn_to_epoch(header_.last_released_lsn))) {
    // note: last released lsn can be LSN_INVALID if recovery hasn't finished
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Got GET_SEQ_STATE_REPLY message from %s with invalid "
                       "lsn %lu for log %lu!",
                       Sender::describeConnection(from).c_str(),
                       header_.last_released_lsn,
                       header_.log_id.val_);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  if (!Worker::onThisThread()->isAcceptingWork()) {
    // Ignore the message if shutting down.
    return Disposition::NORMAL;
  }

  auto& requestMap = Worker::onThisThread()->runningGetSeqState();
  auto entry = requestMap.getGssEntryFromRequestId(header_.log_id, request_id_);

  if (entry != nullptr && entry->request != nullptr) {
    entry->request->onReply(from.id_.node_, *this);
  } else {
    ld_debug("Request for log %lu with rqid:%lu is not present in the map",
             header_.log_id.val_,
             request_id_.val());
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
