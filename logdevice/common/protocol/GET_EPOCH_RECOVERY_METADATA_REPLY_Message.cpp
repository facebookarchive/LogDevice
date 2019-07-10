/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_EPOCH_RECOVERY_METADATA_REPLY_Message::
    GET_EPOCH_RECOVERY_METADATA_REPLY_Message(
        const GET_EPOCH_RECOVERY_METADATA_REPLY_Header& header,
        std::vector<epoch_t> epochs,
        std::vector<Status> status,
        std::vector<std::string> metadata)
    : Message(MessageType::GET_EPOCH_RECOVERY_METADATA_REPLY,
              TrafficClass::RECOVERY),
      header_(header),
      epochs_(std::move(epochs)),
      status_(std::move(status)),
      metadata_(std::move(metadata)) {
  ld_check_ge(header_.end, header_.start);
  if (header_.status == E::OK) {
    auto numEpochs = header_.end.val_ - header_.start.val_ + 1;
    ld_check_eq(numEpochs, epochs_.size());
    ld_check_eq(numEpochs, status_.size());
  }
}

void GET_EPOCH_RECOVERY_METADATA_REPLY_Message::serialize(
    ProtocolWriter& writer) const {
  // Serialize the EpochRecoveryMetadata.
  ld_check_le(header_.start.val_, header_.end.val_);
  auto num_epochs = header_.end.val_ - header_.start.val_ + 1;
  if (header_.status == E::OK) {
    ld_check_eq(num_epochs, epochs_.size());
    ld_check_eq(num_epochs, status_.size());
    ld_check_eq(header_.num_non_empty_epochs, metadata_.size());
  }

  writer.write(header_);
  if (header_.status == E::OK) {
    writer.writeVector(epochs_);
    writer.writeVector(status_);
    for (auto& metadata : metadata_) {
      const uint32_t size = metadata.size();
      ld_check(size != 0);
      writer.write(size);
      writer.write(metadata.data(), size);
    }
  }
}

MessageReadResult
GET_EPOCH_RECOVERY_METADATA_REPLY_Message::deserialize(ProtocolReader& reader) {
  GET_EPOCH_RECOVERY_METADATA_REPLY_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;
  hdr.purging_shard = -1;
  hdr.end = EPOCH_INVALID;
  hdr.id = REQUEST_ID_INVALID;
  hdr.num_non_empty_epochs = 0;
  reader.read(
      &hdr,
      GET_EPOCH_RECOVERY_METADATA_REPLY_Header::headerSize(reader.proto()));

  std::vector<Status> status;
  std::vector<epoch_t> epochs;
  std::vector<std::string> metadata;

  uint64_t num_epochs = hdr.end.val_ - hdr.start.val_ + 1;
  if (hdr.status == E::OK) {
    reader.readVector(&epochs, num_epochs);
    reader.readVector(&status, num_epochs);
    uint64_t i = 0;
    while (i < hdr.num_non_empty_epochs) {
      std::string blob;
      uint32_t blob_length = 0;
      reader.read(&blob_length);
      blob.resize(blob_length);
      reader.read(const_cast<char*>(blob.data()), blob.size());
      metadata.push_back(std::move(blob));
      i++;
    }
  }

  return reader.result([&] {
    return new GET_EPOCH_RECOVERY_METADATA_REPLY_Message(
        hdr, std::move(epochs), std::move(status), std::move(metadata));
  });
}

namespace GET_EPOCH_RECOVERY_METADATA_REPLY {

// convenience function for sending a reply
void createAndSend(
    const Address& to,
    logid_t log_id,
    shard_index_t shard,
    shard_index_t purging_shard,
    epoch_t purge_to,
    epoch_t start,
    epoch_t end,
    uint16_t flags,
    Status status,
    request_id_t id,
    std::unique_ptr<EpochRecoveryStateMap> epoch_recovery_state_map) {
  ld_check(epoch_recovery_state_map || status != E::OK);
  ld_check(end >= start);

  std::vector<epoch_t> epochs;
  std::vector<Status> statuses;
  uint64_t num_non_empty_epochs = 0;
  std::vector<std::string> metadata_blobs;
  if (epoch_recovery_state_map) {
    ld_check(status == E::OK);
    ld_check(epoch_recovery_state_map->size() == end.val_ - start.val_ + 1);
    for (auto& entry : *(epoch_recovery_state_map.get())) {
      epochs.push_back(epoch_t(entry.first));
      Status s = entry.second.first;
      statuses.push_back(s);
      if (s == E::OK) {
        EpochRecoveryMetadata& metadata = entry.second.second;
        ld_check(metadata.valid());
        Slice slice = metadata.serialize();
        metadata_blobs.emplace_back(
            reinterpret_cast<const char*>(slice.data), slice.size);
        num_non_empty_epochs++;
      }
    }
  }

  GET_EPOCH_RECOVERY_METADATA_REPLY_Header hdr{log_id,
                                               purge_to,
                                               start,
                                               flags,
                                               status,
                                               shard,
                                               purging_shard,
                                               end,
                                               num_non_empty_epochs,
                                               id};

  Worker::onThisThread()->sender().sendMessage(
      std::make_unique<GET_EPOCH_RECOVERY_METADATA_REPLY_Message>(
          hdr,
          std::move(epochs),
          std::move(statuses),
          std::move(metadata_blobs)),
      to);
}

} // namespace GET_EPOCH_RECOVERY_METADATA_REPLY

}} // namespace facebook::logdevice
