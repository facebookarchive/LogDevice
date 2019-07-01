/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GET_EPOCH_RECOVERY_METADATA_REPLY_onReceived.h"

#include <folly/Memory.h>

#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/Sender.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"

namespace facebook { namespace logdevice {

Message::Message::Disposition GET_EPOCH_RECOVERY_METADATA_REPLY_onReceived(
    GET_EPOCH_RECOVERY_METADATA_REPLY_Message* msg,
    const Address& from) {
  if (from.isClientAddress()) {
    ld_error("got GET_EPOCH_RECOVERY_METADATA_REPLY_Message message from "
             "client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  const GET_EPOCH_RECOVERY_METADATA_REPLY_Header& header = msg->getHeader();
  ServerWorker* worker = ServerWorker::onThisThread();

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards(
      worker->processor_->getMyNodeID().index());
  ld_check(n_shards > 0); // We already checked we are a storage node.

  shard_index_t shard_idx = header.purging_shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got GET_EPOCH_RECOVERY_METADATA_REPLY_Message message "
                    "from client %s with invalid shard %u, this node only has "
                    "%u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  GetEpochRecoveryMetadataRequest* request = nullptr;

  if (header.id != REQUEST_ID_INVALID) {
    const auto& index =
        worker->runningGetEpochRecoveryMetadata()
            .requests.get<GetEpochRecoveryMetadataRequestMap::RequestIndex>();

    auto it = index.find(header.id);
    if (it == index.end()) {
      // state machine no longer exist, stale reply
      RATELIMIT_DEBUG(std::chrono::seconds(2),
                      1,
                      "Receive GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
                      "client %s for log %lu, purge_to %u, epoch %u, but "
                      "no active GetEpochRecoveryMetadataRequest state machine "
                      "found. This is probably a stale reply.",
                      Sender::describeConnection(from).c_str(),
                      header.log_id.val_,
                      header.purge_to.val_,
                      header.start.val_);

      return Message::Disposition::NORMAL;
    }
    request = it->get();
  }

  if (request == nullptr) {
    // This is only possible if we sent a GET_EPOCH_RECOVERY_METADATA to a
    // node which does not know about range support in
    // GET_EPOCH_RECOVERY_METADATA_Message and it should have
    // originated from PurgeSinlgeEpoch state machine
    // TODO: T23693338 Remove this once all servers are at
    // GET_EPOCH_RECOVERY_RANGE_SUPPORT protocol version
    ld_check(header.id == REQUEST_ID_INVALID);

    auto key = std::make_tuple(
        header.log_id, header.purge_to, header.shard, header.start);
    const auto& index =
        worker->runningGetEpochRecoveryMetadata()
            .requests.get<GetEpochRecoveryMetadataRequestMap::PurgeIndex>();

    auto it = index.find(key);
    if (it == index.end()) {
      // state machine no longer exist, stale reply
      RATELIMIT_DEBUG(std::chrono::seconds(2),
                      1,
                      "Receive GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
                      "client %s for log %lu, purge_to %u, epoch %u, but "
                      "no active GetEpochRecoveryMetadataRequest state "
                      "machine found. This is probably a stale reply.",
                      Sender::describeConnection(from).c_str(),
                      header.log_id.val_,
                      header.purge_to.val_,
                      header.start.val_);
      return Message::Disposition::NORMAL;
    }
    request = it->get();
  }

  ld_check(request);
  EpochRecoveryStateMap epochRecoveryStateMap;

  if (header.status == E::OK) {
    ld_check(!msg->epochs_.empty());
    ld_check(!msg->status_.empty());
    auto epoch_it = msg->epochs_.begin();
    auto status_it = msg->status_.begin();
    auto metadata_it = msg->metadata_.begin();

    if (header.num_non_empty_epochs !=
        std::count(msg->status_.begin(), msg->status_.end(), E::OK)) {
      RATELIMIT_WARNING(
          std::chrono::seconds(2),
          1,
          "Receive GET_EPOCH_RECOVERY_METADATA_REPLY_Message from"
          " client %s for log %lu, purge_to %u, epoch %u, but "
          "no num_non_empty_epochs(%lu) in header does not match "
          "number of epochs with status E::OK (%ld).",
          Sender::describeConnection(from).c_str(),
          header.log_id.val_,
          header.purge_to.val_,
          header.start.val_,
          header.num_non_empty_epochs,
          std::count(msg->status_.begin(), msg->status_.end(), E::OK));
      err = E::PROTO;
      return Message::Disposition::ERROR;
    }

    while (epoch_it != msg->epochs_.end()) {
      epoch_t epoch = *epoch_it;
      ld_check(status_it != msg->status_.end());
      if (*status_it != E::OK && *status_it != E::NOTREADY &&
          *status_it != E::EMPTY) {
        RATELIMIT_CRITICAL(
            std::chrono::seconds(1),
            1,
            "Received GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
            "client %s with invalid status %s for epoch %u, log %lu, "
            "purge_to %u",
            Sender::describeConnection(from).c_str(),
            toString(*status_it).c_str(),
            epoch.val_,
            header.log_id.val_,
            header.purge_to.val_);
        ld_check(false);
        return Message::Disposition::NORMAL;
      }

      EpochRecoveryMetadata metadata;
      if (*status_it == E::OK) {
        if (metadata_it == msg->metadata_.end() || (*metadata_it).empty()) {
          RATELIMIT_ERROR(
              std::chrono::seconds(2),
              1,
              "Received GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
              "client %s with E::OK status for log %lu, purge_to %u, "
              "epoch %u, but EpochRecoveryMetadata is invalid! "
              "metadata: %s.",
              Sender::describeConnection(from).c_str(),
              header.log_id.val_,
              header.purge_to.val_,
              header.start.val_,
              metadata.toString().c_str());
          return Message::Disposition::NORMAL;
        }
        int rv = metadata.deserialize(
            Slice(metadata_it->data(), metadata_it->size()));
        if (rv != 0) {
          RATELIMIT_ERROR(
              std::chrono::seconds(2),
              1,
              "Receive GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
              "client %s with E::OK status for log %lu, purge_to %u, "
              "epoch %u, but failed to deserialze the "
              "EpochRecoveryMetadata. Blob size: %lu, hex: %s",
              Sender::describeConnection(from).c_str(),
              header.log_id.val_,
              header.purge_to.val_,
              header.start.val_,
              metadata_it->size(),
              hexdump_buf(metadata_it->data(), metadata_it->size()).c_str());
          return Message::Disposition::NORMAL;
        }

        if (!metadata.valid()) {
          RATELIMIT_ERROR(
              std::chrono::seconds(2),
              1,
              "Receive GET_EPOCH_RECOVERY_METADATA_REPLY_Message from "
              "client %s with E::OK status for log %lu, purge_to %u, "
              "epoch %u, but EpochRecoveryMetadata is invalid! "
              "metadata: %s.",
              Sender::describeConnection(from).c_str(),
              header.log_id.val_,
              header.purge_to.val_,
              header.start.val_,
              metadata.toString().c_str());
          return Message::Disposition::NORMAL;
        }
        metadata_it++;
      }
      epochRecoveryStateMap.emplace(
          epoch.val(), std::make_pair(*status_it, metadata));

      epoch_it++;
      status_it++;
    }
  }

  shard_index_t remote_shard_idx = header.shard;
  ld_check(remote_shard_idx != -1);

  request->onReply(
      ShardID(from.id_.node_.index(), remote_shard_idx),
      header.status,
      header.status == E::OK
          ? std::make_unique<EpochRecoveryStateMap>(epochRecoveryStateMap)
          : nullptr);

  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
