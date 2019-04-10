/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Address.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file  A task responsible for reading EpochRecoveryMetadata for a
 *        <logid, epoch> pair if the epoch is clean. It is also responsible
 *        for sending reply to the GET_EPOCH_RECOVERY_METADATA message
 *        receieved previously regarding the state of the epoch.
 */

class EpochRecoveryMetadata;
struct GET_EPOCH_RECOVERY_METADATA_Header;
class LocalLogStore;
class LogStorageStateMap;

class GetEpochRecoveryMetadataStorageTask : public StorageTask {
 public:
  GetEpochRecoveryMetadataStorageTask(
      const GET_EPOCH_RECOVERY_METADATA_Header& header,
      const Address& reply_to,
      const shard_index_t shard);

  // see StorageTask.h
  void execute() override;

  void onDone() override;

  void onDropped() override;

  StorageTaskPriority getPriority() const override {
    // metadata access for purging
    return StorageTaskPriority::HIGH;
  }

  // Public for tests
  Status executeImpl(LocalLogStore& store,
                     LogStorageStateMap& state_map,
                     StatsHolder* stats = nullptr);

  void sendReply();

  const EpochRecoveryStateMap& getEpochRecoveryStateMap() const {
    return epochRecoveryStateMap_;
  }

 private:
  const logid_t log_id_;
  const shard_index_t shard_;
  const shard_index_t purging_shard_;
  const epoch_t purge_to_;
  const epoch_t start_;
  const epoch_t end_;
  const Address reply_to_;
  const request_id_t request_id_;

  Status status_;
  EpochRecoveryStateMap epochRecoveryStateMap_;
};

}} // namespace facebook::logdevice
