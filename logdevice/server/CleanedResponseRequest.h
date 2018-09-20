/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Address.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/protocol/CLEAN_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file  CleanedResponseRequest is the request that handles sending CLEANED
 *        reply to the sequencer node which previously sent the CLEAN message
 *        during the recovery of a particular epoch. Before sending the CLEANED
 *        reply, it ensures that EpochRecoveryMetadata (derived from the CLEAN
 *        message) is persistently stored in the local log store if needed.
 *        This is important to guarantee when an epoch of a log finishes
 *        recovery (i.e., global LCE is advanced in epoch store), there must
 *        be at least f-majority of storage nodes that have
 *        EpochRecoveryMetadata for this epoch stored.
 *
 *        One optimizatioin for storing EpochRecoveryMetadata is that we do
 *        *NOT* store EpochRecoveryMetadata if the epoch is known to be empty
 *        (i.e., last_known_good == last_digest_record == ESN_INVALID) according
 *        to the EpochRecovery state machine that sent us the CLEAN message.
 */

class EpochRecoveryMetadata;

class CleanedResponseRequest : public Request {
 public:
  CleanedResponseRequest(Status status,
                         std::unique_ptr<CLEAN_Message> clean_msg,
                         worker_id_t worker_id,
                         Address reply_to,
                         Seal preempted_seal,
                         shard_index_t shard);

  int getThreadAffinity(int /* unused */) override {
    return worker_id_.val_;
  }

  Execution execute() override;

  // called when the storage task of writing EpochRecoveryMetadata
  // finishes
  void onWriteTaskDone(Status status);

 private:
  // status to be included in CLEANED message
  Status status_;
  // the orginal CLEAN message received
  std::unique_ptr<CLEAN_Message> clean_msg_;
  // index of worker that received the initial CLEAN message
  const worker_id_t worker_id_;
  // address of sequencer connection to send reply to
  const Address reply_to_;
  // if preempted, contains the Seal which preempted the original clean
  // message
  const Seal preempted_seal_;
  // shard that was cleaned
  const shard_index_t shard_;

  // per epoch log metadata to be written
  std::unique_ptr<EpochRecoveryMetadata> metadata_;

  // for creating weakref to itself for storage tasks created.
  WeakRefHolder<CleanedResponseRequest> ref_holder_;

  void prepareMetadata();
  void sendResponse();
  void writeMetadata();
};

}} // namespace facebook::logdevice
