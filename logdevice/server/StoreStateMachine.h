/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

#include "logdevice/common/Address.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/settings/Durability.h"
#include "logdevice/server/read_path/LogStorageState.h"

namespace facebook { namespace logdevice {

class STORE_Message;

/**
 * @file  This is a state machine used to process the STORE message. It does so
 *        in several steps:
 *          1. obtains the seal
 *          2. forwards the message if chain sending is requested
 *          3. writes the record to the local log store
 */

class StoreStateMachine {
 public:
  /**
   * Static method handling incoming STORE messages.  If all is well, creates
   * and kicks off a `StoreStateMachine' instance.
   */
  static Message::Disposition onReceived(STORE_Message* msg,
                                         const Address& from);
  /**
   * Check if the copyset of an incoming STORE message is valid.
   *
   * @param msg            STORE message
   * @param from           client Address that this message was received from
   * @param my_node_id     NodeID of this server in the cluster config
   * @param out_my_offset  index of my_node_id in copyset will be stored here
   * @param out_rebuilding_node  if `err' is E::REBUILDING, this will contain
   *             index of some node that's both in copyset and rebuilding set
   *
   * @return   if copyset_ is good, returns 0, otherwise returns -1 and sets
   *           err to:
   *             NOTFOUND    if copyset is valid but doesn't contain my_node_id,
   *             PROTO       if copyset is invalid,
   *             REBUILDING  if copyset intersects rebuilding set.
   */
  static int validateCopyset(const STORE_Message& msg,
                             const Address& from,
                             NodeID my_node_id,
                             copyset_off_t* out_my_offset,
                             ShardID* out_rebuilding_node);

  StoreStateMachine(std::unique_ptr<STORE_Message> message,
                    Address from,
                    std::chrono::steady_clock::time_point start_time,
                    Durability durability);

  ~StoreStateMachine();

  // Starts executing the state machine. When this method returns, the ownership
  // of this object has been transferred or it became self-owned.
  void execute();

 private:
  // called when seal for the log is known
  void onSealRecovered(Status status, LogStorageState::Seals seals);

  // Updates soft seal and calls storeAndForward().
  class SoftSealStorageTask;

  // Forwards the record to the next node in chain (if chain sending is
  // enabled) and stores it in the local log store.
  void storeAndForward();

  std::unique_ptr<STORE_Message> message_;
  shard_index_t shard_;
  Address from_; // sender of the STORE message
  std::chrono::steady_clock::time_point start_time_;
  Durability durability_;
};

}} // namespace facebook::logdevice
