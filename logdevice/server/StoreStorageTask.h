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
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/CopySet.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Second phase of processing a STORE message.  A worker thread got the
 * message and handed off processing to a storage thread via this task.  When
 * done, this should send a STORED message to the peer who sent STORE.
 *
 * LocalLogStoreRecordFormat documents the format of records.
 */

class LogStorageState;
class LogStorageStateMap;
class PayloadHolder;
struct STORE_Header;

class StoreStorageTask : public WriteStorageTask {
 public:
  /**
   * This needs to extract and make copies of parts of STORE message that it
   * needs to keep around (parts of header, copyset, reply address).  Assumes
   * shared ownership of the payload.
   */
  StoreStorageTask(const STORE_Header& store_header,
                   const StoreChainLink* copyset,
                   folly::Optional<lsn_t> block_starting_lsn,
                   std::map<KeyType, std::string> optional_keys,
                   const std::shared_ptr<PayloadHolder>& payload_holder,
                   STORE_Extra extra,
                   ClientID reply_to,
                   std::chrono::steady_clock::time_point start_time,
                   Durability durability,
                   bool write_find_time_index,
                   bool merge_mutable_per_epoch_log_metadata,
                   bool write_shard_id_in_copyset);

  ~StoreStorageTask() override;

  // All WriteStorageTask subclasses are processed the same way, we just
  // specify what to do when it's done.
  void onDone() override;
  void onDropped() override;

  bool isPreempted(Seal* preempted_by) override;

  bool isLsnBeforeTrimPoint() override;

  bool isTimedout() const override;

  ThreadType getThreadType() const override {
    // Stores from Appenders are sensitive to latency (because of store timeout
    // in Appender, and limited sequencer window). They are executed on fast
    // threads to get reads and metadata syncs out of their way.
    // Rebuilding stores, on the other hand, are considered lower-pri and
    // have big timeouts. If the disk can't keep up with both appends and
    // rebuilding, rebuilding is throttled by stalling the FAST_STALLABLE
    // threads.
    return rebuilding_ ? ThreadType::FAST_STALLABLE
                       : ThreadType::FAST_TIME_SENSITIVE;
  }

  size_t getPayloadSize() const override;

  size_t getNumWriteOps() const override;

  size_t getWriteOps(const WriteOp** write_ops,
                     size_t write_ops_len) const override;

  bool allowIfStoreIsNotAcceptingWrites(Status status) const override {
    // Only allowed to override E::NOSPC
    if (status != E::NOSPC) {
      return false;
    }

    // When low on space, still allow mutations and amending copysets.
    // Otherwise rebuilding would stall and recovery would be likely to stall.
    return (rebuilding_ && amend_copyset_) || recovery_;
  }

  // insert the stored record into the record cache. should only be called
  // when the store is successfully finished.
  // @return        0 if record is cached, -1 otherwise
  int putCache() override;

  // check if the write is preempted by only by soft seal
  bool isPreemptedBySoftSealOnly() const {
    return soft_preempted_only_;
  }

  // used in tests
  const std::string& getRecordHeaderBuf() const {
    return record_header_buf_;
  }

  // used in tests
  const std::string& getCopySetIndexEntryBuf() const {
    return copyset_index_entry_buf_;
  }

 protected:
  virtual LogStorageStateMap* getLogStateMap() const;

  virtual StatsHolder* stats();

  virtual shard_index_t getShardIdx() const;

 private:
  // Convenience wrapper for STORED_Message_createAndSend
  void sendReply(Status status) const;

  // called when the storage task is sent back to the worker thread,
  // in case record caching is on, free evicted cache entries previously
  // disposed on the same worker thread
  void drainCacheDisposalOnWorker();

  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;

  LogStorageState& getLogStorageState();

  // Shared ownership of the payload.  Note that all modifications (like
  // getPayload() and destruction) should happen on the worker thread.  The
  // storage thread should get just a raw pointer to the data.
  std::shared_ptr<PayloadHolder> payload_holder_;

  // used for record caching
  uint64_t timestamp_;
  esn_t lng_;
  copyset_t copyset_;
  STORE_flags_t flags_;
  Slice payload_raw_;

  // These are used to reply
  RecordID rid_;
  uint32_t wave_;
  ClientID reply_to_;
  bool recovery_; // is this STORE used during recovery?
  bool rebuilding_;
  bool amend_copyset_;
  bool drain_;
  bool soft_preempted_only_{false};

  // deadline after which the store operation is presumed to have timed out
  std::chrono::steady_clock::time_point task_deadline_;

  STORE_Extra extra_;

  // When did we start processing the store? Used to calculate the store
  // latency.
  std::chrono::steady_clock::time_point start_time_;

  // This holds the record header as it will be written into the local log
  // store.  formRecordHeader() initializes this and returns a Payload that
  // points into this string.
  std::string record_header_buf_;

  // This holds the copyset index entry as it will be written into the
  // local log store. formCopySetIndexEntry() initializes this and returns
  // a Slice that points into this string.
  std::string copyset_index_entry_buf_;

  PutWriteOp write_op_;

  // Mutable per-epoch metadata written/updated together with the record store.
  // Only if constructed with merge_mutable_per_epoch_log_metadata == true.
  folly::Optional<MutablePerEpochLogMetadata> metadata_;
  folly::Optional<MergeMutablePerEpochLogMetadataWriteOp> metadata_write_op_;
};

}} // namespace facebook::logdevice
