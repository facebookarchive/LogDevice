/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/EpochRecordCache.h"

namespace facebook { namespace logdevice {

/**
 * @file CachedDigest manages delivering digesting Records read from
 *       an EpochRecordCache to the EpochRecovery state machine running on the
 *       sequencer node. Unlike ServerReadStream, it only reads records from
 *       the record cache, does not access local log store, and does a one-way
 *       push with no flow control (i.e., WINDOW messages). Therefore, it
 *       is much simpler and implemented separately. However, it does make sure
 *       to not push too many bytes to the same client in one libevent
 *       iteration, and does retry on message send failures.
 */

class AllCachedDigests;
class BackoffTimer;
class ClientDigests;
class Timer;

class CachedDigest {
  enum class State { INIT, SEND_STARTED, SEND_RECORDS, CONCLUDE_DIGEST };
  class ResumeDigestCallback : public BWAvailableCallback {
   public:
    explicit ResumeDigestCallback(CachedDigest* digest)
        : cached_digest_(digest) {}

    void operator()(FlowGroup&, std::mutex&) override;

   private:
    CachedDigest* cached_digest_;
  };

 public:
  /**
   *  Construct a CachedDigest object. The object is in inactive state once
   *  constructed, and will only become active when start() is called.
   *
   *  @param epoch_snapshot  Snapshot of epoch record cache, must be a FULL
   *                         snapshot taken after digest request is received.
   *                         nullptr if the digesting epoch is _empty_
   *  @param client_digests  parent object that manages all CachedDigest_s for a
   *                         client connection. can be nullptr in tests
   *  @param all_digests     object that manages all CachedDigest_s on a Worker
   */
  CachedDigest(logid_t log_id,
               shard_index_t shard,
               read_stream_id_t stream_id,
               ClientID client_id,
               lsn_t start_lsn,
               std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot,
               ClientDigests* client_digests,
               AllCachedDigests* all_digests);

  /**
   * Start the CachedDigest, the object is considered active when start() is
   * called. Should only be called once.
   */
  void start();

  /**
   * Attempt to read from the cache and deliver digest record to the client.
   */
  void pushRecords();

  /**
   *  @return   epoch in which the digest belongs to
   */
  epoch_t getEpoch() const {
    return lsn_to_epoch(start_lsn_);
  }

  /**
   *  @return  next ESN to be delivered to the client
   */
  esn_t nextEsnToDeliver() const {
    return lsn_to_esn(next_lsn_to_deliver_);
  }

  bool started() const {
    return state_ > State::INIT;
  }

  virtual ~CachedDigest();

 protected:
  virtual std::unique_ptr<Timer>
  createPushTimer(std::function<void()> callback);
  virtual void cancelPushTimer();
  virtual void activatePushTimer();
  virtual std::unique_ptr<BackoffTimer>
  createDelayTimer(std::function<void()> callback);
  virtual void onDigestComplete();
  virtual bool canPushRecords() const;
  virtual void onBytesEnqueued(size_t msg_size);

  virtual bool includeExtraMetadata() const {
    return true;
  }
  // for sending messages
  std::unique_ptr<SenderBase> sender_;

 private:
  ResumeDigestCallback resume_cb_;

  const logid_t log_id_;
  const shard_index_t shard_;
  const read_stream_id_t stream_id_;

  // Client to reply to
  const ClientID client_id_;

  // start LSN of the digest
  const lsn_t start_lsn_;

  // true if the epoch is empty and epoch_cache_ should be nullptr
  const bool epoch_empty_;

  // cache that contains unclean records of the epoch
  std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot_;
  // iterator of the epoch cache snapshot
  std::unique_ptr<EpochRecordCache::Snapshot::ConstIterator> snapshot_iterator_;

  // last record lsn to be included in the digest, determined when the object
  // is created
  const lsn_t end_lsn_;

  // parent class that owns all CachedDigest for one client,
  // must outlive `this', maybe null in tests
  ClientDigests* const client_digests_;

  // owns all CachedDigest for the Worker,
  // must outlive `this', maybe null in tests
  AllCachedDigests* const all_digests_;

  // State machine mode.
  State state_{State::INIT};

  // next lsn to be delivered to the client
  lsn_t next_lsn_to_deliver_{LSN_INVALID};

  // esn of the last record delivered
  esn_t last_esn_delivered_{ESN_INVALID};

  // Timers used to defer pushing records. The push timer will cause pushing to
  // resume on the next iteration of this thread's event loop. The delay timer
  // resumes pushing after exponential backoff and is activated when network TX
  // limits are exceed.
  std::unique_ptr<Timer> push_timer_;
  std::unique_ptr<BackoffTimer> delay_timer_;

  // @return true if all required records are enqueued to the client and the
  //         digest should be concluded.
  bool allRecordsShipped() const;

  // Called when the last digest record of the epoch is enqueued to
  // the client. Send the final gap to ESN_MAX and destroy the digest
  // if done.
  //
  // @return -1 with err set if the transmission failed. Otherwise 0.
  int concludeDigest();

  // Send the STARTED message if it has not already been
  // sent.
  //
  // @return -1 with err set if the transmission failed. Otherwise 0.
  int shipStarted();

  // Send the next batch of records.
  //
  // @return -1 with err set if the transmission failed. Otherwise 0.
  int shipRecords(size_t* bytes_pushed);

  // function that ships a record (of _esn_) obtained from the cache to the
  // client by sending a RECORD message.
  ssize_t shipRecord(esn_t esn,
                     const EpochRecordCache::Snapshot::Record& record);

  // used to convert store flags of a record to wire flags to be included in
  // the RECORD message.
  static RECORD_flags_t StoreFlagsToRecordFlags(STORE_flags_t store_flags);

  friend class AllCachedDigests;
  friend class CachedDigestTest;
};

}} // namespace facebook::logdevice
