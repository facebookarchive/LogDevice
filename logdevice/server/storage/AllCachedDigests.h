/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <queue>
#include <unordered_map>

#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/storage/CachedDigest.h"

namespace facebook { namespace logdevice {

/**
 * AllCachedDigests manages CacheDigest state machines for a Worker. It is
 * reponsible for creating, activating and destroying CachedDigest machines.
 * It ensures that only a certain number of CachedDigests can be activated at
 * the same time. It also enforces (via its ClientDigests class) a maximum
 * amount of pending bytes enqueued in evbuffer for each client connection.
 */

class AllCachedDigests;

/**
 * Manages all CachedDigest instances for one client connection.
 */
class ClientDigests {
 public:
  ClientDigests(ClientID cid, AllCachedDigests* parent);

  /**
   * Attempt to construct a CachedDigest object and insert it to the map.
   * If there is already an object with the same read_stream_id for this client,
   * neither construction nor insertion will happen.
   *
   * @return  a pair where the first item is a pointer to the CachedDigest
   * object inserted (or existing object), while the second is a bool which is
   * true if a new CachedDigest instance was created and inserted to the map.
   */
  std::pair<CachedDigest*, bool>
  insert(logid_t log_id,
         shard_index_t shard,
         read_stream_id_t rid,
         lsn_t start_lsn,
         std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot);

  /**
   * Get the CachedDigest instance by read stream id, nullptr if the read stream
   * does not exist.
   */
  CachedDigest* getDigest(read_stream_id_t rid);

  /**
   * @return   true if the digest for @param rid is actually removed. false if
   *           such stream does not exist.
   */
  bool eraseDigest(read_stream_id_t rid);

  /**
   * @return  if it is allowed to enqueue more RECORD messages to the socket
   * buffer
   */
  bool canPushRecords() const;

  /**
   * called when GAP messages are successfully sent by the client socket
   */
  void onGapSent(const GAP_Message& msg);

  /**
   * called when RECORD messages are successfully sent by the client socket
   */
  void onRecordSent(const RECORD_Message& msg);

  /**
   * called when STARTED messages are successfully sent by the client socket
   */
  void onStartedSent(const STARTED_Message& msg);

  /**
   * called when RECORD messages are successfully enqueued in the socket
   * evbuffer
   */
  void onBytesEnqueued(size_t bytes);

  /**
   * take ownership of the socket close callback for the client connection
   */
  void registerDisconnectCallback(std::unique_ptr<SocketCallback> callback);

 private:
  using DigestsMap = std::unordered_map<read_stream_id_t,
                                        std::unique_ptr<CachedDigest>,
                                        read_stream_id_t::Hash>;

  const ClientID client_id_;
  AllCachedDigests* const parent_;
  size_t bytes_queued_{0};
  DigestsMap digests_map_;
  std::unique_ptr<SocketCallback> disconnect_callback_;
};

class AllCachedDigests {
 public:
  /**
   * Construct AllCachedDigests instance.
   * @param max_active_digests     maximum _active_ readstreams allowed at the
   *                               same time
   *
   * @param max_bytes_queued_per_client_kb    maximum number of record bytes
   *                                          enqueued for each client
   */
  AllCachedDigests(size_t max_active_digests,
                   size_t max_bytes_queued_per_client_kb);

  virtual ~AllCachedDigests() {}

  /**
   *  Create, if necessary, and attempt to activate a CachedDigest. Should be
   *  the only function to start cached digesting upon receiving a START
   *  message. The CachedDigest instance is only created if there is no existing
   *  instance for the <client_id, rid> pair, and only activated when the number
   *  of active digest instances is below the limit.
   *
   *  @param epoch_cache         epoch record cache for the digesting epoch,
   *                             nullptr if the epoch is _empty_
   *
   *  @return  E::OK             This is a request for a new digest. The digest
   *                             request is being processed and will include
   *                             transmission of the STARTED message.
   *           E::INTERNAL       This is a duplicate request, which should never
   *                             occur. The request has been ignored. The caller
   *                             is repsonsible for transmitting any STARTED
   *                             message.
   *           E::INVALID_PARAM  This request duplicates the client_id/log_id/
   *                             read_id tuple of an already active request.
   */
  Status
  startDigest(logid_t log_id,
              shard_index_t shard,
              read_stream_id_t rid,
              ClientID client_id,
              lsn_t start_lsn,
              std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot);

  /**
   * Called when a CachedDigest instance is destroyed. @param active is true
   * if the instance has been activated.
   */
  void onDigestDestroyed(bool active);

  void onGapSent(ClientID cid, const GAP_Message& msg);
  void onRecordSent(ClientID cid, const RECORD_Message& msg);
  void onStartedSent(ClientID cid, const STARTED_Message& msg);

  /**
   * @return   true if the digest for <cid, rid> existed and has been removed.
   *           false if such a stream does not exist.
   */
  bool eraseDigest(ClientID cid, read_stream_id_t rid);

  /**
   * Destroy all cached digests for a given client. Called when a client
   * disconnects.
   */
  void eraseClient(ClientID cid);

  /**
   * Clear all active and inactive digests for all clients.
   */
  void clear();

  size_t getMaxQueueKBytesPerClient() const {
    return max_kbytes_queued_per_client_;
  }

  struct ClientDisconnectedCallback : public SocketCallback {
    explicit ClientDisconnectedCallback(AllCachedDigests* all_cached_digests)
        : owner(all_cached_digests) {}
    void operator()(Status st, const Address& name) override;
    AllCachedDigests* owner;
  };

  virtual std::unique_ptr<CachedDigest> createCachedDigest(
      logid_t log_id,
      shard_index_t shard,
      read_stream_id_t rid,
      ClientID client_id,
      lsn_t start_lsn,
      std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot,
      ClientDigests* client_digests);

  // used in tests
  CachedDigest* getDigest(ClientID cid, read_stream_id_t rid);

  size_t queueSize() const {
    return queue_.size();
  }

  std::unique_ptr<Timer>& getRescheduleTimer() {
    return reschedule_timer_;
  }

 protected:
  virtual std::unique_ptr<Timer>
  createRescheduleTimer(std::function<void()> callback);

  virtual void activateRescheduleTimer();

  virtual size_t getMaxStreamsStartedBatch() const {
    return MAX_STREAMS_STARTED_BATCH;
  }

 private:
  // TODO: if needed, make a separate cap and queue for metadata logs
  const size_t max_active_cached_digests_;
  const size_t max_kbytes_queued_per_client_;

  using ClientMap = std::unordered_map<ClientID, ClientDigests, ClientID::Hash>;

  ClientMap clients_;

  size_t num_active_digests_{0};

  // a queue storing identifiers of CachedDigests that were inactive and
  // waiting to be started
  std::queue<std::pair<ClientID, read_stream_id_t>> queue_;

  // To support yielding/re-entrance in scheduleMoreDigests()
  bool scheduling_{false};
  std::unique_ptr<Timer> reschedule_timer_;

  // get the ClientDigests object for the given client ID, create one if no
  // existing object is found.
  ClientDigests* insertOrGet(ClientID cid);

  ClientDigests* getClient(ClientID cid);

  // activate an inactive digest
  void activateDigest(CachedDigest* digest);

  void scheduleMoreDigests();

  bool canStartDigest() const;

  // maximum number of streams processed for each scheduling batch
  static constexpr size_t MAX_STREAMS_STARTED_BATCH = 200;
};

}} // namespace facebook::logdevice
