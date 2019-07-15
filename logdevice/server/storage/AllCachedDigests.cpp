/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/AllCachedDigests.h"

#include <folly/Memory.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

ClientDigests::ClientDigests(ClientID cid, AllCachedDigests* parent)
    : client_id_(cid), parent_(parent) {
  ld_check(parent_ != nullptr);
}

bool ClientDigests::canPushRecords() const {
  return bytes_queued_ < parent_->getMaxQueueKBytesPerClient() * 1024;
}

void ClientDigests::onBytesEnqueued(size_t bytes) {
  bytes_queued_ += bytes;
}

void ClientDigests::onGapSent(const GAP_Message& /*msg*/) {}

void ClientDigests::onRecordSent(const RECORD_Message& msg) {
  auto msg_size = msg.size();
  ld_check(bytes_queued_ >= msg_size);
  bytes_queued_ -= msg_size;
}

void ClientDigests::onStartedSent(const STARTED_Message& /*msg*/) {}

std::pair<CachedDigest*, bool> ClientDigests::insert(
    logid_t log_id,
    shard_index_t shard,
    read_stream_id_t rid,
    lsn_t start_lsn,
    std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot) {
  auto result = digests_map_.insert(std::make_pair(rid, nullptr));
  if (!result.second) {
    // item already there
    return std::make_pair(result.first->second.get(), false);
  }

  // insertion happens, construct the actual digest object
  result.first->second = parent_->createCachedDigest(log_id,
                                                     shard,
                                                     rid,
                                                     client_id_,
                                                     start_lsn,
                                                     std::move(epoch_snapshot),
                                                     this);

  return std::make_pair(result.first->second.get(), true);
}

CachedDigest* ClientDigests::getDigest(read_stream_id_t rid) {
  auto it = digests_map_.find(rid);
  return it != digests_map_.end() ? it->second.get() : nullptr;
}

bool ClientDigests::eraseDigest(read_stream_id_t rid) {
  return digests_map_.erase(rid) > 0;
}

void ClientDigests::registerDisconnectCallback(
    std::unique_ptr<SocketCallback> callback) {
  disconnect_callback_ = std::move(callback);
}

//////   AllCachedDigests  ///////

AllCachedDigests::AllCachedDigests(size_t max_active_digests,
                                   size_t max_kbytes_queued_per_client)
    : max_active_cached_digests_(max_active_digests),
      max_kbytes_queued_per_client_(max_kbytes_queued_per_client) {
  ld_check(max_active_cached_digests_ > 0);
  ld_check(max_kbytes_queued_per_client_ > 0);
}

Status AllCachedDigests::startDigest(
    logid_t log_id,
    shard_index_t shard,
    read_stream_id_t rid,
    ClientID client_id,
    lsn_t start_lsn,
    std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot) {
  ClientDigests* client_digests = insertOrGet(client_id);
  ld_check(client_digests != nullptr);

  auto result = client_digests->insert(
      log_id, shard, rid, start_lsn, std::move(epoch_snapshot));

  if (result.second) {
    CachedDigest* digest = result.first;
    ld_check(digest != nullptr);
    ld_check(!digest->started());
    queue_.push(std::make_pair(client_id, rid));
    scheduleMoreDigests();
    WORKER_STAT_INCR(record_cache_digest_created);
    return E::OK;
  }
  // Even retries for the same digest should get a new read stream id,
  // so this should never happen.
  ld_error("AllServerReadStreams::startDigest(): Duplicate digest request "
           "received. Ignoring.");
  return E::INVALID_PARAM;
}

void AllCachedDigests::onDigestDestroyed(bool active) {
  if (active) {
    ld_check(num_active_digests_ > 0);
    --num_active_digests_;
    WORKER_STAT_DECR(record_cache_digest_active);
  }
  WORKER_STAT_INCR(record_cache_digest_completed);
}

ClientDigests* AllCachedDigests::getClient(ClientID client_id) {
  auto it = clients_.find(client_id);
  return it != clients_.end() ? &it->second : nullptr;
}

CachedDigest* AllCachedDigests::getDigest(ClientID client_id,
                                          read_stream_id_t rid) {
  ClientDigests* client_digests = getClient(client_id);
  if (client_digests != nullptr) {
    return client_digests->getDigest(rid);
  }
  return nullptr;
}

ClientDigests* AllCachedDigests::insertOrGet(ClientID client_id) {
  auto result = clients_.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(client_id),
                                 std::forward_as_tuple(client_id, this));

  ClientDigests* client_digests = &result.first->second;
  ld_check(client_digests != nullptr);

  if (result.second) {
    // newly inserted client, register socket callback for it
    auto disconnect_callback =
        std::make_unique<ClientDisconnectedCallback>(this);

    Worker* worker = Worker::onThisThread(false);
    if (worker != nullptr) {
      int rv = worker->sender().registerOnSocketClosed(
          Address(client_id), *disconnect_callback);
      ld_check(rv == 0);
    }
    client_digests->registerDisconnectCallback(std::move(disconnect_callback));
  }

  return client_digests;
}

bool AllCachedDigests::eraseDigest(ClientID client_id, read_stream_id_t rid) {
  ClientDigests* client_digests = getClient(client_id);
  bool erased = false;
  if (client_digests != nullptr) {
    erased = client_digests->eraseDigest(rid);
    scheduleMoreDigests();
  }
  return erased;
}

void AllCachedDigests::eraseClient(ClientID client_id) {
  clients_.erase(client_id);
  scheduleMoreDigests();
}

void AllCachedDigests::clear() {
  while (!queue_.empty()) {
    queue_.pop();
  }
  clients_.clear();
  num_active_digests_ = 0;
}

bool AllCachedDigests::canStartDigest() const {
  return num_active_digests_ < max_active_cached_digests_;
}

void AllCachedDigests::onGapSent(ClientID client_id, const GAP_Message& msg) {
  ClientDigests* client_digests = getClient(client_id);
  if (client_digests != nullptr) {
    client_digests->onGapSent(msg);
  }
}

void AllCachedDigests::onRecordSent(ClientID client_id,
                                    const RECORD_Message& msg) {
  ClientDigests* client_digests = getClient(client_id);
  if (client_digests != nullptr) {
    client_digests->onRecordSent(msg);
  }
}

void AllCachedDigests::onStartedSent(ClientID client_id,
                                     const STARTED_Message& msg) {
  ClientDigests* client_digests = getClient(client_id);
  if (client_digests != nullptr) {
    client_digests->onStartedSent(msg);
  }
}

void AllCachedDigests::scheduleMoreDigests() {
  if (scheduling_) {
    // Called recursively due to a synchronous completion of
    // a digest started in the loop below. No-op
    return;
  }

  scheduling_ = true;
  size_t num_processed = 0;
  while (!queue_.empty() && canStartDigest() &&
         num_processed < getMaxStreamsStartedBatch()) {
    auto pair = queue_.front();
    queue_.pop();

    CachedDigest* digest = getDigest(pair.first, pair.second);
    if (digest == nullptr) {
      // the digest of the enqueued id has already been destroyed
      continue;
    }
    num_processed++;
    activateDigest(digest);
  }

  scheduling_ = false;

  if (!queue_.empty() && canStartDigest()) {
    // we have reached the maximum number of streams started for this batch,
    // schedule the next batch in the next event loop iteration
    if (!reschedule_timer_) {
      reschedule_timer_ =
          createRescheduleTimer([this] { scheduleMoreDigests(); });
    }
    activateRescheduleTimer();
  } else {
    if (reschedule_timer_) {
      reschedule_timer_->cancel();
    }
  }
}

void AllCachedDigests::activateDigest(CachedDigest* digest) {
  ld_check(digest != nullptr);
  ld_check(!digest->started());

  // num_active_digests_ is increased before starting the digest
  // since digest may finish synchronously
  ++num_active_digests_;
  WORKER_STAT_INCR(record_cache_digest_active);
  digest->start();
}

void AllCachedDigests::ClientDisconnectedCallback::
operator()(Status /*st*/, const Address& name) {
  ld_check(name.isClientAddress());
  ld_check(owner != nullptr);
  owner->eraseClient(name.id_.client_);
}

std::unique_ptr<CachedDigest> AllCachedDigests::createCachedDigest(
    logid_t log_id,
    shard_index_t shard,
    read_stream_id_t rid,
    ClientID client_id,
    lsn_t start_lsn,
    std::unique_ptr<const EpochRecordCache::Snapshot> epoch_snapshot,
    ClientDigests* client_digests) {
  return std::make_unique<CachedDigest>(log_id,
                                        shard,
                                        rid,
                                        client_id,
                                        start_lsn,
                                        std::move(epoch_snapshot),
                                        client_digests,
                                        this);
}

std::unique_ptr<Timer>
AllCachedDigests::createRescheduleTimer(std::function<void()> callback) {
  return std::make_unique<Timer>(callback);
}

void AllCachedDigests::activateRescheduleTimer() {
  ld_check(reschedule_timer_ != nullptr);
  reschedule_timer_->activate(std::chrono::microseconds(0));
}

}} // namespace facebook::logdevice
