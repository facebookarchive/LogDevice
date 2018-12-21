/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ShardAuthoritativeStatusMap.h"

#include <folly/String.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

AuthoritativeStatus
ShardAuthoritativeStatusMap::getShardStatus(node_index_t node,
                                            uint32_t shard) const {
  if (!shards_.count(node) || !shards_.find(node)->second.count(shard)) {
    return AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }
  return shards_.find(node)->second.find(shard)->second.auth_status;
}

AuthoritativeStatus
ShardAuthoritativeStatusMap::getShardStatus(const ShardID& shard_id) const {
  return getShardStatus(shard_id.node(), shard_id.shard());
}

bool ShardAuthoritativeStatusMap::shardIsTimeRangeRebuilding(
    node_index_t node,
    uint32_t shard) const {
  auto node_kv = shards_.find(node);
  if (node_kv != shards_.end()) {
    auto shard_kv = node_kv->second.find(shard);
    if (shard_kv != node_kv->second.end()) {
      return shard_kv->second.time_ranged_rebuild;
    }
  }
  return false;
}

void ShardAuthoritativeStatusMap::setShardStatus(node_index_t node,
                                                 uint32_t shard,
                                                 AuthoritativeStatus status,
                                                 bool time_ranged_rebuild) {
  if (status == AuthoritativeStatus::FULLY_AUTHORITATIVE &&
      !time_ranged_rebuild) {
    shards_[node].erase(shard);
    if (shards_[node].empty()) {
      shards_.erase(node);
    }
  } else {
    shards_[node][shard] = {status, time_ranged_rebuild};
  }
}

void ShardAuthoritativeStatusMap::serialize(ProtocolWriter& writer) const {
  std::vector<SerializedEntry> to_send;
  for (const auto& node : shards_) {
    for (const auto& shard : node.second) {
      to_send.push_back({node.first, shard.first, shard.second.auth_status});
    }
  }
  writer.writeLengthPrefixedVector(to_send);
}

void ShardAuthoritativeStatusMap::deserialize(ProtocolReader& reader) {
  std::vector<SerializedEntry> received;
  reader.readLengthPrefixedVector(&received);
  if (reader.ok()) {
    for (const auto& e : received) {
      setShardStatus(e.node, e.shard, e.status);
    }
  }
}

std::string ShardAuthoritativeStatusMap::describe() const {
  std::vector<std::string> res;
  for (const auto& node : shards_) {
    for (const auto& shard : node.second) {
      std::string tmp =
          "N" + std::to_string(node.first) + ":S" + std::to_string(shard.first);
      switch (shard.second.auth_status) {
        case AuthoritativeStatus::FULLY_AUTHORITATIVE:
          tmp += ":FA";
          ld_check(shard.second.time_ranged_rebuild);
          if (shard.second.time_ranged_rebuild) {
            tmp += "(TRR)";
          }
          break;
        case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
          tmp += ":AE";
          break;
        case AuthoritativeStatus::UNAVAILABLE:
          tmp += ":UA";
          break;
        case AuthoritativeStatus::UNDERREPLICATION:
          tmp += ":UR";
          break;
        case AuthoritativeStatus::Count:
          ld_check(false);
          tmp += ":INVALID";
          break;
      }
      res.push_back(tmp);
    }
  }

  return folly::join(",", res);
}

bool ShardAuthoritativeStatusMap::
operator==(const ShardAuthoritativeStatusMap& other) const {
  return version_ == other.version_ && shards_ == other.shards_;
}

bool ShardAuthoritativeStatusMap::
operator!=(const ShardAuthoritativeStatusMap& other) const {
  return !(*this == other);
}

Request::Execution UpdateShardAuthoritativeMapRequest::execute() {
  ShardAuthoritativeStatusMap& map = Worker::onThisThread()
                                         ->shardStatusManager()
                                         .getShardAuthoritativeStatusMap();
  if (map.getVersion() >= map_.getVersion()) {
    // The worker already has a more up to date version.
    return Execution::COMPLETE;
  }
  map = std::move(map_);

  Worker::onThisThread()->shardStatusManager().notifySubscribers();

  return Execution::COMPLETE;
}

void UpdateShardAuthoritativeMapRequest::broadcastToAllWorkers(
    const ShardAuthoritativeStatusMap& map) {
  const ShardAuthoritativeStatusMap& current_map =
      Worker::onThisThread()
          ->shardStatusManager()
          .getShardAuthoritativeStatusMap();
  // Check if this worker already has either a more up to date version or
  // exactly the same map. Note that equal version doesn't necessarily mean
  // equal map because of authoritative_status_overrides setting.
  // Also note that the O(n)-time current_map == map comparison shouldn't slow
  // things down noticeably because it's amortized by other O(n)-time things
  // that the caller of this method does. In particular, it takes O(n) time to
  // construct (or copy, or deserialize) `map` in the first place.
  if (current_map.getVersion() > map.getVersion() || (current_map == map)) {
    // This worker already has a more up to date version. Do not broadcast as
    // it is likely that all workers were already updated by another
    // UpdateShardAuthoritativeMapRequest.
    return;
  }

  ld_info("Posting shard status update to workers: %s, version=%s",
          map.describe().c_str(),
          lsn_to_string(map.getVersion()).c_str());

  Processor* processor = Worker::onThisThread()->processor_;
  for (int t = 0; t < numOfWorkerTypes(); t++) {
    WorkerType worker_type = workerTypeByIndex(t);
    for (int i = 0; i < processor->getWorkerCount(worker_type); ++i) {
      std::unique_ptr<Request> rq =
          std::make_unique<UpdateShardAuthoritativeMapRequest>(
              map, i, worker_type);
      int rv = processor->postWithRetrying(rq);
      ld_check(rv == 0 || err == E::SHUTDOWN);
    }
  }
}

void ShardAuthoritativeStatusSubscriber::
    registerForShardAuthoritativeStatusUpdates() {
  auto worker = Worker::onThisThread(false);
  // Worker may be not available in tests.
  if (!worker) {
    return;
  }
  worker->shardStatusManager().subscribe(*this);
}

void ShardAuthoritativeStatusManager::notifySubscribers() {
  Worker* w = Worker::onThisThread();
  for (auto it = w->shardStatusManager().subscribers_.begin();
       it != w->shardStatusManager().subscribers_.end();) {
    // onShardStatusUpdate() may remove subscriber from list
    auto subscriber = it;
    it++;
    subscriber->onShardStatusChanged();
  }
}

}} // namespace facebook::logdevice
