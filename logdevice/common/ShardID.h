/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <folly/hash/Hash.h>
#include <folly/small_vector.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file a ShardID is used to identify a storage shard in a tier. It is a pair
 * of node_index_t which identifies a storage server and shard_index_t which is
 * the index of the shard on that storage node.
 */

struct ShardID {
  ShardID() : node_(-1), shard_(-1) {}
  ShardID(node_index_t node, shard_index_t shard)
      : node_(node), shard_(shard) {}

  bool isValid() const {
    return node_ > -1 && shard_ > -1;
  }

  node_index_t node() const {
    return node_;
  }
  shard_index_t shard() const {
    return shard_;
  }

  std::string toString() const {
    return "N" + std::to_string(node_) + ":S" + std::to_string(shard_);
  }

  bool operator<(const ShardID& rhs) const {
    return std::tie(node_, shard_) < std::tie(rhs.node_, rhs.shard_);
  }
  bool operator<=(const ShardID& rhs) const {
    return std::tie(node_, shard_) <= std::tie(rhs.node_, rhs.shard_);
  }
  bool operator==(const ShardID& rhs) const {
    return std::tie(node_, shard_) == std::tie(rhs.node_, rhs.shard_);
  }
  bool operator!=(const ShardID& rhs) const {
    return std::tie(node_, shard_) != std::tie(rhs.node_, rhs.shard_);
  }

  struct Hash {
    size_t operator()(const ShardID& s) const {
      return folly::hash::hash_combine(s.node_, s.shard_);
    }
  };

  // Returns a NodeID that generation==0. This NodeID can be used with
  // Sender to send a message to the node that owns this shard.
  // TODO(T15517759): Sender will be modified to provide an API that accepts
  // node_index_t instead of NodeID. This function can be removed once this is
  // implemented.
  NodeID asNodeID() const {
    return NodeID(node(), 0);
  }

 private:
  node_index_t node_;
  shard_index_t shard_;
};

// We serialize ShardID over the network. Expect its size to never change.
static_assert(sizeof(ShardID) == 4, "ShardID should be 4 bytes");

using StorageSet = std::vector<ShardID>;
using storage_set_size_t = size_t;
using ShardSet = std::unordered_set<ShardID, ShardID::Hash>;

constexpr size_t SHARDSET_INLINE_DEFAULT = 4;

/**
 * In-memory representation of a shard set. Uses a folly::small_vector with a
 * customizable number of inlined elements.
 *
 * @tparam inline_  Number of elements to store inline.
 */
template <size_t inline_ = SHARDSET_INLINE_DEFAULT>
using shardset_custsz_t = folly::small_vector<ShardID, inline_>;
/**
 * In-memory representation of a shard set with default number of elements
 * stored inline.
 */
typedef shardset_custsz_t<> small_shardset_t;

using FailedShardsMap =
    std::unordered_map<Status, std::vector<ShardID>, StatusHasher>;
}} // namespace facebook::logdevice

// Specialize std::hash<ShardID> to allow things like
// std::unordered_set<std::pair<ShardID, int>>
namespace std {
template <>
struct hash<facebook::logdevice::ShardID>
    : public facebook::logdevice::ShardID::Hash {};
} // namespace std
