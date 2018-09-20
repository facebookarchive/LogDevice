/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <unordered_map>

#include "logdevice/common/DataClass.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * @file  Track the set of Nodes that have coordinated writes that are
 *        still outstanding (have yet to be persisted to stable storage).
 *        This dirty state is persisted during dirty<=>not-dirty transitions
 *        so that rebuilding can be triggered for data that may have been
 *        lost during an ungraceful shutdown.
 */

class NodeDirtyData {
 public:
  NodeDirtyData() = default;
  NodeDirtyData(const NodeDirtyData& src)
      : data_token_(src.data_token_.load(std::memory_order_relaxed)),
        wal_token_(src.wal_token_.load(std::memory_order_relaxed)) {}

  /**
   * @return  the largest FlushToken that must be retired before all data for
   *          the class is known to be committed to stable storage.
   */
  FlushToken dirtyUntil() const {
    return data_token_.load();
  }

  /**
   * @return  the largest WAL FlushToken that must be retired before this
   *          node dirty state is known to be persisted.
   */
  FlushToken syncingUntil() const {
    return wal_token_.load();
  }

  /**
   * @return true if no outstanding write data has been recorded for
   *         this node.
   */
  bool isClean() const {
    return data_token_.load() == FlushToken_INVALID;
  }

  /**
   * Mark the node as having outstanding buffered writes
   * at least until writes are retired up through the given
   * FlushToken.
   *
   * @note  If the node is already marked dirty through a higher FlushToken
   *        than what is provided, this operation has no effect. (i.e. this
   *        operation can never reduce the range of uncommitted data for the
   *        given data class).
   *
   * @return  "newly dirtied": True if this update transitions this node
   *          from the clean state (no uncommitted data) to the
   *          dirty state (outstanding data exists that must be flushed).
   *          Extending, or reconfirming some section of the dirty
   *          range will cause this method to return false.
   */
  bool markDirtyUntil(const FlushToken until) {
    return atomic_fetch_max(data_token_, until) == FlushToken_INVALID;
  }

  /**
   * Mark the node as cleaned up through the given FlushToken.
   *
   * @note  If the node is already marked clean, or the given FlushToken
   *        isn't large enough to retire all the dirty data in at least
   *        one data class, this operation has no effect.
   *
   * @return  "newly cleaned": True if this update transitions the node
   *          from the dirty state (outstanding data exists that
   *          must be flushed) to the clean state (no uncommitted data).
   *
   *          Shrinking without eliminating, or reconfirming some section
   *          of the existing dirty range (including any update to an already
   *          clean node) will be considered a false result.
   */
  bool markCleanUpThrough(const FlushToken flushed_up_through) {
    auto can_mark_clean = [](const auto& cur, const auto& value) {
      return cur != FlushToken_INVALID && cur <= value;
    };
    return can_mark_clean(atomic_conditional_store(data_token_,
                                                   FlushToken_INVALID,
                                                   flushed_up_through,
                                                   can_mark_clean),
                          flushed_up_through);
  }

  /**
   * Mark the node as having unpersisted dirty ranges
   * (PartitionDirtyMetadata) until the WAL has retired a
   * FlushToken >= "until".
   *
   * @note  If the data class is already marked syncing until a
   *        higher FlushToken than what is provided, this operation
   *        has no effect.
   *
   * @note  The dirty/clean methods above update in-memory state about
   *        client data records that will be lost if LogDevice crashes.
   *        This method tracks the progress of committing this dirty
   *        state, via a metadata write through the WAL, to stable storage.
   */
  void markSyncingUntil(const FlushToken until) {
    atomic_fetch_max(wal_token_, until);
  }

  std::string toString() const {
    using namespace std::string_literals;

    return "(D:"s + std::to_string(data_token_.load()) + ',' + "W:"s +
        std::to_string(wal_token_.load()) + ')';
  }

 private:
  std::atomic<FlushToken> data_token_{FlushToken_INVALID};
  std::atomic<FlushToken> wal_token_{FlushToken_INVALID};
};

using DirtiedByKey = std::pair<node_index_t, DataClass>;
using DirtiedByMap = std::unordered_map<DirtiedByKey, NodeDirtyData>;

}} // namespace facebook::logdevice
