/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/ClusterMarkerChecker.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace ClusterMarkerChecker {

/**
 * Check that the given shard has the cluster marker. If the cluster is present,
 * verify that it matches the expected value.
 * If the cluster marker is not present and needs to be written,
 * assigns it to *out_to_write.
 *
 * @return true if the marker was present with the expected value or if it was
 *              not present and we were able to successfully write it.
 */
static bool checkShard(LocalLogStore* store,
                       shard_index_t shard,
                       const std::string& prefix,
                       folly::Optional<ClusterMarkerMetadata>* out_to_write) {
  // If the store is in an error state, exempt it from the requirement
  if (store->acceptingWrites() == E::DISABLED) {
    return true;
  }

  std::string marker = prefix + std::to_string(shard);
  ClusterMarkerMetadata meta;
  int rv = store->readStoreMetadata(&meta);
  if (rv == 0) {
    if (meta.marker_ != marker) {
      ld_critical("Cluster marker mismatch for shard %i: expected \"%s\", "
                  "found \"%s\". Is the DB misplaced? "
                  "Use --ignore-cluster-marker to override this check.",
                  shard,
                  marker.c_str(),
                  meta.marker_.c_str());
      return false;
    }
  } else {
    if (err != E::NOTFOUND) {
      ld_critical("Failed to read cluster marker for shard %i: %s",
                  shard,
                  error_description(err));
      return false;
    }

    // The marker was not found. If it's a newly created DB, write it, otherwise
    // error out.
    rv = store->isEmpty();
    if (rv != 1) {
      if (rv == 0) {
        ld_critical("Cluster marker is missing for nonempty shard %i, "
                    "expected: \"%s\". Use --ignore-cluster-marker to override "
                    "this check.",
                    shard,
                    marker.c_str());
      }
      return false;
    }

    meta.marker_ = marker;
    *out_to_write = meta;
  }

  return true;
}

bool check(ShardedLocalLogStore& sharded_store,
           ServerConfig& config,
           const NodeID& my_node_id) {
  std::string prefix = config.getClusterName() + ":N" +
      std::to_string(my_node_id.index()) + ":shard";

  bool all_good = true;
  std::vector<std::pair<shard_index_t, ClusterMarkerMetadata>> to_write_shards;
  for (shard_index_t shard = 0; shard < sharded_store.numShards(); ++shard) {
    LocalLogStore* store = sharded_store.getByIndex(shard);
    folly::Optional<ClusterMarkerMetadata> to_write;
    all_good &= checkShard(store, shard, prefix, &to_write);
    if (to_write.hasValue())
      to_write_shards.emplace_back(shard, to_write.value());
  }

  if (!all_good) {
    return false;
  }

  // Only write the missing cluster markers if all shards passed the check.
  // Otherwise we may get into the following awkward situation.
  // Suppose there are two shards: 0 and 1. Shard 1 was wiped (e.g. disk was
  // replaced), and at the same time the two shards were swapped (because of an
  // operational error). If the loop above writes the marker, we would end
  // up with two shards having a marker saying "shard 0". If it doesn't, we'd
  // end up with shard 0 having no marker and shard 1 having marker
  // that says "shard 0" - from which it's obvious what happened.
  for (auto p : to_write_shards) {
    shard_index_t shard = p.first;
    const ClusterMarkerMetadata& meta = p.second;
    LocalLogStore* store = sharded_store.getByIndex(shard);
    LocalLogStore::WriteOptions options;
    if (store->writeStoreMetadata(meta, options) != 0 ||
        store->sync(Durability::ASYNC_WRITE) != 0) {
      ld_critical("Cannot write cluster marker for shard %u", shard);
      return false;
    }
  }

  return all_good;
}

}}} // namespace facebook::logdevice::ClusterMarkerChecker
