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
 * verify that it matches the expected value. If the cluster marker is not
 * present, write it.
 *
 * @return true if the marker was present with the expected value or if it was
 *              not present and we were able to successfully write it.
 */
static bool checkShard(LocalLogStore* store,
                       shard_index_t shard,
                       const std::string& prefix) {
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

    LocalLogStore::WriteOptions options;
    if (store->writeStoreMetadata(meta, options) != 0 ||
        store->sync(Durability::ASYNC_WRITE) != 0) {
      ld_critical("Cannot write cluster marker for shard %u", shard);
      return false;
    }
  }

  return true;
}

bool check(ShardedLocalLogStore& sharded_store, ServerConfig& config) {
  std::string prefix = config.getClusterName() + ":N" +
      std::to_string(config.getMyNodeID().index()) + ":shard";

  bool all_good = true;
  for (shard_index_t shard = 0; shard < sharded_store.numShards(); ++shard) {
    LocalLogStore* store = sharded_store.getByIndex(shard);
    all_good &= checkShard(store, shard, prefix);
  }

  return all_good;
}

}}} // namespace facebook::logdevice::ClusterMarkerChecker
