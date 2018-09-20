/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice {

/**
 * For more than one reason (documented below), SCD may not want to use the
 * copyset exactly as stored in the record to decide who is the primary node
 * etc, but use the copyset in a different order.  This needs to be done
 * consistently by all servers.
 */
enum class SCDCopysetReordering : uint8_t {
  // Just use the copyset verbatim
  NONE = 0,
  // Hash the copyset and shuffle it based on the hash.  We do this to
  // counteract the bias introduced by (as of late 2016)
  // CrossDomainCopySetSelector on the write path into copysets.  Because it
  // prefers placing the first copy into the same domain as the sequencer, and
  // with SCD picking the first node in a copyset to serve the read, in
  // deployments with unbalanced workloads the nodes in the sequencer's domain
  // can serve a disproportionate amount of read traffic.  By virtually
  // shuffling copysets on the read path, we let non-primary nodes also
  // contribute to reading.
  HASH_SHUFFLE = 1,
  // Similar functionality to HASH_SHUFFLE.
  // This version also seeds the copyset hashing function using the session id
  // of the client initiating the read operation.
  // Reason: Copysets do not change for blocks of records, meaning we may have
  // several megabytes of records that have the same copyset.
  // HASH_SHUFFLE makes it so that the same block will be sent to all clients
  // by the same storage node.
  // Seeding the shuffling with a value provided by the client is likely to
  // reduce potential hotspots.
  //
  // Choosing between HASH_SHUFFLE and HASH_SHUFFLE_CLIENT_SEED
  // involves a tradeoff:
  // - When using HASH_SHUFFLE, only one storage node has to read the block
  // from disk, and then it can serve it to multiple readers from the cache.
  // This setting is desirable when the overall workload is disk-bound.
  // - If the workload is not bottlenecked by disk, HASH_SHUFFLE_CLIENT_SEED
  // enables multiple storage nodes to participate in reading the log.
  HASH_SHUFFLE_CLIENT_SEED = 2,
  MAX
};

}} // namespace facebook::logdevice
