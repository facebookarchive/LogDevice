/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientIdxAllocator.h"

#include <google/dense_hash_map>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

ClientID ClientIdxAllocator::issueClientIdx(WorkerType worker_type,
                                            worker_id_t worker_idx) {
  int client_idx;
  int wraparound_count = 0;
  folly::SharedMutex::WriteHolder write_lock(mutex_);

  // This loops but should be O(1) amortised.  If we take long to issue one
  // client index it means previous allocations were blazing fast. :)

  do {
    client_idx = next_idx_;
    advanceNextIdx(&wraparound_count);
    if (wraparound_count > 1) {
      ld_critical("Wrapped around twice while looking for available client "
                  "index.  This "
                  "should not happen in practice with a 31-bit ID space.");
      std::abort();
    }
    // Loop until we manage to insert into the set (index not already in use)
  } while (idx_in_use_.find(client_idx) != idx_in_use_.end());
  idx_in_use_[client_idx] = std::make_pair(worker_type, worker_idx);
  return ClientID(client_idx);
}

void ClientIdxAllocator::releaseClientIdx(ClientID client_idx) {
  folly::SharedMutex::WriteHolder write_lock(mutex_);
  auto it = idx_in_use_.find(client_idx.getIdx());
  ld_check(it != idx_in_use_.end());
  idx_in_use_.erase(it);
}

void ClientIdxAllocator::advanceNextIdx(int* wraparound_count) {
  if (next_idx_ == max_idx_) {
    next_idx_ = 1;
    ++*wraparound_count;
  } else {
    ++next_idx_;
    ld_check(next_idx_ > 0);
    ld_check(next_idx_ <= max_idx_);
  }
}

}} // namespace facebook::logdevice
