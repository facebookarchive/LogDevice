/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <limits>

#include <folly/Memory.h>
#include <folly/SharedMutex.h>
#include <folly/hash/Hash.h>
#include <google/dense_hash_map>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/WorkerType.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file ClientIdxAllocator allocates and reuses client idx for ClientID
 * handled by a Workers. We use single shared instance of ClientIdxAllocator.
 * The algorithm goes through the space of indexes, allowing wraparound but
 * skipping any indexes still in use.  This approach only reuses any index
 * after wrapping around in the 31-bit space, which should be slow and rare.
 */

class ClientIdxAllocator {
 public:
  ClientIdxAllocator() {
    idx_in_use_.set_empty_key(-1);
    idx_in_use_.set_deleted_key(-2);
    setMaxClientIdx(std::numeric_limits<int32_t>::max());
    next_idx_ = 1;
  }

  ClientID issueClientIdx(WorkerType worker_type, worker_id_t worker_idx);
  void releaseClientIdx(ClientID client_idx);

  /**
   * Returns type and ID of the worker which handles specified client_idx or
   * if there are no such worker, returns (MAX, -1).
   * Later could happen if client_idx was released meantime.
   * Example scenario:
   * - StoreStateMachine is running ...
   * - Worker closes the socket for whatever reason (client disconnects).
   * - StoreStateMachine is done, tries to resolve the ClientID to send
   *    the reply.
   */
  std::pair<WorkerType, worker_id_t> getWorkerId(ClientID client_idx) {
    folly::SharedMutex::ReadHolder read_lock(mutex_);

    auto it = idx_in_use_.find(client_idx.getIdx());
    if (it == idx_in_use_.end()) {
      // client_idx may not be valid if connection was just closed
      ld_debug("cannot find C%d among in-use ClientIDs "
               "(idx_in_use_.size() = %zu)",
               client_idx.getIdx(),
               idx_in_use_.size());
      return std::make_pair(WorkerType::MAX, worker_id_t(-1));
    }

    return it->second;
  }

  // This allows unit tests to test wraparound without going through the full
  // 31-bit space as in production
  void setMaxClientIdx(int32_t max_idx) {
    max_idx_ = max_idx;
  }

  ClientIdxAllocator(const ClientIdxAllocator& that) = delete;
  ClientIdxAllocator& operator=(const ClientIdxAllocator&) = delete;

 private:
  // Method is NOT internally synchronized
  // mutex_ must be held when calling
  // Advanced to next client idx.
  void advanceNextIdx(int* wraparound_count);
  // Largest index to issue from this instance (index maps to `current_worker_`)
  int32_t max_idx_;
  // Next index to try to issue
  int32_t next_idx_;
  // Map of client indexes currently in use.  issueClientIdx() will skip these.
  google::
      dense_hash_map<int32_t, std::pair<WorkerType, worker_id_t>, folly::Hash>
          idx_in_use_;
  folly::SharedMutex mutex_;
};

}} // namespace facebook::logdevice
