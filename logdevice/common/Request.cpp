/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Request.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

std::atomic<uint32_t> Request::next_req_batch_id(1);

Request::~Request() {
  unblockClient();
}

void Request::unblockClient() {
  if (client_blocked_sem_) {
    client_blocked_sem_->post();
    client_blocked_sem_ = nullptr;
  }
}

void Request::bumpStatsWhenPosted(StatsHolder* stats,
                                  RequestType type,
                                  WorkerType /*worker_type*/,
                                  worker_id_t /*worker_idx*/,
                                  bool success) {
  if (!stats) {
    return;
  }

  // worker_type is available so that in the future we might want to group the
  // stats per worker-type.
  STAT_INCR(stats, post_request_total);
  ld_check(static_cast<int>(type) < static_cast<int>(RequestType::MAX));
  REQUEST_TYPE_STAT_INCR(stats, type, post_request);

  if (!success) {
    STAT_INCR(stats, post_request_failed);
  }
}

std::string Request::describe() const {
  return requestTypeNames[type_];
}

request_id_t Request::getNextRequestID() {
  static_assert(sizeof(request_id_t::raw_type) == 8,
                "request_id_t expected to be 64-bit");
  static __thread uint32_t batch_id;
  static __thread uint32_t seq_number_within_batch{
      std::numeric_limits<uint32_t>::max()};
  if (UNLIKELY(++seq_number_within_batch == 0)) {
    // looped around the epoch or this is the first request being posted in
    // this thread
    batch_id = next_req_batch_id++;
    ld_debug("Acquired batch %u for request numbering", batch_id);
  }
  return request_id_t((uint64_t(batch_id) << 32) | seq_number_within_batch);
}

}} // namespace facebook::logdevice
