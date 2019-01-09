/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/IteratorCache.h"

#include <chrono>

#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/locallogstore/IteratorTracker.h"

namespace facebook { namespace logdevice {

std::shared_ptr<LocalLogStore::ReadIterator>
IteratorCache::createOrGet(const LocalLogStore::ReadOptions& options) {
  auto& wrapper = getWrapper(options);
  if (!wrapper.iterator) {
    wrapper.iterator = store_->read(log_id_, options);
  } else {
    wrapper.iterator->setContextString(options.tracking_ctx.more_context);
  }
  wrapper.last_used = std::chrono::steady_clock::now();

  return wrapper.iterator;
}

bool IteratorCache::valid(const LocalLogStore::ReadOptions& options) {
  return getWrapper(options).iterator != nullptr;
}

void IteratorCache::set(const LocalLogStore::ReadOptions& options,
                        std::shared_ptr<LocalLogStore::ReadIterator> iter) {
  auto& wrapper = getWrapper(options);
  wrapper.iterator = iter;
  wrapper.last_used = std::chrono::steady_clock::now();
}

void IteratorCache::invalidateIfUnused(
    std::chrono::steady_clock::time_point now,
    std::chrono::milliseconds ttl) {
  for (auto wrapper : {&blocking_, &nonblocking_}) {
    if (wrapper->iterator && now - wrapper->last_used > ttl) {
      wrapper->iterator.reset();
      WORKER_STAT_INCR(iterator_invalidations);
    }
  }
}

}} // namespace facebook::logdevice
