/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/PermissionChecker.h"

namespace facebook { namespace logdevice {

folly::Optional<PermissionCheckStatus> AclCache::lookup(const uint64_t& key) {
  auto now = std::chrono::steady_clock::now();
  folly::Optional<PermissionCheckStatus> res = folly::none;
  folly::SharedMutex::ReadHolder read_guard(mutex_);

  // AclCache is expected to have partial occupancy most of the time. This
  // allows us to optimize lookups by not promoting the looked up entry, and
  // as a result save a delete and insert operation. This, in turn allows us
  // to use a read lock which improves concurrency. Promotions are handled
  // during inserts.
  const auto ret = cache_.findWithoutPromotion(key);
  if (ret != cache_.end() && ((now - ret->second.time_) < ttl_sec_)) {
    res = ret->second.status_;
  }
  return res;
}

void AclCache::insert(const uint64_t& key, PermissionCheckStatus val) {
  auto now = std::chrono::steady_clock::now();
  folly::SharedMutex::WriteHolder write_guard(mutex_);
  size_t size = cache_.size();
  bool promote = (size < cache_.getMaxSize()) ? false : true;
  // promote flag indicates whether or not to move the entry to the front of
  // the LRU. This only really matters if the entry already exists in the
  // cache. In case it does, we want to make sure that it is promoted
  // whenever the cache is full, so that it does not get evicted in the
  // immediate future.
  cache_.set(key, {val, now}, promote);
}

uint64_t
AclCache::getCacheKey(const uint64_t& identities_hash,
                      const std::string& action,
                      const logsconfig::LogAttributes::ACLList& acl_list) {
  uint64_t cacheKey = identities_hash;
  cacheKey = folly::hash::hash_combine_generic(folly::Hash{}, cacheKey, action);

  for (const auto& acl : acl_list) {
    cacheKey = folly::hash::hash_combine_generic(folly::Hash{}, cacheKey, acl);
  }
  return cacheKey;
}

void PermissionChecker::insertCache(const uint64_t& key,
                                    PermissionCheckStatus val) const {
  acl_cache_->insert(key, val);
}

uint64_t PermissionChecker::getCacheKey(
    const uint64_t& identities_hash,
    const std::string& action,
    const logsconfig::LogAttributes::ACLList& acl_list) const {
  return acl_cache_->getCacheKey(identities_hash, action, acl_list);
}

folly::Optional<PermissionCheckStatus>
PermissionChecker::lookupCache(const uint64_t& key) const {
  return acl_cache_->lookup(key);
}

}} // namespace facebook::logdevice
