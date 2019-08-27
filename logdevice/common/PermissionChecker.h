/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <chrono>

#include "folly/Function.h"
#include "folly/Optional.h"
#include "folly/SharedMutex.h"
#include "folly/container/EvictingCacheMap.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 *  Result of permission check. If its permissions are still loading,
 *  and result not yet know, NOTREADY is returned.
 */
enum class PermissionCheckStatus {
  NONE,
  ALLOWED,
  DENIED,
  NOTREADY,
  SYSLIMIT,
  NOTFOUND
};

using callback_func_t = folly::Function<void(PermissionCheckStatus)>;

/**
 * @file an abstract interface used to determine if an action is allowed
 *       to be performed on a log_id/log_group by a client.
 */

class AclCache {
 public:
  struct Value {
    PermissionCheckStatus status_;
    std::chrono::steady_clock::time_point time_;
  };

  uint64_t getCacheKey(const uint64_t& identities_hash,
                       const std::string& action,
                       const logsconfig::LogAttributes::ACLList& acl_list);

  folly::Optional<PermissionCheckStatus> lookup(const uint64_t& key);

  void insert(const uint64_t& key, PermissionCheckStatus val);

  std::chrono::seconds ttl() {
    return ttl_sec_;
  }

  int size() {
    return size_;
  }

  AclCache(const int size, const std::chrono::seconds ttl)
      : cache_(size), ttl_sec_(ttl), size_(size) {}
  ~AclCache() {}

 private:
  folly::EvictingCacheMap<uint64_t, Value> cache_;
  folly::SharedMutex mutex_;
  std::chrono::seconds ttl_sec_;
  int size_;
};

class PermissionChecker {
 public:
  virtual ~PermissionChecker(){};
  explicit PermissionChecker(const configuration::SecurityConfig& security_cfg);

  /**
   * Queries the permission store to determine if the provided Principal can
   * perform the specified ACTION on the logid. Result is supplied to callback
   * function.
   * Must be called from a worker thread. The callback is called either
   * synchronously inside the isAllowed() call or later on the same worker
   * thread.
   *
   * @param action      The action to be performed by the principal
   * @param principal   The principal containing the identity of the client
   * @param logid       The resource that the client is trying to read or modify
   * @param cb          Result of the check supplied via callback function
   *
   */
  virtual void isAllowed(ACTION action,
                         const PrincipalIdentity& principal,
                         logid_t logid,
                         callback_func_t cb) const = 0;
  /**
   * Returns the PermissionCheckerType that the PermissionChecker is intended to
   * work with.
   */
  virtual PermissionCheckerType getPermissionCheckerType() const = 0;

  /**
   * Translate PermissionChecker status code to logdevice generic error
   * code
   */
  static Status toStatus(const PermissionCheckStatus& status) {
    switch (status) {
      case PermissionCheckStatus::DENIED:
        return E::ACCESS;
      case PermissionCheckStatus::NOTFOUND:
        return E::NOTFOUND;
      case PermissionCheckStatus::NOTREADY:
        return E::AGAIN;
      case PermissionCheckStatus::SYSLIMIT:
        return E::SYSLIMIT;
      case PermissionCheckStatus::NONE:
      case PermissionCheckStatus::ALLOWED:
        return E::OK;
    }
    return E::INVALID_PARAM;
  }

  /**
   * Computes the combined hash value of {identities_hash, action, acl_list},
   * which is used as a key to lookup stored results of past permission checker
   * queries from the ACL cache.
   */
  uint64_t
  getCacheKey(const uint64_t& identities_hash,
              const std::string& action,
              const logsconfig::LogAttributes::ACLList& acl_list) const;
  /**
   * Inserts the result of a permission check into the ACL cache.
   *
   * @param key   The hashed representation of the permissin check query
   * @param val   The result of a permission check query
   */
  void insertCache(const uint64_t& key, PermissionCheckStatus val) const;
  /**
   * Search the ACL cache for cached results.
   *
   * @param key   The hashed representation of the permissin check query
   * @return val  The result of a permission check query
   */
  folly::Optional<PermissionCheckStatus> lookupCache(const uint64_t& key) const;

  bool cacheEnabled() const {
    return acl_cache_enabled_;
  }

  /**
   * Get TTL for cache entries.
   */
  std::chrono::seconds cacheTtl() {
    return acl_cache_->ttl();
  }

  /**
   * Get capacity of the cache
   */
  int cacheSize() {
    return acl_cache_->size();
  }

 private:
  mutable std::unique_ptr<AclCache> acl_cache_{nullptr};
  bool acl_cache_enabled_;
};

}} // namespace facebook::logdevice
