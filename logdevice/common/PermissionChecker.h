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
#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

static constexpr int CACHE_MAX_SIZE(10000);
static constexpr std::chrono::seconds CACHE_TTL_SEC(120);
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

  folly::Optional<PermissionCheckStatus> lookup(const uint64_t& key);

  void insert(const uint64_t& key, PermissionCheckStatus val);

  uint64_t getCacheKey(const uint64_t& identities_hash,
                       const std::string& action,
                       const logsconfig::LogAttributes::ACLList& acl_list);

  AclCache(const int size, const std::chrono::seconds ttl)
      : cache_(size), ttl_sec_(ttl) {}
  ~AclCache() {}

 private:
  folly::EvictingCacheMap<uint64_t, Value> cache_;
  folly::SharedMutex mutex_;
  std::chrono::seconds ttl_sec_;
};

class PermissionChecker {
 public:
  virtual ~PermissionChecker(){};
  PermissionChecker() {
    acl_cache_ = std::make_unique<AclCache>(CACHE_MAX_SIZE, CACHE_TTL_SEC);
  }

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

  void insertCache(const uint64_t& key, PermissionCheckStatus val) const;

  folly::Optional<PermissionCheckStatus> lookupCache(const uint64_t& key) const;

  uint64_t
  getCacheKey(const uint64_t& identities_hash,
              const std::string& action,
              const logsconfig::LogAttributes::ACLList& acl_list) const;

 private:
  std::unique_ptr<AclCache> acl_cache_{nullptr};
};

}} // namespace facebook::logdevice
