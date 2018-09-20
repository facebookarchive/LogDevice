/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

/**
 * @file  IteratorCache provides a way to get a read iterator for a particular
 *        log. The iterator will be created lazily the first time it's
 *        requested; future calls to getIterator() will return the cached value.
 */

class IteratorCache {
 public:
  explicit IteratorCache(LocalLogStore* store,
                         logid_t log_id,
                         bool created_by_rebuilding)
      : store_(store),
        log_id_(log_id),
        created_by_rebuilding_(created_by_rebuilding) {}

  /**
   * Creates a new one or returns an existing (cached) read iterator
   * corresponding to the specified ReadOptions.
   *
   * @note This method, as well as those on the returned iterator, are not
   *       thread-safe. In practice, all calls to createOrGet() come from a
   *       single worker thread.
   * @note If allow_blocking_io=true, creating an iterator may be an expensive
   *       operation. Therefore, CatchupQueue will only call it if it's cached
   *       (valid() returns true). Otherwise ReadStorageTask will create a new
   *       iterator (on a storage thread) and call set() later.
   */
  std::shared_ptr<LocalLogStore::ReadIterator>
  createOrGet(const LocalLogStore::ReadOptions&);

  /**
   * @return  true if a valid iterator exists for the specified ReadOptions
   */
  bool valid(const LocalLogStore::ReadOptions&);

  /**
   * Caches the given iterator, possibly overriding an existing entry.
   */
  void set(const LocalLogStore::ReadOptions&,
           std::shared_ptr<LocalLogStore::ReadIterator>);

  /**
   * Release iterators which haven't been used recently.
   *
   * @param now  current time
   * @param ttl  time-to-live for unused cached iterators
   */
  void invalidateIfUnused(std::chrono::steady_clock::time_point now,
                          std::chrono::milliseconds ttl);

  /**
   * Get direct access to underlying store.
   */
  LocalLogStore* getStore() {
    return store_;
  }

 private:
  struct IterWrapper {
    IterWrapper()
        : iterator(nullptr), last_used(std::chrono::steady_clock::now()) {}

    // This is a shared pointer since the owning ServerReadStream might go away
    // (e.g. client closes the connection), while some storage task still has a
    // reference to the iterator.
    std::shared_ptr<LocalLogStore::ReadIterator> iterator;

    std::chrono::steady_clock::time_point last_used;
  };

  IterWrapper& getWrapper(const LocalLogStore::ReadOptions& options) {
    // Iterator must not snapshot for caching to work correctly
    ld_check(options.tailing);

    return options.allow_blocking_io ? blocking_ : nonblocking_;
  }

  LocalLogStore* store_;
  logid_t log_id_;

  IterWrapper blocking_;
  IterWrapper nonblocking_;

 public:
  // Context for tracking iterators
  bool created_by_rebuilding_;
};

}} // namespace facebook::logdevice
