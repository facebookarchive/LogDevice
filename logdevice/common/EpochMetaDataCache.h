/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <utility>

#include <boost/noncopyable.hpp>
#include <folly/SharedMutex.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLogReader.h"

namespace facebook { namespace logdevice {

/**
 *  EpochMetaDataCache caches epoch metadata results read from metadata logs.
 *  The key for finding an item in the cache is a pair of <logid_t, epoch_t>,
 *  and the value of stored consists of the EpochMetaData and the `until' epoch
 *  to which the metadata remains effective.
 *  Note that users of the cache must make sure the following guarantees:
 *      1) the epoch metadata put in the cache must be `authentic' for the
 *         requested epoch. Specifically, the epoch metadata must be read from
 *         the metadata log AFTER the requested epoch is released. See docblock
 *         in MetaDataLogReader.h for the explanation on correctness.
 *      2) the cache does not store the information regarding whether the
 *         epoch metadata is read from the _last_ record in the metadata log
 *         since metadata logs can be appended after an entry is cached. If such
 *         information is needed, users should not read from the cache but read
 *         directly from metadata logs instead.
 *
 *  The cache is meant to be shared among all worker threads and is proteceted
 *  by locks.
 *
 * TODO: write our own LRU cache implementation that supports:
 *       1) epoch interval ranged looked up
 *       2) finer grained locking
 *       3) reduce write frequency by rate limiting promotions
 */

class EpochMetaData;

class EpochMetaDataCache : boost::noncopyable {
 public:
  using RecordSource = MetaDataLogReader::RecordSource;

  // create the cache with maximum entry size of @param max_entries
  explicit EpochMetaDataCache(size_t max_entries);

  // Given logid and epoch, search the epoch metadata in the cache.
  // @return       true if there is a cache hit, and results (metadata and
  //               until epoch) will be written into @param metadata_out and
  //               @param until_out, respectively. The entry will also be
  //               promoted in the LRU list
  bool getMetaData(logid_t logid,
                   epoch_t epoch,
                   epoch_t* until_out,
                   EpochMetaData* metadata_out,
                   RecordSource* source_out,
                   bool require_consistent = true);

  // same as getMetaData() but do not promote the entry if found
  bool getMetaDataNoPromotion(logid_t logid,
                              epoch_t epoch,
                              epoch_t* until_out,
                              EpochMetaData* metadata_out,
                              RecordSource* source_out,
                              bool require_consistent = true) const;

  // put an entry with epoch metadata and until epoch into the cache
  void setMetaData(logid_t logid,
                   epoch_t epoch,
                   epoch_t until,
                   RecordSource source,
                   const EpochMetaData& metadata);

 private:
  using Key = std::pair<logid_t, epoch_t>;

  struct KeyHasher {
    size_t operator()(const EpochMetaDataCache::Key& key) const {
      return folly::hash::hash_combine(key.first.val_, key.second.val_);
    }
  };

  struct Value {
    epoch_t until;
    RecordSource source;
    EpochMetaData metadata;
  };

  // the internal LRU cache
  using LRUCache = folly::EvictingCacheMap<Key, Value, KeyHasher>;
  LRUCache cache_;

  // protect the access to lru_cache_
  folly::SharedMutex cache_mutex_;
};

}} // namespace facebook::logdevice
