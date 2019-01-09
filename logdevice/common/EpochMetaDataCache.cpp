/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochMetaDataCache.h"

namespace facebook { namespace logdevice {

EpochMetaDataCache::EpochMetaDataCache(size_t max_entries)
    : cache_(max_entries) {
  ld_check(max_entries > 0);
}

bool EpochMetaDataCache::getMetaData(logid_t logid,
                                     epoch_t epoch,
                                     epoch_t* until_out,
                                     EpochMetaData* metadata_out,
                                     RecordSource* source_out,
                                     bool require_consistent) {
  ld_check(until_out != nullptr);
  ld_check(metadata_out != nullptr);
  ld_check(source_out != nullptr);
  folly::SharedMutex::WriteHolder write_guard(cache_mutex_);
  auto it = cache_.find(std::make_pair(logid, epoch));
  if (it == cache_.end()) {
    return false;
  }

  // record in the cache must have a cached source
  ld_check(MetaDataLogReader::isCachedSource(it->second.source));
  if (require_consistent && it->second.source == RecordSource::CACHED_SOFT) {
    // require consistent data but only has soft one in cache, consider it as
    // a miss
    return false;
  }

  *until_out = it->second.until;
  *source_out = it->second.source;
  *metadata_out = it->second.metadata;
  return true;
}

bool EpochMetaDataCache::getMetaDataNoPromotion(logid_t logid,
                                                epoch_t epoch,
                                                epoch_t* until_out,
                                                EpochMetaData* metadata_out,
                                                RecordSource* source_out,
                                                bool require_consistent) const {
  ld_check(until_out != nullptr);
  ld_check(metadata_out != nullptr);
  ld_check(source_out != nullptr);

  // using read locks here since the cache is immutable
  folly::SharedMutex::ReadHolder read_guard(cache_mutex_);
  auto it = cache_.findWithoutPromotion(std::make_pair(logid, epoch));
  if (it == cache_.end()) {
    return false;
  }

  ld_check(MetaDataLogReader::isCachedSource(it->second.source));
  if (require_consistent && it->second.source == RecordSource::CACHED_SOFT) {
    return false;
  }
  *until_out = it->second.until;
  *source_out = it->second.source;
  *metadata_out = it->second.metadata;
  return true;
}

void EpochMetaDataCache::setMetaData(logid_t logid,
                                     epoch_t epoch,
                                     epoch_t until,
                                     RecordSource source,
                                     const EpochMetaData& metadata) {
  if (!MetaDataLogReader::isCachedSource(source)) {
    ld_critical("INTERNAL ERROR: Called with a non-cached source %d for "
                "log %lu, epoch %u.",
                (int)source,
                logid.val_,
                epoch.val_);
    ld_check(false);
    return;
  }

  folly::SharedMutex::WriteHolder write_guard(cache_mutex_);
  auto it = cache_.findWithoutPromotion(std::make_pair(logid, epoch));
  if (it != cache_.end() &&
      it->second.source == RecordSource::CACHED_CONSISTENT &&
      source == RecordSource::CACHED_SOFT) {
    // do not overwrite an existing consistent record with a soft one
    return;
  }

  cache_.set(std::make_pair(logid, epoch), {until, source, metadata});
}

}} // namespace facebook::logdevice
