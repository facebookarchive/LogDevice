/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBCache.h"

namespace facebook { namespace logdevice {

RocksDBCache::RocksDBCache(UpdateableSettings<RocksDBSettings> rocksdb_settings)
    : rocksdb_settings_(rocksdb_settings) {
  rocksdb::LRUCacheOptions opt;
  opt.capacity = rocksdb_settings_->cache_size_;
  opt.num_shard_bits = rocksdb_settings_->cache_numshardbits_;
  opt.high_pri_pool_ratio = rocksdb_settings_->cache_high_pri_pool_ratio_;

  cache_ = rocksdb::NewLRUCache(opt);
}

const char* RocksDBCache::Name() const {
  return "logdevice::RocksDBCache";
}
rocksdb::Status RocksDBCache::Insert(const rocksdb::Slice& key,
                                     void* value,
                                     size_t charge,
                                     void (*deleter)(const rocksdb::Slice& key,
                                                     void* value),
                                     Handle** handle,
                                     Priority priority) {
  if (charge <
      rocksdb_settings_->cache_small_block_threshold_for_high_priority_) {
    priority = Priority::HIGH;
  }
  return cache_->Insert(key, value, charge, deleter, handle, priority);
}

// Everything below is trivially passed through to cache_.

RocksDBCache::Handle* RocksDBCache::Lookup(const rocksdb::Slice& key,
                                           rocksdb::Statistics* stats) {
  return cache_->Lookup(key, stats);
}
bool RocksDBCache::Ref(Handle* handle) {
  return cache_->Ref(handle);
}
bool RocksDBCache::Release(Handle* handle, bool force_erase) {
  return cache_->Release(handle, force_erase);
}
void* RocksDBCache::Value(Handle* handle) {
  return cache_->Value(handle);
}
void RocksDBCache::Erase(const rocksdb::Slice& key) {
  cache_->Erase(key);
}
uint64_t RocksDBCache::NewId() {
  return cache_->NewId();
}
void RocksDBCache::SetCapacity(size_t capacity) {
  cache_->SetCapacity(capacity);
}
void RocksDBCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  cache_->SetStrictCapacityLimit(strict_capacity_limit);
}
bool RocksDBCache::HasStrictCapacityLimit() const {
  return cache_->HasStrictCapacityLimit();
}
size_t RocksDBCache::GetCapacity() const {
  return cache_->GetCapacity();
}
size_t RocksDBCache::GetUsage() const {
  return cache_->GetUsage();
}
size_t RocksDBCache::GetUsage(Handle* handle) const {
  return cache_->GetUsage(handle);
}
size_t RocksDBCache::GetPinnedUsage() const {
  return cache_->GetPinnedUsage();
}
#ifdef LOGDEVICE_ROCKSDB_HAS_CACHE_GET_CHARGE
size_t RocksDBCache::GetCharge(Handle* handle) const {
  return cache_->GetCharge(handle);
}
#endif
void RocksDBCache::DisownData() {
  cache_->DisownData();
}
void RocksDBCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                          bool thread_safe) {
  cache_->ApplyToAllCacheEntries(callback, thread_safe);
}
void RocksDBCache::EraseUnRefEntries() {
  cache_->EraseUnRefEntries();
}
std::string RocksDBCache::GetPrintableOptions() const {
  return cache_->GetPrintableOptions();
}

}} // namespace facebook::logdevice
