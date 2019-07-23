/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/cache.h>

#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

// Custom cache policy used for block cache.
// Currently it just wraps the default LRU cache implementation,
// with small tweaks.

class RocksDBCache : public rocksdb::Cache {
 public:
  explicit RocksDBCache(UpdateableSettings<RocksDBSettings> rocksdb_settings);

  const char* Name() const override;
  rocksdb::Status Insert(const rocksdb::Slice& key,
                         void* value,
                         size_t charge,
                         void (*deleter)(const rocksdb::Slice& key,
                                         void* value),
                         Handle** handle,
                         Priority priority) override;
  Handle* Lookup(const rocksdb::Slice& key,
                 rocksdb::Statistics* stats) override;
  bool Ref(Handle* handle) override;
  bool Release(Handle* handle, bool force_erase) override;
  void* Value(Handle* handle) override;
  void Erase(const rocksdb::Slice& key) override;
  uint64_t NewId() override;
  void SetCapacity(size_t capacity) override;
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  bool HasStrictCapacityLimit() const override;
  size_t GetCapacity() const override;
  size_t GetUsage() const override;
  size_t GetUsage(Handle* handle) const override;
  size_t GetPinnedUsage() const override;
#ifdef LOGDEVICE_ROCKSDB_HAS_CACHE_GET_CHARGE
  size_t GetCharge(Handle* handle) const override;
#endif
  void DisownData() override;
  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override;
  void EraseUnRefEntries() override;
  std::string GetPrintableOptions() const override;

 private:
  std::shared_ptr<rocksdb::Cache> cache_;
  UpdateableSettings<RocksDBSettings> rocksdb_settings_;
};

}} // namespace facebook::logdevice
