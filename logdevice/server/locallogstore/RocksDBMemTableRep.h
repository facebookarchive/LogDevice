/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/IntrusiveList.h>
#include <folly/lang/SafeAssert.h>

#include "logdevice/server/locallogstore/RocksDBColumnFamily.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/RocksDBMemTableRepWrapper.h"

namespace facebook { namespace logdevice {

/**
 * @file RocksDBMemTableRep is a thin wrapper around one of the standard
 *       RocksDB MemTableRep types.
 *
 *       The installation of a RocksDBMemTableRepFactory by
 *       RocksDBLogStoreConfig configures RocksDB to instantiate our
 *       wrapping class when creating new memtables. This allows LogDevice
 *       to track the life cycle of memtables.
 */

class RocksDBMemTableRep : public RocksDBMemTableRepWrapper {
 public:
  RocksDBMemTableRep(RocksDBMemTableRepFactory& factory,
                     std::unique_ptr<rocksdb::MemTableRep> wrapped,
                     rocksdb::Allocator* allocator,
                     uint32_t cf_id);

  ~RocksDBMemTableRep() override;

  void Insert(rocksdb::KeyHandle handle) override;

  void MarkReadOnly() override;

  void MarkFlushed() override;

 protected:
  void ensureRegistered();

  // Used to track MemTableReps on a per-MemTableRepFactory basis.
  folly::IntrusiveListHook links_;

  RocksDBMemTableRepFactory* factory_;
  FlushToken flush_token_ = FlushToken_INVALID;
  std::unique_ptr<rocksdb::MemTableRep> mtr_;
  SteadyTimestamp first_dirtied_time_{SteadyTimestamp::max()};
  uint32_t column_family_id_;
  std::atomic<bool> dirty_ = {false};

  friend class RocksDBMemTableRepFactory;
};

class RocksDBMemTableRepFactory : public RocksDBMemTableRepFactoryWrapper {
  using MemTableRepList =
      folly::IntrusiveList<RocksDBMemTableRep, &RocksDBMemTableRep::links_>;

 public:
  RocksDBMemTableRepFactory(RocksDBLogStoreBase* store,
                            std::unique_ptr<MemTableRepFactory> factory)
      : RocksDBMemTableRepFactoryWrapper(factory.get()),
        store_(store),
        name_("logdevice::RocksDBMemTableRepFactory"),
        mtr_factory_(std::move(factory)) {
    using namespace std::string_literals;
    name_ += "("s + mtr_factory_->Name() + ")"s;
  }

  rocksdb::MemTableRep*
  CreateMemTableRep(const rocksdb::MemTableRep::KeyComparator& /* unused */,
                    rocksdb::Allocator* /* unused */,
                    const rocksdb::SliceTransform* /* unused */,
                    rocksdb::Logger* /* unused */) override {
    // Old interface. Should never be called.
    FOLLY_SAFE_CHECK(false, "Legacy CreateMemTableRep interface accessed");
  }

  rocksdb::MemTableRep*
  CreateMemTableRep(const rocksdb::MemTableRep::KeyComparator& cmp,
                    rocksdb::Allocator* mta,
                    const rocksdb::SliceTransform* st,
                    rocksdb::Logger* logger,
                    uint32_t cf_id) override;

  const char* Name() const override {
    return name_.c_str();
  }

  /**
   * All writes with FlushTokens less than or equal to this value
   * have been retired to stable storage.
   */
  FlushToken flushedUpThrough() const {
    return flushed_up_through_.load();
  }

  FlushToken maxFlushToken() const {
    return next_flush_token_.load() - 1;
  }

  void registerMemTableRep(RocksDBMemTableRep& mtr);

  void markMemtableRepImmutable(RocksDBMemTableRep& mtr);

  void unregisterMemTableRep(RocksDBMemTableRep& mtr);

  SteadyTimestamp oldestUnflushedDataTimestamp() const {
    return oldest_dirtied_time_;
  }

  // Allow to assign store for factory created before store, in case of unit
  // tests.
  void setStore(RocksDBLogStoreBase* store) {
    store_ = store;
  }

 protected:
  std::atomic<FlushToken> next_flush_token_{1};
  std::atomic<FlushToken> flushed_up_through_{FlushToken_INVALID};
  AtomicSteadyTimestamp oldest_dirtied_time_{SteadyTimestamp::max()};
  std::mutex active_memtables_mutex_;
  MemTableRepList active_memtables_;
  RocksDBLogStoreBase* store_;
  std::string name_;
  std::unique_ptr<rocksdb::MemTableRepFactory> mtr_factory_;
};

}} // namespace facebook::logdevice
