/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBMemTableRep.h"

#include "logdevice/common/stats/PerShardHistograms.h"

namespace facebook { namespace logdevice {

RocksDBMemTableRep::RocksDBMemTableRep(RocksDBMemTableRepFactory& factory,
                                       std::unique_ptr<rocksdb::MemTableRep> r,
                                       rocksdb::Allocator* allocator)
    : RocksDBMemTableRepWrapper(r.get(), allocator),
      factory_(&factory),
      mtr_(std::move(r)) {
  // Registration with the factory is deferred until the first time
  // this MemTableRep is dirtied.
}

RocksDBMemTableRep::~RocksDBMemTableRep() {
  ld_debug("Destroyed MemTableRep(%p). ID:%ju", this, (uintmax_t)flush_token_);
  factory_->unregisterMemTableRep(*this);
}

void RocksDBMemTableRep::Insert(rocksdb::KeyHandle handle) {
  factory_->registerMemTableRep(*this);
  mtr_->Insert(handle);
}

rocksdb::MemTableRep* RocksDBMemTableRepFactory::CreateMemTableRep(
    const rocksdb::MemTableRep::KeyComparator& cmp,
    rocksdb::Allocator* mta,
    const rocksdb::SliceTransform* st,
    rocksdb::Logger* logger) {
  std::unique_ptr<rocksdb::MemTableRep> wrapped_mtr(
      mtr_factory_->CreateMemTableRep(cmp, mta, st, logger));
  return new RocksDBMemTableRep(*this, std::move(wrapped_mtr), mta);
}

void RocksDBMemTableRepFactory::registerMemTableRep(RocksDBMemTableRep& mtr) {
  // Ensure the queue is fully sorted by serializing both the allocation
  // of the next ID and insertion into active_memtables_.
  bool dirty = mtr.dirty_.load(std::memory_order_acquire);
  if (!dirty) {
    std::unique_lock<std::mutex> lock(active_memtables_mutex_);
    dirty = mtr.dirty_.load(std::memory_order_relaxed);
    if (!dirty) {
      ld_check(!mtr.links_.is_linked());
      ld_check(mtr.flush_token_ == FlushToken_INVALID);
      mtr.flush_token_ = next_flush_token_.fetch_add(1);
      mtr.first_dirtied_time_ = SteadyTimestamp::now();
      active_memtables_.push_back(mtr);
      oldest_dirtied_time_.storeMin(mtr.first_dirtied_time_);
      mtr.dirty_.store(true, std::memory_order_release);
      PER_SHARD_STAT_INCR(
          store_->getStatsHolder(), num_memtables, store_->getShardIdx());
      ld_debug("Registering MemTableRep(%p). ID:%ju",
               &mtr,
               (uintmax_t)mtr.flush_token_);
    }
  }
}

void RocksDBMemTableRepFactory::unregisterMemTableRep(RocksDBMemTableRep& mtr) {
  if (!mtr.links_.is_linked()) {
    return;
  }

  std::unique_lock<std::mutex> lock(active_memtables_mutex_);

  ld_check(!active_memtables_.empty());

  ld_debug("MemTable window for shard %d is %ju:%ju -> flushed_up_through %ju",
           store_->getShardIdx(),
           (intmax_t)active_memtables_.front().flush_token_,
           (intmax_t)active_memtables_.back().flush_token_,
           (intmax_t)flushed_up_through_.load());

  bool window_slid = &active_memtables_.front() == &mtr;
  mtr.links_.unlink();

  ld_check(mtr.first_dirtied_time_ != SteadyTimestamp::max());
  SteadyTimestamp age(SteadyTimestamp::now() - mtr.first_dirtied_time_);
  PER_SHARD_STAT_ADD(store_->getStatsHolder(),
                     cumulative_memtable_age_ms,
                     store_->getShardIdx(),
                     age.toMilliseconds().count());
  PER_SHARD_HISTOGRAM_ADD(store_->getStatsHolder(),
                          rocks_memtable_age,
                          store_->getShardIdx(),
                          age.toMilliseconds().count());
  if (window_slid) {
    FlushToken now_flushed_up_through;

    ld_debug("MemTable window for shard %d slid due to MemTable %ju",
             store_->getShardIdx(),
             (intmax_t)mtr.flush_token_);
    if (active_memtables_.empty()) {
      now_flushed_up_through = next_flush_token_.load() - 1;
      ld_debug("Shard %d, MemTable Window Empty. Sliding to %ju",
               store_->getShardIdx(),
               (uintmax_t)now_flushed_up_through);
      oldest_dirtied_time_ = SteadyTimestamp::max();
    } else {
      auto& oldest_memtable = active_memtables_.front();
      now_flushed_up_through = oldest_memtable.flush_token_ - 1;
      oldest_dirtied_time_ = oldest_memtable.first_dirtied_time_;
      ld_debug("Shard %d, MemTable Window has %zd entries. Sliding to %ju",
               store_->getShardIdx(),
               active_memtables_.size(), // NOTE: O(n)
               (uintmax_t)now_flushed_up_through);
    }

    ld_check(now_flushed_up_through != FlushToken_INVALID);
    flushed_up_through_.store(now_flushed_up_through);
    PER_SHARD_STAT_INCR(store_->getStatsHolder(),
                        active_memtables_window_move,
                        store_->getShardIdx());
    store_->onMemTableWindowUpdated();
  }
}

}} // namespace facebook::logdevice
