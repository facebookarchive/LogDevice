/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/memtablerep.h>

namespace facebook { namespace logdevice {

// An implementation of MemTableRep that forwards all calls to another
// MemTableRep. May be useful to clients who wish to override just part
// of, or augment the functionality of, another MemTableRep.
class RocksDBMemTableRepWrapper : public rocksdb::MemTableRep {
 public:
  RocksDBMemTableRepWrapper(rocksdb::MemTableRep* w,
                            rocksdb::Allocator* allocator)
      : MemTableRep(allocator), wrapped_(w) {}

  ~RocksDBMemTableRepWrapper() override {}

  rocksdb::KeyHandle Allocate(const size_t len, char** buf) override {
    return wrapped_->Allocate(len, buf);
  }
  void Insert(rocksdb::KeyHandle handle) override {
    wrapped_->Insert(handle);
  }
  void InsertConcurrently(rocksdb::KeyHandle handle) override {
    wrapped_->InsertConcurrently(handle);
  }
  bool Contains(const char* key) const override {
    return wrapped_->Contains(key);
  }
  void MarkReadOnly() override {
    wrapped_->MarkReadOnly();
  }
  void MarkFlushed() override {
    wrapped_->MarkFlushed();
  }
  void Get(const rocksdb::LookupKey& k,
           void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    wrapped_->Get(k, callback_args, callback_func);
  }
  uint64_t ApproximateNumEntries(const rocksdb::Slice& start_ikey,
                                 const rocksdb::Slice& end_key) override {
    return wrapped_->ApproximateNumEntries(start_ikey, end_key);
  }
  size_t ApproximateMemoryUsage() override {
    return wrapped_->ApproximateMemoryUsage();
  }
  Iterator* GetIterator(rocksdb::Arena* arena) override {
    return wrapped_->GetIterator(arena);
  }
  Iterator* GetDynamicPrefixIterator(rocksdb::Arena* arena) override {
    return wrapped_->GetDynamicPrefixIterator(arena);
  }
  bool IsMergeOperatorSupported() const override {
    return wrapped_->IsMergeOperatorSupported();
  }
  bool IsSnapshotSupported() const override {
    return wrapped_->IsSnapshotSupported();
  }

 protected:
  // Can't proxy for a protected method. Hopefully the base class
  // impmementation is good enough.
  //
  // rocksdb::Slice UserKey(const char* key) const override;

 protected:
  rocksdb::MemTableRep* wrapped_;
};

// An implementation of MemTableRepFactory that forwards all calls to another
// MemTableRepFactory. May be useful to clients who wish to override just part
// of, or augment the functionality of, another MemTableRepFactory.
class RocksDBMemTableRepFactoryWrapper : public rocksdb::MemTableRepFactory {
 public:
  explicit RocksDBMemTableRepFactoryWrapper(rocksdb::MemTableRepFactory* f)
      : mtr_factory_(f) {}

  rocksdb::MemTableRep*
  CreateMemTableRep(const rocksdb::MemTableRep::KeyComparator& cmp,
                    rocksdb::Allocator* mta,
                    const rocksdb::SliceTransform* st,
                    rocksdb::Logger* logger,
                    uint32_t cf_id) override {
    return mtr_factory_->CreateMemTableRep(cmp, mta, st, logger, cf_id);
  }

  const char* Name() const override {
    return mtr_factory_->Name();
  }

  bool IsInsertConcurrentlySupported() const override {
    return mtr_factory_->IsInsertConcurrentlySupported();
  }

 private:
  rocksdb::MemTableRepFactory* mtr_factory_;
};

}} // namespace facebook::logdevice
