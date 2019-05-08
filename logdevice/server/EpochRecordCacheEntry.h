/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include <folly/AtomicIntrusiveLinkedList.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/ZeroCopiedRecord.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class EpochRecordCacheDependencies;
class PayloadHolder;

namespace EpochRecordCacheSerializer {
class EpochRecordCacheCompare;
}

/**
 * Descriptor of one cache entry, contains just enough information for
 * log recovery procedure (i.e., store flags, timestamp and payloads)
 */
class EpochRecordCacheEntry : public ZeroCopiedRecord {
 public:
  class Disposer;

  /*
   * Create and repopulate an entry from serialized representation in the
   * given buffer, and with the given disposer. Returns nullptr if the
   * buffer size is too small.
   *
   * If result_size is not nullptr, it will be set to the size in the buffer
   * that the reconstructed entry was using, or 0 if the buffer is too small.
   */
  static std::shared_ptr<EpochRecordCacheEntry>
  createFromLinearBuffer(lsn_t lsn,
                         const char* buffer,
                         size_t size,
                         Disposer disposer,
                         size_t* result_size = nullptr);

  /**
   * Calculate the size that this cache, in its current state, would have in
   * serialized form
   */
  size_t sizeInLinearBuffer() const;

  /**
   * Write a serialized representation of this cache to the given buffer.
   * Returns the resulting size, or -1 if the buffer size was too small.
   */
  ssize_t toLinearBuffer(char* buffer, size_t size) const;

  EpochRecordCacheEntry();

  EpochRecordCacheEntry(lsn_t lsn,
                        STORE_flags_t flags,
                        uint64_t timestamp,
                        esn_t last_known_good,
                        uint32_t wave_or_recovery_epoch,
                        const copyset_t& copyset,
                        OffsetMap offsets_within_epoch,
                        std::map<KeyType, std::string>&& keys,
                        Slice payload_raw,
                        std::shared_ptr<PayloadHolder> payload_holder);

 private:
  int fromLinearBuffer(lsn_t lsn, const char* buffer, size_t size);

  friend class ZeroCopiedRecord;
  friend class EpochRecordCacheSerializer::EpochRecordCacheCompare;
};

class EpochRecordCacheEntry::Disposer {
 public:
  explicit Disposer(EpochRecordCacheDependencies* deps) : deps_(deps) {}
  void operator()(EpochRecordCacheEntry* e);

 private:
  EpochRecordCacheDependencies* const deps_;
};
}} // namespace facebook::logdevice
