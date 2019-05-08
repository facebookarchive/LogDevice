/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/EpochRecordCacheEntry.h"

#include <algorithm>
#include <cstring>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/RecordCacheDependencies.h"

namespace facebook { namespace logdevice {

using namespace EpochRecordCacheSerializer;

EpochRecordCacheEntry::EpochRecordCacheEntry() : ZeroCopiedRecord() {}

EpochRecordCacheEntry::EpochRecordCacheEntry(
    lsn_t lsn,
    STORE_flags_t flags,
    uint64_t timestamp,
    esn_t last_known_good,
    uint32_t wave_or_recovery_epoch,
    const copyset_t& copyset,
    OffsetMap offsets_within_epoch,
    std::map<KeyType, std::string>&& keys,
    Slice payload_raw,
    std::shared_ptr<PayloadHolder> payload_holder)
    : ZeroCopiedRecord(lsn,
                       flags,
                       timestamp,
                       last_known_good,
                       wave_or_recovery_epoch,
                       copyset,
                       std::move(offsets_within_epoch),
                       std::move(keys),
                       payload_raw,
                       std::move(payload_holder)) {}

int EpochRecordCacheEntry::fromLinearBuffer(lsn_t lsn,
                                            const char* buffer,
                                            size_t size) {
  this->lsn = lsn;
  if (sizeof(EntryHeader) > size) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Size of linear buffer too small: the header alone is %zu, "
                    "given buffer of size %zu",
                    sizeof(EntryHeader),
                    size);
    return -1;
  }

  // header fields
  EntryHeader header;
  size_t current_size = sizeof(EntryHeader);
  memcpy(&header, buffer, current_size);
  flags = header.flags;
  timestamp = header.timestamp;
  last_known_good = header.last_known_good;
  wave_or_recovery_epoch = header.wave_or_recovery_epoch;

  if (flags & STORE_Header::OFFSET_MAP) {
    int rv = offsets_within_epoch.deserialize(
        Slice(buffer + current_size, size - current_size));
    ld_check(rv != -1);
  } else {
    offsets_within_epoch.setCounter(BYTE_OFFSET, header.offset_within_epoch);
  }

  size_t copyset_memsize = sizeof(copyset_t::value_type) * header.copyset_size;
  if (current_size + copyset_memsize + header.payload_size > size) {
    err = E::NOBUFS;
    return -1;
  }

  // reconstruct copyset using range assignment
  const auto* copyset_ptr =
      reinterpret_cast<const copyset_t::value_type*>(buffer + current_size);
  copyset.assign(copyset_ptr, copyset_ptr + header.copyset_size);
  current_size += copyset_memsize;

  // reconstruct key
  for (int i{0}; i < to_integral(KeyType::MAX); ++i) {
    if (header.key_size[i] > 0) {
      keys[static_cast<KeyType>(i)] =
          std::string(buffer + current_size, header.key_size[i]);
      current_size += header.key_size[i];
    }
  }

  // reconstruct payload
  if (header.payload_size > 0) {
    void* data = malloc(header.payload_size);
    memcpy(data, buffer + current_size, header.payload_size);
    // Copy payload size to local var since it's in a packed struct; else we'd
    // be passing it by reference to make_shared
    size_t payload_size = header.payload_size;
    payload_holder_ = std::make_shared<PayloadHolder>(data, payload_size);
    payload_raw = Slice(data, header.payload_size);
    current_size += header.payload_size;
  } else {
    payload_raw = Slice(nullptr, 0);
    payload_holder_ = std::make_shared<PayloadHolder>(nullptr, 0);
  }

  return current_size;
}

size_t EpochRecordCacheEntry::sizeInLinearBuffer() const {
  size_t key_sizes{0};
  for (const auto& kv : keys) {
    key_sizes += kv.second.size();
  }

  return sizeof(EntryHeader) +
      (sizeof(copyset_t::value_type) * copyset.size()) + key_sizes +
      payload_raw.size +
      (flags & STORE_Header::OFFSET_MAP
           ? offsets_within_epoch.sizeInLinearBuffer()
           : 0);
}

ssize_t EpochRecordCacheEntry::toLinearBuffer(char* buffer, size_t size) const {
  if (sizeInLinearBuffer() > size) {
    err = E::NOBUFS;
    return -1;
  }

  // Header fields
  EntryHeader header{flags,
                     timestamp,
                     last_known_good,
                     wave_or_recovery_epoch,
                     copyset_size_t(copyset.size()),
                     offsets_within_epoch.getCounter(BYTE_OFFSET),
                     {},
                     payload_raw.size};
  for (const auto& kv : keys) {
    header.key_size[static_cast<int>(kv.first)] = kv.second.size();
  }
  memcpy(buffer, &header, sizeof(EntryHeader));
  int current_size = sizeof(EntryHeader);

  if (header.flags & STORE_Header::OFFSET_MAP) {
    int rv = offsets_within_epoch.serialize(
        buffer + current_size, size - current_size);
    current_size += rv;
  }

  // copyset_
  size_t copyset_memsize = sizeof(copyset_t::value_type) * copyset.size();
  std::copy(copyset.begin(),
            copyset.end(),
            reinterpret_cast<copyset_t::value_type*>(buffer + current_size));
  current_size += copyset_memsize;

  // keys
  for (int i{0}; i < to_integral(KeyType::MAX); ++i) {
    const auto iter = keys.find(static_cast<KeyType>(i));
    if (iter != keys.end()) {
      memcpy(buffer + current_size, iter->second.data(), iter->second.size());
      current_size += iter->second.size();
    }
    // Any keys that don't exist in the map are treated as zero-length keys, so
    // there's nothing to write.
  }

  // payload_raw_
  if (payload_raw.data) {
    memcpy(buffer + current_size, payload_raw.data, payload_raw.size);
  }
  current_size += payload_raw.size;

  ld_assert(current_size == sizeInLinearBuffer());
  return current_size;
}

std::shared_ptr<EpochRecordCacheEntry>
EpochRecordCacheEntry::createFromLinearBuffer(lsn_t lsn,
                                              const char* buffer,
                                              size_t size,
                                              Disposer disposer,
                                              size_t* result_size) {
  auto result = std::shared_ptr<EpochRecordCacheEntry>(
      new EpochRecordCacheEntry(), disposer);
  ssize_t linear_size = result->fromLinearBuffer(lsn, buffer, size);
  if (result_size != nullptr) {
    *result_size = linear_size;
  }
  if (linear_size == -1) {
    return nullptr;
  }
  return result;
}

void EpochRecordCacheEntry::Disposer::operator()(EpochRecordCacheEntry* e) {
  std::unique_ptr<EpochRecordCacheEntry> entry_ptr(e);
  deps_->disposeOfCacheEntry(std::move(entry_ptr));
}
}} // namespace facebook::logdevice
