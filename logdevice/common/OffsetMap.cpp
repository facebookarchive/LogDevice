/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/OffsetMap.h"

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

OffsetMap::OffsetMap(
    std::initializer_list<std::pair<const counter_type_t, uint64_t>>
        list) noexcept {
  for (const auto& it : list) {
    setCounter(it.first, it.second);
  }
}

OffsetMap::OffsetMap(const OffsetMap& om) noexcept {
  counterTypeMap_ = om.getCounterMap();
}

void OffsetMap::setCounter(const counter_type_t counter_type,
                           uint64_t counter_val) {
  if (counter_val == BYTE_OFFSET_INVALID) {
    counterTypeMap_.erase(counter_type);
  } else {
    counterTypeMap_[counter_type] = counter_val;
  }
}

bool OffsetMap::isValid() const {
  return counterTypeMap_.size() > 0;
}

uint64_t OffsetMap::getCounter(const counter_type_t counter_type) const {
  auto it = counterTypeMap_.find(counter_type);
  if (it == counterTypeMap_.end()) {
    return BYTE_OFFSET_INVALID;
  }
  return it->second;
}

const std::map<counter_type_t, uint64_t>& OffsetMap::getCounterMap() const {
  return counterTypeMap_;
}

void OffsetMap::clear() {
  counterTypeMap_.clear();
}

void OffsetMap::unsetCounter(counter_type_t counter_type) {
  counterTypeMap_.erase(counter_type);
}

bool OffsetMap::isValidOffset(const counter_type_t counter_type) const {
  return counterTypeMap_.find(counter_type) != counterTypeMap_.end();
}

void OffsetMap::serialize(ProtocolWriter& writer) const {
  writer.write(static_cast<uint8_t>(counterTypeMap_.size()));
  for (const auto& counter : counterTypeMap_) {
    writer.write(counter.first);
    writer.write(counter.second);
  }
}

void OffsetMap::deserialize(ProtocolReader& reader,
                            bool evbuffer_zero_copy /* unused */,
                            folly::Optional<size_t> /* unused */) {
  uint8_t counter_map_size = 0;
  reader.read(&counter_map_size);
  for (uint8_t counter = 0; counter < counter_map_size; ++counter) {
    counter_type_t counter_type;
    reader.read(&counter_type);
    uint64_t counter_val;
    reader.read(&counter_val);
    if (reader.error()) {
      err = E::BADMSG;
      return;
    }
    counterTypeMap_[counter_type] = counter_val;
  }
}

bool OffsetMap::operator==(const OffsetMap& om) const {
  if (counterTypeMap_.size() != om.counterTypeMap_.size()) {
    return false;
  }
  for (auto& it : counterTypeMap_) {
    if (it.second != om.getCounter(it.first)) {
      return false;
    }
  }
  return true;
}

bool OffsetMap::operator!=(const OffsetMap& om) const {
  return !(*this == om);
}

OffsetMap OffsetMap::mergeOffsets(OffsetMap lhs, const OffsetMap& rhs) {
  OffsetMap om = std::move(lhs);
  for (auto& it : rhs.counterTypeMap_) {
    om.counterTypeMap_[it.first] += it.second;
  }
  return om;
}

OffsetMap OffsetMap::getOffsetsDifference(OffsetMap lhs, const OffsetMap& rhs) {
  OffsetMap om = std::move(lhs);
  for (auto& it : rhs.counterTypeMap_) {
    ld_check(om.counterTypeMap_[it.first] >= it.second);
    om.counterTypeMap_[it.first] -= it.second;
  }
  return om;
}

OffsetMap OffsetMap::operator*(uint64_t scalar) const {
  OffsetMap om;
  for (auto& it : counterTypeMap_) {
    om.counterTypeMap_[it.first] = it.second * scalar;
  }
  return om;
}

OffsetMap& OffsetMap::operator=(const OffsetMap& om) noexcept {
  counterTypeMap_ = om.getCounterMap();
  return *this;
}

OffsetMap& OffsetMap::operator=(OffsetMap&& om) noexcept {
  counterTypeMap_ = std::move(om.counterTypeMap_);
  return *this;
}

OffsetMap::OffsetMap(OffsetMap&& om) noexcept {
  counterTypeMap_ = std::move(om.counterTypeMap_);
}

void OffsetMap::max(const OffsetMap& om) {
  for (auto& it : om.getCounterMap()) {
    counterTypeMap_[it.first] = std::max(it.second, counterTypeMap_[it.first]);
  }
}

OffsetMap OffsetMap::fromLegacy(uint64_t offset) {
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, offset);
  return om;
}

RecordOffset OffsetMap::toRecord(OffsetMap om) {
  RecordOffset ro;
  ro.offset_map->counterTypeMap_ = std::move(om.counterTypeMap_);
  return ro;
}

OffsetMap OffsetMap::fromRecord(RecordOffset record_offset) {
  OffsetMap om;
  om.counterTypeMap_ = std::move(record_offset.offset_map->counterTypeMap_);
  return om;
}

std::string OffsetMap::toString() const {
  std::string res = "{";
  bool first = true;
  for (const auto& counter : counterTypeMap_) {
    if (!first) {
      res += ", ";
    }
    first = false;
    res += std::to_string(counter.first);
    res += ":";
    if (counter.second == BYTE_OFFSET_INVALID) {
      res += "invalid";
    } else {
      res += std::to_string(counter.second);
    }
  }
  res += "}";
  return res;
}

void AtomicOffsetMap::atomicFetchMax(const OffsetMap& offsets_map) {
  FairRWLock::UpgradeHolder upgradeLock(rw_lock_);
  for (const auto& it : offsets_map.getCounterMap()) {
    auto iterator = atomicCounterTypeMap_.find(it.first);
    if (iterator == atomicCounterTypeMap_.end()) {
      FairRWLock::WriteHolder writeLock(std::move(upgradeLock));
      atomic_fetch_max(atomicCounterTypeMap_[it.first], it.second);
      upgradeLock = FairRWLock::UpgradeHolder(std::move(writeLock));
    } else {
      atomic_fetch_max(atomicCounterTypeMap_[it.first], it.second);
    }
  }
}

OffsetMap AtomicOffsetMap::load() const {
  OffsetMap om;
  FairRWLock::ReadHolder read_guard(rw_lock_);
  for (const auto& it : atomicCounterTypeMap_) {
    om.setCounter(it.first, it.second.load());
  }
  return om;
}

OffsetMap AtomicOffsetMap::fetchAdd(const OffsetMap& offsets_map) {
  OffsetMap om;
  uint64_t offset;
  FairRWLock::UpgradeHolder upgradeLock(rw_lock_);
  for (const auto& it : offsets_map.getCounterMap()) {
    auto iterator = atomicCounterTypeMap_.find(it.first);
    if (iterator == atomicCounterTypeMap_.end()) {
      FairRWLock::WriteHolder writeLock(std::move(upgradeLock));
      offset = atomicCounterTypeMap_[it.first].fetch_add(it.second) + it.second;
      upgradeLock = FairRWLock::UpgradeHolder(std::move(writeLock));
    } else {
      offset = atomicCounterTypeMap_[it.first].fetch_add(it.second) + it.second;
    }
    om.setCounter(it.first, offset);
  }
  return om;
}

}} // namespace facebook::logdevice
