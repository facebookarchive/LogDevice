/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "OffsetMap.h"

namespace facebook { namespace logdevice {
OffsetMap::OffsetMap() {}

OffsetMap::OffsetMap(const OffsetMap& om) noexcept {
  this->counterTypeMap_ = om.getCounterMap();
}

void OffsetMap::setCounter(const CounterType counter_type,
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

uint64_t OffsetMap::getCounter(const CounterType counter_type) const {
  auto it = counterTypeMap_.find(counter_type);
  if (it == counterTypeMap_.end()) {
    return BYTE_OFFSET_INVALID;
  }
  return it->second;
}

const std::unordered_map<CounterType, uint64_t, folly::Hash>&
OffsetMap::getCounterMap() const {
  return counterTypeMap_;
}

bool OffsetMap::isValidOffset(const CounterType counter_type) const {
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
    CounterType counter_type;
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
  if (this->counterTypeMap_.size() != om.counterTypeMap_.size()) {
    return false;
  }
  for (auto& it : this->counterTypeMap_) {
    if (it.second != om.getCounter(it.first)) {
      return false;
    }
  }
  return true;
}

OffsetMap& OffsetMap::operator+=(const OffsetMap& om) {
  for (auto& it : om.counterTypeMap_) {
    this->counterTypeMap_[it.first] += it.second;
  }
  return *this;
}

OffsetMap OffsetMap::operator+(const OffsetMap& om) const {
  OffsetMap result = *this;
  result += om;
  return result;
}

OffsetMap& OffsetMap::operator=(const OffsetMap& om) noexcept {
  this->counterTypeMap_ = om.getCounterMap();
  return *this;
}

OffsetMap& OffsetMap::operator=(OffsetMap&& om) noexcept {
  this->counterTypeMap_ = std::move(om.counterTypeMap_);
  return *this;
}

OffsetMap::OffsetMap(OffsetMap&& om) noexcept {
  this->counterTypeMap_ = std::move(om.counterTypeMap_);
}

}} // namespace facebook::logdevice
