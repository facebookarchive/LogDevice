/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "OffsetMap.h"

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

OffsetMap::OffsetMap(const OffsetMap& om) noexcept {
  this->counterTypeMap_ = om.getCounterMap();
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
  this->counterTypeMap_.clear();
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

bool OffsetMap::operator!=(const OffsetMap& om) const {
  return !(*this == om);
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

OffsetMap& OffsetMap::operator-=(const OffsetMap& om) {
  for (auto& it : om.counterTypeMap_) {
    ld_check(counterTypeMap_[it.first] >= it.second);
    this->counterTypeMap_[it.first] -= it.second;
  }
  return *this;
}

OffsetMap OffsetMap::operator-(const OffsetMap& om) const {
  OffsetMap result = *this;
  result -= om;
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

void OffsetMap::max(const OffsetMap& om) {
  for (auto& it : om.getCounterMap()) {
    this->counterTypeMap_[it.first] =
        std::max(it.second, this->counterTypeMap_[it.first]);
  }
}

OffsetMap OffsetMap::fromLegacy(uint64_t offset) {
  OffsetMap om;
  om.setCounter(BYTE_OFFSET, offset);
  return om;
}

uint64_t OffsetMap::toLegacy(const OffsetMap& om) {
  return om.getCounter(BYTE_OFFSET);
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
}} // namespace facebook::logdevice
