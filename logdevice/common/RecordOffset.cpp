/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/RecordOffset.h"

#include "logdevice/common/OffsetMap.h"

namespace facebook { namespace logdevice {

RecordOffset::RecordOffset() noexcept {
  offset_map = std::make_unique<OffsetMap>();
}

RecordOffset::RecordOffset(
    std::initializer_list<std::pair<const counter_type_t, uint64_t>>
        list) noexcept {
  offset_map = std::make_unique<OffsetMap>(OffsetMap(list));
}

uint64_t RecordOffset::getCounter(const counter_type_t counter_type) const {
  return offset_map->getCounter(counter_type);
}

void RecordOffset::setCounter(const counter_type_t counter_type,
                              uint64_t counter_value) {
  offset_map->setCounter(counter_type, counter_value);
}

std::string RecordOffset::toString() const {
  return offset_map->toString();
}

bool RecordOffset::isValid() const {
  return offset_map->isValid();
}

bool RecordOffset::isValidOffset(counter_type_t counter_type) const {
  return offset_map->isValidOffset(counter_type);
}

bool RecordOffset::operator==(const RecordOffset& record_offset) const {
  return (*offset_map == *(record_offset.offset_map));
}

bool RecordOffset::operator!=(const RecordOffset& record_offset) const {
  return !(*this == record_offset);
}

RecordOffset::RecordOffset(RecordOffset&& record_offset) noexcept {
  offset_map = std::move(record_offset.offset_map);
}

RecordOffset::RecordOffset(const RecordOffset& record_offset) noexcept {
  offset_map = std::make_unique<OffsetMap>(*(record_offset.offset_map));
}

RecordOffset& RecordOffset::operator=(RecordOffset&& record_offset) noexcept {
  offset_map = std::move(record_offset.offset_map);
  return *this;
}

RecordOffset& RecordOffset::
operator=(const RecordOffset& record_offset) noexcept {
  offset_map = std::make_unique<OffsetMap>(*(record_offset.offset_map));
  return *this;
}

RecordOffset::~RecordOffset() {}

}} // namespace facebook::logdevice
