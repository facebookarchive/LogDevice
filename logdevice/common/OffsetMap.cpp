/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "OffsetMap.h"

namespace facebook { namespace logdevice {
void OffsetMap::setCounter(CounterType counter_type, uint64_t counter_val) {
  counterTypeMap_[counter_type] = counter_val;
}

uint64_t OffsetMap::getCounter(CounterType counter_type) {
  if (counterTypeMap_.find(counter_type) != counterTypeMap_.end()) {
    return counterTypeMap_[counter_type];
  }
  return BYTE_OFFSET_INVALID;
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

}} // namespace facebook::logdevice
