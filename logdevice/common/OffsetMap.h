/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/hash/Hash.h>
#include <unordered_map>

#include "logdevice/common/SerializableData.h"

namespace facebook { namespace logdevice {

enum class CounterType : uint8_t { BYTE_OFFSET = 0 };

class OffsetMap : public SerializableData {
 public:
  using SerializableData::deserialize;
  using SerializableData::serialize;

  OffsetMap() {}

  /**
   * get CounterType value from CounterTypeMap
   * @param counter_type CounterType to read
   * @return  value of CounterType
   */
  uint64_t getCounter(CounterType counter_type);

  /**
   * set CounterType value from CounterTypeMap
   * @param counter_type CounterType to add
   * @param counter_val  value to set for counter_type
   */
  void setCounter(CounterType counter_type, uint64_t counter_val);

  /**
   * See SerializableData::serialize().
   */
  void serialize(ProtocolWriter& writer) const override;

  /**
   * See SerializableData::deserialize().
   */
  void
  deserialize(ProtocolReader& reader,
              bool evbuffer_zero_copy /* unused */,
              folly::Optional<size_t> expected_size = folly::none) override;

  /**
   * See SerializableData::name().
   */
  const char* name() const override {
    return "OffsetMap";
  }

 private:
  std::unordered_map<CounterType, uint64_t, folly::Hash> counterTypeMap_;
};

}} // namespace facebook::logdevice
