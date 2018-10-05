/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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

// Reserve the last 10 counters for internal use
enum class CounterType : uint8_t { BYTE_OFFSET = 246 };

class OffsetMap : public SerializableData {
 public:
  using SerializableData::deserialize;
  using SerializableData::serialize;

  OffsetMap();

  OffsetMap(const OffsetMap& om) noexcept;
  OffsetMap& operator=(const OffsetMap& om) noexcept;

  OffsetMap(OffsetMap&& om) noexcept;
  OffsetMap& operator=(OffsetMap&& om) noexcept;

  /**
   * get CounterType value from CounterTypeMap
   * @param counter_type CounterType to read
   * @return  value of CounterType
   */
  uint64_t getCounter(CounterType counter_type) const;

  /**
   * get counterTypeMap_
   * @return  counterTypeMap_
   */
  const std::unordered_map<CounterType, uint64_t, folly::Hash>&
  getCounterMap() const;

  /**
   * set CounterType value from CounterTypeMap
   * @param counter_type CounterType to add
   * @param counter_val  value to set for counter_type
   */
  void setCounter(const CounterType counter_type, uint64_t counter_val);

  /**
   * Check if counter value is valid
   * @return true if counter value is valid
   */
  bool isValidOffset(const CounterType count) const;

  /**
   * Check if OffsetMap is valid
   * @return true if contains at least one CounterType (counterTypeMap_ size >
   * 0)
   */
  bool isValid() const;

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

  // Add two counterTypeMap_ and return a new OffsetMap object
  OffsetMap operator+(const OffsetMap& om) const;

  // Increment counterTypeMap_ entries based on passed OffsetMap
  OffsetMap& operator+=(const OffsetMap& om);

  // Check if the counterTypeMap_ are equal
  bool operator==(const OffsetMap& om) const;

 private:
  std::unordered_map<CounterType, uint64_t, folly::Hash> counterTypeMap_;
};

}} // namespace facebook::logdevice
