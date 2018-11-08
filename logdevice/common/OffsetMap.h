/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>

#include "logdevice/common/SerializableData.h"

namespace facebook { namespace logdevice {

/**
 * @file map of counters that contains information on amount of data within
 *       epoch or at the end of epoch. Currently contains information on number
 *       of bytes.
 */

class OffsetMap : public SerializableData {
 public:
  using SerializableData::deserialize;
  using SerializableData::serialize;

  OffsetMap() noexcept = default;

  OffsetMap(const OffsetMap& om) noexcept;
  OffsetMap& operator=(const OffsetMap& om) noexcept;

  OffsetMap(OffsetMap&& om) noexcept;
  OffsetMap& operator=(OffsetMap&& om) noexcept;

  /**
   * get counter_type value from CounterTypeMap
   * @param counter_type to read
   * @return  value of counter_type
   */
  uint64_t getCounter(counter_type_t counter_type) const;

  /**
   * get counterTypeMap_
   * @return  counterTypeMap_
   */
  const std::map<counter_type_t, uint64_t>& getCounterMap() const;

  /**
   * removes counter_type from counterTypeMap_
   * @param counter_type  counter_type to remove from counterTypeMap_
   */
  void unsetCounter(counter_type_t counter_type);

  /**
   * set counter_type value from CounterTypeMap
   * @param counter_type counter_type_t to add to counterTypeMap_
   * @param counter_val  value to set for counter_type
   */
  void setCounter(const counter_type_t counter_type, uint64_t counter_val);

  /**
   * Check if counter value is valid
   * @return true if counter value is valid
   */
  bool isValidOffset(const counter_type_t count) const;

  /**
   * counterTypeMap_ would have BYTE_OFFSET_INVALID value for all counters
   */
  void clear();

  /**
   * Check if OffsetMap is valid
   * @return true if contains at least one counter exists
   * (counterTypeMap_ size > 0)
   */
  bool isValid() const;

  /**
   * set counterTypeMap_ entries to be equal to the max of both OffsetMap
   * entries
   */
  void max(const OffsetMap& om);

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
   * Used to convert uint64_t offset to new OffsetMap format
   * returns  OffsetMap with only <BYTE_OFFSET, offset> stored
   */
  static OffsetMap fromLegacy(uint64_t offset);

  /**
   * return counter with index counter_type_t BYTE_OFFSET
   */
  static uint64_t toLegacy(const OffsetMap& om);

  /**
   * See SerializableData::name().
   */
  const char* name() const override {
    return "OffsetMap";
  }

  std::string toString() const;

  // Add two counterTypeMap_ and return a new OffsetMap object
  OffsetMap operator+(const OffsetMap& om) const;

  // Increment counterTypeMap_ entries based on passed OffsetMap
  OffsetMap& operator+=(const OffsetMap& om);

  // Subtract two counterTypeMap_ and return a new OffsetMap object
  OffsetMap operator-(const OffsetMap& om) const;

  // Decrement counterTypeMap_ entries based on passed OffsetMap
  OffsetMap& operator-=(const OffsetMap& om);

  // Check if the counterTypeMap_ are equal
  bool operator==(const OffsetMap& om) const;

  // Check if the counterTypeMap_ are not equal
  bool operator!=(const OffsetMap& om) const;

 private:
  std::map<counter_type_t, uint64_t> counterTypeMap_;
};

}} // namespace facebook::logdevice
