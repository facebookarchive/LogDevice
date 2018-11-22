/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <map>

#include <folly/SharedMutex.h>

#include "logdevice/common/SerializableData.h"
#include "logdevice/include/RecordOffset.h"

namespace facebook { namespace logdevice {

/**
 * @file map of counters that contains information on amount of data within
 *       epoch or at the end of epoch. Currently contains information on number
 *       of bytes. Refer to counter_type_t for information on tracked counters.
 */

class OffsetMap : public SerializableData {
 public:
  using SerializableData::deserialize;
  using SerializableData::serialize;

  OffsetMap() noexcept = default;
  /*
   * Constructs an OffsetMap object from intializer_list. This constructor
   * performs setCounter(counter_type, counter_value) on each pair of the list
   * @param list of pairs <counter_type, counter_value>
   */
  explicit OffsetMap(
      std::initializer_list<std::pair<const counter_type_t, uint64_t>>
          list) noexcept;

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

  /*
   * Fill OffsetMap values from RecordOffset
   */
  static OffsetMap fromRecord(RecordOffset record_offset);

  /*
   * Returns an RecordOffset from current OffsetMap
   */
  static RecordOffset toRecord(OffsetMap om);

  /**
   * See SerializableData::name().
   */
  const char* name() const override {
    return "OffsetMap";
  }

  std::string toString() const;

  /*
   * Iterates over valid entries of rhs and add them to entries of lhs.
   * Invalid Entries in lhs will be considered to be zero when adding them with
   * valid ones from rhs.
   * @return merge operation results
   */
  static OffsetMap mergeOffsets(OffsetMap lhs, const OffsetMap& rhs);

  /*
   * Iterates over valid entries of rhs and computes the difference between lhs
   * and rhs (lhs - rhs). All entries of lhs have to be greater than or equal
   * to entries in rhs. Invalid entries of lhs will be considered to be zero.
   */
  static OffsetMap getOffsetsDifference(OffsetMap lhs, const OffsetMap& rhs);

  // Multiply all entry by scalar
  OffsetMap operator*(uint64_t scalar) const;

  // Check if the counterTypeMap_ are equal
  bool operator==(const OffsetMap& om) const;

  // Check if the counterTypeMap_ are not equal
  bool operator!=(const OffsetMap& om) const;

 private:
  std::map<counter_type_t, uint64_t> counterTypeMap_;
};

class AtomicOffsetMap {
 public:
  AtomicOffsetMap() noexcept = default;

  /*
   * Performs atomic_fetch_max on every entry of offsets_map
   */
  void atomicFetchMax(const OffsetMap& offsets_map);

  /*
   * Atomically loads OffsetMap entries and returns the resulting OffsetMap
   */
  OffsetMap load() const;

  /*
   * Performs atomic fetch_add on all OffsetMap entries and adds the entries.
   */
  OffsetMap fetchAdd(const OffsetMap& offsets_map);

 private:
  using FairRWLock = folly::SharedMutexWritePriority;
  FairRWLock rw_lock_;
  std::map<counter_type_t, std::atomic<uint64_t>> atomicCounterTypeMap_;
};

}} // namespace facebook::logdevice
