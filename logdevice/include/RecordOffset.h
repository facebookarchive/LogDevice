/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class OffsetMap;

class RecordOffset {
  /**
   * Structure that contains information on amount of data written to the log
   * Currently supports only BYTE_OFFSET.
   * BYTE_OFFSET represents the amount of data in bytes written to the log.
   */
 public:
  /*
   * Constructs an invalid RecordOffset
   */
  RecordOffset() noexcept;
  /*
   * Constructs a RecordOffset with specified BYTE_OFFSET
   * @param list of pairs<counter_type, counter_value>. Constructor will call
   *        setCounter(counter_type, counter_value).
   */
  RecordOffset(std::initializer_list<std::pair<const counter_type_t, uint64_t>>
                   list) noexcept;

  /**
   * get counter_type value from CounterTypeMap
   * @param counter_type to read
   * @return  value of counter_type
   */
  uint64_t getCounter(const counter_type_t counter_type) const;

  /**
   * set counter_type value from CounterTypeMap
   * @param counter_type counter_type_t to add to counterTypeMap_
   * @param counter_val  value to set for counter_type
   */
  void setCounter(const counter_type_t counter_type, uint64_t counter_value);

  /*
   * Prints the content of the RecordOffset in a string format.
   */
  std::string toString() const;

  /*
   * Checks if counter_type is valid
   * @param counter_type to check
   * @return true if valid, false otherwise
   */
  bool isValidOffset(counter_type_t counter_type) const;
  /*
   * RecordOffset is valid if it contains at least one valid counter
   * @return true if the RecordOffset object is valid.
   */
  bool isValid() const;

  bool operator==(const RecordOffset& record_offset) const;
  bool operator!=(const RecordOffset& record_offset) const;

  RecordOffset(RecordOffset&& record_offset) noexcept;
  RecordOffset& operator=(RecordOffset&& record_offset) noexcept;
  RecordOffset(const RecordOffset& record_offset) noexcept;
  RecordOffset& operator=(const RecordOffset& record_offset) noexcept;

  ~RecordOffset();

 private:
  friend class OffsetMap;
  std::unique_ptr<OffsetMap> offset_map;
};

}} // namespace facebook::logdevice
