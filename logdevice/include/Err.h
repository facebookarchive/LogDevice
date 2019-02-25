/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <cstring>
#include <iosfwd>

#include "logdevice/include/EnumMap.h"

namespace facebook { namespace logdevice {

/**
 * Errors reported by various methods and functions in the public LogDevice
 * API as well as in the implementation. I chose a short name to improve
 * the readability of error handling code.
 */
enum class E : std::uint16_t {
#define ERROR_CODE(id, val, str) id = val,
#include "logdevice/include/errors.inc"
  UNKNOWN = 1024, // a special value that variables of type E (Status) may
                  // assume before any status is known. Never reported.
  MAX
};

// a longer, more descriptive alias to use in function declarations
typedef E Status;

// hasher to allow Status to be used as a key in an unordered_map
struct StatusHasher {
  size_t operator()(Status status) const {
    return static_cast<size_t>(status);
  }
};

// a (name, description) record for an error code
struct ErrorCodeInfo {
  const char* name;
  const char* description;

  bool valid() const {
    return name != nullptr && strcmp(name, "UNKNOWN") != 0;
  }
  bool operator==(const ErrorCodeInfo& rhs) const {
    return name == rhs.name && description == rhs.description;
  }
};

/**
 * All LogDevice functions report failures through this thread-local. This
 * is a LogDevice-level errno.
 */
extern __thread E err;

/**
 * errorStrings() returns a sole instance of an EnumMap specialization that
 * maps logdevice::E error codes into short names and longer full
 * descriptions (that also include name)
 */
using ErrorCodeStringMap = EnumMap<E, ErrorCodeInfo, E::UNKNOWN>;
const ErrorCodeStringMap& errorStrings();

inline const char* error_name(E error) {
  return errorStrings()[error].name;
}

inline const char* error_description(E error) {
  return errorStrings()[error].description;
}

std::ostream& operator<<(std::ostream&, const E&);
}} // namespace facebook::logdevice
