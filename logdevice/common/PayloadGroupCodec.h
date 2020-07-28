/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/Portability.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * Allows encoding/decoding PayloadGroup objects to/from binary
 * representation.
 */
class PayloadGroupCodec {
 public:
  /**
   * Encodes single PayloadGroup to binary representation.
   */
  static std::string encode(const PayloadGroup& payload_group);

  /**
   * Decodes single PayloadGroup from binary representation.
   * Returns number of bytes consumed, or 0 in case of error.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary, PayloadGroup& payload_group_out);

  /**
   * Decodes PayloadGroups from binary representation.
   * Decoded PayloadGroups are appended to payload_groups_out on success.
   * Returns number of bytes consumed, or 0 in case of error.
   */
  FOLLY_NODISCARD
  static size_t decode(Slice binary,
                       std::vector<PayloadGroup>& payload_groups_out);
};

}} // namespace facebook::logdevice
