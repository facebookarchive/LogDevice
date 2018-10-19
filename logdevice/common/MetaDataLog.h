/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Utilities for handling metadata logids.
 */

namespace MetaDataLog {

// use the first (most significant) bit in logid_t internal type
// to indicate a metadata logid
const uint64_t ID_SHIFT = LOGID_BITS_INTERNAL - 1;
const uint64_t LOGID_MASK = ~(1ull << ID_SHIFT);

// return the metadata logid of a log
constexpr logid_t metaDataLogID(logid_t log_id) {
  return logid_t(log_id.val_ | 1ull << ID_SHIFT);
}

// return true if the log_id given is a metadata logid
constexpr bool isMetaDataLog(logid_t log_id) {
  return static_cast<bool>(log_id.val_ & 1ull << ID_SHIFT);
}

// return the data logid of a log
constexpr logid_t dataLogID(logid_t log_id) {
  return logid_t(log_id.val_ & LOGID_MASK);
}

// max metadata logid is the metadata logid of the max data logid
constexpr logid_t META_LOGID_MAX(metaDataLogID(LOGID_MAX));

// must be consistent with the LOGID_MAX_INTERNAL defined in types_internal.h
static_assert(LOGID_MAX_INTERNAL.val_ == META_LOGID_MAX.val_,
              "max logid of all logs must equal to max metadata logid");
} // namespace MetaDataLog

}} // namespace facebook::logdevice
