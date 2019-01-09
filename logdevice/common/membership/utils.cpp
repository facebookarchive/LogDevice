/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/membership/utils.h"

namespace facebook { namespace logdevice { namespace membership {

#define GEN_FLAG(_t, _v, _f, _o) \
  if ((_v)&_t::_f) {             \
    if (!(_o).empty()) {         \
      (_o) += "|";               \
    }                            \
    (_o) += #_f;                 \
  }

std::string StorageStateFlags::toString(StorageStateFlags::Type flags) {
  std::string res;
  if (flags == StorageStateFlags::NONE) {
    return "NONE";
  }
#define FLAG(_f) GEN_FLAG(StorageStateFlags, flags, _f, res)
  FLAG(UNRECOVERABLE)
#undef FLAG
  return res;
}

std::string Condition::toString(StateTransitionCondition conditions) {
  if (conditions == Condition::NONE) {
    return "NONE";
  }
  std::string res;
#define FLAG(_f) GEN_FLAG(Condition, conditions, _f, res)
  FLAG(EMPTY_SHARD)
  FLAG(LOCAL_STORE_READABLE)
  FLAG(NO_SELF_REPORT_MISSING_DATA)
  FLAG(CAUGHT_UP_LOCAL_CONFIG)
  FLAG(COPYSET_CONFIRMATION)
  FLAG(LOCAL_STORE_WRITABLE)
  FLAG(WRITE_AVAILABILITY_CHECK)
  FLAG(CAPACITY_CHECK)
  FLAG(FMAJORITY_CONFIRMATION)
  FLAG(DATA_MIGRATION_COMPLETE)
  FLAG(SELF_AWARE_MISSING_DATA)
  FLAG(CANNOT_ACCEPT_WRITES)
  FLAG(METADATA_WRITE_AVAILABILITY_CHECK)
  FLAG(METADATA_CAPACITY_CHECK)
  FLAG(FORCE)

#undef FLAG
  return res;
}

#undef GEN_FLAG

}}} // namespace facebook::logdevice::membership
