/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <folly/Range.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/membership/StorageStateTransitions.h"

namespace facebook { namespace logdevice { namespace membership {

inline constexpr folly::StringPiece toString(StorageState state) {
  switch (state) {
    case StorageState::NONE:
      return "none";
    case StorageState::NONE_TO_RO:
      return "n2ro";
    case StorageState::READ_ONLY:
      return "ro";
    case StorageState::READ_WRITE:
      return "rw";
    case StorageState::RW_TO_RO:
      return "rw2ro";
    case StorageState::DATA_MIGRATION:
      return "dm";
    case StorageState::INVALID:
      return "invalid";
  };
  return "internal error";
}

inline constexpr folly::StringPiece toString(MetaDataStorageState state) {
  switch (state) {
    case MetaDataStorageState::NONE:
      return "none";
    case MetaDataStorageState::METADATA:
      return "metadata";
    case MetaDataStorageState::PROMOTING:
      return "promoting";
    case MetaDataStorageState::INVALID:
      return "invalid";
  };
  return "internal error";
}

inline constexpr folly::StringPiece
toString(StorageStateTransition transition) {
  switch (transition) {
#define GEN_STR(_s)                \
  case StorageStateTransition::_s: \
    return #_s;

    GEN_STR(ADD_EMPTY_SHARD)
    GEN_STR(ADD_EMPTY_METADATA_SHARD)
    GEN_STR(REMOVE_EMPTY_SHARD)
    GEN_STR(ENABLING_READ)
    GEN_STR(COMMIT_READ_ENABLED)
    GEN_STR(ENABLE_WRITE)
    GEN_STR(DISABLING_WRITE)
    GEN_STR(COMMIT_WRITE_DISABLED)
    GEN_STR(START_DATA_MIGRATION)
    GEN_STR(DATA_MIGRATION_COMPLETED)
    GEN_STR(ABORT_ENABLING_READ)
    GEN_STR(ABORT_DISABLING_WRITE)
    GEN_STR(CANCEL_DATA_MIGRATION)
    GEN_STR(PROMOTING_METADATA_SHARD)
    GEN_STR(COMMIT_PROMOTION_METADATA_SHARD)
    GEN_STR(ABORT_PROMOTING_METADATA_SHARD)
    GEN_STR(MARK_SHARD_UNRECOVERABLE)
#undef GEN_STR
    case StorageStateTransition::Count:
      return "invalid";
  }
  return "internal error";
}

}}} // namespace facebook::logdevice::membership
