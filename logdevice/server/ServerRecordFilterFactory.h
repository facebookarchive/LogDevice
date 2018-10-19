/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <string>

#include <folly/Memory.h>
#include <folly/Range.h>

#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/ServerRecordFilter.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerRecordEqualityFilter.h"
#include "logdevice/server/ServerRecordRangeFilter.h"

namespace facebook { namespace logdevice {

/**
 *  @file Factory class for creatin a ServerRecordFilter predicate with
 *        specified type.
 */

class ServerRecordFilterFactory {
 public:
  /**
   *  @param type  specifies type of filter we are constructing here.
   *               Type is defined in ServerRecordFilter.h
   *         key1  param for constructing ServerRecordFilter
   *         key2  param for constructing ServerRecordFilter, only used for
   *               ServerRecordRangeFilter. Serves as high_limit_.
   *  @return      unique_ptr to a ServerRecordFilter object; return nullptr
   *               if parameters are invalid.
   */
  static std::unique_ptr<ServerRecordFilter> create(ServerRecordFilterType type,
                                                    folly::StringPiece key1,
                                                    folly::StringPiece key2) {
    switch (type) {
      case ServerRecordFilterType::EQUALITY:
        return std::make_unique<ServerRecordEqualityFilter>(key1);
      case ServerRecordFilterType::RANGE:
        if (key1 > key2) {
          ld_error("ServerRecordRangeFilter failed to construct. Low limit is "
                   "greater than high limit. "
                   "key1: %s, key2: %s",
                   key1.data(),
                   key2.data());
          return nullptr;
        }
        return std::make_unique<ServerRecordRangeFilter>(key1, key2);
      case ServerRecordFilterType::NOFILTER:
        return nullptr;
      default:
        ld_error(
            "ServerRecordFilterType provided in constructor is not defined. "
            "key1: %s, key2: %s, type value: %d",
            key1.data(),
            key2.data(),
            static_cast<int>(type));
    }
    return nullptr;
  }

  static std::unique_ptr<ServerRecordFilter>
  create(const ReadStreamAttributes& attrs) {
    return create(attrs.filter_type, attrs.filter_key1, attrs.filter_key2);
  }
};
}} // namespace facebook::logdevice
