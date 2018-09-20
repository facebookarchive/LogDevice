/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ServerRecordFilter.h"

/**
 *  @file (EXPERIMENTAL) Server-side filtering attributes. Used by server
 *                       to construct record filter. Use with caution.
 */

namespace facebook { namespace logdevice {

/**
 * @param       filter_type     Used to construct ServerRecordFilter to do
 *                              record filtering. @see ServerRecordFilter.h
 *              filter_key1     param for constructing ServerRecordFilter
 *              filter_key2     param for constructing ServerRecordFilter
 */

struct ReadStreamAttributes {
  ReadStreamAttributes() : filter_type(ServerRecordFilterType::NOFILTER) {}

  ReadStreamAttributes(ServerRecordFilterType type,
                       const std::string& key1,
                       const std::string& key2)
      : filter_type(type), filter_key1(key1), filter_key2(key2) {}

  ReadStreamAttributes(const ReadStreamAttributes& rhs)
      : filter_type(rhs.filter_type),
        filter_key1(rhs.filter_key1),
        filter_key2(rhs.filter_key2) {}

  ReadStreamAttributes& operator=(const ReadStreamAttributes& rhs) {
    filter_type = rhs.filter_type;
    filter_key1 = rhs.filter_key1;
    filter_key2 = rhs.filter_key2;
    return *this;
  }

  bool operator==(const ReadStreamAttributes& other) const {
    return filter_type == other.filter_type &&
        filter_key1 == other.filter_key1 && filter_key2 == other.filter_key2;
  }

  ServerRecordFilterType filter_type;
  std::string filter_key1;
  std::string filter_key2;
};
}} // namespace facebook::logdevice
