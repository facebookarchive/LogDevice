/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/small_vector.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

constexpr size_t COPYSET_INLINE_DEFAULT = 6;

/**
 * In-memory representation of a copyset. Uses a folly::small_vector with a
 * customizable number of inlined elements.
 *
 * @tparam inline_  Number of elements to store inline.
 */
template <size_t inline_ = COPYSET_INLINE_DEFAULT>
using copyset_custsz_t = folly::small_vector<ShardID, inline_>;

template <size_t inline_ = COPYSET_INLINE_DEFAULT>
using copyset_chain_custsz_t = folly::small_vector<StoreChainLink, inline_>;

/**
 * In-memory representation of a copyset with default number of elements
 * stored inline.
 */
typedef copyset_custsz_t<> copyset_t;
typedef copyset_chain_custsz_t<> copyset_chain_t;

}} // namespace facebook::logdevice
