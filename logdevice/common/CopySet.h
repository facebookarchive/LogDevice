/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/small_vector.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/ShardID.h"

namespace facebook { namespace logdevice {

constexpr size_t COPYSET_INLINE_DEFAULT = 6;

// a StoreChainLink describes one link in a message delivery chain
struct StoreChainLink {
  // Shard to which a STORE is to be delivered.
  ShardID destination;

  // id of the "client" connection on the destination node whose remote end
  // (from the point of view of that node) is the sender of this message.
  // The destination server uses this ClientID to send a reply to the message
  // directly to the sender.
  ClientID origin;

  bool operator==(const StoreChainLink& other) const {
    return destination == other.destination && origin == other.origin;
  }

  std::string toString() const {
    return destination.toString() + origin.toString();
  }
};

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
