/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

/**
 * @file A LogDevice server (a Node in a LogDevice cluster) is
 * identified by a pair (generation, cluster_index) where generation
 * is a 16-bit signed with only positive values valid (highest-order
 * bit reserved), cluster_index is a 16-bit unsigned. Server addresses
 * are globally unique. An Address object that identifies a server can
 * be serialized and reconstructed on a different box and still be
 * understood to identify the same server instance.
 *
 * Generation 0 in some contexts means "any generation".
 * Eventually we should probably get rid of generations altogether and use
 * node_index_t directly everywhere.
 */

typedef int16_t node_index_t; // offset of a node in a cluster, zero-based,
                              // only the low 15 bits are usable.

typedef uint16_t node_gen_t; // counts how many times a node at a particular
                             // cluster offset was replaced. May wrap around.

// the type of values representing sizes of sets of nodes
typedef std::make_unsigned<node_index_t>::type nodeset_size_t;

using NodeSetIndices = std::vector<node_index_t>;

struct NodeID {
  explicit NodeID(node_index_t ind, node_gen_t gen = 0)
      : val_((ind << 16) | gen) {
    ld_check(ind >= 0);
  }

  // Default constructor makes an invalid NodeID. Used for array initialization.
  NodeID() : val_(1u << 31) {}

  node_index_t index() const {
    ld_check(isNodeID());
    return val_ >> 16;
  }

  // Zero is a special value meaning "any generation".
  node_gen_t generation() const {
    return val_ & 0xffff;
  }

  bool isNodeID() const {
    return !(val_ & (1u << 31));
  }

  explicit operator unsigned() const {
    return val_;
  } // used in error messages

  // More relaxed comparison of `this` and `other` than operator==. Returns true
  // if both the index and generation are equal but if the generation of either
  // operand is 0, only compare the index.
  // This function will be used as a relaxed comparison function as we
  // deprecate the use of generation in all components over time.
  bool equalsRelaxed(NodeID other) const {
    if (generation() == 0 || other.generation() == 0) {
      return index() == other.index();
    }
    return *this == other;
  }

  std::string toString() const {
    if (!isNodeID()) {
      if (val_ == (1u << 31)) {
        return "[invalid NodeID]";
      }
      // Looks like ClientID.
      return "[invalid NodeID: C" + std::to_string(val_ ^ (1u << 31)) + "]";
    }
    if (generation() == 0) {
      return "N" + std::to_string(index());
    }
    return "N" + std::to_string(index()) + ":" + std::to_string(generation());
  }

  struct Hash {
    std::size_t operator()(const NodeID& nid) const {
      return nid.index();
    }
  };

 private:
  uint32_t val_;

  friend bool operator==(const NodeID& a, const NodeID& b);
  friend bool operator!=(const NodeID& a, const NodeID& b);
  friend bool operator<(const NodeID& a, const NodeID& b);
} __attribute__((__packed__));

static_assert(sizeof(node_index_t) == 2, "node_index_t must be 2 bytes");
static_assert(sizeof(node_gen_t) == 2, "node_gen_t must be 2 bytes");

constexpr node_index_t NODE_INDEX_INVALID = -1;
constexpr node_index_t NODE_INDEX_INVALID2 = -2;

}} // namespace facebook::logdevice
