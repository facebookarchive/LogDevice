/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <type_traits>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  A seal record indicates that a log was sealed up to the specified
 *        epoch, and that a storage node shouldn't accept any records within
 *        that and earlier epochs.
 */

struct Seal {
  epoch_t epoch;   // epoch number the log was sealed up to (and including)
  NodeID seq_node; // sequencer node that sealed the log

  Seal() noexcept : epoch(EPOCH_INVALID) {}
  Seal(epoch_t epoch, NodeID node) : epoch(epoch), seq_node(node) {}

  bool empty() const {
    return epoch == EPOCH_INVALID;
  }

  bool valid() const {
    return epoch_valid(epoch) && seq_node.isNodeID();
  }

  bool validOrEmpty() const {
    return empty() || valid();
  }

  std::string toString() const {
    return "e" + std::to_string(epoch.val_) + seq_node.toString();
  }

  bool operator<(const Seal& rhs) const {
    return epoch < rhs.epoch;
  }
  bool operator>(const Seal& rhs) const {
    return epoch > rhs.epoch;
  }
  bool operator==(const Seal& rhs) const {
    return epoch == rhs.epoch && seq_node == rhs.seq_node;
  }
} __attribute__((packed));

static_assert(sizeof(Seal) == 8, "Seal has to be exactly 8-bytes wide");

}} // namespace facebook::logdevice
