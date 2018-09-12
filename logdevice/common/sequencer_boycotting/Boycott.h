/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice {
struct Boycott {
  Boycott() {}
  Boycott(node_index_t node_index, std::chrono::nanoseconds time)
      : Boycott(node_index, time, false) {}
  Boycott(node_index_t node_index, std::chrono::nanoseconds time, bool reset)
      : node_index(node_index), boycott_in_effect_time(time), reset(reset) {}

  node_index_t node_index{-1};
  // At what time should the boycott be in effect. This may be in the future, as
  // the time it goes into effect should be after it has been propagated to all
  // nodes
  // nano seconds with signed 64-bit covers a range of at least +/-292 years
  std::chrono::duration<int64_t, std::nano> boycott_in_effect_time{0};
  // if false, the node is supposed to be boycotted.
  // if true, if there is another boycott for the same node with a older time
  //          than this instance, disregard that boycott
  bool reset{false};

  bool operator==(const Boycott& other) const {
    return node_index == other.node_index &&
        boycott_in_effect_time == other.boycott_in_effect_time &&
        reset == other.reset;
  }
  bool operator!=(const Boycott& other) const {
    return !(*this == other);
  }
};
}} // namespace facebook::logdevice
