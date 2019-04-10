/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice { namespace configuration {

struct SequencersConfig {
  // List of sequencer nodes' ids. The node indices are always sequential,
  // starting from 0, even if some of these nodes don't exist or aren't
  // sequencer nodes. E.g. if config contains sequencer nodes 1, 3, 5
  // (with weight 1), then
  // `nodes`   vector will be {0, 1, 2, 3, 4, 5},
  // `weights` vector will be {0, 1, 0, 1, 0, 1}.
  // This is needed to keep hash function consistent when nodes are
  // added/removed in the middle of the node index range.
  std::vector<NodeID> nodes;

  // normalized effective weights of sequencer nodes
  std::vector<double> weights;

  bool operator==(const SequencersConfig& rhs) const {
    return nodes == rhs.nodes && weights == rhs.weights;
  }

  bool operator!=(const SequencersConfig& rhs) const {
    return !(*this == rhs);
  }
};

}}} // namespace facebook::logdevice::configuration
