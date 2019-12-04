/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Memory.h>

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/nodeset_selection/NodeSetSelector.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file A trivial NodeSetSelector that repeats the nodeset from the previous
 * EpochMetaData (crashing if that is not possible). This is useful when writing
 * integration tests where nodesets are manually provisioned in the epoch store
 * and we wish to avoid nodeset selection to pick a different nodeset.
 */

class PickCurrentNodeSetSelector : public NodeSetSelector {
 public:
  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr /* ignored */
      ) override {
    Result res;
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        cfg->getLogGroupByIDShared(log_id);
    if (!logcfg) {
      res.decision = Decision::FAILED;
      return res;
    }
    res.decision = Decision::KEEP;
    return res;
  }
};

}} // namespace facebook::logdevice
