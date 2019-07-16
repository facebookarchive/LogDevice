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

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Test NodeSetSelector thats returns a nodeset specified by the
 *       previous setStorageSet() call regardless of log_id or config. However,
 *       caller needs to ensure that the nodeset provided must be consistent
 *       with the configuration.
 */

class TestNodeSetSelector : public NodeSetSelector {
 public:
  void setStorageSet(StorageSet storage_set) {
    ld_check(!storage_set.empty());
    storage_set_ = std::move(storage_set);
  }

  // must be called after setStorageSet();
  Result getStorageSet(logid_t /*log_id*/,
                       const Configuration* /*cfg*/,
                       const configuration::nodes::NodesConfiguration&,
                       nodeset_size_t /* target_nodeset_size */,
                       uint64_t /* seed */,
                       const EpochMetaData* prev,
                       const Options* /*options*/ = nullptr /* ignored */
                       ) override {
    ld_check(!storage_set_.empty());
    Result res;
    res.storage_set = storage_set_;
    res.decision = (prev && prev->shards == res.storage_set)
        ? Decision::KEEP
        : Decision::NEEDS_CHANGE;
    return res;
  }

 private:
  StorageSet storage_set_;
};

}} // namespace facebook::logdevice
