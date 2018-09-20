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
  std::tuple<Decision, std::unique_ptr<StorageSet>> getStorageSet(
      logid_t /*log_id*/,
      const std::shared_ptr<Configuration>& /*cfg*/,
      const StorageSet* prev,
      const Options* /*options*/ = nullptr /* ignored */
      ) override {
    ld_check(!storage_set_.empty());
    if (prev && storage_set_ == *prev) {
      return std::make_tuple(Decision::KEEP, nullptr);
    }
    return std::make_tuple(
        Decision::NEEDS_CHANGE, std::make_unique<StorageSet>(storage_set_));
  }

  storage_set_size_t getStorageSetSize(logid_t,
                                       const std::shared_ptr<Configuration>&,
                                       folly::Optional<int>,
                                       ReplicationProperty,
                                       const Options* /*options*/) override {
    return storage_set_.size();
  }

 private:
  StorageSet storage_set_;
};

}} // namespace facebook::logdevice
