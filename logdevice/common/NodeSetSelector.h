/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file  NodeSetSelector defines an interface for algorithms that select
 *        nodesets for a log based on cluster configuration.
 */

class NodeSetSelector {
 public:
  enum class Decision : uint8_t {
    KEEP = 0,     // nodeset is the same as the previous one
    NEEDS_CHANGE, // nodeset needs to be updated
    FAILED,       // failed to determine new nodeset for the log
  };

  /**
   * Additional options used for nodeset selection. May not be supported
   * in certain implementations.
   */
  struct Options {
    // Nodes that shouldn't be picked into nodeset
    // (as if they weren't in config).
    std::unordered_set<node_index_t> exclude_nodes;
  };

  /**
   * Determine the latest nodeset for a given log
   *
   * @param  log_id   logid of the log
   * @param  cfg      Configuration used to generate the nodeset
   * @param  prev     Previous nodeset, used to determine whether an update is
   *                  needed, can be nullptr indicating the information is not
   *                  available
   * @param  options  Additional options used for selection
   *
   * @return  a pair of <Status, std::unique_ptr<StorageSet>>, containing
   *          1) decision of the computation
   *          2) the new nodeset, only valid when 1) is Status::NEEDS_CHANGE,
   *             otherwise nullptr
   */
  virtual std::tuple<Decision, std::unique_ptr<StorageSet>>
  getStorageSet(logid_t log_id,
                const std::shared_ptr<Configuration>& cfg,
                const StorageSet* prev,
                const Options* options = nullptr) = 0;

  // The size of the generated storage set can be different from the one
  // specified in the config. E.g. if the nodeset size in config is smaller
  // than replication factor or bigger than cluster size, it'll be clamped.
  // This method returns the size of the nodeset that would generated
  // by getStorageSet().
  // Note that the existence of this method implies that all NodeSetSelector
  // implementations must use a deterministic nodeset size. It's not a problem
  // for the currently existing nodeset selectors, but may turn out to be too
  // constraining in future.
  //
  // If the config makes it impossible to pick a valid nodeset, returns 0.
  //
  // Parameters `storage_set_size_target` and `replication` are there just for
  // convenience; the same values can be obtained from LogAttributes using
  // `cfg` and `log_id`.
  virtual storage_set_size_t
  getStorageSetSize(logid_t log_id,
                    const std::shared_ptr<Configuration>& cfg,
                    folly::Optional<int> /* storage_set_size_target */,
                    ReplicationProperty /* replication */,
                    const Options* options = nullptr) {
    // The default implementation just selects a storage set and returns its
    // size. Subclasses can override this with something more efficient.
    auto s = getStorageSet(log_id, cfg, nullptr, options);
    return std::get<0>(s) == Decision::NEEDS_CHANGE ? std::get<1>(s)->size()
                                                    : 0ul;
  }

  virtual ~NodeSetSelector() {}
};

}} // namespace facebook::logdevice
