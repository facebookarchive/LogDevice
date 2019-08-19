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

class Configuration;
class EpochMetaData;

class NodeSetSelector {
 public:
  enum class Decision : uint8_t {
    // Nodeset is the same as previous one, but signature may be different.
    KEEP = 0,
    // Nodeset needs to be updated.
    NEEDS_CHANGE,
    // Failed to determine new nodeset for the log.
    FAILED,
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

  struct Result {
    Decision decision;

    // Below fields have the same meaning as in EpochMetaData.
    // See comments in EpochMetaData.h for documentation.
    // signature needs to be filled out even if decision is KEEP.

    // Nodeset selector implementation may choose to not use signature and
    // decide whether a change is needed based on contents of the config and
    // nodeset itself.
    uint64_t signature = 0;
    StorageSet storage_set;
    std::vector<double> weights;
  };

  /**
   * Determine if nodeset needs to be updated, and if it is, generate the new
   * nodeset.
   *
   * @param  log_id   logid of the log
   * @param  cfg      contains log attributes.
   *
   * @param nodes_configuration   contains the storage node membership and
   *                              varios attributes of storage nodes
   *
   * @param  target_nodeset_size  Recommended size of the nodeset to select.
   * @param  seed     Seed/salt for any RNGs/hash functions used by the nodeset
   *                  selector. Nondeterministic nodeset selector may ignore it.
   * @param  prev     Previous nodeset, used to determine whether an update is
   *                  needed. Can be nullptr indicating the information is not
   *                  available. If nullptr, Result::decision must not be KEEP.
   * @param  options  Additional options used for selection
   *
   * Currently it is required that getStorageSet() never requests an update
   * twice in a row. I.e. if you call getStorageSet(), then convert the returned
   * result into EpochMetaData, then call getStorageSet() again on that
   * EpochMetaData with the same arguments, the result will be Decision::KEEP.
   * This is usually achieved in one of two ways:
   *   1. Make nodeset selector deterministic, and make it return KEEP if the
   *      newly generated nodeset is equal to the previous one.
   *   2. Use `signature`. Set signature to a hash of the parts of input that
   *      potentially affect the nodeset, e.g. nodes config, target nodeset
   *      size, seed, nodeset selector type. When selecting a nodeset, first
   *      calculate the hash of the current inputs and compare it to signature
   *      of the existing nodeset. If it matches, don't generate a new nodeset
   *      and return KEEP.
   * Note that deterministic nodeset selectors may still want to use signature
   * as an optimization, to avoid generating new nodeset if nothing changed in
   * the input.
   */
  virtual Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr) = 0;

  virtual ~NodeSetSelector() {}
};

}} // namespace facebook::logdevice
