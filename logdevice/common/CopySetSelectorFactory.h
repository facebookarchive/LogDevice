/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  create a CopySetSelector object with the given type.
 */

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class CopySetManager;
class ServerConfig;
struct Settings;

class CopySetSelectorFactory {
 public:
  // create a copyset selector
  // @param log_attrs
  //   Attributes of the log `logid`. Can be nullptr if you don't care about
  //   balance (e.g. ShardSetAccessor).
  // @param init_rng
  //   Random number generator to use *only* when initializing the copyset
  //   selector. To set the RNG that is used during actual copyset selection,
  //   pass it as argument to select()/augment().
  //   If nullptr, a default thread safe RNG is used.
  //   (We didn't make select() and augment() use init_rng by default because
  //   that would be error-prone if init_rng is not thread safe.)
  static std::unique_ptr<CopySetSelector>
  create(logid_t logid,
         const EpochMetaData& epoch_metadata,
         std::shared_ptr<NodeSetState> nodeset_state,
         std::shared_ptr<const configuration::nodes::NodesConfiguration>
             nodes_configuration,
         folly::Optional<NodeID> my_node_id,
         const logsconfig::LogAttributes* log_attrs,
         const Settings& settings,
         RNG& init_rng = DefaultRNG::get());

  // create a copyset selector & manager
  static std::unique_ptr<CopySetManager>
  createManager(logid_t logid,
                const EpochMetaData& epoch_metadata,
                std::shared_ptr<NodeSetState> nodeset_state,
                std::shared_ptr<const configuration::nodes::NodesConfiguration>
                    nodes_configuration,
                folly::Optional<NodeID> my_node_id,
                const logsconfig::LogAttributes* log_attrs,
                const Settings& settings,
                bool sticky_copysets,
                size_t sticky_copysets_block_size,
                std::chrono::milliseconds sticky_copysets_block_max_time);
};

}} // namespace facebook::logdevice
