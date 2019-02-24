/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Map.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

// This class keeps track of the _intermediary_ state for each shard and
// the first known timestamp of that transition. It is intended to be used by
// the NCM such that NCM could propose updates to transition shards out of the
// intermediary states after a certain timeout.
//
// The tracker will ignore lower-versioned NodesConfiguration-s. Since the
// tracker may not hear about each NC, the timestamp(s) it has for each shard
// transition may be later than when the transition happened; however, this is
// sufficient for time-out purposes.
//
// The tracker keeps track of StorageState and MetaDataStorageState, but not
// Sequencer states, since sequencer state transitions have no intermediary
// states.
class ShardStateTracker {
 public:
  // Update the state of the tracker:
  // * add new shards that entered an intermediary state;
  // * update shards that transitioned into a different intermediary state (from
  //   what we previously knew of);
  // * remove shards that transitioned out of an intermediary state;
  void onNewConfig(std::shared_ptr<const NodesConfiguration> config);

  // Compile all shard states that are known to have happened before or at
  // till_timestamp into a StorageMembership Update that includes the
  // transitions for those shards into a non-intermediary state.
  folly::Optional<NodesConfiguration::Update>
  extractNCUpdate(SystemTimestamp till_timestamp) const;

 private:
  struct Entry {
    membership::ShardState shard_state_;
    SystemTimestamp since_timestamp_{};
  };

  folly::Optional<Entry> getEntry(node_index_t node_idx,
                                  shard_index_t shard_idx);

  // Implementation note: assuming that the number of shards in an
  // interintermediary state is relatively small, we don't do anything clever
  // here. Should that become an issue, we could index or bucket shard states by
  // time, etc.
  using PerNodeMap = folly::F14FastMap<shard_index_t, Entry>;
  using NodeStatesMap = folly::F14FastMap<node_index_t, PerNodeMap>;
  NodeStatesMap node_states_;

  membership::MembershipVersion::Type last_known_nc_version_{
      membership::MembershipVersion::EMPTY_VERSION};
  membership::MembershipVersion::Type last_known_storage_membership_version_{
      membership::MembershipVersion::EMPTY_VERSION};
};

}}}} // namespace facebook::logdevice::configuration::nodes
