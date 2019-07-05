/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CopySetManager.h"

#include <algorithm>
#include <utility>

#include <folly/Random.h>

#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

static StorageSet
getEffectiveNodeSet(const StorageSet& full_nodeset,
                    const membership::StorageMembership& storage_membership) {
  return storage_membership.writerView(full_nodeset);
}

CopySetManager::CopySetManager(std::unique_ptr<CopySetSelector> css,
                               std::shared_ptr<NodeSetState> nodeset_state)
    : underlying_selector_(std::move(css)), nodeset_state_(nodeset_state) {
  ld_check(underlying_selector_);
  ld_check(nodeset_state_);
}

CopySetManager::~CopySetManager() = default;

void CopySetManager::shuffleCopySet(StoreChainLink* copyset,
                                    int size,
                                    bool chain) {
  if (shuffle_copysets_ && !chain) {
    // Do not shuffle copysets on chainsending, otherwise it would:
    // 1. potentially cause extra cross-domain hops when storing the record
    // 2. cause uneven load distribution on rebuilding (t9522847)
    std::shuffle(copyset, copyset + size, folly::ThreadLocalPRNG());
  }
}

void CopySetManager::disableCopySetShuffling() {
  shuffle_copysets_ = false;
}

bool CopySetManager::matchesConfig(
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  ld_check(!full_nodeset_.empty());
  return effective_nodeset_ ==
      getEffectiveNodeSet(
             full_nodeset_, *nodes_configuration.getStorageMembership());
}

void CopySetManager::prepareConfigMatchCheck(
    StorageSet nodeset,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  full_nodeset_ = std::move(nodeset);
  effective_nodeset_ = getEffectiveNodeSet(
      full_nodeset_, *nodes_configuration.getStorageMembership());
}

}} // namespace facebook::logdevice
