/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/debug.h"

using namespace facebook::logdevice::membership;

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {
//////// OperationMode ////////
using OperationMode = NodesConfigurationManager::OperationMode;

/* static */ OperationMode OperationMode::forClient() {
  OperationMode mode;
  mode.setFlags(kIsClient);
  ld_assert(mode.isValid());
  return mode;
}

/* static */ OperationMode OperationMode::forTooling() {
  OperationMode mode;
  mode.setFlags(kIsTooling);
  ld_assert(mode.isValid());
  return mode;
}

/* static */ OperationMode
OperationMode::forNodeRoles(NodeServiceDiscovery::RoleSet roles) {
  OperationMode mode;
  // Storage nodes or sequencers might get upgraded to proposers or coordinators
  // later, but they start out as an observer.
  if (roles.test(static_cast<size_t>(NodeRole::SEQUENCER))) {
    mode.setFlags(kIsSequencer);
  }
  if (roles.test(static_cast<size_t>(NodeRole::STORAGE))) {
    mode.setFlags(kIsStorageMember);
  }
  ld_assert(mode.isValid());
  return mode;
}

/* static */ OperationMode
OperationMode::upgradeToProposer(OperationMode current_mode) {
  current_mode.setFlags(kIsProposer);
  return current_mode;
}

bool OperationMode::isClient() const {
  return hasFlags(kIsClient);
}

bool OperationMode::isTooling() const {
  return hasFlags(kIsTooling);
}

bool OperationMode::isStorageMember() const {
  return hasFlags(kIsStorageMember);
}

bool OperationMode::isSequencer() const {
  return hasFlags(kIsSequencer);
}

bool OperationMode::isProposer() const {
  return hasFlags(kIsProposer);
}

bool OperationMode::isCoordinator() const {
  return hasFlags(kIsCoordinator);
}

bool OperationMode::isValid() const {
  if (isClient() && isCoordinator()) {
    return false;
  }

  if (!isClient() && !isTooling() && !isStorageMember() && !isSequencer()) {
    return false;
  }

  return true;
}

/* static */ constexpr const OperationMode::Flags OperationMode::kIsProposer;
/* static */ constexpr const OperationMode::Flags OperationMode::kIsCoordinator;

/* static */ constexpr const OperationMode::Flags OperationMode::kIsClient;
/* static */ constexpr const OperationMode::Flags OperationMode::kIsTooling;
/* static */ constexpr const OperationMode::Flags
    OperationMode::kIsStorageMember;
/* static */ constexpr const OperationMode::Flags OperationMode::kIsSequencer;

void OperationMode::setFlags(Flags flags) {
  mode_ |= flags;
}

bool OperationMode::hasFlags(Flags flags) const {
  return (mode_ & flags) != 0;
}

bool OperationMode::onlyHasFlags(Flags flags) const {
  return mode_ == flags;
}

//////// STATE MACHINE ////////
NodesConfigurationManager::NodesConfigurationManager(
    NCMTag,
    OperationMode mode,
    std::unique_ptr<ncm::Dependencies> deps)
    : mode_(mode), deps_(std::move(deps)) {
  ld_assert(mode_.isValid());
  ld_check(deps_ != nullptr);
}

void NodesConfigurationManager::init() {
  auto wp = weak_from_this();
  ld_check(wp.lock() != nullptr);
  deps_->init(wp);
}

void NodesConfigurationManager::initOnNCM() {
  deps_->dcheckOnNCM();
  startPollingFromStore();
}

void NodesConfigurationManager::startPollingFromStore() {
  deps_->readFromStoreAndActivateTimer();
}

void NodesConfigurationManager::onNewConfig(std::string new_config,
                                            bool overwrite) {
  deps_->dcheckOnNCM();

  auto new_version_opt =
      NodesConfigurationCodecFlatBuffers::extractConfigVersion(new_config);
  if (!new_version_opt) {
    // Invalid serialized blob.
    err = E::BADMSG;
    return;
  }
  auto local_nodes_config_ptr = local_nodes_config_.get();
  if (!overwrite && local_nodes_config_ptr &&
      local_nodes_config_ptr->getVersion() >= new_version_opt.value()) {
    // Early return
    return;
  }

  auto parsed_config_ptr =
      NodesConfigurationCodecFlatBuffers::deserialize(new_config);
  if (!parsed_config_ptr) {
    // err is set by deserialize()
    // TODO: Add and bump stats for deserialization error
    return;
  }

  onNewConfig(std::move(parsed_config_ptr), overwrite);
}

void NodesConfigurationManager::onNewConfig(
    std::shared_ptr<const NodesConfiguration> new_config,
    bool overwrite) {
  ld_check(new_config);
  deps_->dcheckOnNCM();

  // Since all write access happen on the NCM work context, no need to
  // synchronize here.
  auto local_nodes_config_ptr = local_nodes_config_.get();
  auto new_version = new_config->getVersion();
  if (local_nodes_config_ptr == nullptr || overwrite ||
      local_nodes_config_ptr->getVersion() < new_version) {
    // TODO: currently, we just conditionally overwrite the in-memory copy;
    // actual parsing / processing will be gradually added in follow up diffs.
    local_nodes_config_.update(std::move(new_config));
    ld_info("Updated config to version %lu...", new_version.val());
  }
}

}}}} // namespace facebook::logdevice::configuration::nodes
