/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"

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

void NodesConfigurationManager::onNewConfig(std::string new_config) {
  deps_->dcheckOnNCM();

  auto new_version_opt =
      NodesConfigurationCodecFlatBuffers::extractConfigVersion(new_config);
  if (!new_version_opt) {
    // Invalid serialized blob.
    err = E::BADMSG;
    return;
  }
  if (hasProcessedVersion(new_version_opt.value())) {
    // Early return to avoid deserialization
    return;
  }

  auto parsed_config_ptr =
      NodesConfigurationCodecFlatBuffers::deserialize(new_config);
  if (!parsed_config_ptr) {
    // err is set by deserialize()
    // TODO: Add and bump stats for deserialization error
    return;
  }

  onNewConfig(std::move(parsed_config_ptr));
}

void NodesConfigurationManager::onNewConfig(
    std::shared_ptr<const NodesConfiguration> new_config) {
  ld_check(new_config);
  deps_->dcheckOnNCM();

  // Since all accesses to staged and pending configs happen in the NCM context,
  // no need to synchronize here.
  auto new_config_version = new_config->getVersion();
  if (hasProcessedVersion(new_config_version) ||
      !shouldStageVersion(new_config_version)) {
    return;
  }
  // Incoming config has a higher version, use it as the staged config
  staged_nodes_config_ = std::move(new_config);
  maybeProcessStagedConfig();
}

void NodesConfigurationManager::maybeProcessStagedConfig() {
  deps_->dcheckOnNCM();

  // nothing is staged or we're already processing a version
  if (!staged_nodes_config_ || pending_nodes_config_) {
    return;
  }
  ld_check(!hasProcessedVersion(staged_nodes_config_->getVersion()));

  // process the staged one now.
  pending_nodes_config_ = std::move(staged_nodes_config_);
  auto futures = fulfill_on_all_workers<folly::Unit>(
      deps_->processor_,
      [config = pending_nodes_config_](folly::Promise<folly::Unit> p) {
        Worker* w = Worker::onThisThread();
        ld_debug("Processing config version %lu on Worker %d of pool %s",
                 config->getVersion().val(),
                 w->idx_.val(),
                 workerTypeStr(w->worker_type_));

        // TODO: perhaps return highest config version?
        w->getUpdateableConfig()->updateableNodesConfiguration()->update(
            config);
        w->onNodesConfigurationUpdated();
        p.setValue();
      },
      RequestType::NODES_CONFIGURATION_MANAGER,
      /* with_retrying = */ true);

  // If one of the worker is stuck, it will block us from making progress.
  // This is probably OK since we would need to propagate new configs to every
  // worker anyway, so there's little we could do in that case.
  // TODO: handle / monitor worker config processing getting stuck, e.g., by
  // timeout.
  folly::collectAllSemiFuture(std::move(futures))
      .toUnsafeFuture()
      .thenTry([pending_nodes_config = pending_nodes_config_,
                ncm_weak_ptr = weak_from_this()](auto&& t) mutable {
        // The collective future will complete in the last finished worker
        // thread. If the NCM is still alive, send a request to notify NCM
        // context that we've processed the config update.
        auto ncm = ncm_weak_ptr.lock();
        ld_debug("processing complete for version %lu",
                 pending_nodes_config->getVersion().val());
        // Assume a worker never fails to process a new config.
        ld_assert(t.hasValue());
        if (ncm) {
          auto req =
              ncm->deps()->makeNCMRequest<ncm::ProcessingFinishedRequest>(
                  std::move(pending_nodes_config));
          ncm->deps()->processor_->postWithRetrying(req);
        }
      });
}

void NodesConfigurationManager::onProcessingFinished(
    std::shared_ptr<const NodesConfiguration> new_config) {
  deps_->dcheckOnNCM();
  ld_check(new_config);

  auto new_version = new_config->getVersion();
  ld_check(pending_nodes_config_);
  ld_check(new_version == pending_nodes_config_->getVersion());

  ld_check(!hasProcessedVersion(new_version));
  // Only the NCM thread is allowed to update local_nodes_config_
  local_nodes_config_.update(std::move(pending_nodes_config_));
  ld_info("Updated local nodes config to version %lu...", new_version.val());

  maybeProcessStagedConfig();
}

bool NodesConfigurationManager::shouldStageVersion(
    membership::MembershipVersion::Type version) {
  return !staged_nodes_config_ || staged_nodes_config_->getVersion() < version;
}

bool NodesConfigurationManager::hasProcessedVersion(
    membership::MembershipVersion::Type version) {
  auto local_nodes_config_ptr = local_nodes_config_.get();
  return local_nodes_config_ptr != nullptr &&
      local_nodes_config_ptr->getVersion() >= version;
}

}}}} // namespace facebook::logdevice::configuration::nodes
