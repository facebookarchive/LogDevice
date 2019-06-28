/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/stats/Stats.h"

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
  // we allow emgergency/oncall toolings to make changes to
  // nodes configuration
  mode.setFlags(kIsTooling | kIsProposer);
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

void OperationMode::upgradeToProposer() {
  setFlags(kIsProposer);
}

bool OperationMode::isClient() const {
  return hasFlags(kIsClient);
}

bool OperationMode::isClientOnly() const {
  return onlyHasFlags(kIsClient);
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

bool NodesConfigurationManager::init(
    std::shared_ptr<const NodesConfiguration> init_nc,
    bool wait_until_initialized) {
  if (shutdownSignaled()) {
    return true;
  }
  auto wp = weak_from_this();
  ld_check(wp.lock() != nullptr);
  deps_->init(wp, std::move(init_nc));
  if (wait_until_initialized) {
    // We should not be on _any_ worker thread, otherwise we will deadlock.
    deps_->dcheckNotOnProcessor();
    return initialized_.try_wait_for(std::chrono::seconds(30));
  }
  return true;
}

void NodesConfigurationManager::upgradeToProposer() {
  // TODO; this is done separately from init because a storage node should only
  // be upgraded to a proposer after it sees itself as not-NONE in the
  // membership config.
  //
  // For now we manually set this in tests
  mode_.upgradeToProposer();
}

bool NodesConfigurationManager::shouldDoConsistentConfigFetch() const {
  return mode_.isStorageMember() && getConfig() == nullptr;
}

void NodesConfigurationManager::shutdown() {
  shutdown_signaled_.store(true);
  ld_info("NCM shutting down...");
  deps_->shutdown();
  // Just in case shutdown was signaled before / while the Processor is handling
  // the InitRequest.
  initialized_.wait();
  // Since the Processor doesn't complete pending requests when joining the
  // worker threads, we wait for the ShutdownRequest to execute before
  // returning.
  shutdown_completed_.wait();
}

bool NodesConfigurationManager::shutdownSignaled() const {
  return shutdown_signaled_.load();
}

void NodesConfigurationManager::update(NodesConfiguration::Update update,
                                       CompletionCb callback) {
  if (shutdownSignaled()) {
    callback(E::SHUTDOWN, nullptr);
    return;
  }

  ncm::UpdateContext ctx{deps()->getTraceLogger()};
  ctx.data_.update_ = std::move(update);

  // ensure we are allowed to propose updates
  if (!mode_.isProposer()) {
    ctx.setStatus(E::ACCESS);
    callback(E::ACCESS, nullptr);
    return;
  }
  STAT_INCR(deps_->getStats(), nodes_configuration_manager_updates_requested);

  std::unique_ptr<Request> req = deps()->makeNCMRequest<ncm::UpdateRequest>(
      std::move(ctx), std::move(callback));
  deps()->processor_->postWithRetrying(req);
}

void NodesConfigurationManager::overwrite(
    std::shared_ptr<const NodesConfiguration> configuration,
    CompletionCb callback) {
  if (shutdownSignaled()) {
    callback(E::SHUTDOWN, nullptr);
    return;
  }

  ncm::OverwriteContext ctx{deps()->getTraceLogger()};
  ctx.data_.nc_ = configuration;
  // ensure we are allowed to overwrite
  if (!mode_.isTooling()) {
    ctx.setStatus(E::ACCESS);
    callback(E::ACCESS, nullptr);
    return;
  }

  if (!configuration) {
    ctx.setStatus(E::INVALID_PARAM);
    callback(E::INVALID_PARAM, nullptr);
    return;
  }

  STAT_INCR(
      deps()->getStats(), nodes_configuration_manager_overwrites_requested);

  deps()->overwrite(std::move(ctx), std::move(callback));
}

void NodesConfigurationManager::initOnNCM(
    std::shared_ptr<const NodesConfiguration> init_nc) {
  deps_->dcheckOnNCM();
  // start polling from NCS
  onHeartBeat();
  deps_->scheduleHeartBeat();
  STAT_SET(deps_->getStats(), nodes_configuration_manager_started, 1);

  ld_assert(init_nc);
  onNewConfig(std::move(init_nc));
}

void NodesConfigurationManager::onNewConfig(std::string new_config) {
  deps_->dcheckOnNCM();
  STAT_INCR(deps_->getStats(), nodes_configuration_manager_config_received);
  if (shutdownSignaled()) {
    return;
  }

  auto new_version_opt =
      NodesConfigurationCodec::extractConfigVersion(new_config);
  if (!new_version_opt) {
    // Invalid serialized blob.
    STAT_INCR(
        deps()->getStats(), nodes_configuration_manager_serialization_errors);
    err = E::BADMSG;
    return;
  }
  if (hasProcessedVersion(new_version_opt.value())) {
    // Early return to avoid deserialization
    return;
  }

  auto parsed_config_ptr = NodesConfigurationCodec::deserialize(new_config);
  if (!parsed_config_ptr) {
    // err is set by deserialize()
    STAT_INCR(
        deps()->getStats(), nodes_configuration_manager_serialization_errors);
    return;
  }
  deps_->reportPropagationLatency(parsed_config_ptr);
  onNewConfig(std::move(parsed_config_ptr));
}

void NodesConfigurationManager::onNewConfig(
    std::shared_ptr<const NodesConfiguration> new_config) {
  ld_check(new_config);
  deps_->dcheckOnNCM();
  if (shutdownSignaled()) {
    return;
  }

  // Since all accesses to staged and pending configs happen in the NCM context,
  // no need to synchronize here.
  auto new_config_version = new_config->getVersion();
  if (!shouldStageVersion(new_config_version)) {
    return;
  }
  ld_debug("Staging nodes configuration of version %lu....",
           new_config_version.val());
  // Incoming config has a higher version, use it as the staged config
  staged_nodes_config_ = std::move(new_config);

  ld_debug("Updating ShardStateTracker with NC version %lu",
           new_config_version.val());
  tracker_.onNewConfig(staged_nodes_config_);
  advanceIntermediaryShardStates();

  STAT_SET(deps_->getStats(),
           nodes_configuration_manager_staged_version,
           staged_nodes_config_->getVersion().val());
  maybeProcessStagedConfig();
}

namespace {
std::shared_ptr<const NodesConfiguration>
max(const std::shared_ptr<const NodesConfiguration>& lhs,
    const std::shared_ptr<const NodesConfiguration>& rhs) {
  if (!lhs) {
    return rhs;
  }
  if (!rhs) {
    return lhs;
  }
  return (lhs->getVersion() > rhs->getVersion()) ? lhs : rhs;
}
} // namespace

std::shared_ptr<const NodesConfiguration>
NodesConfigurationManager::getLatestKnownConfig() const {
  auto c = max(getConfig(), pending_nodes_config_);
  c = max(c, staged_nodes_config_);
  if (!c) {
    c = std::make_shared<const NodesConfiguration>();
  }
  return c;
}

void NodesConfigurationManager::onUpdateRequest(ncm::UpdateContext ctx,
                                                CompletionCb callback) {
  deps_->dcheckOnNCM();
  ctx.addTimestamp("start_exec_on_ncm");

  // ensure we are allowed to propose updates
  if (!mode_.isProposer()) {
    ctx.setStatus(E::ACCESS);
    callback(E::ACCESS, nullptr);
    return;
  }

  auto current_config = getLatestKnownConfig();
  ld_assert(current_config);
  auto current_version = current_config->getVersion();

  ld_assert(current_config);
  ctx.data_.nc_ = current_config->applyUpdate(ctx.data_.update_);
  if (!ctx.data_.nc_) {
    // TODO: better visibility into why particular updates failed
    ctx.setStatus(err);
    callback(err, (err == E::VERSION_MISMATCH) ? current_config : nullptr);
    return;
  }
  // NOTE: This is not a necessary condition for NCM to work, however, I (xshi)
  // think that keeping the version numbers sequential and without gaps will aid
  // observability and debuggability. We could loosen this constraint in the
  // future.
  ld_assert_eq(ctx.data_.nc_->getVersion().val(), current_version.val() + 1);

  auto serialized = NodesConfigurationCodec::serialize(*ctx.data_.nc_);
  if (serialized.empty()) {
    ctx.setStatus(err);
    callback(err, nullptr);
    return;
  }

  ctx.addTimestamp("call_ncs_update");
  deps()->store_->updateConfig(
      std::move(serialized),
      /* base_version = */ current_version,
      [callback = std::move(callback),
       ctx = std::move(ctx),
       current_version,
       ncm = weak_from_this()](
          Status status,
          NodesConfigurationStore::version_t stored_version,
          std::string stored_data) mutable {
        ctx.addTimestamp("ncs_upate_cb");
        // In NCS callback thread
        auto notify_ncm_of_new_config =
            [ncm = std::move(ncm)](std::shared_ptr<const NodesConfiguration>
                                       new_config_ptr) mutable {
              auto ncm_ptr = ncm.lock();
              if (!ncm_ptr || ncm_ptr->shutdownSignaled()) {
                // NCM shut down, no need to notify it
                return;
              }
              ld_info("notified ncm of new config %lu",
                      new_config_ptr->getVersion().val());
              ncm_ptr->deps()->postNewConfigRequest(std::move(new_config_ptr));
            };

        // If we know which version / what config prevented the update:
        if (status == E::VERSION_MISMATCH &&
            stored_version != membership::MembershipVersion::EMPTY_VERSION &&
            !stored_data.empty()) {
          if (folly::kIsDebug) {
            auto extracted_version_opt =
                NodesConfigurationCodec::extractConfigVersion(stored_data);
            ld_assert(extracted_version_opt.hasValue());
            ld_assert_eq(stored_version, extracted_version_opt.value());
            // VERSION_MISMATCH <=> stored_version != current_version (i.e.,
            // base_version).
            //
            // Furthermore, stored_version should never be less than
            // current_version since current_version was the latest known
            // version of this NCM; otherwise, it means that this NCM has gotten
            // a newer-versioned NC than what's in the NCS--this could
            // technically happen when the read of the ZK-NCS's naive
            // implementation of read-modify-write returns stale data.
            ld_assert_ne(stored_version, current_version);
          }
          auto stored_config =
              NodesConfigurationCodec::deserialize(std::move(stored_data));
          ld_assert(stored_config);
          notify_ncm_of_new_config(stored_config);
          ctx.setStatus(E::VERSION_MISMATCH);
          callback(E::VERSION_MISMATCH, std::move(stored_config));
          return;
        }

        if (status != E::OK) {
          // TODO: we could add retries here for E::AGAIN and
          // E::VERSION_MISMATCH
          ctx.setStatus(status);
          callback(status, nullptr);
          return;
        }

        ld_check_eq(stored_version, ctx.data_.nc_->getVersion());
        notify_ncm_of_new_config(ctx.data_.nc_);
        ctx.setStatus(E::OK);
        callback(E::OK, ctx.data_.nc_);
      });
}

void NodesConfigurationManager::maybeProcessStagedConfig() {
  deps_->dcheckOnNCM();

  // nothing is staged or we're already processing a version
  if (!staged_nodes_config_ || pending_nodes_config_) {
    return;
  }
  ld_debug("Processing staged nodes configuration of version %lu.",
           staged_nodes_config_->getVersion().val());
  ld_check(!hasProcessedVersion(staged_nodes_config_->getVersion()));

  // process the staged one now.
  pending_nodes_config_ = std::move(staged_nodes_config_);
  STAT_SET(deps_->getStats(),
           nodes_configuration_manager_pending_version,
           pending_nodes_config_->getVersion().val());

  // Publish the NodesConfiguration to the NCM NC updateable.
  deps_->processor_->config_->updateableNCMNodesConfiguration()->update(
      pending_nodes_config_);

  auto futures = fulfill_on_all_workers<folly::Unit>(
      deps_->processor_,
      [config = pending_nodes_config_](folly::Promise<folly::Unit> p) {
        Worker* w = Worker::onThisThread();
        ld_debug("Processing config version %lu on Worker %d of pool %s",
                 config->getVersion().val(),
                 w->idx_.val(),
                 workerTypeStr(w->worker_type_));
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
        if (!ncm || ncm->shutdownSignaled()) {
          return;
        }
        auto req = ncm->deps()->makeNCMRequest<ncm::ProcessingFinishedRequest>(
            std::move(pending_nodes_config));
        ncm->deps()->processor_->postWithRetrying(req);
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
  initialized_.post();

  ld_info("Updated local nodes config to version %lu...", new_version.val());
  STAT_INCR(deps_->getStats(), nodes_configuration_manager_config_published);
  STAT_SET(deps_->getStats(),
           nodes_configuration_manager_published_version,
           local_nodes_config_.get()->getVersion().val());

  maybeProcessStagedConfig();
}

bool NodesConfigurationManager::shouldStageVersion(
    membership::MembershipVersion::Type version) {
  return (!staged_nodes_config_ ||
          staged_nodes_config_->getVersion() < version) &&
      !isProcessingEqualOrHigherVersion(version) &&
      !hasProcessedVersion(version);
}

bool NodesConfigurationManager::isProcessingEqualOrHigherVersion(
    membership::MembershipVersion::Type version) {
  return pending_nodes_config_ &&
      pending_nodes_config_->getVersion() >= version;
}

bool NodesConfigurationManager::hasProcessedVersion(
    membership::MembershipVersion::Type version) {
  auto local_nodes_config_ptr = local_nodes_config_.get();
  return local_nodes_config_ptr != nullptr &&
      local_nodes_config_ptr->getVersion() >= version;
}

void NodesConfigurationManager::onHeartBeat() {
  deps()->dcheckOnNCM();
  deps()->readFromStore(shouldDoConsistentConfigFetch());
  advanceIntermediaryShardStates();

  if (deps()->processor_->settings()->server) {
    deps()->checkAndReportConsistency();
  }
}

void NodesConfigurationManager::advanceIntermediaryShardStates() {
  deps()->dcheckOnNCM();
  if (!mode_.isProposer() || shutdownSignaled()) {
    return;
  }

  SystemTimestamp till_timestamp = SystemTimestamp::now() -
      deps()
          ->processor_->settings()
          ->nodes_configuration_manager_intermediary_shard_state_timeout;
  auto update_opt = tracker_.extractNCUpdate(till_timestamp);
  if (update_opt.hasValue()) {
    ld_info("Proposing update to transition shards out of intermediary "
            "states that entered the state before %s...",
            till_timestamp.toString().c_str());
    update(
        std::move(update_opt).value(),
        [ncm = weak_from_this()](
            Status status, std::shared_ptr<const NodesConfiguration>) mutable {
          if (status == Status::OK || status == E::VERSION_MISMATCH) {
            return;
          }

          RATELIMIT_ERROR(
              std::chrono::seconds(10),
              5,
              "Attempt to advance intermediary state failed with error %s",
              error_name(status));

          auto ncm_ptr = ncm.lock();
          if (!ncm_ptr || ncm_ptr->shutdownSignaled()) {
            return;
          }

          ncm_ptr->deps()->reportEvent(
              NCMReportType::ADVANCE_INTERMEDIARY_SHARD_STATES_FAILED);
        });
  }
}
}}}} // namespace facebook::logdevice::configuration::nodes
