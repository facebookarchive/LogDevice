/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/synchronization/Baton.h>
#include <folly/synchronization/SaturatingSemaphore.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerDependencies.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/configuration/nodes/ShardStateTracker.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

enum class NCMReportType : uint16_t {
  NCS_READ_FAILED,
  ADVANCE_INTERMEDIARY_SHARD_STATES_FAILED,
};

// NodesConfigurationManager is the singleton state machine that persists and
// manages the service discovery info as well as storage membership. It also
// provides a public API for our tools.
class NodesConfigurationManager
    : public NodesConfigurationAPI,
      public folly::enable_shared_from_this<NodesConfigurationManager> {
 private:
  struct NCMTag {};

 public:
  class OperationMode {
   public:
    static OperationMode forClient();
    static OperationMode forTooling();
    static OperationMode forNodeRoles(NodeServiceDiscovery::RoleSet roles);

    // self roles
    bool isClient() const;
    bool isClientOnly() const;
    bool isTooling() const;
    bool isStorageMember() const;
    bool isSequencer() const;

    // protocol modes
    // By default, everyone is an observer.
    bool isProposer() const;
    bool isCoordinator() const;

    bool isValid() const;

   protected:
    void upgradeToProposer();

   private:
    using Flags = uint16_t;

    constexpr static const Flags kIsProposer = static_cast<Flags>(1u << 0);
    constexpr static const Flags kIsCoordinator = static_cast<Flags>(1u << 1);

    constexpr static const Flags kIsClient = static_cast<Flags>(1u << 2);
    constexpr static const Flags kIsTooling = static_cast<Flags>(1u << 3);
    constexpr static const Flags kIsStorageMember = static_cast<Flags>(1u << 4);
    constexpr static const Flags kIsSequencer = static_cast<Flags>(1u << 5);

    // Only use the static methods to construct OperationMode
    explicit OperationMode() : mode_{0} {}

    void setFlags(Flags flags);
    bool hasFlags(Flags flags) const;
    bool onlyHasFlags(Flags flags) const;

    Flags mode_{0};

    friend class NodesConfigurationManager;
  }; // OperationMode

  template <typename... Args>
  static auto create(Args&&... args) {
    return std::make_shared<NodesConfigurationManager>(
        NCMTag{}, std::forward<Args>(args)...);
  }

  explicit NodesConfigurationManager(NCMTag,
                                     OperationMode mode,
                                     std::unique_ptr<ncm::Dependencies> deps);

  NodesConfigurationManager(const NodesConfigurationManager&) = delete;
  NodesConfigurationManager& operator=(const NodesConfigurationManager&) =
      delete;
  NodesConfigurationManager(NodesConfigurationManager&&) = delete;
  NodesConfigurationManager& operator=(NodesConfigurationManager&&) = delete;

  ~NodesConfigurationManager() override {}

  // Some init procedure needs to happen on Processor worker threads. If
  // wait_until_initialized is true, init() will block until the async prodecure
  // has finished on the worker thread.
  //
  // Returns whether initialization was successful.
  bool init(std::shared_ptr<const NodesConfiguration> init_nc,
            bool wait_until_initialized = true);
  void upgradeToProposer();

  // Called by the owning Processor on its own shutdown. Guarantees that all
  // other threads and eventbases spawn from the NCM will be joined and that
  // upon return, NCM will not accept new user calls.
  void shutdown();

  //////// PROPOSER ////////
  void update(NodesConfiguration::Update, CompletionCb) override;

  // For emergency tooling; can be invoked from any thread.
  //
  // Implementation notes:
  //
  // The implementation of overwrite() does not go through the state machine, so
  // that it'd be suitable to use even if the NCM is having issues.
  //
  // For now, the operation may fail if the version number of the configuration
  // is not high enough (invoking the callback with E::VERSION_MISMATCH). In the
  // future, NCM will encapsulate this complexity.
  void overwrite(std::shared_ptr<const NodesConfiguration> configuration,
                 CompletionCb callback) override;

  //////// OBSERVER ////////
  std::shared_ptr<const NodesConfiguration> getConfig() const override {
    return local_nodes_config_.get();
  }

  ncm::Dependencies* deps() const {
    return deps_.get();
  }

 private:
  void initOnNCM(std::shared_ptr<const NodesConfiguration> init_nc);

  // Returns true when we should fetch the latest config from the store.
  // NCM needs to ensure that the locally committed config version never
  // decreases, even across restarts. Currently we achieve this by doing a
  // strongly consistent read when storage node starts up.
  bool shouldDoConsistentConfigFetch() const;
  bool shutdownSignaled() const;

  // onNewConfig should only be called by NewConfigRequest.
  // TODO: implement overwrite (the blind write option) for emergency tooling.
  void onNewConfig(std::shared_ptr<const NodesConfiguration>);
  void onNewConfig(std::string);

  // returns the highest-versioned config known to NCM, which may be
  // unprocessed. Could return a config with EMPTY_VERSION, but never returns
  // nullptr.
  std::shared_ptr<const NodesConfiguration> getLatestKnownConfig() const;
  void onUpdateRequest(ncm::UpdateContext ctx, CompletionCb callback);

  // A new version of the config goes through the following phases:
  //   S: staged, to be processed by the NCM
  //   |
  //   | maybeProcessStagedConfig()
  //   v
  //   P: pending, currently being processed by the NCM (e.g., propagated to
  //      each Worker, waiting to hear back)
  //   |
  //   | onProcessingFinished()
  //   v
  //   L: locally processed, all Workers have acknowledged and processed this
  //      version.
  //
  // For simplicity, we maintain the following invariants:
  // (1) NCM only keeps the highest-versioned staged config, since later configs
  // include the effects of previous configs, skipping config versions is
  // acceptable. (2) NCM only allows one pending config at any given time.
  // Hence, maybeProcessStagedConfig() only starts processing a staged config if
  // there isn't an existing pending config.
  //
  // TODO: storage nodes need to persist the config after processing finished
  // and before marking the config as locally processed, i.e., there is a
  // separate phase between P and L.
  //
  // Must be called from the NCM context.
  void maybeProcessStagedConfig();
  // Must be called from the NCM context.
  void onProcessingFinished(std::shared_ptr<const NodesConfiguration>);

  // The following helper functions should only be called from the NCM context.
  bool shouldStageVersion(membership::MembershipVersion::Type);
  bool isProcessingEqualOrHigherVersion(membership::MembershipVersion::Type);
  bool hasProcessedVersion(membership::MembershipVersion::Type);

  // Called regularly to read config from NCS and check for various timeouts.
  void onHeartBeat();

  // Check whether there are shards in the intermediary states past the timeout,
  // if so, propose an update to transition them out of the intermediary states.
  //
  // No-op if NCM is not run as a proposer.
  //
  // Must be called from the NCM context.
  void advanceIntermediaryShardStates();

  OperationMode mode_;
  std::unique_ptr<ncm::Dependencies> deps_{nullptr};

  // The nodes config that is staged to be processed. Among all the staged nodes
  // configs, we only keep the highest versioned one. All accesses happen in the
  // NCM context.
  std::shared_ptr<const NodesConfiguration> staged_nodes_config_{nullptr};
  // The nodes config that the NCM is currently processing (propagating to every
  // worker). All accesses happen in the NCM context.
  std::shared_ptr<const NodesConfiguration> pending_nodes_config_{nullptr};

  // The locally processed and committed version of the NodesConfiguration, the
  // version of which _strictly_ increases. All writes are done in the state
  // machine context, but reads may be from different threads.
  UpdateableSharedPtr<const NodesConfiguration, NCMTag> local_nodes_config_{
      nullptr};

  // Basically a baton but allows multiple waiters and posters: both init() and
  // shutdown() would want to wait until initialization has finished, and we
  // don't post until we finish processing the initial NC, whose logic is
  // separated from the init logic so can be called many times.
  folly::SaturatingSemaphore</* MayBlock */ true> initialized_{};
  std::atomic<bool> shutdown_signaled_{false};
  folly::Baton<> shutdown_completed_;

  ShardStateTracker tracker_{};

  friend class ncm::NCMRequest;
  friend class ncm::Dependencies::Dependencies;
  friend class ncm::Dependencies::InitRequest;
  friend class ncm::NewConfigRequest;
  friend class ncm::ProcessingFinishedRequest;
  friend class ncm::UpdateRequest;
};

}}}} // namespace facebook::logdevice::configuration::nodes
