/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerDependencies.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

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
    bool isTooling() const;
    bool isStorageMember() const;
    bool isSequencer() const;

    // protocol modes
    // By default, everyone is an observer.
    bool isProposer() const;
    bool isCoordinator() const;

    bool isValid() const;

   protected:
    static OperationMode upgradeToProposer(OperationMode current_mode);

   private:
    using Flags = uint16_t;

    constexpr static const Flags kIsProposer = static_cast<Flags>(1 << 0);
    constexpr static const Flags kIsCoordinator = static_cast<Flags>(1 << 1);

    constexpr static const Flags kIsClient = static_cast<Flags>(1 << 2);
    constexpr static const Flags kIsTooling = static_cast<Flags>(1 << 3);
    constexpr static const Flags kIsStorageMember = static_cast<Flags>(1 << 4);
    constexpr static const Flags kIsSequencer = static_cast<Flags>(1 << 5);

    // Only use the static methods to construct OperationMode
    explicit OperationMode() {}

    void setFlags(Flags flags);
    bool hasFlags(Flags flags) const;
    bool onlyHasFlags(Flags flags) const;

    Flags mode_;
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

  void init();

  //////// PROPOSER ////////
  int update(NodesConfiguration::Update, CompletionCb) override {
    throw std::runtime_error("unimplemented.");
  }
  int overwrite(std::shared_ptr<const NodesConfiguration>,
                CompletionCb) override {
    throw std::runtime_error("unimplemented.");
  }

  //////// OBSERVER ////////
  std::shared_ptr<const NodesConfiguration> getConfig() const override {
    return local_nodes_config_.get();
  }

  ncm::Dependencies* deps() const {
    return deps_.get();
  }

 private:
  void initOnNCM();
  void startPollingFromStore();

  // onNewConfig should only be called by NewConfigRequest. overwrite triggers
  // the blind write option of the commit, useful for emergency tooling.
  void onNewConfig(std::shared_ptr<const NodesConfiguration>,
                   bool overwrite = false);
  void onNewConfig(std::string, bool overwrite = false);

  OperationMode mode_;
  std::unique_ptr<ncm::Dependencies> deps_{nullptr};

  // The locally processed and committed version of the NodesConfiguration, the
  // version of which _strictly_ increases. All writes are done on the state
  // machine thread, but reads may be from different threads.
  UpdateableSharedPtr<const NodesConfiguration, NCMTag> local_nodes_config_{
      nullptr};

  friend class ncm::NCMRequest;
  friend class ncm::Dependencies::InitRequest;
  friend class ncm::NewConfigRequest;
};

}}}} // namespace facebook::logdevice::configuration::nodes
