/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <folly/ThreadLocal.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockClusterState : public ClusterState {
 public:
  explicit MockClusterState(size_t max_nodes)
      : ClusterState(max_nodes, nullptr) {}

  // override this method to do nothing
  void refreshClusterStateAsync() override {}
};

class MockSequencerRouter : public SequencerRouter {
 public:
  MockSequencerRouter(logid_t log_id,
                      Handler* handler,
                      std::shared_ptr<ServerConfig> config,
                      std::shared_ptr<SequencerLocator> locator,
                      ClusterState* cluster_state)
      : SequencerRouter(log_id, handler),
        settings_(create_default_settings<Settings>()),
        config_(config),
        locator_(locator),
        cluster_state_(cluster_state) {
    ld_check(locator_ != nullptr);
  }

  ~MockSequencerRouter() override {}

  std::shared_ptr<ServerConfig> getServerConfig() const override {
    return config_;
  }
  const Settings& getSettings() const override {
    return settings_;
  }
  SequencerLocator& getSequencerLocator() const override {
    return *locator_;
  }
  ClusterState* getClusterState() const override {
    return cluster_state_;
  }

  Settings settings_;
  void startClusterStateRefreshTimer() override {}

 private:
  std::shared_ptr<ServerConfig> config_;
  std::shared_ptr<SequencerLocator> locator_;
  ClusterState* cluster_state_;
};

class MockHashBasedSequencerLocator : public HashBasedSequencerLocator {
 public:
  MockHashBasedSequencerLocator(
      std::shared_ptr<UpdateableServerConfig> server_config,
      ClusterState* cluster_state,
      std::shared_ptr<const Configuration> config,
      Settings settings = create_default_settings<Settings>())
      : HashBasedSequencerLocator(server_config),
        settings_(settings),
        cluster_state_(cluster_state),
        config_(config) {}

  ~MockHashBasedSequencerLocator() override {}

  ClusterState* getClusterState() const override {
    return cluster_state_;
  }

  std::shared_ptr<const Configuration> getConfig() const override {
    return config_;
  }

  const Settings& getSettings() const override {
    return settings_;
  }

 private:
  Settings settings_;
  ClusterState* cluster_state_;
  std::shared_ptr<const Configuration> config_;
};

}} // namespace facebook::logdevice
