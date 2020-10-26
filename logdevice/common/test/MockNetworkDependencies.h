/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/io/async/SSLContext.h>
#include <gtest/gtest.h>

#include "logdevice/common/NetworkDependencies.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/MockTimer.h"

namespace facebook { namespace logdevice {

////////////////////////////////////////////////////////////////////////////////
// TestNetworkDependencies

class MockNetworkDependencies : public NetworkDependencies {
 public:
  MockNetworkDependencies() : NetworkDependencies(nullptr){};

  MOCK_CONST_METHOD0(getSettings, const Settings&());
  MOCK_CONST_METHOD0(getStats, StatsHolder*());
  MOCK_CONST_METHOD0(getConfig, std::shared_ptr<Configuration>());
  MOCK_CONST_METHOD0(getServerConfig, std::shared_ptr<ServerConfig>());
  MOCK_CONST_METHOD0(
      getNodesConfiguration,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>());

  MOCK_CONST_METHOD0(getSSLContext, std::shared_ptr<folly::SSLContext>());
  MOCK_CONST_METHOD0(shuttingDown, bool());

  MOCK_METHOD0(getMyNodeID, NodeID());
  MOCK_METHOD0(getConnBudgetExternal, ResourceBudget&());
  MOCK_METHOD0(getHELLOCredentials, const std::string&());
  MOCK_METHOD0(getCSID, const std::string&());
  MOCK_METHOD0(includeHELLOCredentials, bool());
  MOCK_METHOD0(getClientBuildInfo, std::string());
  MOCK_METHOD0(authenticationEnabled, bool());
  MOCK_METHOD1(onStartedRunning, void(RunContext));
  MOCK_METHOD1(onStoppedRunning, void(RunContext));
  MOCK_METHOD1(getResourceToken, ResourceBudget::Token(size_t));

  MOCK_CONST_METHOD0(getExecutor, folly::Executor*());

  MOCK_METHOD0(createMockTimer, std::unique_ptr<MockTimer>());
  std::unique_ptr<TimerInterface>
  createTimer(std::function<void()>&& cb,
              std::chrono::microseconds delay) override {
    auto mock = createMockTimer();
    if (mock) {
      mock->assign(std::move(cb));
      mock->activate(delay);
    }
    return mock;
  }
};
}} // namespace facebook::logdevice
