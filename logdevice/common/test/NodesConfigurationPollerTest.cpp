/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodesConfigurationPoller.h"

#include <algorithm>

#include <folly/Random.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/ObjectPoller.h"
#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/toString.h"

using namespace facebook::logdevice;

namespace {

class MockNodesConfigurationPoller : public NodesConfigurationPoller {
 public:
  virtual ~MockNodesConfigurationPoller() {}
  using NodesConfigurationPoller::NodesConfigurationPoller;

  auto testFunction() {
    return buildPollingSet({1, 2, 3, 4, 5}, {}, {}, {}, 3, 0);
  }

 protected:
  Poller::RequestResult sendRequestToNode(Poller::RoundID round,
                                          node_index_t node) override {
    return Poller::RequestResult::OK;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    const ssize_t nnodes = 5;
    std::vector<node_index_t> node_idxs(nnodes);
    std::iota(node_idxs.begin(), node_idxs.end(), 1);

    return NodesConfigurationTestUtil::provisionNodes(node_idxs);
  }

  MOCK_METHOD0(getMyNodeID, folly::Optional<node_index_t>());
  MOCK_METHOD0(getClusterState, ClusterState*());

  // return true if the NCPoller is running in the bootstrapping environment
  // (e.g., NodesConfigurationInit). If so, conditional polling will be disabled
  bool isBootstrapping() const override {
    return false;
  }
};

TEST(NodesConfigurationPoller, simple) {
  NodesConfigurationPoller::Poller::Options options;
  options.mode = NodesConfigurationPoller::Poller::Mode::ONE_TIME;
  auto mockNodesConfigurationPoller = MockNodesConfigurationPoller(
      options,
      configuration::nodes::NodesConfigurationCodec::extractConfigVersion,
      [this](Status st,
             NodesConfigurationPoller::Poller::RoundID /*round*/,
             folly::Optional<std::string> config_str) {},
      12 /* hard-coded seed value*/,
      folly::none);

  RandomNodeSelector::NodeSourceSet set = {3, 4, 5};
  EXPECT_EQ(set, mockNodesConfigurationPoller.testFunction());
  EXPECT_EQ(set, mockNodesConfigurationPoller.testFunction());
  EXPECT_EQ(set, mockNodesConfigurationPoller.testFunction());
  EXPECT_EQ(set, mockNodesConfigurationPoller.testFunction());

  // Creating object with different seed
  auto mockNodesConfigurationPoller2 = MockNodesConfigurationPoller(
      options,
      configuration::nodes::NodesConfigurationCodec::extractConfigVersion,
      [this](Status st,
             NodesConfigurationPoller::Poller::RoundID /*round*/,
             folly::Optional<std::string> config_str) {},
      11 /* hard-coded seed value*/,
      folly::none);
  EXPECT_NE(set, mockNodesConfigurationPoller2.testFunction());
}
} // namespace
