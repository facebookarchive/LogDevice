/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "logdevice/server/sequencer_boycotting/NodeStatsControllerLocator.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace ::testing;

// Convenient shortcuts for writting NodeIDs.
#define N0 NodeID(0, 1)
#define N1 NodeID(1, 1)
#define N2 NodeID(2, 1)
#define N3 NodeID(3, 1)
#define N4 NodeID(4, 1)
#define N5 NodeID(5, 1)
#define N6 NodeID(6, 1)

using StateList = NodeStatsControllerLocator::StateList;

class MockLocator : public NodeStatsControllerLocator {
 public:
  MOCK_CONST_METHOD1(getNodeState, StateList(node_index_t));
  MOCK_CONST_METHOD0(getNodes, std::shared_ptr<const Nodes>());
};

class NodeStatsControllerLocatorTest : public Test {
 public:
  std::shared_ptr<Nodes>
  nodesWithLocations(std::vector<std::string> locations) {
    auto nodes = std::make_shared<Nodes>();
    for (const auto& location_str : locations) {
      NodeLocation location;
      location.fromDomainString(location_str);
      Node node;
      node.location = location;
      node.generation = 1;
      nodes->emplace(node_index, std::move(node));
      ++node_index;
    }

    return nodes;
  }

  MockLocator locator;
  node_index_t node_index{0};
};

TEST_F(NodeStatsControllerLocatorTest, MoreControllersThanNodes) {
  auto nodes = nodesWithLocations({"rg0.dc0.cl0.ro0.rk0"});

  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE}));
  // best effort
  EXPECT_TRUE(locator.isController(N0, 2));
}

TEST_F(NodeStatsControllerLocatorTest, SingleNode) {
  auto nodes = nodesWithLocations({"rg0.dc0.cl0.ro0.rk0"});
  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE}));

  EXPECT_TRUE(locator.isController(N0, 1));
}

TEST_F(NodeStatsControllerLocatorTest, DifferentRack) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1"});
  // two in the same rack, one in another
  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  // any of the nodes in the first rack may be chosen
  EXPECT_TRUE(locator.isController(N0, 2) || locator.isController(N1, 2));
  EXPECT_TRUE(locator.isController(N2, 2));
}

// will choose from same rack if necessary
TEST_F(NodeStatsControllerLocatorTest, SameRack) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1"});
  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  EXPECT_TRUE(locator.isController(N0, 3));
  EXPECT_TRUE(locator.isController(N1, 3));
  EXPECT_TRUE(locator.isController(N2, 3));
}

TEST_F(NodeStatsControllerLocatorTest, DeadNode) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1"});
  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));
  // if a node is DEAD, pick another one, even if it's in the same rack
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, DEAD}));
  EXPECT_TRUE(locator.isController(N0, 2));
  EXPECT_TRUE(locator.isController(N1, 2));
}

TEST_F(NodeStatsControllerLocatorTest, GapInIndex) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1"});
  // make gap
  nodes->emplace(std::make_pair(2, nodes->at(1)));
  nodes->erase(1);

  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));

  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, DEAD, ALIVE}));

  EXPECT_TRUE(locator.isController(N0, 2));
  EXPECT_TRUE(locator.isController(N2, 2));
}

TEST_F(NodeStatsControllerLocatorTest, WithoutLocation) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1", "rg0.dc0.cl0.ro0.rk2"});

  nodes->at(1).location.clear();

  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));

  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  // if only 2 are chosen, the one with location (and different rack) should be
  // chosen
  EXPECT_TRUE(locator.isController(N0, 2));
  EXPECT_TRUE(locator.isController(N2, 2));

  // when we are forced to not consider location, choose any
  EXPECT_TRUE(locator.isController(N0, 3));
  EXPECT_TRUE(locator.isController(N1, 3));
  EXPECT_TRUE(locator.isController(N2, 3));
}
