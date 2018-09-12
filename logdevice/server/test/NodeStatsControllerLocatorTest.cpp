/**
 * Copyright (c) 2017-present, Facebook, Inc.
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

using StateList = NodeStatsControllerLocator::StateList;

class MockLocator : public NodeStatsControllerLocator {
 public:
  MOCK_CONST_METHOD1(getNodeState, StateList(node_index_t));
  MOCK_CONST_METHOD0(getNodes, Nodes());
};

class NodeStatsControllerLocatorTest : public Test {
 public:
  Nodes nodesWithLocations(std::vector<std::string> locations) {
    Nodes nodes;
    for (const auto& location_str : locations) {
      NodeLocation location;
      location.fromDomainString(location_str);
      Node node;
      node.location = location;
      node.generation = 0;
      nodes[node_index] = node;
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
  EXPECT_TRUE(locator.isController(NodeID{0}, 2));
}

TEST_F(NodeStatsControllerLocatorTest, SingleNode) {
  EXPECT_CALL(locator, getNodes())
      .WillRepeatedly(Return(nodesWithLocations({"rg0.dc0.cl0.ro0.rk0"})));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE}));

  EXPECT_TRUE(locator.isController(NodeID{0}, 1));
}

TEST_F(NodeStatsControllerLocatorTest, DifferentRack) {
  // two in the same rack, one in another
  EXPECT_CALL(locator, getNodes())
      .WillRepeatedly(Return(nodesWithLocations({"rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk1"})));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  // any of the nodes in the first rack may be chosen
  EXPECT_TRUE(locator.isController(NodeID{0}, 2) ||
              locator.isController(NodeID{1}, 2));
  EXPECT_TRUE(locator.isController(NodeID{2}, 2));
}

// will choose from same rack if necessary
TEST_F(NodeStatsControllerLocatorTest, SameRack) {
  EXPECT_CALL(locator, getNodes())
      .WillRepeatedly(Return(nodesWithLocations({"rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk1"})));
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  EXPECT_TRUE(locator.isController(NodeID{0}, 3));
  EXPECT_TRUE(locator.isController(NodeID{1}, 3));
  EXPECT_TRUE(locator.isController(NodeID{2}, 3));
}

TEST_F(NodeStatsControllerLocatorTest, DeadNode) {
  EXPECT_CALL(locator, getNodes())
      .WillRepeatedly(Return(nodesWithLocations({"rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk0",
                                                 "rg0.dc0.cl0.ro0.rk1"})));
  // if a node is DEAD, pick another one, even if it's in the same rack
  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, DEAD}));
  EXPECT_TRUE(locator.isController(NodeID{0}, 2));
  EXPECT_TRUE(locator.isController(NodeID{1}, 2));
}

TEST_F(NodeStatsControllerLocatorTest, GapInIndex) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1"});
  // make gap
  nodes.emplace(std::make_pair(2, nodes.at(1)));
  nodes.erase(1);

  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));

  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, DEAD, ALIVE}));

  EXPECT_TRUE(locator.isController(NodeID{0}, 2));
  EXPECT_TRUE(locator.isController(NodeID{2}, 2));
}

TEST_F(NodeStatsControllerLocatorTest, WithoutLocation) {
  auto nodes = nodesWithLocations(
      {"rg0.dc0.cl0.ro0.rk0", "rg0.dc0.cl0.ro0.rk1", "rg0.dc0.cl0.ro0.rk2"});

  nodes.at(1).location.clear();

  EXPECT_CALL(locator, getNodes()).WillRepeatedly(Return(nodes));

  EXPECT_CALL(locator, getNodeState(_))
      .WillRepeatedly(Return(StateList{ALIVE, ALIVE, ALIVE}));

  // if only 2 are chosen, the one with location (and different rack) should be
  // chosen
  EXPECT_TRUE(locator.isController(NodeID{0}, 2));
  EXPECT_TRUE(locator.isController(NodeID{2}, 2));

  // when we are forced to not consider location, choose any
  EXPECT_TRUE(locator.isController(NodeID{0}, 3));
  EXPECT_TRUE(locator.isController(NodeID{1}, 3));
  EXPECT_TRUE(locator.isController(NodeID{2}, 3));
}
