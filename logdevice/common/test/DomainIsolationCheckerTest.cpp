/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DomainIsolationChecker.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/NodeSetTestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

class MockDomainIsolationChecker;

class DomainIsolationTest : public ::testing::Test {
 private:
  std::unordered_set<node_index_t> down_nodes_;

 public:
  node_index_t my_node_idx_{2};
  std::shared_ptr<ServerConfig> config_;
  std::unique_ptr<MockDomainIsolationChecker> checker_;

  bool isNodeAlive(node_index_t idx) const {
    return down_nodes_.count(idx) == 0;
  }

  void nodeDown(node_index_t idx);
  void nodeUp(node_index_t idx);

  void setDownNodes(std::unordered_set<node_index_t> down_nodes) {
    down_nodes_ = std::move(down_nodes);
  }

  void setUp(std::unique_ptr<ServerConfig::Nodes> nodes = nullptr);
};

class MockDomainIsolationChecker : public DomainIsolationChecker {
 public:
  explicit MockDomainIsolationChecker(DomainIsolationTest* test) : test_(test) {
    ld_check(test != nullptr);
  }

  using DomainIsolationChecker::localDomainContainsWholeCluster;

 protected:
  worker_id_t getThreadID() const override {
    // always run on the same thread
    return worker_id_t(0);
  }

  bool isNodeAlive(node_index_t index) const override {
    return test_->isNodeAlive(index);
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    // TODO: migrate it to use NodesConfiguration with switchable source
    return test_->config_->getNodesConfigurationFromServerConfigSource();
  }

  NodeID getMyNodeID() const override {
    return NodeID(test_->my_node_idx_, 1);
  }

 private:
  DomainIsolationTest* const test_;
};

void DomainIsolationTest::setUp(
    std::unique_ptr<ServerConfig::Nodes> preset_nodes) {
  ServerConfig::Nodes nodes;
  if (preset_nodes != nullptr) {
    nodes = *preset_nodes;
  } else {
    addNodes(&nodes, 1, 1, "rg0.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 2, 1, "rg1.dc0.cl0.ro0.rk1", 2);
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk2", 1);
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0..", 1);
    addNodes(&nodes, 1, 1, "rg2.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 1, 1, "rg2.dc0.cl0.ro0.rk1", 1);
    addNodes(&nodes, 1, 1, "....", 1);
  }
  Configuration::NodesConfig nodes_config(std::move(nodes));
  // auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
  // addLog(logs_config.get(), logid_t(1), 1, 0, 2, {});
  config_ = ServerConfig::fromDataTest(
      "copyset_selector_test", std::move(nodes_config));
  checker_ = std::make_unique<MockDomainIsolationChecker>(this);
  checker_->init();
}

void DomainIsolationTest::nodeDown(node_index_t idx) {
  ld_check(checker_);
  checker_->onNodeDead(idx);
  auto result = down_nodes_.insert(idx);
  ASSERT_TRUE(result.second);
}

void DomainIsolationTest::nodeUp(node_index_t idx) {
  ld_check(checker_);
  checker_->onNodeAlive(idx);
  auto erased = down_nodes_.erase(idx);
  ASSERT_EQ(1, erased);
}

#define ASSERT_ISOLATED_SCOPE(isolated_scope)                            \
  do {                                                                   \
    for (NodeLocationScope scope = NodeLocationScope::NODE;              \
         scope < NodeLocationScope::ROOT;                                \
         scope = NodeLocation::nextGreaterScope(scope)) {                \
      if (scope < isolated_scope) {                                      \
        ASSERT_FALSE(checker_->isMyDomainIsolated(scope));               \
      } else if (checker_->localDomainContainsWholeCluster(scope)) {     \
        ASSERT_FALSE(checker_->isMyDomainIsolated(scope));               \
      } else {                                                           \
        ASSERT_TRUE(checker_->isMyDomainIsolated(scope));                \
      }                                                                  \
    }                                                                    \
    ASSERT_FALSE(checker_->isMyDomainIsolated(NodeLocationScope::ROOT)); \
  } while (0)

#define ASSERT_NO_ISOLATED_SCOPE() \
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::ROOT);

TEST_F(DomainIsolationTest, Basics) {
  my_node_idx_ = 2;
  // N6 is initially down
  setDownNodes({6});
  setUp();

  for (int iter = 0; iter < 100; ++iter) {
    auto refresh = [&]() {
      if (iter % 2 == 0) {
        // rebuild state
        checker_->noteConfigurationChanged();
      }
    };
    for (auto idx : NodeSetIndices{0, 7}) {
      nodeDown(idx);
      ASSERT_NO_ISOLATED_SCOPE();
    }
    refresh();
    nodeDown(8);
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::CLUSTER);
    refresh();
    nodeUp(6);
    ASSERT_NO_ISOLATED_SCOPE();
    nodeDown(6);
    refresh();
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::CLUSTER);
    nodeDown(5);
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::ROW);
    refresh();
    nodeDown(1);
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::ROW);
    refresh();
    nodeDown(4);
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::RACK);
    nodeDown(3);
    refresh();
    ASSERT_ISOLATED_SCOPE(NodeLocationScope::NODE);
    nodeUp(0);
    refresh();
    ASSERT_NO_ISOLATED_SCOPE();
    refresh();
    // restore all other nodes but 6
    for (auto idx : NodeSetIndices{1, 3, 4, 5, 7, 8}) {
      nodeUp(idx);
      ASSERT_NO_ISOLATED_SCOPE();
    }
  }
}

// similar to the last test, but rebuild the
TEST_F(DomainIsolationTest, ClusterExpansion) {
  my_node_idx_ = 7;
  setDownNodes({0, 1, 2, 3, 4, 5, 6, 8});
  setUp();
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::NODE);
  nodeUp(6);
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::ROW);

  // add a new node to the config with a specific location
  auto add_node = [this](const std::string& location_str) {
    auto nodes = config_->getNodes();
    Configuration::Node node;
    node.addStorageRole();
    node.address = Sockaddr("::1", std::to_string(nodes.size()));
    NodeLocation location;
    ASSERT_EQ(0, location.fromDomainString(location_str));
    node.location = std::move(location);
    nodes.insert({nodes.size(), std::move(node)});
    auto new_config =
        config_->withNodes(ServerConfig::NodesConfig(std::move(nodes)));
    config_ = std::move(new_config);
  };

  // add node 9 to region2 dc0 but a different cluster
  add_node("rg2.dc0.cl1.ro0.rk0");
  checker_->noteConfigurationChanged();
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::DATA_CENTER);
  // add node 10 to region1 but it is dead
  add_node("rg1.dc0...");
  nodeDown(10);
  checker_->noteConfigurationChanged();
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::DATA_CENTER);
  // add node 11 to region0, and it is up
  add_node("rg0....");
  checker_->noteConfigurationChanged();
  // there should be no isolation
  ASSERT_NO_ISOLATED_SCOPE();
}

TEST_F(DomainIsolationTest, WholeClusterScopeNotIsolated) {
  auto nodes = std::make_unique<configuration::Nodes>();
  addNodes(nodes.get(), 1, 1, "rg0.dc0.cl0.ro0.rk0", 1);
  addNodes(nodes.get(), 1, 1, "rg0.dc0.cl0.ro0.rk1", 1);
  addNodes(nodes.get(), 1, 1, "rg0.dc0.cl0.ro0.rk1", 1);
  my_node_idx_ = 2;
  setDownNodes({0});
  setUp(std::move(nodes));
  ASSERT_ISOLATED_SCOPE(NodeLocationScope::RACK);
}
