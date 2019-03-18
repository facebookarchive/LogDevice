/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <numeric>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/RecoverySet.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/NodeSetTestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)
#define N11 ShardID(11, 0)
#define N12 ShardID(12, 0)
#define N13 ShardID(13, 0)
#define N14 ShardID(14, 0)
#define N15 ShardID(15, 0)
#define N16 ShardID(16, 0)
#define N17 ShardID(17, 0)
#define N18 ShardID(18, 0)
#define N19 ShardID(19, 0)
#define N20 ShardID(20, 0)
#define N21 ShardID(21, 0)
#define N22 ShardID(22, 0)

namespace {

enum class TestAttr : uint8_t { A = 0, B, C, D, E, F, G, Count };

class FailureDomainTest : public ::testing::Test {
 public:
  const logid_t LOG_ID{2};
  static const std::set<TestAttr> ALL_ATTR;

  // default failure domain scope used in the test is CLUSTER
  NodeLocationScope sync_replication_scope_{NodeLocationScope::CLUSTER};
  copyset_size_t replication_{3};

  StorageSet storage_set_;
  std::shared_ptr<Configuration> config_;

  using FailureDomainTestSet =
      FailureDomainNodeSet<TestAttr, HashEnum<TestAttr>>;

  using F = FmajorityResult;

  std::unique_ptr<FailureDomainTestSet> failure_set_;

  void setUp();
  void setUpWithMultiScopes();
  void setUpWithShards();
  void setUpWithShardsAndOnlyRackReplication();
  void setShardsAttr(TestAttr attr, const StorageSet& shards);
  void setAllShardsAttr(TestAttr attr);
  void setShardAuthoritativeStatus(AuthoritativeStatus st,
                                   const StorageSet& shards);
  void setShardAuthoritativeStatus(AuthoritativeStatus st);
  void verifyFmajority(FmajorityResult result, const std::set<TestAttr>& mset);
};

const std::set<TestAttr> FailureDomainTest::ALL_ATTR{TestAttr::A,
                                                     TestAttr::B,
                                                     TestAttr::C,
                                                     TestAttr::D,
                                                     TestAttr::E,
                                                     TestAttr::F,
                                                     TestAttr::G};

void FailureDomainTest::setUp() {
  dbg::assertOnData = true;

  // 10 nodes in the cluster each with 5 shards, from 4 different
  // network-clusters
  configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, "rg0.dc0.cl0.ro0.rk1", 1); // 0
  addNodes(&nodes, 1, 1, "rg0.dc0.cl0.ro0.rk2", 1); // 1
  addNodes(&nodes, 4, 1, "rg0.dc0.cl1.ro0.rk3", 2); // 2-5
  addNodes(&nodes, 1, 1, "rg0.dc0.cl2.ro0.rk6", 1); // 6
  addNodes(&nodes, 2, 1, "rg0.dc0.cl3.ro0.rk7", 2); // 7-8
  addNodes(&nodes, 1, 1, "rg0.dc0.cl4.ro0.rk8", 1); // 9

  for (node_index_t nid = 0; nid < nodes.size(); ++nid) {
    storage_set_.push_back(ShardID(nid, 0));
  }

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "failure_domain_test", std::move(nodes_config)),
      std::move(logs_config));

  failure_set_ = std::make_unique<FailureDomainTestSet>(
      storage_set_,
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      ReplicationProperty(replication_, sync_replication_scope_));
  failure_set_->fullConsistencyCheck();
}

void FailureDomainTest::setUpWithMultiScopes() {
  dbg::assertOnData = true;

  configuration::Nodes nodes;
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk1", 1); // 0-3
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk2", 1); // 4-7
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk3", 2); // 8-11
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk6", 1); // 12-15
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk7", 2); // 16-19
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk8", 1); // 20-23

  for (node_index_t nid = 0; nid < nodes.size(); ++nid) {
    storage_set_.push_back(ShardID(nid, 0));
  }

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "failure_domain_test", std::move(nodes_config)),
      std::move(logs_config));

  ReplicationProperty rep;
  rep.setReplication(NodeLocationScope::NODE, 3);
  rep.setReplication(NodeLocationScope::RACK, 3);
  rep.setReplication(NodeLocationScope::REGION, 2);

  failure_set_ = std::make_unique<FailureDomainTestSet>(
      storage_set_,
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      rep);
  failure_set_->fullConsistencyCheck();
}

void FailureDomainTest::setUpWithShards() {
  dbg::assertOnData = true;

  // 24 shards spanning 3 racks.
  configuration::Nodes nodes;
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk1", 1); // 0-3
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk2", 1); // 4-7
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk3", 2); // 8-11

  // Shard set contains 24 nodes (2 shards per node in the cluster).
  for (node_index_t i = 0; i < 12; ++i) {
    storage_set_.push_back(ShardID(i, 0));
    storage_set_.push_back(ShardID(i, 1));
  }

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "failure_domain_test", std::move(nodes_config)),
      std::move(logs_config));

  ReplicationProperty rep;
  rep.setReplication(NodeLocationScope::NODE, 3);
  rep.setReplication(NodeLocationScope::RACK, 2);

  failure_set_ = std::make_unique<FailureDomainTestSet>(
      storage_set_,
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      rep);
  failure_set_->fullConsistencyCheck();
}

void FailureDomainTest::setUpWithShardsAndOnlyRackReplication() {
  dbg::assertOnData = true;

  // 24 shards spanning 3 racks.
  configuration::Nodes nodes;
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk1", 1); // 0-3
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk2", 1); // 4-7
  addNodes(&nodes, 4, 2, "rg0.dc0.cl0.ro0.rk3", 2); // 8-11

  // Shard set contains 24 nodes (2 shards per node in the cluster).
  for (node_index_t i = 0; i < 12; ++i) {
    storage_set_.push_back(ShardID(i, 0));
    storage_set_.push_back(ShardID(i, 1));
  }

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "failure_domain_test", std::move(nodes_config)),
      std::move(logs_config));

  // We only require 2-way replication at rack scope.
  // replication at NODE and SHARD scope is implicit.
  ReplicationProperty rep;
  rep.setReplication(NodeLocationScope::RACK, 2);

  failure_set_ = std::make_unique<FailureDomainTestSet>(
      storage_set_,
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      rep);
  failure_set_->fullConsistencyCheck();
}

void FailureDomainTest::setShardsAttr(TestAttr attr, const StorageSet& shards) {
  for (const auto shard : shards) {
    failure_set_->setShardAttribute(shard, attr);
  }
}

void FailureDomainTest::setAllShardsAttr(TestAttr attr) {
  setShardsAttr(attr, storage_set_);
}

void FailureDomainTest::setShardAuthoritativeStatus(AuthoritativeStatus st) {
  setShardAuthoritativeStatus(st, storage_set_);
}

void FailureDomainTest::setShardAuthoritativeStatus(AuthoritativeStatus st,
                                                    const StorageSet& shards) {
  for (ShardID shard : shards) {
    failure_set_->setShardAuthoritativeStatus(shard, st);
  }
}

inline void FailureDomainTest::verifyFmajority(FmajorityResult result,
                                               const std::set<TestAttr>& mset) {
  for (size_t att = 0; att < (size_t)TestAttr::Count; ++att) {
    TestAttr attr = (TestAttr)att;
    if (mset.find(attr) != mset.end()) {
      ASSERT_EQ(result, failure_set_->isFmajority(attr));
    } else {
      ASSERT_NE(result, failure_set_->isFmajority(attr));
    }
  }
}

TEST_F(FailureDomainTest, FmajorityBasicTest) {
  replication_ = 3;
  setUp();

  // turn 8 and 9 into B
  setShardsAttr(TestAttr::B, {N8, N9});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // turn 6 into B
  setShardsAttr(TestAttr::B, {N6});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // turn 0, 4, 7 into B
  setShardsAttr(TestAttr::B, {N0, N4, N7});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // turn 1 into B, we should have 4 clusters out of 5
  setShardsAttr(TestAttr::B, {N1});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Cancel 1
  setShardsAttr(TestAttr::A, {N1});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Turn 3 onto B
  setShardsAttr(TestAttr::B, {N3});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Turn 5 onto B
  setShardsAttr(TestAttr::B, {N5});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Turn 2 onto B
  setShardsAttr(TestAttr::B, {N2});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Turn 1 onto B
  setShardsAttr(TestAttr::B, {N1});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_COMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, FmajorityBasicTest2) {
  replication_ = 3;
  setUp();
  for (int i = 0; i < 100; ++i) {
    // set all nodes to have A 100 times
    setAllShardsAttr(TestAttr::A);
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::A});

    // turn 8 and 9 into B
    setShardsAttr(TestAttr::B, {N8, N9});
    // A should still have authoritative f-majority since replication_ is 3,
    // but should be AUTHORITATIVE_INCOMPLETE since there are fully
    // authoritative missing
    verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::A});
    // turn 7 into B
    setShardsAttr(TestAttr::B, {N7});
    // A should lose its fmajority
    verifyFmajority(F::NONE, ALL_ATTR);

    // now turn all nodes except 2-5 to B
    setShardsAttr(TestAttr::B, {N0, N1, N6, N7, N8, N9});
    // despite 4 nodes do not have B, B is still f-majority
    verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});

    // however, if we swap attribute of 7 and 5, B will lose its majority
    setShardsAttr(TestAttr::B, {N5});
    setShardsAttr(TestAttr::A, {N7});
    verifyFmajority(F::NONE, ALL_ATTR);

    setShardsAttr(TestAttr::A, {N2, N3, N4, N5});
    setShardsAttr(TestAttr::B, {N0, N1});
    setShardsAttr(TestAttr::C, {N6});
    setShardsAttr(TestAttr::D, {N7, N8, N9});

    // B, C, D together, should form a f-majority
    auto pred = [](TestAttr attr) {
      return attr == TestAttr::B || attr == TestAttr::C || attr == TestAttr::D;
    };

    ASSERT_EQ(F::AUTHORITATIVE_INCOMPLETE, failure_set_->isFmajority(pred));

    ASSERT_EQ(4, failure_set_->countShards(TestAttr::A));
    ASSERT_EQ(2, failure_set_->countShards(TestAttr::B));
    ASSERT_EQ(1, failure_set_->countShards(TestAttr::C));
    ASSERT_EQ(3, failure_set_->countShards(TestAttr::D));
    ASSERT_EQ(6, failure_set_->countShards(pred));
  }
}

TEST_F(FailureDomainTest, CanReplicationTest) {
  replication_ = 3;
  setUp();
  // turn 8 and 9 into B
  setShardsAttr(TestAttr::B, {N8, N9});
  // cannot replicate on nodes with B
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));
  // add node 7
  setShardsAttr(TestAttr::B, {N7});
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::B));
  setShardsAttr(TestAttr::A, {N2, N3, N4, N5});
  // cannot replicate on nodes with A since all 4 nodes are in the same
  // domain
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::A));
  // node 7: B->A
  setShardsAttr(TestAttr::A, {N7});
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::A));
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // node 4 turns into C
  setShardsAttr(TestAttr::C, {N4});
  // should still be replicatible for A
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::A));
}

TEST_F(FailureDomainTest, AuthoritativeEmptyBasic) {
  replication_ = 3;
  setUp();

  // make 9 empty
  setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N9});

  // make 6 empty
  setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N6});

  // make 8 empty
  setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N8});

  // turn 0, 4, 7 into B
  setShardsAttr(TestAttr::B, {N0, N4, N7});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // turn 1 into B, we should have 4 clusters out of 5
  setShardsAttr(TestAttr::B, {N1});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Cancel 1
  setShardsAttr(TestAttr::A, {N1});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Turn 3 onto B
  setShardsAttr(TestAttr::B, {N3});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Turn 5 onto B
  setShardsAttr(TestAttr::B, {N5});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Turn 2 onto B
  setShardsAttr(TestAttr::B, {N2});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Turn 1 onto B
  setShardsAttr(TestAttr::B, {N1});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_COMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, AuthoritativeStatusBasic) {
  replication_ = 3;
  setUp();

  for (int i = 0; i < 100; ++i) {
    // got all clusters except `cl1`, expect f-majority
    setShardsAttr(TestAttr::B, {N0, N1, N6, N7, N8, N9});
    verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});
    // N9 changed attribute, expect f-majority to be lost
    setShardsAttr(TestAttr::C, {N9});
    // N9 become UNDERREPLICATE, B still shouldn't have f-majority
    setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION, {N9});
    verifyFmajority(F::NONE, ALL_ATTR);
    // However, if N9 becomes AUTHORITATIVE_EMPTY, B should regain f-majority
    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N9});
    verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});

    // N2, N3, N4, N5 become UNDERREPLICATE, B should have complete f-majority,
    // since only one domain is absent
    setShardAuthoritativeStatus(
        AuthoritativeStatus::UNDERREPLICATION, {N2, N3, N4, N5});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::B});

    // N2, N3, N4 become AUTHORITATIVE_EMPTY, B should remain complete
    // f-majority
    setShardAuthoritativeStatus(
        AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N2, N3, N4});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::B});

    // make N9 fully authoritative again, B should be incomplete f-majority
    setShardAuthoritativeStatus(AuthoritativeStatus::FULLY_AUTHORITATIVE, {N9});
    verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});
    // change it back
    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N9});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::B});

    // N5 become AUTHORITATIVE_EMPTY, B should have complete f-majority
    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N5});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::B});

    // N6 and N7 become UNDERREPLICATE, B should be complete f-majority,
    // since it 1) satisfy authoritative f-majority at the NODE scope; and
    // 2) has all fully authoritative nodes
    setShardAuthoritativeStatus(
        AuthoritativeStatus::UNDERREPLICATION, {N6, N7});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, {TestAttr::B});

    // N9 becomes fully authoritative, since it still has attribute of
    // C, B should lost f-majority
    setShardAuthoritativeStatus(AuthoritativeStatus::FULLY_AUTHORITATIVE, {N9});
    verifyFmajority(F::NONE, ALL_ATTR);

    // set 9 back to B, make N1, and N5 UNDERREPLICATE, B should become
    // NON_AUTHORITATIVE majority
    setShardsAttr(TestAttr::B, {N9});
    setShardAuthoritativeStatus(
        AuthoritativeStatus::UNDERREPLICATION, {N1, N5});
    verifyFmajority(F::NON_AUTHORITATIVE, {TestAttr::B});

    // multi-attribute test
    // set all nodes fully authoritative
    setShardAuthoritativeStatus(AuthoritativeStatus::FULLY_AUTHORITATIVE);
    setShardsAttr(TestAttr::C, {N6, N7});
    setShardsAttr(TestAttr::A, {N2, N3, N4, N5});
    setShardsAttr(TestAttr::B, {N0, N1, N8, N9});

    // A+B should be incomplete f-majority
    auto pred = [](TestAttr attr) {
      return attr == TestAttr::A || attr == TestAttr::B;
    };

    ASSERT_EQ(F::AUTHORITATIVE_INCOMPLETE, failure_set_->isFmajority(pred));

    setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION, {N3});
    ASSERT_EQ(F::NONE, failure_set_->isFmajority(pred));

    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N5});
    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N7});
    ASSERT_EQ(F::AUTHORITATIVE_INCOMPLETE, failure_set_->isFmajority(pred));

    setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION, {N4});
    ASSERT_EQ(F::NONE, failure_set_->isFmajority(pred));

    setShardsAttr(TestAttr::B, {N6});
    ASSERT_EQ(F::AUTHORITATIVE_COMPLETE, failure_set_->isFmajority(pred));

    // restore status for next iteration
    failure_set_->resetAttributeCounters();
    setShardAuthoritativeStatus(AuthoritativeStatus::FULLY_AUTHORITATIVE);
  }
}

// if all nodes are in under-replication status, the nodeset should satisfy
// non-authoritative f-majority for any attribute
TEST_F(FailureDomainTest, AllNodesUnderReplication) {
  replication_ = 3;
  setUp();
  for (int i = 0; i < 100; ++i) {
    setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION,
                                {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});
    verifyFmajority(F::NON_AUTHORITATIVE, ALL_ATTR);
  }
}

// if all nodes are in authoritative empty status, the nodeset should satisfy
// complete authoritative f-majority for any attribute
TEST_F(FailureDomainTest, AllNodesAuthoritativeEmpty) {
  replication_ = 3;
  setUp();
  for (int i = 0; i < 100; ++i) {
    setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY,
                                {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9});
    verifyFmajority(F::AUTHORITATIVE_COMPLETE, ALL_ATTR);
  }
}

TEST_F(FailureDomainTest, ReplicationFactorOneScopeRACK) {
  replication_ = 1;
  sync_replication_scope_ = NodeLocationScope::CLUSTER;
  setUp();

  setShardsAttr(TestAttr::B, {N1});
  // can replicate on nodes with B despite its replication scope
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::B));
}

TEST_F(FailureDomainTest, CanReplicateOnMultiScopes) {
  setUpWithMultiScopes();

  setShardsAttr(TestAttr::B, {N0, N1, N2});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  setShardsAttr(TestAttr::B, {N20});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  setShardsAttr(TestAttr::B, {N22});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  setShardsAttr(TestAttr::B, {N8});
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::B));
}

TEST_F(FailureDomainTest, MultiScopeFMajorityWholeRegion) {
  setUpWithMultiScopes();

  // We set the attribute on all the nodes in rg0.

  for (node_index_t i = 0; i < 11; ++i) {
    setShardsAttr(TestAttr::B, {ShardID(i, 0)});
  }
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));
  setShardsAttr(TestAttr::B, {N11});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, MultiScopeFMajorityWholeRegionIsEmpty) {
  setUpWithMultiScopes();

  // A whole region is empty, because we have 2 regions and R(region)=2, we
  // should have a majority.

  for (node_index_t i = 0; i < 11; ++i) {
    setShardAuthoritativeStatus(
        AuthoritativeStatus::AUTHORITATIVE_EMPTY, {ShardID(i, 0)});
  }
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));
  setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N11});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, MultiScopeFMajorityRacks) {
  setUpWithMultiScopes();

  // We set the attribute on 4 racks, leaving 2 racks not complete.

  // rg0.*.rk1
  setShardsAttr(TestAttr::B, {N0, N1, N2, N3});

  // rgN0.*.rk3
  setShardsAttr(TestAttr::B, {N8, N9, N10, N11});

  // rg1.*.rk6
  setShardsAttr(TestAttr::B, {N12, N13, N14, N15});

  // rg1.*.rk7
  setShardsAttr(TestAttr::B, {N16, N17, N18});

  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));
  setShardsAttr(TestAttr::B, {N19});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, MultiScopeFMajorityRacksWithOneRackEmpty) {
  setUpWithMultiScopes();

  // We set the attribute on 3 racks, with 1 rack empty, leaving 2 racks not
  // complete.

  // rg0.*.rk1
  setShardsAttr(TestAttr::B, {N0, N1, N2, N3});

  // rg0.*.rk3
  setShardAuthoritativeStatus(
      AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N8, N9, N10, N11});

  // rg1.*.rk6
  setShardsAttr(TestAttr::B, {N12, N13, N14, N15});

  // rg1.*.rk7
  setShardsAttr(TestAttr::B, {N16, N17, N18});

  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));
  setShardsAttr(TestAttr::B, {N19});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, SeveralShardsFromSameNodeCanReplicate) {
  setUpWithShards();

  // We have two shards spanning two racks. However two of the shards are on N6,
  // so we can't replicate.
  setShardsAttr(TestAttr::B, {ShardID(6, 0), ShardID(6, 1)});
  setShardsAttr(TestAttr::B, {ShardID(10, 0)});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // Adding a shard in N10 will not help either.
  setShardsAttr(TestAttr::B, {ShardID(10, 1)});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // Adding another shard will help.
  setShardsAttr(TestAttr::B, {ShardID(2, 1)});
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::B));
}

TEST_F(FailureDomainTest, SeveralShardsFromSameNodeFMajority) {
  setUpWithShards();

  // Set the attribute on all but 2 shards.
  for (node_index_t i = 0; i < 11; ++i) {
    setShardsAttr(TestAttr::B, {ShardID(i, 0)});
    setShardsAttr(TestAttr::B, {ShardID(i, 1)});
  }

  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, RandomTest1) {
  replication_ = 3;
  setUp();
  // got all clusters except `cl1`, expect f-majority
  setShardsAttr(TestAttr::B, {N0, N1, N6, N7, N8, N9});
  verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});
  // N9 changed attribute, expect f-majority to be lost
  setShardsAttr(TestAttr::C, {N9});
  // N9 become UNDERREPLICATE, B still shouldn't have f-majority
  setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION, {N9});
  verifyFmajority(F::NONE, ALL_ATTR);
  // However, if N9 becomes AUTHORITATIVE_EMPTY, B should regain f-majority
  setShardAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N9});
  verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});
}

TEST_F(FailureDomainTest, RandomTest2) {
  replication_ = 3;
  setUp();
  setShardAuthoritativeStatus(
      AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N0, N1, N6, N7, N8});
  setShardsAttr(TestAttr::B, {N5, N4});
  setShardAuthoritativeStatus(
      AuthoritativeStatus::AUTHORITATIVE_EMPTY, {N3, N2});
  verifyFmajority(F::AUTHORITATIVE_INCOMPLETE, {TestAttr::B});
}

TEST_F(FailureDomainTest, AuthoritativeIncompleteWithShards) {
  setUpWithShards();

  // We have 3 shards spanning 3 nodes and 2 racks underreplicated, not enough
  // to get an authoritative f-majority.

  setShardAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION,
                              {ShardID(0, 1), ShardID(4, 0), ShardID(6, 1)});

  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));
  ld_info("We don't have a majority."); // TOD0

  // Let's set attr B to all the other shards but one, we should still have no
  // majority.

  setShardsAttr(TestAttr::B, {ShardID(0, 0)});
  setShardsAttr(TestAttr::B, {ShardID(1, 0)});
  setShardsAttr(TestAttr::B, {ShardID(1, 1)});
  setShardsAttr(TestAttr::B, {ShardID(2, 0)});
  setShardsAttr(TestAttr::B, {ShardID(2, 1)});
  setShardsAttr(TestAttr::B, {ShardID(3, 0)});
  setShardsAttr(TestAttr::B, {ShardID(3, 1)});
  setShardsAttr(TestAttr::B, {ShardID(4, 1)});
  setShardsAttr(TestAttr::B, {ShardID(5, 0)});
  setShardsAttr(TestAttr::B, {ShardID(5, 1)});
  setShardsAttr(TestAttr::B, {ShardID(6, 0)});
  setShardsAttr(TestAttr::B, {ShardID(7, 1)});
  setShardsAttr(TestAttr::B, {ShardID(8, 0)});
  setShardsAttr(TestAttr::B, {ShardID(8, 1)});
  setShardsAttr(TestAttr::B, {ShardID(9, 0)});
  setShardsAttr(TestAttr::B, {ShardID(9, 1)});
  setShardsAttr(TestAttr::B, {ShardID(10, 0)});
  setShardsAttr(TestAttr::B, {ShardID(10, 1)});
  setShardsAttr(TestAttr::B, {ShardID(11, 0)});
  setShardsAttr(TestAttr::B, {ShardID(11, 1)});

  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Now let's set the attribute to the last shard that's fully authoritative.

  setShardsAttr(TestAttr::B, {ShardID(7, 0)});

  ASSERT_EQ(FmajorityResult::NON_AUTHORITATIVE,
            failure_set_->isFmajority(TestAttr::B));
}

// We only define 2-way replication at RACK scope, replication at SHARD and
// NODE scope is implicit. Check canReplicate works fine.
TEST_F(FailureDomainTest, ImplicitNodeAndShardReplicationCanReplicate) {
  setUpWithShardsAndOnlyRackReplication();

  // With only one shard available, we can't replicate.
  setShardsAttr(TestAttr::B, {ShardID(11, 1)});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // With 2 shards on the same node, we still can't replicate.
  setShardsAttr(TestAttr::B, {ShardID(11, 0)});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // With 3 shards on the same rack, we still can't replicate.
  setShardsAttr(TestAttr::B, {ShardID(8, 1)});
  ASSERT_FALSE(failure_set_->canReplicate(TestAttr::B));

  // After adding one more shard on a different rack we can replicate.
  setShardsAttr(TestAttr::B, {ShardID(0, 1)});
  ASSERT_TRUE(failure_set_->canReplicate(TestAttr::B));
}

// We only define 2-way replication at RACK scope, replication at SHARD and
// NODE scope is implicit. Check isFmajority works fine.
TEST_F(FailureDomainTest, ImplicitNodeAndShardReplicationIsFmajority) {
  setUpWithShardsAndOnlyRackReplication();

  setShardsAttr(TestAttr::B, {ShardID(11, 1)});
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Set the attribute on all shards in rack 3. We need 2 racks to have an
  // f-majority.
  for (node_index_t i = 8; i <= 11; ++i) {
    setShardsAttr(TestAttr::B, {ShardID(i, 0)});
    setShardsAttr(TestAttr::B, {ShardID(i, 1)});
  }
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Set the attribute on all remaining shards in each remaining rack, but each
  // time leaving one shard.
  for (node_index_t i = 0; i <= 3; ++i) {
    setShardsAttr(TestAttr::B, {ShardID(i, 0)});
    if (i != 3) {
      setShardsAttr(TestAttr::B, {ShardID(i, 1)});
    }
  }
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  for (node_index_t i = 4; i <= 7; ++i) {
    setShardsAttr(TestAttr::B, {ShardID(i, 0)});
    if (i != 7) {
      setShardsAttr(TestAttr::B, {ShardID(i, 1)});
    }
  }
  ASSERT_EQ(FmajorityResult::NONE, failure_set_->isFmajority(TestAttr::B));

  // Set the attribute to N3S1. We have all of rack 1 and all of rack 3 so we
  // should have a f-majority.
  setShardsAttr(TestAttr::B, {ShardID(3, 1)});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE,
            failure_set_->isFmajority(TestAttr::B));

  // Set the attribute to N7S1. We have all shards so we should have a complete
  // f-majority.
  setShardsAttr(TestAttr::B, {ShardID(7, 1)});
  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_COMPLETE,
            failure_set_->isFmajority(TestAttr::B));
}

TEST_F(FailureDomainTest, CanReplicationWithPredTest) {
  replication_ = 3;
  setUpWithShardsAndOnlyRackReplication();

  auto pred = [](TestAttr attr) {
    return attr == TestAttr::A || attr == TestAttr::B || attr == TestAttr::C;
  };

  setShardsAttr(TestAttr::A, {N0});
  setShardsAttr(TestAttr::B, {N1});

  // We only have shards tagged with the attribute in one rack.
  ASSERT_FALSE(failure_set_->canReplicate(pred));

  setShardsAttr(TestAttr::C, {N8});

  // Now they span 2 racks.
  ASSERT_TRUE(failure_set_->canReplicate(pred));
}

TEST_F(FailureDomainTest, T30067676) {
  dbg::assertOnData = true;

  configuration::Nodes nodes;
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk1", 4); // 0-3
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk2", 4); // 4-7
  addNodes(&nodes, 4, 1, "rg0.dc0.cl0.ro0.rk3", 4); // 8-11
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk6", 4); // 12-15
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk7", 4); // 16-19
  addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk8", 4); // 20-23

  for (node_index_t nid = 0; nid < 20; ++nid) {
    storage_set_.push_back(ShardID(nid, 0));
  }

  Configuration::NodesConfig nodes_config;
  const size_t nodeset_size = nodes.size();
  nodes_config.setNodes(std::move(nodes));

  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, 0, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "failure_domain_test", std::move(nodes_config)),
      std::move(logs_config));

  ReplicationProperty rep;
  rep.setReplication(NodeLocationScope::NODE, 2);
  rep.setReplication(NodeLocationScope::RACK, 2);

  failure_set_ = std::make_unique<FailureDomainTestSet>(
      storage_set_,
      *config_->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      rep);
  failure_set_->fullConsistencyCheck();

  setShardsAttr(TestAttr(RecoveryNode::State::DIGESTING), {N0});
  setShardsAttr(TestAttr(RecoveryNode::State::DIGESTED), {N19});
  setShardsAttr(TestAttr(RecoveryNode::State::SEALING), {N3});
  setShardsAttr(TestAttr(RecoveryNode::State::MUTATABLE),
                {N1,
                 N2,
                 N4,
                 N5,
                 N6,
                 N7,
                 N8,
                 N9,
                 N10,
                 N11,
                 N12,
                 N13,
                 N14,
                 N15,
                 N16,
                 N17,
                 N18});

  const auto state = RecoveryNode::State::DIGESTED;
  auto ret = failure_set_->isFmajority(
      [](TestAttr node_state) { return node_state >= TestAttr(state); });

  ASSERT_EQ(FmajorityResult::AUTHORITATIVE_INCOMPLETE, ret);
}

} // anonymous namespace
