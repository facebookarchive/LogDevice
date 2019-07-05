/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <queue>

#include <folly/Memory.h>
#include <folly/String.h>
#include <gtest/gtest.h>
#include <logdevice/common/CrossDomainCopySetSelector.h>
#include <logdevice/common/LinearCopySetSelector.h>
#include <logdevice/common/PassThroughCopySetManager.h>
#include <logdevice/common/RebuildingTypes.h>
#include <logdevice/common/StickyCopySetManager.h>

#include "logdevice/common/CopySetSelectorFactory.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/CopySetSelectorTestUtil.h"
#include "logdevice/common/test/NodeSetStateTest.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;
using NodeStatus = NodeAvailabilityChecker::NodeStatus;

#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)

namespace { // anonymous namespace
class MockLinearCopySetSelector;
class MockCrossDomainCopySetSelector;
class MockStickyCopySetManager;

enum class CopySetSelectorType {
  // LinearCopySetSelector
  LINEAR,
  // CrossDomainCopySetSelector
  CROSS_DOMAIN,
  // WeightedCopySetSelector
  WEIGHTED,
};

class CopySetSelectorTest : public ::testing::Test {
 public:
  const logid_t LOG_ID{2};

  //// properties of log
  copyset_size_t replication_{3};
  copyset_size_t extras_{0};
  NodeLocationScope sync_replication_scope_{NodeLocationScope::RACK};

  // append properties - its size and LSN
  CopySetManager::AppendContext append_ctx_{1024, lsn_t(1)};

  // indicate if chaining is enabled
  bool enable_chain_{false};

  CopySetSelectorType copyset_selector_type_{CopySetSelectorType::CROSS_DOMAIN};

  StorageSet nodeset_;
  std::shared_ptr<NodeSetState> nodeset_state_;
  std::unique_ptr<CopySetManager> copyset_manager_;
  bool sticky_copysets_{false};
  size_t sticky_copysets_block_size_{64 * 1024 * 1024};
  std::chrono::milliseconds sticky_copysets_block_max_time_{3600 * 1000};

  // internal state of the copyset_selector_
  std::unique_ptr<CopySetManager::State> csm_state_;

  ////// cluster information

  // cluster config
  std::shared_ptr<Configuration> config_;

  // NodeID of the sequencer node
  node_index_t seq_node_idx_{2};

  // location domain name of the sequencer node
  std::string seq_domain_name_;
  // node location hierarchy computed from the nodeset
  std::unique_ptr<NodeLocationHierarchy> hierarchy_;

  // group of nodes grouped by sync_replication_scope_
  std::map<std::string, StorageSet> node_groups_;

  // existing copyset, used for testing restoring replication factor
  std::vector<ShardID> existing_copyset_;
  std::vector<StoreChainLink> existing_copyset_chain_;

  TestCopySetSelectorDeps deps_;

  std::shared_ptr<Configuration> getConfig() const {
    return config_;
  }

  size_t getClusterSize() const {
    // TODO: test non-consecutive node indexes
    // TODO: migrate it to use NodesConfiguration with switchable source
    return config_->serverConfig()
        ->getNodesConfigurationFromServerConfigSource()
        ->clusterSize();
  }

  NodeID getMyNodeID() {
    return NodeID(seq_node_idx_, 1);
  }

  // helper functions
  void setUp();

  // ASSERT internally
  void verifyCopySet(CopySetSelector::Result result,
                     const StoreChainLink copyset[],
                     copyset_size_t size,
                     bool chain_result) const;

  void verifyCopySet() const;

  // ASSERT internally
  void verifySingleLocationScope(const std::vector<ShardID>& nodes,
                                 bool chain_result) const;

  size_t countShardsWithDomainName(const StorageSet& nodes,
                                   const std::string& domain_name) const;

  size_t countShardsWithDomainName(const StoreChainLink copyset[],
                                   copyset_size_t size,
                                   const std::string& domain_name) const;

  size_t countShardsWithDomainName(const std::string& domain_name) const;

  std::string
  getNodeDomainName(node_index_t node,
                    NodeLocationScope scope = NodeLocationScope::NODE) const;

  inline void augmentTestBody();
  inline void augmentChainTestBody();

 private:
  std::map<std::string, StorageSet>
  buildNodeGroups(const StorageSet& shards) const;

 public:
  // store results of the selection
  struct Result {
    std::array<StoreChainLink, COPYSET_SIZE_MAX> copyset;
    copyset_size_t ndest{0};
    bool chain_out;
    folly::Optional<lsn_t> block_starting_lsn;
    CopySetSelector::Result rv;
  };

  Result result_;

  // For sticky copyset selector - the queue of results that the underlying
  // copyset selector should return
  std::queue<Result> underlying_result_queue_;

  void selectCopySet();
  void augmentCopySet(RNG& rng = DefaultRNG::get());
  void augmentCopySetChain();
};

class MockLinearCopySetSelector : public LinearCopySetSelector {
 public:
  explicit MockLinearCopySetSelector(CopySetSelectorTest* test)
      : LinearCopySetSelector(test->replication_,
                              test->nodeset_,
                              test->nodeset_state_,
                              &test->deps_) {}
};

class MockCrossDomainCopySetSelector : public CrossDomainCopySetSelector {
 public:
  explicit MockCrossDomainCopySetSelector(CopySetSelectorTest* test)
      : CrossDomainCopySetSelector(
            test->LOG_ID,
            test->nodeset_,
            test->nodeset_state_,
            test->getConfig()
                ->serverConfig()
                ->getNodesConfigurationFromServerConfigSource(),
            test->getMyNodeID(),
            test->replication_,
            test->sync_replication_scope_,
            &test->deps_) {}
};

class MockStickyCopySetManager : public StickyCopySetManager {
  // This MockStickyCopySetSelector's underlying selector just returns
  // results from a prepopulated queue on every SubSelector::select() call.
  // We thus verify correctness by feeding it the results we'd like returned
  // and checking that we are getting these from
  // MockStickyCopySetSelector::select() when we expect to.
  class SubSelector : public CopySetSelector {
   public:
    class State : public CopySetSelector::State {
      void reset() override {}
    };

    explicit SubSelector(CopySetSelectorTest* test) : test_(test) {}
    Result select(copyset_size_t /*extras*/,
                  StoreChainLink copyset_out[],
                  copyset_size_t* copyset_size_out,
                  bool* chain_out,
                  CopySetSelector::State* /*selector_state*/,
                  RNG&,
                  bool /* retry, unused */) const override {
      // If the following assert fails, it means more calls were done to
      // select() than the test expected, and the result queue was drained
      // prematurely.
      ld_check(test_->underlying_result_queue_.size() > 0);
      auto& result_set = test_->underlying_result_queue_.front();
      const StoreChainLink* src = result_set.copyset.data();
      std::copy(src, src + result_set.ndest, copyset_out);
      if (copyset_size_out) {
        *copyset_size_out = result_set.ndest;
      }
      if (chain_out) {
        *chain_out = result_set.chain_out;
      }
      Result rv = result_set.rv;
      test_->underlying_result_queue_.pop();
      return rv;
    }

    // Not used in tests
    Result augment(ShardID[],
                   copyset_size_t,
                   copyset_size_t*,
                   RNG&,
                   bool) const override {
      ld_check(false);
      return Result::FAILED;
    }

    Result augment(StoreChainLink[] /* unused */,
                   copyset_size_t /* unused */,
                   copyset_size_t* /* unused */,
                   bool /* unused */,
                   bool* /* unused */,
                   RNG& /* unused */,
                   bool /* unused */) const override {
      throw std::runtime_error("unimplemented");
    }

    copyset_size_t getReplicationFactor() const override {
      return test_->replication_;
    }

   private:
    CopySetSelectorTest* const test_;
  };

 public:
  explicit MockStickyCopySetManager(CopySetSelectorTest* test)
      : StickyCopySetManager(std::make_unique<SubSelector>(test),
                             test->nodeset_state_,
                             test->sticky_copysets_block_size_,
                             test->sticky_copysets_block_max_time_,
                             &test->deps_) {}

 private:
};

} // end of anonymous namespace

/**
 * Default Cluster Topology:
 *                              ROOT
 *                     _         |       _
 *              /                |               \
 *            rg0               rg1              rg2
 *             |                 |                |
 *            dc0               dc0              dc0
 *             |                 |                |
 *            cl0               cl0              cl0
 *             |                 |                |
 *            ro0               ro0              ro0
 *             |              /  |  \           /   \
 *            rk0           rk0 rk1 rk2        rk0  rk1
 *
 *   node 0:     rg0.dc0.cl0.ro0.rk0
 *   node 1:     rg1.dc0.cl0.ro0.rk0
 *   node 2, 3:  rg1.dc0.cl0.ro0.rk1
 *   node 4:     rg1.dc0.cl0.ro0.rk2
 *   node 5:     rg1.dc0.cl0..
 *   node 6:     rg2.dc0.cl0.ro0.rk0
 *   node 7:     rg2.dc0.cl0.ro0.rk1
 *   node 8:     ....
 */

void CopySetSelectorTest::setUp() {
  dbg::assertOnData = true;

  configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, "rg0.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 2, 1, "rg1.dc0.cl0.ro0.rk1", 2);
  addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk2", 1);
  addNodes(&nodes, 1, 1, "rg1.dc0.cl0..", 1);
  addNodes(&nodes, 1, 1, "rg2.dc0.cl0.ro0.rk0", 1);
  addNodes(&nodes, 1, 1, "rg2.dc0.cl0.ro0.rk1", 1);
  addNodes(&nodes, 1, 1, "....", 1);

  const size_t nodeset_size = nodes.size();
  configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_unique<configuration::LocalLogsConfig>();
  addLog(logs_config.get(), LOG_ID, replication_, extras_, nodeset_size, {});

  config_ = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "copyset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  NodeID seq_node_id = getMyNodeID();
  ASSERT_TRUE(seq_node_id.isNodeID());
  ld_info("My node id: %s", seq_node_id.toString().c_str());

  const Configuration::Node* my_node =
      config_->serverConfig()->getNode(seq_node_id.index());
  ASSERT_NE(nullptr, my_node);
  if (my_node->location.hasValue()) {
    seq_domain_name_ = my_node->location.value().getDomain();
  }

  StorageSet nodeset_indices(getClusterSize());
  for (size_t i = 0; i < getClusterSize(); ++i) {
    nodeset_indices[i] = ShardID(i, 0);
  }
  nodeset_ = nodeset_indices;
  nodeset_state_ = std::make_shared<NodeSetState>(
      nodeset_indices, LOG_ID, NodeSetState::HealthCheck::DISABLED);

  hierarchy_ = std::make_unique<NodeLocationHierarchy>(
      getConfig()
          ->serverConfig()
          ->getNodesConfigurationFromServerConfigSource(),
      nodeset_indices);

  if (sticky_copysets_) {
    copyset_manager_.reset(new MockStickyCopySetManager(this));
  } else {
    // perform location-based setup
    std::unique_ptr<CopySetSelector> copyset_selector;
    if (copyset_selector_type_ == CopySetSelectorType::CROSS_DOMAIN) {
      node_groups_ = buildNodeGroups(nodeset_indices);
      copyset_selector.reset(new MockCrossDomainCopySetSelector(this));
    } else if (copyset_selector_type_ == CopySetSelectorType::LINEAR) {
      copyset_selector.reset(new MockLinearCopySetSelector(this));
    } else {
      // currently we only test the CROSS_DOMAIN and LINEAR selector
      ld_check(false);
    }
    copyset_manager_.reset(new PassThroughCopySetManager(
        std::move(copyset_selector), nodeset_state_));
  }

  ASSERT_NE(nullptr, copyset_manager_);
  copyset_manager_->disableCopySetShuffling();
  csm_state_ = copyset_manager_->createState();
}

inline void CopySetSelectorTest::verifyCopySet(CopySetSelector::Result result,
                                               const StoreChainLink copyset[],
                                               copyset_size_t size,
                                               bool chain_result) const {
  ASSERT_NE(CopySetSelector::Result::FAILED, result);
  // check copyset size
  if (result == CopySetSelector::Result::SUCCESS) {
    ASSERT_EQ(replication_ + extras_, size);
  } else if (result == CopySetSelector::Result::PARTIAL) {
    ASSERT_LE(size, replication_ + extras_);
    ASSERT_GE(size, replication_);
  } else {
    ld_check(false);
  }

  std::vector<ShardID> indices(size);
  std::transform(
      copyset, copyset + size, indices.begin(), [](const StoreChainLink& c) {
        return c.destination;
      });

  std::vector<std::string> s(size);
  std::transform(indices.begin(), indices.end(), s.begin(), [](ShardID i) {
    return i.toString();
  });
  ld_info("Copyset { %s }, chain sending: %s",
          folly::join(',', s).c_str(),
          chain_result ? "Enabled" : "Disabled");

  // if chainning is disabled, the result should not enable chainning
  if (!enable_chain_) {
    ASSERT_FALSE(chain_result);
  }

  // each node must be in the nodeset
  ASSERT_TRUE(std::all_of(
      indices.cbegin(), indices.cend(), [this](const ShardID& shard) {
        const auto& nodes = nodeset_;
        return std::find(nodes.begin(), nodes.end(), shard) != nodes.end();
      }));

  // make sure there is no duplicates in the copyset
  std::vector<ShardID> dup(indices);
  std::sort(dup.begin(), dup.end());
  ASSERT_EQ(dup.end(), std::unique(dup.begin(), dup.end()));

  // make sure unavailable nodes are not selected
  ASSERT_EQ(0, deps_.countUnavailable(indices));

  if (copyset_selector_type_ == CopySetSelectorType::CROSS_DOMAIN) {
    if (!sticky_copysets_) {
      // If sticky copysets are enabled, we are only testing the
      // StickyCopySetSelector, not the underlying selector
      verifySingleLocationScope(indices, chain_result);
    }
  }
}

inline void CopySetSelectorTest::verifyCopySet() const {
  verifyCopySet(
      result_.rv, result_.copyset.data(), result_.ndest, result_.chain_out);
}

std::map<std::string, StorageSet>
CopySetSelectorTest::buildNodeGroups(const StorageSet& shards) const {
  ld_check(shards.size() > 0);
  std::map<std::string, StorageSet> scope_map;
  for (const auto& i : shards) {
    const Configuration::Node* node =
        config_->serverConfig()->getNode(i.node());
    ld_check(node);
    ld_check(node->location.hasValue());
    const auto& location = node->location.value();
    scope_map[location.getDomain(sync_replication_scope_)].push_back(i);
  }

  return scope_map;
}

std::string
CopySetSelectorTest::getNodeDomainName(node_index_t index,
                                       NodeLocationScope scope) const {
  const Configuration::Node* node = config_->serverConfig()->getNode(index);
  if (node && node->location.hasValue()) {
    return node->location.value().getDomain(scope);
  }
  return "";
}

size_t CopySetSelectorTest::countShardsWithDomainName(
    const StorageSet& shards,
    const std::string& domain_name) const {
  return std::count_if(
      shards.begin(), shards.end(), [this, &domain_name](const ShardID& i) {
        NodeLocation loc;
        const Configuration::Node* node =
            config_->serverConfig()->getNode(i.node());
        int rv = loc.fromDomainString(domain_name);
        ld_check(rv == 0);
        return node && node->location.hasValue() &&
            node->location.value().sharesScopeWith(loc, loc.lastScope());
      });
}

size_t CopySetSelectorTest::countShardsWithDomainName(
    const StoreChainLink copyset[],
    copyset_size_t size,
    const std::string& domain_name) const {
  std::vector<ShardID> shards(size);
  std::transform(
      copyset, copyset + size, shards.begin(), [](const StoreChainLink& c) {
        return c.destination;
      });
  return countShardsWithDomainName(shards, domain_name);
}

size_t CopySetSelectorTest::countShardsWithDomainName(
    const std::string& domain_name) const {
  return countShardsWithDomainName(
      result_.copyset.data(), result_.ndest, domain_name);
}

inline void CopySetSelectorTest::verifySingleLocationScope(
    const std::vector<ShardID>& shards,
    bool chain_result) const {
  auto groups = buildNodeGroups(shards);

  // if replication_ > 1, then copyset must span across at least
  // 2 different domains to satisfy failure domain requirements
  if (replication_ > 1) {
    ASSERT_GE(groups.size(), 2);
  }

  // if local scope group has at least one available node,
  // the copyset must contain nodes from the local scope group
  if (!seq_domain_name_.empty()) {
    auto it = node_groups_.find(seq_domain_name_);
    if (it != node_groups_.end()) {
      const size_t avail_local_nodes =
          it->second.size() - deps_.countUnavailable(it->second);
      if (avail_local_nodes > 0) {
        auto it = groups.find(seq_domain_name_);
        ASSERT_NE(groups.end(), it);
        ASSERT_GE(
            it->second.size(),
            std::min(avail_local_nodes,
                     (size_t)CrossDomainCopySetSelector::nodesInPrimaryDomain(
                         sync_replication_scope_, replication_, chain_result)));
      } else {
        ASSERT_EQ(groups.end(), groups.find(seq_domain_name_));
      }
    }
  }
}

inline void CopySetSelectorTest::selectCopySet() {
  ASSERT_NE(nullptr, copyset_manager_);
  result_.chain_out = enable_chain_;
  result_.rv = copyset_manager_->getCopySet(extras_,
                                            result_.copyset.data(),
                                            &result_.ndest,
                                            &result_.chain_out,
                                            append_ctx_,
                                            result_.block_starting_lsn,
                                            *csm_state_);
}

inline void CopySetSelectorTest::augmentCopySet(RNG& rng) {
  ASSERT_NE(nullptr, copyset_manager_);
  result_.chain_out = false;

  CopySetSelector* copyset_selector = copyset_manager_->getCopySetSelector();
  ASSERT_NE(nullptr, copyset_selector);

  ASSERT_LT(existing_copyset_.size(), replication_);

  std::vector<ShardID> copyset = existing_copyset_;
  copyset.resize(existing_copyset_.size() + replication_);
  copyset_size_t new_size;
  result_.rv = copyset_selector->augment(
      copyset.data(), existing_copyset_.size(), &new_size, rng);

  // This is not true in general but true for all existing tests here.
  ASSERT_EQ(new_size, replication_);

  copyset.resize(new_size);
  EXPECT_EQ(existing_copyset_,
            std::vector<ShardID>(
                copyset.begin(), copyset.begin() + existing_copyset_.size()));

  if (result_.rv == CopySetSelector::Result::SUCCESS) {
    std::transform(copyset.begin(),
                   copyset.end(),
                   result_.copyset.begin(),
                   [](ShardID idx) {
                     return StoreChainLink{idx, ClientID()};
                   });

    result_.ndest = replication_;
  }
}

// TODO(T16599789): DRY after deprecating the old augment API
inline void CopySetSelectorTest::augmentCopySetChain() {
  ASSERT_NE(nullptr, copyset_manager_);
  result_.chain_out = true;

  CopySetSelector* copyset_selector = copyset_manager_->getCopySetSelector();
  ASSERT_NE(nullptr, copyset_selector);

  ASSERT_LT(existing_copyset_.size(), replication_);

  std::vector<StoreChainLink> copyset_chain = existing_copyset_chain_;
  copyset_chain.resize(existing_copyset_.size() + replication_);
  copyset_size_t new_size;
  result_.rv = copyset_selector->augment(copyset_chain.data(),
                                         existing_copyset_chain_.size(),
                                         &new_size,
                                         /* fill_client_id */ true,
                                         &result_.chain_out);

  // This is not true in general but true for all existing tests here.
  ASSERT_EQ(new_size, replication_);

  copyset_chain.resize(new_size);
  EXPECT_EQ(existing_copyset_chain_,
            std::vector<StoreChainLink>(
                copyset_chain.begin(),
                copyset_chain.begin() + existing_copyset_chain_.size()));

  if (result_.rv == CopySetSelector::Result::SUCCESS) {
    // TODO(T32598982): currently, the TestCopySetSelectorDeps does not mock
    // ClientIDs and only issues ClientID::INVALID
    std::move(
        copyset_chain.begin(), copyset_chain.end(), result_.copyset.begin());

    result_.ndest = replication_;
  }
}

TEST_F(CopySetSelectorTest, SimpleRack) {
  replication_ = 3;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  setUp();

  selectCopySet();

  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  ASSERT_EQ(3, result_.ndest);
  verifyCopySet();

  // in case all nodes are available, there should be 2 copies on the local
  // rack
  const size_t local_nodes = countShardsWithDomainName(seq_domain_name_);
  ASSERT_EQ(2, local_nodes);
}

TEST_F(CopySetSelectorTest, SimpleRackWithChaining) {
  replication_ = 3;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  enable_chain_ = true;
  setUp();

  selectCopySet();

  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  ASSERT_EQ(3, result_.ndest);
  verifyCopySet();

  // in case all nodes are available, there should be 2 copies on the local
  // rack
  const size_t local_nodes = countShardsWithDomainName(seq_domain_name_);
  ASSERT_EQ(1, local_nodes);
}

TEST_F(CopySetSelectorTest, SingleReplicationWithLocality) {
  replication_ = 1;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  // sequencer is on node 1
  seq_node_idx_ = 1;
  setUp();

  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // the copy should land on node 1
  ASSERT_EQ(1, countShardsWithDomainName(seq_domain_name_));

  // disable node 1
  deps_.setNotAvailableNodes({N1});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  // the copy should still land on the same row (ro0)
  ASSERT_EQ(
      1,
      countShardsWithDomainName(getNodeDomainName(1, NodeLocationScope::ROW)));

  // disable node 1, 2 and 3
  deps_.setNotAvailableNodes({N1, N2, N3});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  // the copy should land on node 4 exactly
  ASSERT_EQ(
      1,
      countShardsWithDomainName(getNodeDomainName(4, NodeLocationScope::RACK)));

  // disable node 1, 2, 3, 4
  deps_.setNotAvailableNodes({N1, N2, N3, N4});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  // the copy should land on node 5 exactly
  ASSERT_EQ(1, countShardsWithDomainName(getNodeDomainName(5)));
}

// currently extras are ignored in implementation, but if it is set > 0,
// CopySetSelector::Result::PARTIAL will be returned instead of
// SUCCESS
TEST_F(CopySetSelectorTest, TestExtras) {
  replication_ = 3;
  extras_ = 1;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  selectCopySet();

  ASSERT_EQ(CopySetSelector::Result::PARTIAL, result_.rv);
  ASSERT_EQ(3, result_.ndest); // only 3 copies are selected
  verifyCopySet();
  ASSERT_EQ(2, countShardsWithDomainName(seq_domain_name_));
}

// despite that the location of sequencer is unknown, the selector should
// still be able to select nodes with failure domain requirements
TEST_F(CopySetSelectorTest, SequencerLocationUnknown) {
  // node 8 does not have any effective scope specified
  seq_node_idx_ = 8;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  selectCopySet();

  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  ASSERT_EQ(3, result_.ndest);
  verifyCopySet();
}

// not enough available nodes in the local domain, but other racks have enough
// nodes to satisfy the request
TEST_F(CopySetSelectorTest, LocalDomainUnavailable) {
  replication_ = 3;
  extras_ = 0;
  // sequencer node is in region 2
  seq_node_idx_ = 6;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  // all nodes in region 2 are unavailable
  deps_.setNotAvailableNodes({N6, N7});
  // despite that nodes are not available in the local, we should still be
  // able to pick a copyset
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // there should never be nodes in region 2
  const size_t local_nodes = countShardsWithDomainName(
      getNodeDomainName(6, NodeLocationScope::REGION));
  ASSERT_EQ(0, local_nodes);
}

// One rack has enough nodes available to pick a copyset but all other racks
// are unavailable
TEST_F(CopySetSelectorTest, OnlyOneRackAvailable) {
  replication_ = 3;
  extras_ = 0;
  // sequencer node is in region 1
  seq_node_idx_ = 1;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  // all nodes in region 0 and region 2 are not available
  deps_.setNotAvailableNodes({ShardID(0, 0), N6, N7});

  selectCopySet();
  // should not be able to select copysets despite there are 5 nodes available
  // in region 1
  ASSERT_EQ(CopySetSelector::Result::FAILED, result_.rv);

  // this time makes one node available in region 2
  deps_.setNotAvailableNodes({ShardID(0, 0), N6});

  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // result should be 2 copies in region 1 and 1 copy in node 7
  ASSERT_EQ(0, countShardsWithDomainName("rg0...."));
  ASSERT_EQ(2, countShardsWithDomainName("rg1...."));
  ASSERT_EQ(1, countShardsWithDomainName("rg2...."));
}

TEST_F(CopySetSelectorTest, HighReplication) {
  replication_ = 5;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // disable node 1, 2, 3 from region 1
  deps_.setNotAvailableNodes({N1, N2, N3});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  ASSERT_EQ(1, countShardsWithDomainName("rg0...."));
  ASSERT_EQ(2, countShardsWithDomainName("rg1...."));
  ASSERT_EQ(2, countShardsWithDomainName("rg2...."));
}

TEST_F(CopySetSelectorTest, HighReplicationChainSending) {
  replication_ = 5;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::CLUSTER;
  // sequencer node is in region 1, cluster 0
  seq_node_idx_ = 2;
  enable_chain_ = true;
  setUp();

  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  EXPECT_TRUE(result_.chain_out);

  // disable node 1, 5, 6
  deps_.setNotAvailableNodes({N1, N5, N6});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // final copyset should be {2, 3, 4, 7, 0}
  ASSERT_EQ(1, countShardsWithDomainName("rg0.dc0.cl0.."));
  ASSERT_EQ(3, countShardsWithDomainName("rg1.dc0.cl0.."));
  ASSERT_EQ(1, countShardsWithDomainName("rg2.dc0.cl0.."));
}

TEST_F(CopySetSelectorTest, LinearSelectionCanFish) {
  replication_ = 3;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::LINEAR;
  setUp();

  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // disable all nodes
  deps_.setNotAvailableNodes({ShardID(0, 0), N1, N2, N3, N4, N5, N6, N7, N8});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::FAILED, result_.rv);
}

TEST_F(CopySetSelectorTest, CrossDomainSelectionCanFish) {
  replication_ = 3;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  // disable all nodes
  deps_.setNotAvailableNodes({ShardID(0, 0), N1, N2, N3, N4, N5, N6, N7, N8});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::FAILED, result_.rv);
  // disable some nodes, but leave 1 in each domain to trigger selecting nodes
  // from 2 secondary domains
  deps_.setNotAvailableNodes({N2, N3, N4, N5, N7, N8});
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
}

TEST_F(CopySetSelectorTest, AugmentCrossDomain) {
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  augmentTestBody();

  // TODO(T32598982): Currently this statement does not do anything until we
  // mock out NodeAvailabilityChecker and actually test the returned ClientIDs.
  enable_chain_ = true;
  augmentChainTestBody();
}

TEST_F(CopySetSelectorTest, AugmentLinear) {
  copyset_selector_type_ = CopySetSelectorType::LINEAR;
  sync_replication_scope_ = NodeLocationScope::NODE;
  augmentTestBody();

  // TODO(T32598982): Currently this statement does not do anything until we
  // mock out NodeAvailabilityChecker and actually test the returned ClientIDs.
  enable_chain_ = true;
  augmentChainTestBody();
}

inline void CopySetSelectorTest::augmentTestBody() {
  replication_ = 5;
  extras_ = 0;
  // sequencer node is in region 1
  seq_node_idx_ = 1;
  setUp();

  // existing copyset is empty
  existing_copyset_ = {};
  augmentCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from primary domain (region 1)
  existing_copyset_ = {N1, N2, N3};
  augmentCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from other domains
  existing_copyset_ = {ShardID(0, 0), N6};
  augmentCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from both primary and other domains
  existing_copyset_ = {N1, N7};
  augmentCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  auto gen_rand = [](int a, int b) {
    ld_check(a <= b);
    ld_check(a >= 0);
    ld_check(b < std::numeric_limits<int>::max());
    return folly::Random::rand32(a, b + 1);
  };

  // random testing
  for (size_t i = 0; i < 1000; ++i) {
    seq_node_idx_ = gen_rand(0, 8);
    existing_copyset_.resize(9);
    for (size_t j = 0; j <= 8; ++j) {
      existing_copyset_[j] = ShardID(j, 0);
    }
    std::shuffle(existing_copyset_.begin(),
                 existing_copyset_.end(),
                 folly::ThreadLocalPRNG());
    copyset_size_t existing_size = gen_rand(0, replication_ - 1);
    existing_copyset_.resize(existing_size);

    augmentCopySet();
    ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
    verifyCopySet();
  }
}

inline void CopySetSelectorTest::augmentChainTestBody() {
  replication_ = 5;
  extras_ = 0;
  // sequencer node is in region 1
  seq_node_idx_ = 1;
  setUp();

  // existing copyset is empty
  existing_copyset_chain_ = {};
  augmentCopySetChain();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from primary domain (region 1)
  existing_copyset_chain_ = {
      {N1, ClientID()}, {N2, ClientID()}, {N3, ClientID()}};
  augmentCopySetChain();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from other domains
  existing_copyset_ = {ShardID(0, 0), N6};
  augmentCopySetChain();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // existing copyset are all from both primary and other domains
  existing_copyset_ = {N1, N7};
  augmentCopySetChain();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  auto gen_rand = [](int a, int b) {
    ld_check(a <= b);
    ld_check(a >= 0);
    ld_check(b < std::numeric_limits<int>::max());
    return folly::Random::rand32(a, b + 1);
  };

  // random testing
  for (size_t i = 0; i < 1000; ++i) {
    seq_node_idx_ = gen_rand(0, 8);
    existing_copyset_.resize(9);
    for (size_t j = 0; j <= 8; ++j) {
      existing_copyset_[j] = ShardID(j, 0);
    }
    std::shuffle(existing_copyset_.begin(),
                 existing_copyset_.end(),
                 folly::ThreadLocalPRNG());
    copyset_size_t existing_size = gen_rand(0, replication_ - 1);
    existing_copyset_.resize(existing_size);

    augmentCopySetChain();
    ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
    verifyCopySet();
  }
}

TEST_F(CopySetSelectorTest, CustomRNG) {
  replication_ = 5;
  extras_ = 0;
  // sequencer node is in region 1
  seq_node_idx_ = 1;
  XorShift128PRNG rng;
  setUp();

  std::vector<std::vector<ShardID>> copysets_to_test = {
      {},
      {N1, N2, N3},
      {ShardID(0, 0), N6},
      {N1, N7},
  };

  for (int i = 0; i < copysets_to_test.size(); i++) {
    std::vector<ShardID> new_copyset;
    for (int x = 0; x < 2; ++x) {
      // Seeding with a hash of the existing copyset
      uint32_t rng_seed[4];
      RecordRebuildingInterface::getRNGSeedFromRecord(
          rng_seed, existing_copyset_.data(), existing_copyset_.size(), 0);
      rng.seed(rng_seed);

      augmentCopySet(rng);
      ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
      verifyCopySet();
      if (x == 0) {
        // Saving the copyset for comparing later
        new_copyset.resize(result_.ndest);
        std::transform(result_.copyset.data(),
                       result_.copyset.data() + result_.ndest,
                       new_copyset.begin(),
                       [](const StoreChainLink& c) { return c.destination; });
      } else {
        // Comparing that results are identical
        std::vector<ShardID> indices;
        indices.resize(result_.ndest);
        std::transform(result_.copyset.data(),
                       result_.copyset.data() + result_.ndest,
                       indices.begin(),
                       [](const StoreChainLink& c) { return c.destination; });
        ASSERT_EQ(new_copyset, indices);
      }
    }
  }
}

TEST_F(CopySetSelectorTest, StickyCopySetManager) {
  replication_ = 3;
  extras_ = 0;

  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;

  sticky_copysets_ = true;
  sticky_copysets_block_size_ = 2048;
  sticky_copysets_block_max_time_ = std::chrono::milliseconds(3600 * 1000);
  setUp();

  Result res;
  res.copyset[0] = {ShardID(0, 0), ClientID()};
  res.copyset[1] = {N1, ClientID()};
  res.copyset[2] = {N2, ClientID()};
  res.ndest = 3;
  res.chain_out = false;
  res.rv = CopySetSelector::Result::SUCCESS;
  underlying_result_queue_.push(res);

  // selecting the copyset for the first time. This should drain the result
  // queue.
  ld_info("Selecting copyset 1");
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  ASSERT_EQ(ShardID(0, 0), result_.copyset[0].destination);
  ASSERT_EQ(N1, result_.copyset[1].destination);
  ASSERT_EQ(N2, result_.copyset[2].destination);

  // selecting the second time - we expect no calls to the underlying selector
  // at this point
  ld_info("Selecting copyset 2");
  csm_state_ = copyset_manager_->createState();
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();

  // selecting the third time - we are going over the block size boundary,
  // so we expect the copyset selector to select a new copyset
  res.copyset[0] = {N3, ClientID()};
  res.copyset[1] = {N4, ClientID()};
  res.copyset[2] = {N5, ClientID()};
  underlying_result_queue_.push(res);

  ld_info("Selecting copyset 3");
  csm_state_ = copyset_manager_->createState();
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  ASSERT_EQ(0, underlying_result_queue_.size());
  ASSERT_EQ(N3, result_.copyset[0].destination);
  ASSERT_EQ(N4, result_.copyset[1].destination);
  ASSERT_EQ(N5, result_.copyset[2].destination);

  // making a node unavailable - this should force a reselection of the copyset
  res.copyset[0] = {N6, ClientID()};
  res.copyset[1] = {N7, ClientID()};
  res.copyset[2] = {N8, ClientID()};
  underlying_result_queue_.push(res);
  deps_.setNodeStatus(N3, NodeStatus::NOT_AVAILABLE, ClientID::MIN);

  ld_info("Selecting copyset 4");
  csm_state_ = copyset_manager_->createState();
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  ASSERT_EQ(0, underlying_result_queue_.size());
  ASSERT_EQ(N6, result_.copyset[0].destination);
  ASSERT_EQ(N7, result_.copyset[1].destination);
  ASSERT_EQ(N8, result_.copyset[2].destination);

  // simulating a 2nd wave in case a STORE failed by not re-creating the
  // copyset selector state we have from the last call.
  csm_state_->reset();
  res.copyset[0] = {N5, ClientID()};
  res.copyset[1] = {N6, ClientID()};
  res.copyset[2] = {N7, ClientID()};
  underlying_result_queue_.push(res);
  deps_.setNodeStatus(N8, NodeStatus::NOT_AVAILABLE, ClientID::MIN);

  ld_info("Selecting copyset 5");
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
  verifyCopySet();
  ASSERT_EQ(0, underlying_result_queue_.size());
  ASSERT_EQ(N5, result_.copyset[0].destination);
  ASSERT_EQ(N6, result_.copyset[1].destination);
  ASSERT_EQ(N7, result_.copyset[2].destination);

  // Making a selected nodes unavailable - this should force a change in
  // the copyset set. If the underlying selector is unable to select a copyset
  // though, we should fail to establish a copyset set.
  res.ndest = 0;
  res.rv = CopySetSelector::Result::FAILED;
  underlying_result_queue_.push(res);

  deps_.setNodeStatus(N6, NodeStatus::NOT_AVAILABLE, ClientID::MIN);

  ld_info("Selecting copyset 6");
  csm_state_->reset();
  selectCopySet();
  ASSERT_EQ(CopySetSelector::Result::FAILED, result_.rv);
}

TEST_F(CopySetSelectorTest, CrossDomainCopysetSelectorDistribution) {
  replication_ = 3;
  extras_ = 0;
  copyset_selector_type_ = CopySetSelectorType::CROSS_DOMAIN;
  sync_replication_scope_ = NodeLocationScope::REGION;
  setUp();

  std::unordered_map<ShardID, size_t, ShardID::Hash> node_stores;

  size_t num_writes = 20000;

  for (size_t i = 0; i < num_writes; ++i) {
    selectCopySet();
    ASSERT_EQ(CopySetSelector::Result::SUCCESS, result_.rv);
    ASSERT_EQ(replication_, result_.ndest);
    for (size_t offset = 0; offset < result_.ndest; ++offset) {
      ShardID idx = result_.copyset[offset].destination;
      ++node_stores[idx];
    }
  }

  std::map<std::string, size_t> scope_stores;

  for (const auto& s : node_stores) {
    const Configuration::Node* node =
        config_->serverConfig()->getNode(s.first.node());
    ld_check(node);
    ld_check(node->location.hasValue());
    const auto& location = node->location.value();
    scope_stores[location.getDomain(sync_replication_scope_)] += s.second;
  }

  for (auto& kv : scope_stores) {
    ld_info("Stored in domain %s: %lu records", kv.first.c_str(), kv.second);
  }
  // The following is probabilistic, so it might fail spuriously. If it fails
  // too much, adjust tolerance

#define VERIFY_RESULT(idx, expected_share)                                     \
  do {                                                                         \
    ASSERT_GT(scope_stores[idx], (num_writes * (expected_share - tolerance))); \
    ASSERT_LT(scope_stores[idx], (num_writes * (expected_share + tolerance))); \
  } while (0)

  double tolerance = 0.02;
  // All of these have to be changed if the node configuration of the tests
  // changes
  VERIFY_RESULT("rg0....", 0.5);
  VERIFY_RESULT("rg1....", 2.0);
  VERIFY_RESULT("rg2....", 0.5);
#undef VERIFY_RESULT
}

TEST_F(CopySetSelectorTest, RandomLinearIteratorBase) {
  size_t num_entries = 50;
  std::vector<int> src(num_entries, 0);
  std::vector<bool> blacklisted(num_entries, false);
  std::vector<size_t> seen_entries(num_entries, 0);
  for (int i = 0; i < num_entries; ++i) {
    src[i] = i;
  }

  RandomLinearIteratorBase<int, std::vector> it(DefaultRNG::get());
  for (int blacklist_period = 0; blacklist_period < 10; ++blacklist_period) {
    for (int i = 0; i < num_entries; ++i) {
      blacklisted[i] = false;
      seen_entries[i] = 0;
    }
    ld_info("Blacklisting every %dth entry", blacklist_period);
    it.setContainerAndReset(&src); // clears the blacklist
    int out;
    while (it.next(&out) == 0) {
      ASSERT_GE(out, 0);
      ASSERT_LT(out, num_entries);
      ASSERT_FALSE(blacklisted[out]);
      ++seen_entries[out];
      if ((blacklist_period != 0) && (out % blacklist_period == 0)) {
        bool blacklist_result = it.blacklistCurrentAndReset();
        ASSERT_TRUE(blacklist_result);
        blacklisted[out] = true;
      }
    }

    size_t sum = 0;
    for (int i = 0; i < num_entries; ++i) {
      ld_info("Entry %d seen %lu times", i, seen_entries[i]);
      sum += seen_entries[i];
      ASSERT_GT(seen_entries[i], 0);
      if (blacklisted[i]) {
        ASSERT_EQ(1, seen_entries[i]);
      }
    }
    ld_info("Total: %lu reads", sum);
    ASSERT_GE(sum, num_entries);
  }
}
