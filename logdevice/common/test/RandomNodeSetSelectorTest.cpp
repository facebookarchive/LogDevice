/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <functional>
#include <numeric>
#include <utility>

#include <folly/Memory.h>
#include <folly/String.h>
#include <gtest/gtest.h>
#include <logdevice/common/toString.h>

#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/util.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::NodeSetTestUtil;

using Decision = NodeSetSelector::Decision;

using verify_func_t = std::function<void(StorageSet*)>;

// Wrapper to provide always defaulted args to addNodes().
static inline void addWeightedNodes(ServerConfig::Nodes* nodes,
                                    size_t num_nodes,
                                    shard_size_t num_shards,
                                    std::string location_string,
                                    size_t num_non_zw_nodes) {
  return addNodes(nodes,
                  num_nodes,
                  num_shards,
                  location_string,
                  /*storage_capacity*/ 1.,
                  /*sequencer_weight*/ 1.,
                  num_non_zw_nodes);
}

static void
verify_result(NodeSetSelector* selector,
              std::shared_ptr<Configuration>& config,
              logid_t logid,
              Decision expected_decision,
              verify_func_t verify,
              const NodeSetSelector::Options* options = nullptr,
              size_t iteration = 10,
              folly::Optional<nodeset_size_t> target_size = folly::none) {
  SCOPED_TRACE("log " + toString(logid.val_));

  if (!target_size.hasValue()) {
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        config->getLogGroupByIDShared(logid);
    ASSERT_NE(nullptr, logcfg);
    target_size =
        logcfg->attrs().nodeSetSize().value().value_or(NODESET_SIZE_MAX);
  }

  ld_check(iteration > 0);
  for (size_t i = 0; i < iteration; ++i) {
    auto res = selector->getStorageSet(
        logid,
        config.get(),
        *config->getNodesConfigurationFromServerConfigSource(),
        target_size.value(),
        /* seed */ 0,
        nullptr,
        options);
    ASSERT_EQ(expected_decision, res.decision);
    if (res.decision != Decision::NEEDS_CHANGE) {
      continue;
    }

    ASSERT_FALSE(res.storage_set.empty());

    // perform basic checks
    // nodes in nodeset must be unique and in increasing order
    ASSERT_TRUE(std::is_sorted(
        res.storage_set.begin(), res.storage_set.end(), std::less<ShardID>()));

    // must comply with the config
    const std::shared_ptr<LogsConfig::LogGroupNode> logcfg =
        config->getLogGroupByIDShared(logid);
    ASSERT_NE(nullptr, logcfg);
    const auto& attrs = logcfg->attrs();
    const auto& nodes_config =
        *config->getNodesConfigurationFromServerConfigSource();
    ASSERT_TRUE(configuration::nodes::validStorageSet(
        nodes_config,
        res.storage_set,
        ReplicationProperty::fromLogAttributes(attrs)));

    // Run nodeset selector on its own output and check that it doesn't want
    // to change nodeset again.
    EpochMetaData meta(
        res.storage_set, ReplicationProperty::fromLogAttributes(attrs));
    meta.weights = res.weights;
    meta.nodeset_params.signature = res.signature;
    auto res2 = selector->getStorageSet(logid,
                                        config.get(),
                                        nodes_config,
                                        target_size.value(),
                                        0,
                                        &meta,
                                        options);
    EXPECT_EQ(Decision::KEEP, res2.decision);
    EXPECT_EQ(res.signature, res2.signature);

    // perform the user provided check
    verify(&res.storage_set);
  }
}

// return true if nodesets for a given log based on 2 node configs are the same;
// false otherwise
static std::pair<size_t, size_t>
compare_nodesets(NodeSetSelector* selector,
                 std::shared_ptr<Configuration>& config1,
                 std::shared_ptr<Configuration>& config2,
                 logid_t logid,
                 std::map<ShardID, size_t>& old_distribution,
                 std::map<ShardID, size_t>& new_distribution,
                 const NodeSetSelector::Options* options = nullptr) {
  auto old_res = selector->getStorageSet(
      logid,
      config1.get(),
      *config1->getNodesConfigurationFromServerConfigSource(),
      config1->getLogGroupByIDShared(logid)
          ->attrs()
          .nodeSetSize()
          .value()
          .value_or(NODESET_SIZE_MAX),
      0,
      nullptr,
      options);
  auto new_res = selector->getStorageSet(
      logid,
      config2.get(),
      *config2->getNodesConfigurationFromServerConfigSource(),
      config1->getLogGroupByIDShared(logid)
          ->attrs()
          .nodeSetSize()
          .value()
          .value_or(NODESET_SIZE_MAX),
      0,
      nullptr,
      options);

  ld_check(old_res.decision == Decision::NEEDS_CHANGE);
  ld_check(new_res.decision == Decision::NEEDS_CHANGE);
  ld_check(
      std::is_sorted(old_res.storage_set.begin(), old_res.storage_set.end()));
  ld_check(
      std::is_sorted(new_res.storage_set.begin(), new_res.storage_set.end()));

  std::vector<ShardID> common_nodes(
      std::min(old_res.storage_set.size(), new_res.storage_set.size()));
  std::vector<ShardID>::iterator node_it =
      std::set_intersection(old_res.storage_set.begin(),
                            old_res.storage_set.end(),
                            new_res.storage_set.begin(),
                            new_res.storage_set.end(),
                            common_nodes.begin());
  common_nodes.resize(node_it - common_nodes.begin());

  for (ShardID current_shard_id : old_res.storage_set) {
    old_distribution[current_shard_id]++;
  }

  for (ShardID current_shard_id : new_res.storage_set) {
    new_distribution[current_shard_id]++;
  }

  return std::make_pair(old_res.storage_set.size() - common_nodes.size(),
                        new_res.storage_set.size() - common_nodes.size());
}

TEST(RandomCrossDomainNodeSetSelectorTest, RackAssignment) {
  // 100-node cluster with nodes from 5 different racks
  Nodes nodes;
  addWeightedNodes(&nodes, 10, 5, "region0.datacenter1.01.a.a", 10);
  addWeightedNodes(&nodes, 35, 5, "region0.datacenter2.01.a.a", 35);
  addWeightedNodes(&nodes, 20, 5, "region0.datacenter1.01.a.b", 10);
  addWeightedNodes(&nodes, 20, 5, "region1.datacenter1.02.a.a", 20);
  addWeightedNodes(&nodes, 15, 5, "region1.datacenter1.02.a.b", 15);

  ld_check(nodes.size() == 100);

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(), logid_t{1}, 3, 0, 10, {}, NodeLocationScope::RACK);
  addLog(logs_config.get(), logid_t{2}, 3, 0, 20, {}, NodeLocationScope::RACK);
  addLog(logs_config.get(), logid_t{3}, 5, 0, 18, {}, NodeLocationScope::RACK);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM_CROSSDOMAIN);

  const Configuration& cfg = *config;
  // generate a verify_func_t function for checking nodeset with racks
  auto gen = [&cfg](size_t racks, size_t nodes_per_rack) {
    return [racks, nodes_per_rack, &cfg](StorageSet* storage_set) {
      ld_check(storage_set != nullptr);
      std::map<std::string, StorageSet> node_map;
      for (const ShardID i : *storage_set) {
        const Configuration::Node* node = cfg.serverConfig()->getNode(i.node());
        ASSERT_NE(nullptr, node);
        ASSERT_TRUE(node->location.hasValue());
        node_map[node->locationStr()].push_back(i);
      }

      ASSERT_EQ(racks, node_map.size());
      for (const auto& kv : node_map) {
        ASSERT_EQ(nodes_per_rack, kv.second.size());
      }
    };
  };

  verify_result(
      selector.get(), config, logid_t{1}, Decision::NEEDS_CHANGE, gen(5, 2));
  verify_result(
      selector.get(), config, logid_t{2}, Decision::NEEDS_CHANGE, gen(5, 4));
  verify_result(
      selector.get(), config, logid_t{3}, Decision::NEEDS_CHANGE, gen(5, 4));
}

TEST(RandomNodeSetSelectorTest, NodeExclusion) {
  // 10 node cluster
  configuration::Nodes nodes;
  const int SHARDS_PER_NODE = 5;
  addWeightedNodes(&nodes, 10, SHARDS_PER_NODE, "", 10);
  ld_check(nodes.size() == 10);

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(), logid_t{1}, 3, 0, 5, {}, NodeLocationScope::NODE);
  addLog(logs_config.get(), logid_t{5}, 3, 0, 8, {}, NodeLocationScope::NODE);
  addLog(logs_config.get(), logid_t{6}, 3, 0, 8, {}, NodeLocationScope::NODE);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM_CROSSDOMAIN);

  NodeSetSelector::Options options;

  // generate a verify_func_t function
  auto gen = [](std::vector<node_index_t> exclude) {
    return [exclude](StorageSet* storage_set) {
      ld_check(storage_set);
      for (ShardID n : *storage_set) {
        for (node_index_t e : exclude) {
          ASSERT_NE(e, n.node());
        }
      }
    };
  };

  options.exclude_nodes = {1, 2, 3};
  verify_result(selector.get(),
                config,
                logid_t{1},
                Decision::NEEDS_CHANGE,
                gen({1, 2, 3}),
                &options);

  options.exclude_nodes = {1, 3};
  verify_result(selector.get(),
                config,
                logid_t{5},
                Decision::NEEDS_CHANGE,
                gen({1, 3}),
                &options);

  options.exclude_nodes = {1, 2, 3};
  // there are not enough nodes for log 6
  verify_result(selector.get(),
                config,
                logid_t{6},
                Decision::FAILED,
                gen({1, 2, 3}),
                &options);
}

TEST(RandomNodeSetSelector, ImpreciseNodeSetSize) {
  // 26-node cluster with nodes from 5 different racks
  dbg::currentLevel = dbg::Level::SPEW;
  Nodes nodes;
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter1.01.a.a", 5);
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter2.01.a.a", 5);
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter1.01.a.b", 5);
  addWeightedNodes(&nodes, 5, 1, "region1.datacenter1.02.a.a", 5);
  addWeightedNodes(&nodes, 6, 1, "region1.datacenter1.02.a.b", 6);

  ASSERT_EQ(26, nodes.size());

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  for (size_t i = 1; i <= 200; ++i) {
    // log_id == nodeset_size for r=3 logs, log_id == nodeset_size + 100 for r=6
    // logs
    size_t nodeset_size = (i - 1) % 100 + 1;
    size_t replication_factor = i <= 100 ? 3 : 6;
    addLog(logs_config.get(),
           logid_t(i),
           replication_factor,
           0,
           nodeset_size,
           {},
           NodeLocationScope::RACK);
  }

  MetaDataLogsConfig metadata_config;
  metadata_config.nodeset_selector_type =
      NodeSetSelectorType::RANDOM_CROSSDOMAIN;

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest("nodeset_selector_test",
                                 std::move(nodes_config),
                                 std::move(metadata_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM_CROSSDOMAIN);

  auto check_ns_size = [&](logid_t log_id, size_t expected_actual_size) {
    verify_result(selector.get(),
                  config,
                  log_id,
                  Decision::NEEDS_CHANGE,
                  [expected_actual_size](StorageSet* storage_set) {
                    ld_check(storage_set != nullptr);
                    ASSERT_EQ(expected_actual_size, storage_set->size());
                  });
  };

  auto check_ns_size_r3 = [&](size_t setting_size,
                              size_t expected_actual_size) {
    check_ns_size(logid_t(setting_size), expected_actual_size);
  };

  auto check_ns_size_r6 = [&](size_t setting_size,
                              size_t expected_actual_size) {
    check_ns_size(logid_t(setting_size + 100), expected_actual_size);
  };

  // r = 3
  check_ns_size_r3(1, 5);
  check_ns_size_r3(7, 5);
  check_ns_size_r3(8, 10);
  check_ns_size_r3(12, 10);
  check_ns_size_r3(13, 15);
  check_ns_size_r3(17, 15);
  check_ns_size_r3(18, 20);
  check_ns_size_r3(20, 20);
  check_ns_size_r3(22, 20);
  check_ns_size_r3(23, 25);
  check_ns_size_r3(26, 25);
  check_ns_size_r3(100, 25);

  // r = 6
  check_ns_size_r6(1, 10);
  check_ns_size_r6(4, 10);
  check_ns_size_r6(5, 10);
  check_ns_size_r6(6, 10);
  check_ns_size_r6(10, 10);
  check_ns_size_r6(12, 10);
  check_ns_size_r6(26, 25);
}

TEST(RandomCrossDomainNodeSetSelectorTest, NodeExclusion) {
  // 26-node cluster with nodes from 5 different racks
  Nodes nodes;
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter1.01.a.a", 5);
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter2.01.a.a", 5);
  addWeightedNodes(&nodes, 5, 1, "region0.datacenter1.01.a.b", 5);
  addWeightedNodes(&nodes, 5, 1, "region1.datacenter1.02.a.a", 5);
  addWeightedNodes(&nodes, 6, 1, "region1.datacenter1.02.a.b", 6);

  ASSERT_EQ(26, nodes.size());

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(),
         logid_t(1),
         3 /* replication_factor */,
         0,
         25 /* nodeset_size */,
         {},
         NodeLocationScope::RACK);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::RANDOM_CROSSDOMAIN);

  const Configuration& cfg = *config;

  auto verify_domains = [&](size_t num_domains,
                            size_t nodes_per_domain,
                            StorageSet* storage_set) {
    ld_check(storage_set != nullptr);
    std::unordered_map<std::string, int> domains; // location to count map
    for (ShardID shard : *storage_set) {
      auto node = cfg.serverConfig()->getNode(shard.node());
      ld_check(node);
      ++domains[node->locationStr()];
    }
    ASSERT_EQ(num_domains, domains.size());
    for (auto& d : domains) {
      ASSERT_EQ(nodes_per_domain, d.second);
    }
  };

  // nodeset_size with one fully excluded rack in options
  NodeSetSelector::Options options;
  options.exclude_nodes = {20, 21, 22, 23, 24, 25};
  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                // should select 4 racks of 5 nodes each
                std::bind(verify_domains, 4, 5, std::placeholders::_1),
                &options);

  // nodeset generation and nodeset size if one rack is partially removed
  options.exclude_nodes = {20, 21, 22, 23};
  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                // should select 4 racks of 5 nodes each
                std::bind(verify_domains, 4, 5, std::placeholders::_1),
                &options);

  // nodeset generation and nodeset size if two racks is partially removed
  options.exclude_nodes = {15, 16, 17, 20, 21, 22, 23};
  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                // should select 3 racks of 5 nodes each
                std::bind(verify_domains, 3, 5, std::placeholders::_1),
                &options);

  // nodeset generation and nodeset size if three racks is partially removed
  options.exclude_nodes = {10, 11, 15, 16, 20, 21, 22};
  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                // should select 5 racks of 3 nodes each, not 2 racks of 5
                // nodes each
                std::bind(verify_domains, 5, 3, std::placeholders::_1),
                &options);
}

void basic_test(NodeSetSelectorType ns_type) {
  // 25-node cluster with nodes from 6 different racks, 1 of them unwritable
  Nodes nodes;
  std::vector<int> rack_sizes = {1, 5, 5, 6, 5, 3};
  addWeightedNodes(&nodes, rack_sizes[0], 1, "region0.datacenter1.01.a.a", 1);
  addWeightedNodes(&nodes, rack_sizes[1], 1, "region0.datacenter2.01.a.a", 5);
  // Only 2 out of 5 nodes are writable.
  addWeightedNodes(&nodes, rack_sizes[2], 1, "region0.datacenter1.01.a.b", 2);
  addWeightedNodes(&nodes, rack_sizes[3], 1, "region1.datacenter1.02.a.a", 6);
  addWeightedNodes(&nodes, rack_sizes[4], 1, "region1.datacenter1.02.a.b", 5);
  // Unwritable rack. Should still be picked in nodesets.
  addWeightedNodes(&nodes, rack_sizes[5], 1, "region1.datacenter1.02.a.c", 0);

  ASSERT_EQ(25, nodes.size());

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(),
         logid_t(1),
         ReplicationProperty(
             {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}}),
         0,
         14 /* nodeset_size */);
  addLog(logs_config.get(),
         logid_t(2),
         ReplicationProperty(
             {{NodeLocationScope::RACK, 1}, {NodeLocationScope::NODE, 3}}),
         0,
         5 /* nodeset_size */);
  addLog(logs_config.get(),
         logid_t(3),
         ReplicationProperty({{NodeLocationScope::NODE, 4}}),
         0,
         2 /* nodeset_size */);
  addLog(logs_config.get(),
         logid_t(4),
         ReplicationProperty(
             {{NodeLocationScope::RACK, 3}, {NodeLocationScope::NODE, 4}}),
         0,
         150 /* nodeset_size */);
  addLog(logs_config.get(),
         logid_t(5),
         ReplicationProperty({{NodeLocationScope::RACK, 3}}),
         0,
         6 /* nodeset_size */);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector = NodeSetSelectorFactory::create(ns_type);

  auto keep_only_writable = [&](StorageSet ss) -> StorageSet {
    StorageSet res;
    for (ShardID s : ss) {
      const configuration::Node* n = config->serverConfig()->getNode(s.node());
      ld_check(n != nullptr);
      if (n->isWritableStorageNode()) {
        res.push_back(s);
      }
    }
    return res;
  };

  auto nodes_per_domain = [&](StorageSet ss) -> std::vector<int> {
    std::vector<int> count(rack_sizes.size());
    size_t rack = 0;
    int nodes_before_rack = 0;
    for (ShardID s : ss) {
      EXPECT_EQ(0, s.shard());
      assert(s.node() >= nodes_before_rack);
      while (rack < rack_sizes.size() &&
             s.node() >= nodes_before_rack + rack_sizes[rack]) {
        nodes_before_rack += rack_sizes[rack];
        ++rack;
      }
      if (rack == rack_sizes.size()) {
        ADD_FAILURE() << toString(ss);
        return {};
      }
      ++count[rack];
    }
    assert(std::accumulate(count.begin(), count.end(), 0) == (int)ss.size());
    return count;
  };

  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                [&](StorageSet* ss) {
                  {
                    StorageSet w = keep_only_writable(*ss);
                    EXPECT_EQ(14, w.size());
                    auto count = nodes_per_domain(w);
                    EXPECT_EQ(1, count[0]);
                    EXPECT_EQ(2, count[2]);
                    EXPECT_EQ(0, count[5]);
                    EXPECT_GE(count[1], 3);
                    EXPECT_GE(count[3], 3);
                    EXPECT_GE(count[4], 3);
                    EXPECT_LE(count[1], 4);
                    EXPECT_LE(count[3], 4);
                    EXPECT_LE(count[4], 4);
                  }
                  {
                    EXPECT_GE(ss->size(), 16);
                    EXPECT_LE(ss->size(), 20);
                    auto count = nodes_per_domain(*ss);
                    EXPECT_GE(count[5], 2);
                    EXPECT_LE(count[5], 3);
                  }
                });

  verify_result(
      selector.get(),
      config,
      logid_t(2),
      Decision::NEEDS_CHANGE,
      [&](StorageSet* ss) {
        auto count = nodes_per_domain(*ss);
        EXPECT_GE(ss->size(), 16);
        EXPECT_LE(ss->size(), 18);
        EXPECT_EQ(
            std::vector<int>({1, 3, (int)ss->size() - 13, 3, 3, 3}), count);
      });

  verify_result(selector.get(),
                config,
                logid_t(3),
                Decision::NEEDS_CHANGE,
                [&](StorageSet* ss) {
                  EXPECT_EQ(4, keep_only_writable(*ss).size());
                  EXPECT_GE(ss->size(), 4);
                  EXPECT_LE(ss->size(), 10);
                });

  verify_result(selector.get(),
                config,
                logid_t(4),
                Decision::NEEDS_CHANGE,
                [&](StorageSet* ss) {
                  // Should select all 25 nodes.
                  EXPECT_EQ(25, ss->size());
                });

  verify_result(selector.get(),
                config,
                logid_t(5),
                Decision::NEEDS_CHANGE,
                [&](StorageSet* ss) {
                  StorageSet w = keep_only_writable(*ss);
                  EXPECT_EQ(6, w.size());
                  EXPECT_GE(ss->size(), 7);
                  EXPECT_LE(ss->size(), 9);
                  // Should cover all 6 racks.
                  const auto& all_nodes = config->serverConfig()->getNodes();
                  std::set<std::string> racks;
                  std::set<std::string> writable_racks;
                  for (auto s : *ss) {
                    const configuration::Node& n = all_nodes.at(s.node());
                    std::string rack =
                        n.location->getDomain(NodeLocationScope::RACK);
                    racks.insert(rack);
                    if (n.isWritableStorageNode()) {
                      writable_racks.insert(rack);
                    }
                  }
                  EXPECT_EQ(5, writable_racks.size()) << toString(racks);
                  EXPECT_EQ(6, racks.size()) << toString(racks);
                });

  // Exclude a rack in options.
  NodeSetSelector::Options options;
  options.exclude_nodes = {1, 2, 3, 4, 5};
  verify_result(
      selector.get(),
      config,
      logid_t(2),
      Decision::NEEDS_CHANGE,
      [&](StorageSet* ss) {
        {
          StorageSet w = keep_only_writable(*ss);
          EXPECT_EQ(9, w.size());
          auto count = nodes_per_domain(w);
          EXPECT_EQ(std::vector<int>({1, 0, 2, 3, 3, 0}), count);
        }
        {
          auto count = nodes_per_domain(*ss);
          EXPECT_GE(ss->size(), 13);
          EXPECT_LE(ss->size(), 15);
          EXPECT_EQ(
              std::vector<int>({1, 0, (int)ss->size() - 10, 3, 3, 3}), count);
        }
      },
      &options);
}

TEST(WeightAwareNodeSetSelectorTest, ExcludeFromNodesets) {
  // 6-node cluster with nodes in 2 different racks
  Nodes nodes;
  addNodes(&nodes, 3, 1, "region0.datacenter1.01.a.a");
  addNodes(&nodes, 3, 1, "region0.datacenter1.01.a.b");

  ASSERT_EQ(6, nodes.size());
  // Settings exclude_from_nodesets on 3 nodes
  for (node_index_t node_id : {0, 1, 3}) {
    nodes[node_id].storage_attributes->exclude_from_nodesets = true;
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(),
         logid_t(1),
         ReplicationProperty(
             {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}}),
         0,
         5 /* nodeset_size */);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::WEIGHT_AWARE);

  verify_result(selector.get(),
                config,
                logid_t(1),
                Decision::NEEDS_CHANGE,
                [&](StorageSet* ss) { EXPECT_EQ(3, ss->size()); });
}

TEST(WeightAwareNodeSetSelectorTest, Basic) {
  basic_test(NodeSetSelectorType::WEIGHT_AWARE_V2);
}

TEST(ConsistentHashingWeightAwareNodeSetSelectorTest, Basic) {
  basic_test(NodeSetSelectorType::CONSISTENT_HASHING_V2);
}

TEST(ConsistentHashingWeightAwareNodeSetSelectorTest, AddNode) {
  Nodes nodes1, nodes2;
  addWeightedNodes(&nodes1, 16, 1, "region0.datacenter1.01.a.a", 16);
  addWeightedNodes(&nodes1, 16, 1, "region0.datacenter2.01.a.a", 16);
  addWeightedNodes(&nodes1, 16, 1, "region0.datacenter1.01.a.b", 16);
  addWeightedNodes(&nodes1, 16, 1, "region1.datacenter1.02.a.a", 16);
  addWeightedNodes(&nodes1, 15, 1, "region1.datacenter1.02.a.b", 15);

  nodes2 = nodes1;

  // another node added to the 5th rack
  addWeightedNodes(&nodes2, 1, 1, "region1.datacenter1.02.a.b", 1);
  Configuration::NodesConfig nodes_config1(std::move(nodes1));
  Configuration::NodesConfig nodes_config2(std::move(nodes2));

#ifdef FOLLY_SANITIZE_ADDRESS
  // ASAN builds are 3-10 times slower than normal, use much fewer iterations.
  const int numlogs = 1000;
#else
  const int numlogs = 10000;
#endif

  auto logs_config = std::make_shared<LocalLogsConfig>();

  for (int i = 1; i <= numlogs; i++) {
    addLog(logs_config.get(),
           logid_t(i),
           ReplicationProperty(
               {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}}),
           0,
           21 /* nodeset_size */);
  }
  auto logs_config2 = logs_config;

  auto config1 = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config1)),
      std::move(logs_config));

  auto config2 = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config2)),
      std::move(logs_config2));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::CONSISTENT_HASHING);

  auto old_selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::WEIGHT_AWARE);

  size_t old_totalremoved = 0, old_totaladded = 0;
  size_t new_totalremoved = 0, new_totaladded = 0;
  std::map<ShardID, size_t> old_before_adding_distribution;
  std::map<ShardID, size_t> old_after_adding_distribution;
  std::map<ShardID, size_t> new_before_adding_distribution;
  std::map<ShardID, size_t> new_after_adding_distribution;
  for (int i = 1; i <= numlogs; i++) {
    std::pair<size_t, size_t> old_diff, new_diff;
    new_diff = compare_nodesets(selector.get(),
                                config1,
                                config2,
                                logid_t(i),
                                new_before_adding_distribution,
                                new_after_adding_distribution);
    old_diff = compare_nodesets(old_selector.get(),
                                config1,
                                config2,
                                logid_t(i),
                                old_before_adding_distribution,
                                old_after_adding_distribution);
    old_totalremoved += old_diff.first;
    old_totaladded += old_diff.second;
    new_totalremoved += new_diff.first;
    new_totaladded += new_diff.second;
  }

  ld_info("New selector: removed = %zu, added = %zu\n",
          new_totalremoved,
          new_totaladded);
  ld_info("Old selector: removed = %zu, added = %zu\n",
          old_totalremoved,
          old_totaladded);

  ld_info("Distribution before adding for old selector: %s",
          toString(old_before_adding_distribution).c_str());
  ld_info("Distribution after adding for old selector: %s",
          toString(old_after_adding_distribution).c_str());

  ld_info("Distribution before adding for new selector: %s",
          toString(new_before_adding_distribution).c_str());
  ld_info("Distribution after adding for new selector: %s",
          toString(new_after_adding_distribution).c_str());

  for (auto& kv : old_after_adding_distribution) {
    // We expect each shard to be picked in around 1/4 of the nodesets
    // (nodeset size 21, cluster size 80).
    EXPECT_GE(kv.second, numlogs / 20);
    EXPECT_LE(kv.second, numlogs / 2);
  }

  for (auto& kv : new_after_adding_distribution) {
    EXPECT_GE(kv.second, numlogs / 20);
    EXPECT_LE(kv.second, numlogs / 2);
  }

  EXPECT_EQ(new_totalremoved, new_totaladded);
  // We expect around 1/4 of the nodesets to change since we're adding
  // one node to a rack of 15 nodes, and nodeset size is ~4 nodes per rack.
  EXPECT_LE(new_totalremoved, numlogs / 2);
}

TEST(ConsistentHashingWeightAwareNodeSetSelectorTest, DisabledNodes) {
  Nodes nodes;
  addWeightedNodes(&nodes, 3, 1, "a.a.a.a.rack0", 3);
  addWeightedNodes(&nodes, 3, 1, "a.a.a.a.rack1", 3);
  addWeightedNodes(&nodes, 5, 1, "a.a.a.a.rack2", 5);
  ASSERT_EQ(11, nodes.size());

  for (node_index_t node_id : {0, 1, 2, 3}) {
    nodes[node_id].storage_attributes->state =
        (node_id % 2) ? StorageState::DISABLED : StorageState::READ_ONLY;
  }

  Configuration::NodesConfig nodes_config(std::move(nodes));

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(),
         logid_t(1),
         ReplicationProperty(
             {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}}),
         0,
         6 /* nodeset_size */);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::CONSISTENT_HASHING);

  auto res = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      6,
      0,
      nullptr,
      nullptr);
  ASSERT_EQ(Decision::NEEDS_CHANGE, res.decision);

  std::array<int, 3> per_domain{};
  std::array<int, 3> per_domain_writable{};
  for (ShardID s : res.storage_set) {
    int d = std::min(2, s.node() / 3);
    ++per_domain[d];
    if (s.node() > 3) {
      ++per_domain_writable[d];
    }
  }

  EXPECT_EQ(2, per_domain[0]);
  EXPECT_EQ(2, per_domain_writable[1]);
  EXPECT_EQ(4, per_domain[2]);
}

TEST(ConsistentHashingWeightAwareNodeSetSelectorTest, Seed) {
  Nodes nodes;
  addWeightedNodes(&nodes, 3, 1, "a.a.a.a.rack0", 3);
  addWeightedNodes(&nodes, 3, 1, "a.a.a.a.rack1", 3);
  addWeightedNodes(&nodes, 3, 1, "a.a.a.a.rack2", 3);
  ASSERT_EQ(9, nodes.size());

  Configuration::NodesConfig nodes_config(std::move(nodes));
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 2}});

  auto logs_config = std::make_shared<LocalLogsConfig>();
  addLog(logs_config.get(),
         logid_t(1),
         replication,
         0,
         3 /* nodeset_size (unused) */);

  auto config = std::make_shared<Configuration>(
      ServerConfig::fromDataTest(
          "nodeset_selector_test", std::move(nodes_config)),
      std::move(logs_config));

  auto selector =
      NodeSetSelectorFactory::create(NodeSetSelectorType::CONSISTENT_HASHING);

  auto res = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      /* target_nodeset_size */ 5,
      /* seed */ 0,
      nullptr,
      nullptr);
  ASSERT_EQ(Decision::NEEDS_CHANGE, res.decision);
  ASSERT_EQ(5, res.storage_set.size());

  EpochMetaData meta(res.storage_set, replication);
  meta.nodeset_params.signature = res.signature;
  auto res2 = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      5,
      0,
      &meta,
      nullptr);
  EXPECT_EQ(Decision::KEEP, res2.decision);
  EXPECT_EQ(res.signature, res2.signature);

  // Change seed and check that nodeset selector wants to change nodeset.
  // There's a small chance that it happens to pick the same one, but since
  // everything is deterministic, it it passes once it passes always.
  // If you changed the nodeset selector's logic, and this fails, maybe you
  // just got unlucky and need to set a different seed here.
  auto res3 = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      5,
      1,
      &meta,
      nullptr);
  EXPECT_EQ(Decision::NEEDS_CHANGE, res3.decision);
  EXPECT_NE(res3.signature, res2.signature);
  ASSERT_EQ(5, res3.storage_set.size());

  meta.shards = res3.storage_set;
  meta.nodeset_params.signature = res3.signature;
  auto res4 = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      5,
      1,
      &meta,
      nullptr);
  EXPECT_EQ(Decision::KEEP, res4.decision);
  EXPECT_EQ(res3.signature, res4.signature);

  // Change target nodeset size and check that nodeset changes.
  auto res5 = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      6,
      1,
      &meta,
      nullptr);
  EXPECT_EQ(Decision::NEEDS_CHANGE, res5.decision);
  EXPECT_NE(res5.signature, res4.signature);
  ASSERT_EQ(6, res5.storage_set.size());

  meta.shards = res5.storage_set;
  meta.nodeset_params.signature = res5.signature;
  auto res6 = selector->getStorageSet(
      logid_t(1),
      config.get(),
      *config->getNodesConfigurationFromServerConfigSource(),
      6,
      1,
      &meta,
      nullptr);
  EXPECT_EQ(Decision::KEEP, res6.decision);
  EXPECT_EQ(res5.signature, res6.signature);
}
