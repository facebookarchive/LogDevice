/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <logdevice/common/configuration/NodeLocation.h>

#include <folly/Memory.h>
#include <gtest/gtest.h>
#include <logdevice/common/NodeLocationHierarchy.h>

#include "logdevice/common/test/NodeSetTestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

TEST(NodeLocationTest, Simple) {
  NodeLocation location;
  ASSERT_TRUE(location.isEmpty());
  ASSERT_EQ("", location.toString());

  int rv = location.fromDomainString("rg0.ds1.cl2.row3.rack4");
  ASSERT_EQ(0, rv);
  ASSERT_FALSE(location.isEmpty());
  ASSERT_EQ(5, location.numScopes());
  ASSERT_EQ("", location.getLabel(NodeLocationScope::NODE));
  ASSERT_EQ("rack4", location.getLabel(NodeLocationScope::RACK));
  ASSERT_EQ("row3", location.getLabel(NodeLocationScope::ROW));
  ASSERT_EQ("cl2", location.getLabel(NodeLocationScope::CLUSTER));
  ASSERT_EQ("ds1", location.getLabel(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("rg0", location.getLabel(NodeLocationScope::REGION));
  ASSERT_EQ("", location.getLabel(NodeLocationScope::ROOT));

  ASSERT_EQ("rg0.ds1.cl2.row3.rack4", location.getDomain());
  ASSERT_EQ(
      "rg0.ds1.cl2.row3.rack4", location.getDomain(NodeLocationScope::NODE));
  ASSERT_EQ(
      "rg0.ds1.cl2.row3.rack4", location.getDomain(NodeLocationScope::RACK));
  ASSERT_EQ("rg0.ds1.cl2.row3.", location.getDomain(NodeLocationScope::ROW));
  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain(NodeLocationScope::CLUSTER));
  ASSERT_EQ("rg0.ds1...", location.getDomain(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("rg0....", location.getDomain(NodeLocationScope::REGION));
  ASSERT_EQ("....", location.getDomain(NodeLocationScope::ROOT));
  ASSERT_EQ(5, location.numScopes());
  ASSERT_EQ(NodeLocationScope::RACK, location.lastScope());

  rv = location.fromDomainString("rg0.ds1.cl2..");
  ASSERT_EQ(0, rv);
  ASSERT_EQ(3, location.numScopes());
  ASSERT_EQ("", location.getLabel(NodeLocationScope::NODE));
  ASSERT_EQ("", location.getLabel(NodeLocationScope::RACK));
  ASSERT_EQ("", location.getLabel(NodeLocationScope::ROW));
  ASSERT_EQ("cl2", location.getLabel(NodeLocationScope::CLUSTER));
  ASSERT_EQ("ds1", location.getLabel(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("rg0", location.getLabel(NodeLocationScope::REGION));
  ASSERT_EQ("", location.getLabel(NodeLocationScope::ROOT));

  ASSERT_TRUE(location.scopeSpecified(NodeLocationScope::REGION));
  ASSERT_TRUE(location.scopeSpecified(NodeLocationScope::DATA_CENTER));
  ASSERT_TRUE(location.scopeSpecified(NodeLocationScope::CLUSTER));
  ASSERT_FALSE(location.scopeSpecified(NodeLocationScope::ROW));
  ASSERT_FALSE(location.scopeSpecified(NodeLocationScope::RACK));

  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain());
  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain(NodeLocationScope::NODE));
  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain(NodeLocationScope::RACK));
  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain(NodeLocationScope::ROW));
  ASSERT_EQ("rg0.ds1.cl2..", location.getDomain(NodeLocationScope::CLUSTER));
  ASSERT_EQ("rg0.ds1...", location.getDomain(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("rg0....", location.getDomain(NodeLocationScope::REGION));
  ASSERT_EQ("....", location.getDomain(NodeLocationScope::ROOT));

  ASSERT_EQ(3, location.numScopes());
  ASSERT_EQ(NodeLocationScope::CLUSTER, location.lastScope());

  rv = location.fromDomainString("....");
  ASSERT_EQ(0, rv);
  ASSERT_EQ(0, location.numScopes());
  ASSERT_EQ(NodeLocationScope::ROOT, location.lastScope());
}

TEST(NodeLocationTest, InvalidDomainString) {
  NodeLocation location;
  ASSERT_EQ(0, location.fromDomainString("a.b.c.d.e"));
  ASSERT_EQ(0, location.fromDomainString("a...."));
  ASSERT_EQ(-1, location.fromDomainString(""));
  ASSERT_EQ(-1, location.fromDomainString("a"));
  ASSERT_EQ(-1, location.fromDomainString("a.b.c.d.e.f"));
  ASSERT_EQ(-1, location.fromDomainString("a.b.c.d.e."));
  ASSERT_EQ(-1, location.fromDomainString("a..."));
  ASSERT_EQ(-1, location.fromDomainString("a....."));
  ASSERT_EQ(-1, location.fromDomainString("a.b.c..d"));
}

TEST(NodeLocationTest, TwoNodes) {
  NodeLocation l1, l2;

  ASSERT_EQ(0, l1.fromDomainString("region0.datacenter1.cluster1.row3."));
  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.row3.rack1"));

  ASSERT_EQ("region0", l2.getLabel(NodeLocationScope::REGION));
  ASSERT_EQ("datacenter1", l2.getLabel(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("cluster1", l2.getLabel(NodeLocationScope::CLUSTER));
  ASSERT_EQ("row3", l2.getLabel(NodeLocationScope::ROW));
  ASSERT_EQ("rack1", l2.getLabel(NodeLocationScope::RACK));
  ASSERT_EQ("", l1.getLabel(NodeLocationScope::RACK));
  ASSERT_EQ("region0....", l2.getDomain(NodeLocationScope::REGION));
  ASSERT_EQ(
      "region0.datacenter1...", l2.getDomain(NodeLocationScope::DATA_CENTER));
  ASSERT_EQ("region0.datacenter1.cluster1..",
            l2.getDomain(NodeLocationScope::CLUSTER));
  ASSERT_EQ("region0.datacenter1.cluster1.row3.",
            l2.getDomain(NodeLocationScope::ROW));
  ASSERT_EQ("region0.datacenter1.cluster1.row3.rack1",
            l2.getDomain(NodeLocationScope::RACK));
  ASSERT_EQ("region0.datacenter1.cluster1.row3.rack1", l2.getDomain());

  ASSERT_TRUE(l1.sharesScopeWith(l2, NodeLocationScope::ROOT));
  ASSERT_TRUE(l1.sharesScopeWith(l2, NodeLocationScope::REGION));
  ASSERT_TRUE(l1.sharesScopeWith(l2, NodeLocationScope::DATA_CENTER));
  ASSERT_TRUE(l1.sharesScopeWith(l2, NodeLocationScope::CLUSTER));
  ASSERT_TRUE(l1.sharesScopeWith(l2, NodeLocationScope::ROW));
  ASSERT_FALSE(l1.sharesScopeWith(l2, NodeLocationScope::RACK));
  ASSERT_FALSE(l1.sharesScopeWith(l2, NodeLocationScope::NODE));
}

TEST(NodeLocationTest, ClosestSharedScope) {
  NodeLocation l1, l2;

  ASSERT_EQ(0, l1.fromDomainString("region0.datacenter1.cluster1.row3.rack1"));
  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.row3.rack1"));
  ASSERT_EQ(NodeLocationScope::RACK, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::RACK, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.row3.rack2"));
  ASSERT_EQ(NodeLocationScope::ROW, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::ROW, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.row3."));
  ASSERT_EQ(NodeLocationScope::ROW, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::ROW, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.row2.rack1"));
  ASSERT_EQ(NodeLocationScope::CLUSTER, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::CLUSTER, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster1.."));
  ASSERT_EQ(NodeLocationScope::CLUSTER, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::CLUSTER, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1.cluster2.row3.rack1"));
  ASSERT_EQ(NodeLocationScope::DATA_CENTER, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::DATA_CENTER, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter1..."));
  ASSERT_EQ(NodeLocationScope::DATA_CENTER, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::DATA_CENTER, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0.datacenter2.cluster1.row3.rack1"));
  ASSERT_EQ(NodeLocationScope::REGION, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::REGION, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region0...."));
  ASSERT_EQ(NodeLocationScope::REGION, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::REGION, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("region1.datacenter1.cluster1.row3.rack1"));
  ASSERT_EQ(NodeLocationScope::ROOT, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::ROOT, l2.closestSharedScope(l1));

  ASSERT_EQ(0, l2.fromDomainString("...."));
  ASSERT_EQ(NodeLocationScope::ROOT, l1.closestSharedScope(l2));
  ASSERT_EQ(NodeLocationScope::ROOT, l2.closestSharedScope(l1));
}

using Domain = facebook::logdevice::NodeLocationHierarchy::Domain;

TEST(NodeLocationTest, BuildHierarchy) {
  Configuration::Nodes nodes;
  addNodes(&nodes, 1, 1, "rg1.dc1...", 1);          // node 0
  addNodes(&nodes, 1, 1, "rg2.dc2.cl1.ro1.rk1", 1); // node 1
  addNodes(&nodes, 1, 1, "....", 1);                // node 2
  addNodes(&nodes, 1, 1, "rg2.dc2...", 1);          // node 3
  addNodes(&nodes, 1, 1, "rg2.dc1.cl1.ro1.", 1);    // node 4
  addNodes(&nodes, 1, 1, "rg2.dc1.cl1.ro1.rk1", 1); // node 5

  Configuration::NodesConfig nodes_config;
  nodes_config.setNodes(std::move(nodes));
  std::shared_ptr<ServerConfig> config =
      ServerConfig::fromDataTest("node_location_test", std::move(nodes_config));
  StorageSet nodeset = {ShardID(0, 0),
                        ShardID(1, 0),
                        ShardID(2, 0),
                        ShardID(3, 0),
                        ShardID(4, 0),
                        ShardID(5, 0)};
  NodeLocationHierarchy h(
      config->getNodesConfigurationFromServerConfigSource(), nodeset);
  ASSERT_EQ(6, h.numClusterNodes());
  const Domain* root = h.getRoot();
  ASSERT_EQ(nullptr, root->parent_);
  ASSERT_EQ(NodeLocationScope::ROOT, root->scope_);
  ASSERT_EQ(2, root->subdomains_.size());
  ASSERT_EQ(2, root->subdomain_list_.size());
  ASSERT_EQ(StorageSet{ShardID(2, 0)}, root->direct_nodes_);
  std::map<ShardID, const Domain*> node_subdomains;
  for (const auto shard : nodeset) {
    const Configuration::Node* node = config->getNode(shard.node());
    const Domain* domain = h.findDomain(node->location.value());
    const Domain* domain_from_index = h.findDomainForShard(shard);
    ASSERT_NE(nullptr, domain);
    ASSERT_EQ(domain, domain_from_index);
    const auto& d_nodes = domain->direct_nodes_;
    ASSERT_NE(std::find(d_nodes.begin(), d_nodes.end(), shard), d_nodes.end());
    node_subdomains[shard] = domain;
  }

  NodeLocation other_location;
  ASSERT_EQ(0, other_location.fromDomainString("rg2.dc1.cl1.ro2.rk1"));
  const Domain* search_domain = h.searchSubdomain(other_location);
  ASSERT_NE(nullptr, search_domain);
  ASSERT_EQ(NodeLocationScope::CLUSTER, search_domain->scope_);
  ASSERT_EQ(node_subdomains[ShardID(4, 0)]->parent_, search_domain);

  std::set<const Domain*> all_domains;
  std::vector<size_t> num_domains;
  for (NodeLocationScope scope = NodeLocationScope::ROOT;
       scope != NodeLocationScope::NODE;
       scope = NodeLocation::nextSmallerScope(scope)) {
    auto domains_in_scope = h.domainsInScope(scope);
    num_domains.push_back(domains_in_scope.size());
    ASSERT_TRUE(std::all_of(
        domains_in_scope.begin(),
        domains_in_scope.end(),
        [&scope](const Domain* domain) { return domain->scope_ == scope; }));
    all_domains.insert(domains_in_scope.begin(), domains_in_scope.end());
  }

  ASSERT_EQ(std::vector<size_t>({1, 2, 3, 2, 2, 2}), num_domains);
  ASSERT_EQ(12, all_domains.size());
}

TEST(NodeLocationTest, PrefixMatch) {
  NodeLocation l;
  EXPECT_EQ(0, l.fromDomainString("rg2.dc1.cl1.ro2.rk1"));

  EXPECT_TRUE(l.matchesPrefix("rg2"));
  EXPECT_TRUE(l.matchesPrefix("rg2.dc1.cl1"));
  EXPECT_TRUE(l.matchesPrefix("rg2.dc1.cl1.")); // TODO
  EXPECT_TRUE(l.matchesPrefix("rg2.dc1.cl1.ro2"));
  EXPECT_TRUE(l.matchesPrefix("rg2.dc1.cl1.ro2.")); // TODO
  EXPECT_TRUE(l.matchesPrefix("rg2.dc1.cl1.ro2.rk1"));

  EXPECT_FALSE(l.matchesPrefix("rg1"));
  EXPECT_FALSE(l.matchesPrefix("rg2.dc1.cl2"));
  EXPECT_FALSE(l.matchesPrefix("rg2.lol.cl1."));
  EXPECT_FALSE(l.matchesPrefix("rg2.dc1.lol.ro2"));
  EXPECT_FALSE(l.matchesPrefix("rg3.dc1.cl1.ro2."));
  EXPECT_FALSE(l.matchesPrefix("rg2.dc1.cl1.ro2.rk9"));

  EXPECT_FALSE(l.matchesPrefix("rg2.d"));
  EXPECT_FALSE(l.matchesPrefix("rg2.dc1.c"));
  EXPECT_FALSE(l.matchesPrefix("rg2.dc1.cl1.ro2.r"));
}
