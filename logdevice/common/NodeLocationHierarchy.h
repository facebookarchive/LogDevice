/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice {

/**
 * @file NodeLocationHierarchy represents the location-based hierarchical
 *       organization of storage nodes in a node set in LogDevice. Location
 *       domains are organized in a trie with the prefix to be the location
 *       label of each location scope.
 */

class NodeLocationHierarchy {
 public:
  /**
   * Domain represents a node location domain of a particular scope
   * (not including NodeLocationScope::NODE). It is the entity that makes up the
   * hierarchy tree structure. It may contain a list of children of Domains
   * representing its subdomains in the next smaller location scope. It may also
   * contain a list of storage nodes in the cluster that are _directly_ attached
   * to its domain (see comments for direct_nodes_ below).
   */
  struct Domain {
    // construct a Domain in the specific scope
    explicit Domain(NodeLocationScope scope, Domain* parent);

    // the location scope of the location domain
    const NodeLocationScope scope_;

    // pointer to the parent Domain, nullptr if the Domain
    // represents the root domain
    Domain* const parent_;

    // all child subdomains organized in a map in which the key is the
    // NodeLocation label of a subdomain in the next smaller location scope
    std::unordered_map<std::string, std::unique_ptr<Domain>> subdomains_;

    // an array of subdomains, for faciliating random access and selection
    std::vector<const Domain*> subdomain_list_;

    // Cluster nodes that _directly_ attached to the domain. A cluster node
    // is ``directly attached'' to a Domain means that the domain is the
    // smallest specified domain that contains the storage node in
    // configuration.
    StorageSet direct_nodes_;

    // how many cluster nodes are in this location domain as well as
    // ALL its subdomains
    nodeset_size_t total_nodes_{0};

    // This method is pretty slow. Use for debugging only.
    std::string toString() const;
  };

  /**
   * Construct the NodeLocationHierarchy for a given set of nodes
   * @param nodes_configuration       NodesConfiguration of the cluster
   * @param indices     indices of nodes in the cluster to be included in
   *                    the hierarchy. Nodes not specified in _indices_ are
   *                    not included
   */
  NodeLocationHierarchy(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      const StorageSet& indices);

  // expose the root Domain
  const Domain* getRoot() const {
    return &root_;
  }

  // return the number of storage nodes in the hierarchy
  nodeset_size_t numClusterNodes() const {
    return root_.total_nodes_;
  }

  // given a NodeLocation object, find the Domain for the smallest subdomain
  // that contains the @param location. If none of the subdomains in the
  // hierarchy contains the location, the root Domain is returned. So this
  // function always returns a valid (non-null) Domain
  const Domain* searchSubdomain(const NodeLocation& location) const;

  // given a NodeLocation object, find the Domain that exactly represents
  // @param location. nullptr if the location is not in the hierarchy
  const Domain* findDomain(const NodeLocation& location) const;

  // given a node index, find the Domain that the node directly attaches
  // to. nullptr if the node is not in the hierarchy
  const Domain* findDomainForShard(ShardID shard) const;

  // get all node location domains in the specific scope
  std::vector<const Domain*> domainsInScope(NodeLocationScope scope) const;

 private:
  // the node represents the ROOT level domain of the hierarchy
  Domain root_;

  // a mapping between nodes in the node set and the Domain it directly attaches
  // to
  std::unordered_map<ShardID, const Domain*, ShardID::Hash> node_domain_map_;

  // insert a node with @param index to the hierarchy. Note that the caller
  // need to ensure that the _index_ must not already exist in the hierarchy
  // (assert in debug build)
  void insertShardWithLocation(ShardID shard, const NodeLocation& location);

  // perform consistency checks on the hierarchy, used in debug builds
  void checkConsistency() const;

  // recursive subroutine used by checkConsistency()
  size_t checkConsistencyRecursive(
      const Domain* root,
      size_t level,
      std::unordered_set<ShardID, ShardID::Hash>* nodes) const;
};

}} // namespace facebook::logdevice
