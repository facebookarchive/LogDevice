/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <functional>
#include <vector>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/CopySetSelectorDependencies.h"
#include "logdevice/common/NodeLocationHierarchy.h"

namespace facebook { namespace logdevice {

/**
 * @file CrossDomainCopySetSelector is a copyset selector that makes
 *       copyset selection decisions based on the location information of
 *       storage nodes, and enforces failure domain properties in a specified
 *       location scope (e.g., RACK, CLUSTER, REGION...). Specifically, there
 *       are two goals for the copyset selection:
 *
 *       (1) Enforce failure domain property on the specified location scope.
 *           It is a mandatory requirement that for replication >= 2, the
 *           the copyset selected must span at least two domains in the failure
 *           domain scope. Such guarantee is crucial for LD to tolerate the
 *           failure of an entire failure domain and preserve correctness
 *           in sequencer recover and gap detection.
 *
 *       (2) Exploit locality to place copies for optimization. This is used
 *           to reduce the network traffic across nodes from different domains
 *           in the specified scope (e.g., cross-rack traffic). To achieve that
 *           the selector tries to place more copies on nodes that share the
 *           same domain in the given scope with the sequencer node that
 *           runs the copyset selector. Note that this is an optimization
 *           but not strictly required: in case there are not enough available
 *           nodes in the local domain, the copyset selector will pick nodes
 *           from other domain to complete the selection.
 */

class CrossDomainCopySetSelector : public CopySetSelector {
 public:
  /**
   * @param sync_replication_scope   NodeLocationScope to enforce
   *                                 failure domain placement policy so that the
   *                                 final copyset must span two domains in this
   *                                 scope. value can be RACK, ROW, CLUSTER,
   *                                 ... REGION. Cannot be ROOT or NODE.
   */
  CrossDomainCopySetSelector(
      logid_t logid,
      StorageSet storage_set,
      std::shared_ptr<NodeSetState> nodeset_state,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      NodeID my_node_id,
      copyset_size_t replication_factor,
      NodeLocationScope sync_replication_scope,
      const CopySetSelectorDependencies* deps =
          CopySetSelectorDependencies::instance());

  // stateful selection is not needed for now
  class State : public CopySetSelector::State {
   public:
    void reset() override{};
  };

  std::string getName() const override;

  // see docblock in CopySetSelector::select()
  CopySetSelector::Result
  select(copyset_size_t extras,
         StoreChainLink copyset_out[],
         copyset_size_t* copyset_size_out,
         bool* chain_out = nullptr,
         CopySetSelector::State* selector_state = nullptr,
         RNG& rng = DefaultRNG::get(),
         bool retry = true) const override;

  // see docblock in CopySetSelector::augment()
  CopySetSelector::Result augment(ShardID inout_copyset[],
                                  copyset_size_t existing_copyset_size,
                                  copyset_size_t* out_full_size,
                                  RNG& rng = DefaultRNG::get(),
                                  bool retry = true) const override;

  CopySetSelector::Result augment(StoreChainLink inout_copyset[],
                                  copyset_size_t existing_copyset_size,
                                  copyset_size_t* out_full_size,
                                  bool fill_client_id = false,
                                  bool* chain_out = nullptr,
                                  RNG& rng = DefaultRNG::get(),
                                  bool retry = true) const override;

  copyset_size_t getReplicationFactor() const override {
    return replication_factor_;
  }

  /**
   * Given the required number of copies to replicate, determine how many
   * copies of record the selector will first attempt to place on the primary
   * domain.
   *
   * @param sync_replication_scope   location scope to replicate across
   * @param copies                   number of required copies
   * @param chain_enabled            indicate if chaining is allowed
   */
  static copyset_size_t
  nodesInPrimaryDomain(NodeLocationScope sync_replication_scope,
                       copyset_size_t copies,
                       bool chain_enabled);

  /**
   * Utility method to print the copyset selected so far.
   *
   * @param copyset : copyset chain
   * @size          : size of chain
   *
   * @return        : a string representing list of nodes in the chain
   */
  std::string printChainLink(StoreChainLink* copyset, size_t size) const;

 private:
  // helper functions implementing the selection algorithm

  // base iterator type
  template <typename Item>
  class Iterator;

  // iterator on location domains in the node location hierarchy
  using DomainIterator = Iterator<const NodeLocationHierarchy::Domain*>;
  // iterator on nodes of a particular location domain
  using NodeIterator = Iterator<ShardID>;

  // predicate function on whether a node should be selected
  using pred_func_t = std::function<bool(ShardID)>;

  /**
   * Implementation function for select(). Return more information such as
   * nodes in the primary domain in the copyset.
   *
   * @param  ncopies             number of requested copies in the copyset
   * @param  primary_domain_out  if not null, will store the primary domain of
   *                             the selected copyset
   * @param  primary_nodes_out   if not null, will store the number of primary
   *                             domain nodes in the copyset
   * @return                     number of nodes selected
   */
  copyset_size_t
  selectImpl(copyset_size_t ncopies,
             StoreChainLink copyset_out[],
             const NodeLocationHierarchy::Domain** primary_domain_out,
             copyset_size_t* primary_nodes_out,
             bool* chain_out,
             RNG& rng) const;

  /**
   * Select specific number of nodes from a given location domain. Peferences
   * are given to the nodes that are close to the sequencer nodes.
   *
   * @param domain          location domain to select nodes from
   * @param num_copies      number of copies to select
   * @param copyset_out     array to append selection results to, must have
   *                        enough space
   * @param chain_out       in-and-out parameter indicating whether chaining is
   *                        enabled. can be changed by the function
   * @param pred            predicate function for filtering out certain storage
   *                        nodes
   * @return                0 if there is 0 nodes available in the domain
   *                        otherwise, return num of nodes selected
   *                        (0 < n <= num_copies)
   */
  copyset_size_t
  selectNodesInDomain(const NodeLocationHierarchy::Domain* domain,
                      copyset_size_t num_copies,
                      StoreChainLink* copyset_out,
                      bool* chain_out,
                      RNG& rng,
                      pred_func_t pred = nullptr) const;

  /**
   *  Select storage nodes from a container of candidate storage nodes
   *
   *  @param start       an iterator which binds to the node container
   *                     also decides the starting position for selection
   *
   *  refer selectNodesInDomain() for other parameters
   *
   *  @return            num of nodes selected
   */
  copyset_size_t selectNodes(NodeIterator* start,
                             copyset_size_t num_copies,
                             StoreChainLink* copyset_out,
                             bool* chain_out,
                             pred_func_t pred = nullptr) const;

  // check if a location domain is a local domain (contains the node running
  // the sequencer).
  // @param domain must be from the same NodeLocationHierarchy
  bool isLocalDomain(const NodeLocationHierarchy::Domain* domain) const;

  // get node's ancestor domain of scope sync_replication_scope_
  const NodeLocationHierarchy::Domain* getDomainForNode(ShardID index) const;

 private:
  // initial internal state, won't change once the Selector object is
  // constructed

  // Injectable dependencies.
  const CopySetSelectorDependencies* deps_;

  // logid for logging purpose
  const logid_t logid_;

  const std::shared_ptr<NodeSetState> nodeset_state_;

  const copyset_size_t replication_factor_;

  // the location scope for enforcing failure domain requirements
  const NodeLocationScope sync_replication_scope_;

  // data structure for maintaining node location topology
  const NodeLocationHierarchy hierarchy_;

  // a list of ALL subdomains in the sync_replication_scope_ in hierarchy_
  const std::vector<const NodeLocationHierarchy::Domain*> domains_in_scope_;

  // location domains for the local sequencer node that runs the copyset
  // selector, indexed by location scopes
  std::array<const NodeLocationHierarchy::Domain*, NodeLocation::NUM_ALL_SCOPES>
      local_domains_;
};

}} // namespace facebook::logdevice
