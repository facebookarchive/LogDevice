/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RandomNodeSetSelector.h"
#include "logdevice/common/configuration/NodeLocation.h"

namespace facebook { namespace logdevice {

/**
 * @file RandomCrossDomainNodeSetSelector is a basic nodeset selector that
 *       selects nodes based on the location-based failure domain of storage
 *       nodes. It takes into account the `sync_replicate_across' property
 *       in the Log config in order to make sure sequencers can always select
 *       copysets that span across at least two domains in the location scope
 *       specified by `sync_replicate_across' from the generated node set.
 *       Moreover, sequencers can leverage the location information of nodes in
 *       its nodeset to reduce the amount of cross-domain traffic in the
 *       specified synchronous replication scope.
 *
 *       Requirements and Limitations:
 *
 *       Since currently sequencer placement is not location-aware, to ensure
 *       that every sequencer can store copies of record in its own domain, a
 *       nodeset needs to contain storage nodes from all domains in the cluster.
 *       Furthermore, it has the following requirements to simplify the
 *       selection and achieve load balancing:
 *
 *       (1) each node must have location information specified and the
 *           specified synchronous replication scope must not be empty in
 *           its location
 *       (2) the nodeset size must be divisible by the number of domains so that
 *           a nodeset contains equal amount of nodes from each domain
 *       (3) each domain must have at least nodeset_size/n_domains nodes.
 *       (4) if replication > 2, each domain  must have at least 2 non-zero
 *           weighted nodes to make it possible to reduce cross-domain bandwidth
 *
 *       Another limitation is that the selector does not consider node locality
 *       besides synchronous replication scope. For example, if the log is
 *       configured to be cross-region replicated, the nodeset selector does not
 *       ensure the nodeset will contain the nodes from the same _rack_ on which
 *       the sequencer is located. Therefore, there is no guarantee that
 *       cross-rack traffic will be optimized in such case.
 *
 *       Note that for better load distribution, each domain should have similar
 *       number of storage nodes. This is not enforced by the selector.
 */

class RandomCrossDomainNodeSetSelector : public RandomNodeSetSelector {
 public:
  explicit RandomCrossDomainNodeSetSelector(
      RandomNodeSetSelector::MapLogToShardFn map_log_to_shard)
      : RandomNodeSetSelector(map_log_to_shard) {}

  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      uint64_t seed,
      const EpochMetaData* prev,
      const Options* options = nullptr) override;

 private:
  using DomainMap = std::map<std::string, NodeSetIndices>;

  // used privately with a broader argument set. Will use (and potentially
  // mutate, if removing some shards yields a better storage_set_size match) the
  // domain_map if supplied, otherwise generates one from the given config. Uses
  // log_id for logging if it is supplied
  storage_set_size_t getStorageSetSizeImpl(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t target_nodeset_size,
      NodeLocationScope sync_replication_scope,
      int replication_factor,
      DomainMap* domain_map,
      const Options* options);
  static int buildDomainMap(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      NodeLocationScope sync_replication_scope,
      const Options* options,
      DomainMap* map);

  // Extracts replication factor and highest replication scope from
  // ReplicationProperty.
  static ReplicationProperty::OldRepresentation
  convertReplicationProperty(ReplicationProperty replication);
};

}} // namespace facebook::logdevice
