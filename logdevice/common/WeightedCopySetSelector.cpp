/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WeightedCopySetSelector.h"

#include "logdevice/common/CopySet.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

using NodeStatus = NodeAvailabilityChecker::NodeStatus;

// See select() for explanation.
static constexpr int MAX_BLACKLISTING_ITERATIONS = 100;

// Returns either floor(x) or ceil(x), so that, on average, the return value
// is equal to x. In particular, if x is integer returns x.
static copyset_size_t randomRound(double x, RNG& rng) {
  if (x < 0) {
    ld_check(x >= -Sampling::EPSILON); // rounding error
    x = 0;
  }
  auto res = (int)floor(x + folly::Random::randDouble01(rng));
  // Be paranoid of rounding errors and clamp the result to [floor(x), ceil(x)].
  int x_floor = (int)x;
  int x_ceil = x_floor + (x != (double)x_floor);
  res = std::max(x_floor, std::min(x_ceil, res));
  return (copyset_size_t)res;
}

WeightedCopySetSelector::WeightedCopySetSelector(
    logid_t logid,
    const EpochMetaData& epoch_metadata,
    std::shared_ptr<NodeSetState> nodeset_state,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    folly::Optional<NodeID> my_node_id,
    const logsconfig::LogAttributes* log_attrs,
    bool locality_enabled,
    StatsHolder* stats,
    RNG& init_rng,
    bool print_bias_warnings,
    const CopySetSelectorDependencies* deps)
    : logid_(logid),
      deps_(deps),
      nodeset_state_(nodeset_state),
      print_bias_warnings_(print_bias_warnings),
      locality_enabled_(locality_enabled),
      stats_(stats),
      nodeset_indices_(epoch_metadata.shards) {
  {
    ld_check(nodes_configuration != nullptr);
    // Convert replication requirement from the more general ReplictionProperty
    // representation to the more restrictive two-level representation that this
    // copyset selector can work with.
    auto rf = epoch_metadata.replication.getDistinctReplicationFactors();
    ld_check(!rf.empty());
    if (rf.size() > 2) {
      // The given replication property has more than two differente replication
      // factors. It can't be represented with just two scopes. Let's transform
      // it into a stricter replication requirement to coerce it into
      // the two-level representation. E.g. if the replication property is
      // {{REGION, 2}, {RACK, 3}, {NODE, 4}}, it will be coerced to
      // {{REGION, 2}, {RACK, 4}}.
      rf[1].second = rf.back().second;
      rf.resize(2);
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          2,
          "Replication property %s has more than two different replication "
          "factors. It will be widened to %s for copyset selection purposes.",
          epoch_metadata.replication.toString().c_str(),
          ReplicationProperty(rf).toString().c_str());
    }
    if (rf.size() == 1) {
      // One-level replication. To avoid having special treatment for this case
      // everywhere, just add a redundant equal replication factor to get
      // two-level replication.
      auto r = rf.back();
      rf.push_back(r); // fun fact: rf.push_back(rf.back()) would be illegal
    }
    secondary_replication_scope_ = rf[0].first;
    secondary_replication_ = rf[0].second;
    replication_scope_ = rf[1].first;
    replication_ = rf[1].second;
  }

  // Find my node's failure domain name.
  folly::Optional<std::string> my_domain;
  if (my_node_id.hasValue()) {
    node_index_t my_node = my_node_id.value().index();
    const auto* node_sd = nodes_configuration->getNodeServiceDiscovery(my_node);
    if (node_sd && node_sd->location.hasValue()) {
      my_domain =
          node_sd->location->getDomain(secondary_replication_scope_, my_node);
    }
  }

  std::vector<double> weights = epoch_metadata.weights.empty()
      ? calculateWeightsBasedOnConfig(
            epoch_metadata, *nodes_configuration, log_attrs)
      : epoch_metadata.weights;

  // Build the hierarchy. It'll always have these three levels:
  // 1. Root.
  // 2. Domains of scope secondary_replication_scope.
  // 3. Domains of scope replication_scope.
  //    They're leaves, i.e. nodes are attached directly to them.
  // For simplicity, there'll be all 3 levels even if
  // secondary_replication_scope == replication_scope or
  // replication_scope == NODE. There's no special treatment of these cases.

  std::unordered_map<std::string, size_t> secondary_domain_idx;
  std::unordered_map<std::string, size_t> domain_idx;

  // Shuffle the nodeset before building hierarchy. This randomizes the order
  // of domains in the tree, increasing scatter width for rebuilding.
  //
  // Example: there are 4 racks of the same weight, and we're replicating to
  // 2 racks; if all WeightedCopySetSelectors have the 4 racks in the same
  // order in their Hierarchies, say [a, b, c, d], then, most of the time,
  // none of the copysets containing nodes from rack a would contain nodes from
  // racks b or d (because of systematic sampling). So if we were to
  // rebuild rack a, rack c would have to do all of the rebuilding work.
  std::vector<size_t> nodeset_permutation(epoch_metadata.shards.size());
  std::iota(nodeset_permutation.begin(), nodeset_permutation.end(), 0);
  simple_shuffle(
      nodeset_permutation.begin(), nodeset_permutation.end(), init_rng);

  for (size_t i : nodeset_permutation) {
    ShardID shard = epoch_metadata.shards[i];
    double weight = weights[i];

    const auto* node_sd =
        nodes_configuration->getNodeServiceDiscovery(shard.node());
    if (node_sd == nullptr) {
      continue;
    }

    const auto& storage_membership =
        nodes_configuration->getStorageMembership();
    if (!storage_membership->shouldReadFromShard(shard)) {
      // storage membership now has more granular view of per-shard storage
      // state. exclude shards that are not readable (e.g., in NONE storage
      // state).
      continue;
    }

    NodeLocation location = node_sd->location.value_or(NodeLocation());
    std::string domain = location.getDomain(replication_scope_, shard.node());
    std::string secondary_domain =
        location.getDomain(secondary_replication_scope_, shard.node());

    // Add node to the tree even if node has zero weight (and so will never
    // be picked).
    // It's needed for augment() in case some existing copyset contains it.

    // Insert or get secondary domain.
    size_t secondary_idx;
    if (secondary_domain_idx.count(secondary_domain)) {
      secondary_idx = secondary_domain_idx[secondary_domain];
    } else {
      secondary_idx = hierarchy_.root.subdomains.size();
      hierarchy_.root.subdomains.emplace_back();
      secondary_domain_idx[secondary_domain] = secondary_idx;
    }
    Domain* secondary = &hierarchy_.root.subdomains[secondary_idx];

    // Insert or get leaf domain.
    size_t idx;
    if (domain_idx.count(domain)) {
      idx = domain_idx[domain];
    } else {
      idx = secondary->subdomains.size();
      secondary->subdomains.emplace_back();
      domain_idx[domain] = idx;
    }
    Domain* leaf = &secondary->subdomains[idx];

    // Add node to the leaf.
    size_t node_idx_in_leaf = leaf->node_ids.size();
    leaf->node_ids.push_back(shard);
    leaf->weights.push_back(weight);

    // Remember where this node lives.
    hierarchy_.node_paths[shard] = {secondary_idx, idx, node_idx_in_leaf};
  }

  // Calculate weights of secondary domains.
  for (size_t secondary_idx = 0;
       secondary_idx < hierarchy_.root.subdomains.size();
       ++secondary_idx) {
    Domain* secondary = &hierarchy_.root.subdomains[secondary_idx];
    for (size_t idx = 0; idx < secondary->subdomains.size(); ++idx) {
      secondary->weights.push_back(
          secondary->subdomains[idx].weights.totalWeight());
    }
    hierarchy_.root.weights.push_back(secondary->weights.totalWeight());
  }

  // Find my node's domain.
  if (my_domain.hasValue() && secondary_domain_idx.count(my_domain.value())) {
    my_domain_idx_ = secondary_domain_idx[my_domain.value()];
  }

  ld_debug("Created WeightedCopySetSelector for log %lu. Nodeset: %s, "
           "weights: %s, R: %d, R2: %d, hierarchy: %s",
           logid_.val_,
           logdevice::toString(nodeset_indices_).c_str(),
           logdevice::toString(weights).c_str(),
           (int)replication_,
           (int)secondary_replication_,
           hierarchy_.toString().c_str());
}

std::vector<double> WeightedCopySetSelector::calculateWeightsBasedOnConfig(
    const EpochMetaData& epoch_metadata,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const logsconfig::LogAttributes* log_attrs) {
  // Epoch metadata doesn't contain weights. Assume the following about how
  // nodeset was selected:
  //  * nodeset spans all nonzero-weight racks,
  //  * nodeset has the same number of nodes from each rack,
  //  * nodes in the same rack have equal weights in config,
  //  * nodes in each rack were selected uniformly at random,
  // (where "rack" is a domain of scope secondary_replication_scope)

  // We'll need to rescale weights of nodes to account for differences in domain
  // weights. Consider an example: in config rack A has 2 nodes of
  // weight 2 each, rack B has 1 nodes of weight 1, nodeset selector picked
  // 1 node from each rack, and replication factor is 1.
  // We need to pick the node B with probability 1/5 (because B's total weight
  // is 1/5 of total weight), so the effective weight of node from B should
  // be 4 times smaller than the node from A. If we used the weights from
  // config, it would be only 2 times smaller.
  // The right weight to use is:
  // node_weight / domain_nodeset_weight * domain_total_weight.

  // Calculate the auxiliary total weights for rescaling.

  // Sum of weights in config of nodes in each domain of
  // secondary_replication_scope, considering only nodes in nodeset.
  std::unordered_map<std::string, double> domain_nodeset_weight;
  for (ShardID shard : epoch_metadata.shards) {
    const auto* node_sd =
        nodes_configuration.getNodeServiceDiscovery(shard.node());
    if (node_sd == nullptr) {
      continue;
    }
    NodeLocation location = node_sd->location.value_or(NodeLocation());
    std::string domain =
        location.getDomain(secondary_replication_scope_, shard.node());
    // use the more granular per-shard storage state in StorageMembership.
    // if _shard_ is writable (e.g., in RW state), then use the storage capacity
    // in it's node's storage attributes.
    domain_nodeset_weight[domain] +=
        nodes_configuration.getWritableStorageCapacity(shard);
  }

  // Target sum of effective weights of nodes in each domain. Without locality,
  // equal to the sum of weights in config of all nodes in the domain (including
  // the ones not in nodeset). With locality, there's an additional adjustment
  // to increase the relative weight of sequencer domain.
  std::unordered_map<std::string, double> domain_target_weight;

  for (const auto& node : *nodes_configuration.getStorageMembership()) {
    const auto* node_sd = nodes_configuration.getNodeServiceDiscovery(node);
    // storage membership nodes must be in the service discovery config
    ld_check(node_sd != nullptr);
    NodeLocation location = node_sd->location.value_or(NodeLocation());
    std::string domain = location.getDomain(secondary_replication_scope_, node);
    const auto* storage_attr =
        nodes_configuration.getNodeStorageAttribute(node);
    ld_check(storage_attr != nullptr);

    if (domain_nodeset_weight.count(domain) &&
        domain_nodeset_weight.at(domain) != 0.0) {
      // Note: we do not take into account of the storage state in the
      // domain multiplier calculation to correctly take into account of
      // node selection during nodeset calculation
      domain_target_weight[domain] += storage_attr->capacity;
    } else {
      // Some positive-weight domain has no positive-weight nodes in nodeset.
      // This is unusual but possible if e.g. a new rack was added to config,
      // and nodesets haven't been updated yet.
      // Act as if this domain had zero weight.
      domain_target_weight[domain]; // make sure domain is in the map
    }
  }

  if (locality_enabled_ && log_attrs != nullptr) {
    optimizeWeightsForLocality(
        nodes_configuration, log_attrs, domain_target_weight);
  }

  std::vector<double> weights(epoch_metadata.shards.size());
  for (size_t i = 0; i < weights.size(); ++i) {
    ShardID shard = epoch_metadata.shards[i];

    const auto* node_sd =
        nodes_configuration.getNodeServiceDiscovery(shard.node());
    if (node_sd == nullptr) {
      continue;
    }

    const auto& storage_membership = nodes_configuration.getStorageMembership();
    if (!storage_membership->shouldReadFromShard(shard)) {
      // storage membership now has more granular view of per-shard storage
      // state. exclude shards that are not readable (e.g., in NONE storage
      // state).
      continue;
    }

    NodeLocation location = node_sd->location.value_or(NodeLocation());
    std::string secondary_domain =
        location.getDomain(secondary_replication_scope_, shard.node());

    const auto* storage_attr =
        nodes_configuration.getNodeStorageAttribute(shard.node());
    ld_check(storage_attr != nullptr);

    // Similarily, use the more granular per-shard storage state in
    // StorageMembership. This is used to prevent the Appender/Sequencer
    // from sending any STORE to any shard that is not RW.
    double weight = nodes_configuration.getWritableStorageCapacity(shard);
    ld_check(weight >= 0);
    if (weight > 0) {
      ld_assert(domain_nodeset_weight.at(secondary_domain) > 0);
      weight *= domain_target_weight.at(secondary_domain) /
          domain_nodeset_weight.at(secondary_domain);
    }

    weights[i] = weight;
  }

  return weights;
}

void WeightedCopySetSelector::optimizeWeightsForLocality(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const logsconfig::LogAttributes* log_attrs,
    std::unordered_map<std::string, double>& domain_target_weight) {
  // Find primary sequencer domain.
  ld_check(log_attrs != nullptr);
  node_index_t sequencer_node =
      HashBasedSequencerLocator::getPrimarySequencerNode(
          logid_, nodes_configuration, log_attrs);

  const auto* node_sd =
      nodes_configuration.getNodeServiceDiscovery(sequencer_node);
  ld_check(node_sd != nullptr);
  std::string sequencer_domain =
      node_sd->location.value_or(NodeLocation())
          .getDomain(secondary_replication_scope_, sequencer_node);
  ld_check(domain_target_weight.count(sequencer_domain));

  // See doc/weighted-copyset-selector-locality.md for explanation.

  // First calculate some total weights.

  std::unordered_map<std::string, double> domain_sequencer_weight;
  double total_sequencer_weight = 0;

  for (const auto& [n, node_sd] : *nodes_configuration.getServiceDiscovery()) {
    NodeLocation location = node_sd.location.value_or(NodeLocation());
    std::string domain = location.getDomain(secondary_replication_scope_, n);
    const auto sequencer_weight = nodes_configuration.getSequencerMembership()
                                      ->getEffectiveSequencerWeight(n);
    domain_sequencer_weight[domain] += sequencer_weight;
    total_sequencer_weight += sequencer_weight;
  }

  // Note that, unlike the doc, we have unnormalized weights, so we'll need to
  // divide by the total weight in a few places.
  double total_weight = 0;
  for (const auto& p : domain_target_weight) {
    total_weight += p.second;
  }

  if (total_sequencer_weight < Sampling::EPSILON ||
      total_weight < Sampling::EPSILON) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Total storage weight (%lf) or sequencer weight (%lf) is zero. Weird.",
        total_weight,
        total_sequencer_weight);
    return;
  }

  // Pick domain_weight_shift[s] (a.k.a. D[s]) for every s. D[s] is by how much
  // we want to increase the weight of sequencer domain for logs with
  // sequencers in domain s.
  // This is just a heuristic; from balance point of view it's valid to set
  // these numbers to anything nonnegative. If locality doesn't work well in
  // some cluster setups, this is the part of code that you may want to tweak.

  std::unordered_map<std::string, double> domain_weight_shift;
  double total_weight_shift = 0;
  std::pair<double, double> max_shifts(0, 0);
  for (const auto& p : domain_target_weight) {
    std::string domain = p.first;
    double weight = p.second;
    ld_check_gt(domain_sequencer_weight.count(domain), 0);
    double sequencer_weight = domain_sequencer_weight.at(domain);

    // Target weight = total_weight / replication_, i.e. one copy of every
    // record goes to local rack.
    double shift = total_weight / replication_ - weight;

    // Avoid increasing domain's weight if:
    //  1. Weight is zero. It's impossible to increase it since we're not
    //     allowed to turn zero-weight nodes into nonzero-weight nodes.
    //  2. Sequencer weight is zero. Value of D[s] doesn't matter in this case.
    //     Let's set it to zero to avoid confusion.
    //  3. Sequencer weight is much greater than storage weight. I.e. it's
    //     almost a sequencer-only domain; e.g. we had a sequencer-only rack
    //     and then accidentally added a single storage node to it.
    //     Better not to shift weight to this domain because such shift would
    //     be hard to counterbalance.
    if (weight / total_weight < Sampling::EPSILON ||
        sequencer_weight / total_sequencer_weight < Sampling::EPSILON ||
        sequencer_weight / total_sequencer_weight >
            10 * weight / total_weight) {
      shift = 0;
    }

    // Avoid negative D[s], i.e. trying to decrease sequencer's rack weight.
    // It would accomplish nothing. If domain's weight is too big for our taste
    // (greater than 1 / replication_ of the total weight), we can't really do
    // much about it: if we decrease it for one s we would have to increase
    // it for another s.
    if (shift < 0) {
      shift = 0;
    }

    domain_weight_shift[domain] = shift;
    total_weight_shift += shift * sequencer_weight;
    if (shift > max_shifts.first) {
      max_shifts = std::make_pair(shift, max_shifts.first);
    } else if (shift > max_shifts.second) {
      max_shifts.second = shift;
    }
  }

  if (total_weight_shift / total_weight / total_sequencer_weight <
      Sampling::EPSILON) {
    // None of the racks need weight adjustment. In particular, this path
    // is hit for "disaggregated" clusters: when all nodes are either
    // sequencer-only or storage-only.
    return;
  }

  // Scale all shifts so that all adjusted weights are valid.

  // "coef" here is "C" in the doc.
  // In reasonable cluster setups the coefficient is expected to be
  // just a little above 1. As a sanity check, don't make it greater than 10.
  double max_unbiased_coef = 10;
  double min_coef_for_full_locality = 0;
  for (const auto& p : domain_target_weight) {
    std::string domain = p.first;
    double weight = p.second;
    double sequencer_weight = domain_sequencer_weight.at(domain);
    double shift = domain_weight_shift.at(domain);

    double share = sequencer_weight * shift / total_weight_shift;

    if (share < Sampling::EPSILON) {
      // No weight adjustment when sequencer is in this domain.
      continue;
    }

    double max_other_shift =
        shift == max_shifts.first ? max_shifts.second : max_shifts.first;
    if (max_other_shift / total_weight < Sampling::EPSILON) {
      // This is the only domain eligible to run sequencers.
      // Make no weight adjustments.
      max_unbiased_coef = 0;
      continue;
    }

    // Big weight shifts can skew the overall distribution in two ways:
    //  1. If some logs shift too much weight to some domain, it may be
    //     impossible for other logs to counterbalance this shift - even if they
    //     use zero weight the domain would still be overused in total.
    //     To avoid this we need:
    //     C < W[r]/D[s]*(sum{i} S[i]*D[i])/(S[r]*D[r])  for all r != s
    //     We want this inequality to be satisfied with some margin
    //     to make sure that positive weights remain strictly positive;
    //     otherwise we can lose some write availability, since zero-weight
    //     domains are not picked in copysets.
    //  2. Some logs may shift so much weight into a domain that it's impossible
    //     for these logs to select unbiased copysets. More precisely, the
    //     increased weight shouldn't be greater than (R - K + 1) / R of the
    //     total weight, where R is replication factor, K is secondary
    //     replication factor. The corresponding condition for C is:
    //     C <= ((R - K + 1)/R - W[s])
    //            / (D[s] * (1 - S[s]*D[s]/(sum{i} S[i]*D[i])))

    // 1.
    const double coef_margin = 1e-4;
    max_unbiased_coef =
        std::min(max_unbiased_coef,
                 std::max(0., weight / max_other_shift / share - coef_margin));

    // 2.
    max_unbiased_coef = std::min(max_unbiased_coef,
                                 ((replication_ - secondary_replication_ + 1.) /
                                      replication_ * total_weight -
                                  weight) /
                                     (shift * (1 - share)));

    // We get maximal locality when, for each sequencer domain, the sequencer
    // domain's weight is >= 1/R of the total weight. Analogous to case 2 above:
    min_coef_for_full_locality = std::max(
        min_coef_for_full_locality,
        (total_weight / replication_ - weight) / (shift * (1 - share)));
  }

  double coef = std::min(max_unbiased_coef, min_coef_for_full_locality);

  // Finally actually adjust the weights.

  double my_shift = domain_weight_shift.at(sequencer_domain);
  double new_total_weight = 0;

  for (auto& p : domain_target_weight) {
    if (p.second <= Sampling::EPSILON * total_weight) {
      ld_check(p.second >= -Sampling::EPSILON * total_weight);
      // Zero weights must stay zero.
      continue;
    }

    std::string domain = p.first;
    double sequencer_weight = domain_sequencer_weight.at(domain);
    double shift = domain_weight_shift.at(domain);
    double share = sequencer_weight * shift / total_weight_shift;

    double is_sequencer_domain = p.first == sequencer_domain;
    p.second += coef * my_shift * (is_sequencer_domain - share);

    new_total_weight += p.second; // before clamping

    const double weight_margin = 1e-4;
    if (p.second / total_weight < weight_margin) {
      if (p.second / total_weight <
          -Sampling::EPSILON) { // not a rounding error
        // This should be impossible given that we picked `coef` specifically
        // to avoid negative weights.
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            2,
            "Internal error: weight adjustment produced a negative weight. "
            "Weights (half-adjusted): %s, sequencer weights: %s, "
            "weight shifts: %s, sequencer domain: %s, coef: %lf",
            toString(domain_target_weight).c_str(),
            toString(domain_sequencer_weight).c_str(),
            toString(domain_weight_shift).c_str(),
            sequencer_domain.c_str(),
            coef);
        ld_check(false);
      }
      // Positive weights must stay strictly positive.
      p.second = weight_margin * total_weight;
    }
  }

  // Our adjustments are not expected to change total weight.
  if (fabs(new_total_weight - total_weight) / total_weight >
      Sampling::EPSILON) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Internal error: weight adjustment changed total weight. "
        "Adjusted weights: %s, sequencer weights: %s, weight shifts: %s, "
        "sequencer domain: %s, coef: %lf, old total weight: %lf, new total "
        "weight: %lf, difference: %lf",
        toString(domain_target_weight).c_str(),
        toString(domain_sequencer_weight).c_str(),
        toString(domain_weight_shift).c_str(),
        sequencer_domain.c_str(),
        coef,
        total_weight,
        new_total_weight,
        new_total_weight - total_weight);
    ld_check(false);
  }

  ld_debug("Adjusted domain weights for log %lu, sequencer domain %s: %s; "
           "shift values: %s, coef: %lf",
           logid_.val_,
           sequencer_domain.c_str(),
           toString(domain_target_weight).c_str(),
           toString(domain_weight_shift).c_str(),
           coef);
}

std::string WeightedCopySetSelector::getName() const {
  return "WeightedCopySetSelector";
}

WeightedCopySetSelector::NodeAvailabilityCache&
WeightedCopySetSelector::prepareCachedNodeAvailability() const {
  NodeAvailabilityCache& cache = *node_availability_cache_.get();
  if (!cache.adjusted_hierarchy.hasValue()) {
    ld_check(cache.unavailable_nodes.empty());
    cache.adjusted_hierarchy.emplace(&hierarchy_);
  }
  // Re-check all the nodes that were unavailable last time.
  for (int i = (int)cache.unavailable_nodes.size() - 1; i >= 0; --i) {
    ShardID shard = cache.unavailable_nodes[i];
    StoreChainLink unused;
    auto node_status = deps_->getNodeAvailability()->checkNode(
        nodeset_state_.get(), shard, &unused);
    if (node_status != NodeStatus::NOT_AVAILABLE) {
      cache.unavailable_nodes.erase(cache.unavailable_nodes.begin() + i);
      bool was_disabled = cache.adjusted_hierarchy->attachNode(shard);
      ld_check(was_disabled);
      cache.avoid_detaching_my_domain = false;
    }
  }

  return cache;
}

bool WeightedCopySetSelector::checkAvailabilityAndBlacklist(
    const StoreChainLink copyset_chain[],
    size_t copyset_size,
    AdjustedHierarchy& hierarchy,
    NodeAvailabilityCache& cache,
    bool* out_biased,
    StoreChainLink* out_chain_links,
    bool* out_chain) const {
  ld_check(out_biased);
  bool ok = true;
  for (size_t i = 0; i < copyset_size; ++i) {
    StoreChainLink destination;
    ShardID node = copyset_chain[i].destination;
    auto node_status = deps_->getNodeAvailability()->checkNode(
        nodeset_state_.get(), node, &destination);
    if (node_status == NodeStatus::NOT_AVAILABLE) {
      ld_assert(!std::count(cache.unavailable_nodes.begin(),
                            cache.unavailable_nodes.end(),
                            node));
      // Node is unavailable. Disable it both in cache and in the local
      // AdjustedHierarchy.
      bool was_enabled_local = hierarchy.detachNode(node);
      bool was_enabled_cache = cache.adjusted_hierarchy->detachNode(node);
      ld_check(was_enabled_local);
      if (was_enabled_cache) {
        cache.unavailable_nodes.push_back(node);
        cache.avoid_detaching_my_domain = false;
      } else {
        // Be extra paranoid and don't allow cache.unavailable_nodes to grow
        // unboundedly if there's a bug.
        ld_check(false);
      }
      // Don't count the copyset as biased if it's caused by graylisting of a
      // node. Graylisting is too noisy.
      if (!*out_biased &&
          (!nodeset_state_ ||
           nodeset_state_->getNotAvailableReason(node) !=
               NodeSetState::NotAvailableReason::SLOW)) {
        if (print_bias_warnings_) {
          RATELIMIT_INFO(std::chrono::seconds(10),
                         1,
                         "Copyset for log %lu is biased because we've just "
                         "blacklisted node "
                         "%s. This should be transient.",
                         logid_.val_,
                         node.toString().c_str());
        }
        *out_biased = true;
      }
      ok = false;
    } else {
      if (out_chain) {
        *out_chain &= node_status == NodeStatus::AVAILABLE;
      }
      if (out_chain_links) {
        out_chain_links[i] = destination;
      }
    }
  }
  return ok;
}

CopySetSelector::Result
WeightedCopySetSelector::select(copyset_size_t extras,
                                StoreChainLink copyset_out[],
                                copyset_size_t* copyset_size_out,
                                bool* chain_out,
                                State* selector_state,
                                RNG& rng,
                                bool retry) const {
  NodeAvailabilityCache& cache = prepareCachedNodeAvailability();
  // Need to make a copy in case we'll detach local domain - don't want to
  // cache that.
  AdjustedHierarchy hierarchy = cache.adjusted_hierarchy.value();
  const double total_weight = hierarchy_.root.weights.totalWeight();

  bool biased = false;
  copyset_chain_t copyset_chain(replication_);

  // On each attempt we pick a copyset and check availability of its nodes.
  // If all are available, we return this copyset, otherwise blacklist
  // the unavailable nodes and proceed to the the next attempt.
  //
  // Note that this is biased when not all nodes are available. E.g. imagine
  // a 4-node nodeset with equal weights and a copyset selector that picks
  // two consecutive non-blacklisted nodes. If one node is blacklisted, its
  // two neighbors will be less likely to be picked that the remaining node
  // (7/12 vs 5/6 to be exact).
  // The purpose of NodeAvailabilityCache is to mitigate this bias.

  Result ret;
  size_t attempts = 0;
  SCOPE_EXIT {
    if (!retry) {
      // not updating stats twice
      return;
    }
    if (ret == Result::SUCCESS) {
      STAT_ADD(stats_, copyset_selection_attempts, attempts);
      STAT_INCR(stats_, copyset_selected);
      if (biased) {
        STAT_INCR(stats_, copyset_biased);
      }
    } else {
      STAT_INCR(stats_, copyset_selection_failed);
    }
  };

  // retry if allowed, else log errors
  auto retry_or_complain_on_too_many_unavailable = [&] {
    if (retry) {
      nodeset_state_->resetGrayList(
          NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
      auto worker = Worker::onThisThread(false);
      if (worker) {
        worker->resetGraylist();
      }
      return select(extras,
                    copyset_out,
                    copyset_size_out,
                    chain_out,
                    selector_state,
                    rng,
                    false /* retry */);
    }
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Failed to select %d nodes for log %lu because too many nodes are "
        "unavailable. Nodeset: %s. Unavailable nodes: %s",
        (int)replication_,
        logid_.val_,
        toString(nodeset_indices_).c_str(),
        toString(cache.unavailable_nodes).c_str());
    return Result::FAILED;
  };

  bool pick_local_separately = false;
  if (locality_enabled_ && my_domain_idx_.hasValue() &&
      !cache.avoid_detaching_my_domain) {
    // If locality is enabled, we should maximize the probability that copyset
    // will have at least one copy from local domain, but we should still
    // strictly adhere to weights. Let's detach local domain and process it
    // separately.
    // Motivating example: suppose we have 3 racks of equal weight:
    // A (local), B and C; replication property is {rack: 2, node: 3}.
    // If we don't process local rack separately, with probability 1/3 the
    // copyset will contain only racks B and C, without local rack. In contrast,
    // if we detach rack A and always select one copy from it, all copysets will
    // contain A, and weights will still be followed precisely. The difference
    // between the two approaches is that the first one sometimes picks 2 copies
    // in rack A, which forces it to sometimes pick 0 copies in rack A (so that
    // it's 1 on average, as required by weights).
    bool wasnt_detached = hierarchy.detachDomain({my_domain_idx_.value()});
    ld_check(wasnt_detached);
    pick_local_separately = true;
  }

  while (true) {
    if (attempts >= MAX_BLACKLISTING_ITERATIONS) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Copyset selection for log %lu (R=%d) neither succeeded nor failed "
          "after %d attempts. Something's broken (or you have a really huge "
          "nodeset and most of it is unavailable). Nodeset: %s",
          logid_.val_,
          (int)replication_,
          MAX_BLACKLISTING_ITERATIONS,
          toString(nodeset_indices_).c_str());
      return ret = Result::FAILED;
    }
    ++attempts;

    // Select copyset.

    double outside_weight = hierarchy.getRoot().getWeights().totalWeight();
    if (secondary_replication_ > 1 &&
        outside_weight <= Sampling::EPSILON * total_weight) {
      return ret = retry_or_complain_on_too_many_unavailable();
    }

    if (!pick_local_separately) {
      // Simple case: all domains are treated the same way.
      size_t num_picked = selectCrossDomain(secondary_replication_,
                                            replication_,
                                            hierarchy.getRoot(),
                                            copyset_chain.data(),
                                            &biased,
                                            rng);
      if (num_picked != replication_) {
        return ret = retry_or_complain_on_too_many_unavailable();
      }
    } else {
      // Local domain is detached. Pick copies from it separately.
      AdjustedDomain my_domain =
          hierarchy.getRoot().getSubdomain(my_domain_idx_.value());
      double local_weight = my_domain.getWeights().totalWeight();
      copyset_size_t target_num_local_copies = randomRound(
          local_weight / (outside_weight + local_weight) * replication_, rng);
      copyset_size_t max_allowed = replication_ - secondary_replication_ + 1;
      if (target_num_local_copies > max_allowed) {
        if (!biased) {
          // Transient because we'll stop detaching local domain.
          if (print_bias_warnings_) {
            RATELIMIT_INFO(
                std::chrono::seconds(10),
                1,
                "Copyset for log %lu is biased because local %s is too heavy: "
                "%.3f. Other weights: %s. This should be transient.",
                logid_.val_,
                NodeLocation::scopeNames()[secondary_replication_scope_]
                    .c_str(),
                local_weight,
                hierarchy.getRoot().getWeights().toString().c_str());
          }
          biased = true;
        }
        target_num_local_copies = max_allowed;
      }

      size_t num_picked_locally;
      size_t num_picked_outside;

      auto pick = [&] {
        num_picked_locally = selectFlat(target_num_local_copies,
                                        my_domain,
                                        copyset_chain.data(),
                                        &biased,
                                        rng);

        num_picked_outside =
            selectCrossDomain(secondary_replication_ - (num_picked_locally > 0),
                              replication_ - num_picked_locally,
                              hierarchy.getRoot(),
                              copyset_chain.data() + num_picked_locally,
                              &biased,
                              rng);
      };

      pick();

      if (num_picked_outside == 0 && target_num_local_copies == 0) {
        // We tried to not pick any copies from local domain, but there are
        // not enough non-local domains to satisfy secondary_replication_.
        // Must use local domain.
        if (!biased) {
          // Treansient because we'll stop detaching local domain.
          if (print_bias_warnings_) {
            RATELIMIT_INFO(
                std::chrono::seconds(10),
                1,
                "Copyset for log %lu is biased because there are fewer than %d "
                "non-local %ss. Weights: %s. This should be transient.",
                logid_.val_,
                (int)secondary_replication_,
                NodeLocation::scopeNames()[secondary_replication_scope_]
                    .c_str(),
                hierarchy.getRoot().getWeights().toString().c_str());
          }
          biased = true;
        }
        target_num_local_copies = 1;
        pick();
      }

      if (num_picked_outside + num_picked_locally < replication_) {
        if ( // not enough domains
            num_picked_outside == 0 ||
            // already have all local nodes
            num_picked_locally < target_num_local_copies) {
          return ret = retry_or_complain_on_too_many_unavailable();
        }

        ld_check(biased);

        // Picked all the non-local nodes and still don't have enough nodes.
        // Try picking more from local domain.

        // Move the outside copies to the end of copyset.
        std::rotate(
            copyset_chain.begin(),
            copyset_chain.begin() + num_picked_locally + num_picked_outside,
            copyset_chain.end());
        num_picked_locally = selectFlat(replication_ - num_picked_outside,
                                        my_domain,
                                        copyset_chain.data(),
                                        &biased,
                                        rng);

        if (num_picked_outside + num_picked_locally < replication_) {
          // Picked all available nodes in the nodeset, still not enough.
          return ret = retry_or_complain_on_too_many_unavailable();
        }
      }

      if (biased) {
        // See comment for avoid_detaching_my_domain.
        cache.avoid_detaching_my_domain = true;
      }
    }

    // Check if all selected nodes are available. If not, blacklist and retry.
    if (checkAvailabilityAndBlacklist(copyset_chain.data(),
                                      replication_,
                                      hierarchy,
                                      cache,
                                      &biased,
                                      copyset_out,
                                      chain_out)) {
      *copyset_size_out = replication_;
      // TODO #8329263: support extras
      return ret = extras ? Result::PARTIAL : Result::SUCCESS;
    }
  }
}

// See comment in .h for explanation.
void WeightedCopySetSelector::splitExistingCopySet(
    StoreChainLink inout_copyset[],
    copyset_size_t existing_copyset_size,
    copyset_size_t* out_full_size,
    AdjustedHierarchy& hierarchy,
    size_t& num_useful_existing_copies,
    folly::small_vector<size_t, 4>& existing_domains, // except my domain
    bool& have_local_copies) const {
  // The transformtation can be done in place, but for simplicity let's use
  // a temporary array.
  copyset_chain_custsz_t<3> useful_existing_copies;
  size_t redundant_existing_copies = 0;
  // How many more domains we need to pick to satisfy secondary_replication_.
  size_t need_new_domains = secondary_replication_;

  // Updates hierarchy. Returns true if the node is useful, false if redundant.
  auto process_node = [&](ShardID node) {
    auto it = hierarchy_.node_paths.find(node);
    if (it == hierarchy_.node_paths.end()) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "%s in existing copyset does not belong to nodeset (%s) or is not a "
          "storage node. Log: %lu, current inout_copyset: [%s], "
          "useful_existing_copies so far: %s, existing_copyset_size: %d",
          node.toString().c_str(),
          toString(nodeset_indices_).c_str(),
          logid_.val_,
          rangeToString(inout_copyset,
                        inout_copyset +
                            std::max((size_t)existing_copyset_size,
                                     replication_ + redundant_existing_copies))
              .c_str(),
          toString(useful_existing_copies).c_str(),
          (int)existing_copyset_size);
      // Treat this bad copy as redundant.
      return false;
    }
    const Hierarchy::Path& path = it->second;

    // Disable nodes from existing copyset, along with their
    // replication_scope-level domains (a.k.a. leaf domains).
    // We're obviously not allowed to pick them.
    bool leaf_wasnt_detached = hierarchy.detachDomain(path, /* level */ 2);
    if (!leaf_wasnt_detached) {
      // Existing copyset has multiple copies from this leaf.
      // Consider all but the first one redundant.
      return false;
    }

    // Detach domain, if not yet detached.
    bool domain_wasnt_detached = hierarchy.detachDomain(path, /* level */ 1);

    if (domain_wasnt_detached) {
      // It's the first node from this domain in the copyset.
      if (need_new_domains > 0) {
        --need_new_domains;
        ld_check(replication_ - useful_existing_copies.size() >
                 need_new_domains);
      }
      size_t domain_idx = path[0];
      if (my_domain_idx_.hasValue() && domain_idx == my_domain_idx_.value()) {
        ld_check(!have_local_copies);
        have_local_copies = true;
      } else {
        ld_assert(std::find(existing_domains.begin(),
                            existing_domains.end(),
                            domain_idx) == existing_domains.end());
        existing_domains.push_back(domain_idx);
      }
    }

    return replication_ - useful_existing_copies.size() > need_new_domains;
  };

  for (size_t i = 0; i < existing_copyset_size; ++i) {
    ShardID node = inout_copyset[i].destination;
    bool useful = process_node(node);

    // Move redundant nodes to the beginning of inout_copyset.
    // Move useful nodes to a separate array. We could do it the other way
    // around, but then the temporary array would be bigger in the worst case.
    if (useful) {
      useful_existing_copies.push_back({node, ClientID::INVALID});
    } else {
      inout_copyset[redundant_existing_copies++].destination = node;
    }
  }

  ld_check(useful_existing_copies.size() <= replication_);
  ld_check(useful_existing_copies.size() + redundant_existing_copies ==
           existing_copyset_size);
  ld_check(replication_ - useful_existing_copies.size() >= need_new_domains);

  // Redundant to the end, useful to the beginning.
  std::copy_backward(inout_copyset,
                     inout_copyset + redundant_existing_copies,
                     inout_copyset + redundant_existing_copies + replication_);
  std::copy(useful_existing_copies.begin(),
            useful_existing_copies.end(),
            inout_copyset);

  if (out_full_size) {
    *out_full_size = replication_ + redundant_existing_copies;
  }
  num_useful_existing_copies = useful_existing_copies.size();
}

CopySetSelector::Result
WeightedCopySetSelector::augment(ShardID inout_copyset[],
                                 copyset_size_t existing_copyset_size,
                                 copyset_size_t* out_full_size,
                                 RNG& rng,
                                 bool retry) const {
  ld_check(inout_copyset != nullptr);
  copyset_chain_t new_copyset(COPYSET_SIZE_MAX);
  copyset_size_t new_out_full_size = 0;

  std::transform(inout_copyset,
                 inout_copyset + existing_copyset_size,
                 new_copyset.begin(),
                 [](const ShardID& idx) {
                   return StoreChainLink{idx, ClientID::INVALID};
                 });
  auto result = augment(new_copyset.data(),
                        existing_copyset_size,
                        &new_out_full_size,
                        /* fill_client_id = */ false,
                        /* chain_out = */ nullptr,
                        rng,
                        retry);

  if (result != CopySetSelector::Result::SUCCESS) {
    return result;
  }

  std::transform(new_copyset.data(),
                 new_copyset.data() + new_out_full_size,
                 inout_copyset,
                 [](const StoreChainLink& c) { return c.destination; });
  set_if_not_null(out_full_size, new_out_full_size);

  return CopySetSelector::Result::SUCCESS;
}

CopySetSelector::Result
WeightedCopySetSelector::augment(StoreChainLink inout_copyset[],
                                 copyset_size_t existing_copyset_size,
                                 copyset_size_t* out_full_size,
                                 bool fill_client_id,
                                 bool* chain_out,
                                 RNG& rng,
                                 bool retry) const {
  // make a copy of original input for retry
  copyset_chain_t inout_copyset_dup(
      inout_copyset, inout_copyset + existing_copyset_size);
  // retry if allowed
  auto retry_on_copyset_failure = [&] {
    if (!retry) {
      return Result::FAILED;
    }

    std::copy(
        inout_copyset_dup.begin(), inout_copyset_dup.end(), inout_copyset);
    nodeset_state_->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }

    return augment(inout_copyset,
                   existing_copyset_size,
                   out_full_size,
                   fill_client_id,
                   chain_out,
                   rng,
                   false /* retry */
    );
  };
  NodeAvailabilityCache& cache = prepareCachedNodeAvailability();
  // Need to make a copy because we'll be detaching nodes and domains based on
  // existing copyset, and we don't want to cache that.
  AdjustedHierarchy hierarchy = cache.adjusted_hierarchy.value();

  bool biased = false;
  folly::small_vector<size_t, 4> existing_domains; // except my domain
  bool have_local_copies = false;
  size_t useful_existing_copies;

  splitExistingCopySet(inout_copyset,
                       existing_copyset_size,
                       out_full_size,
                       hierarchy,
                       useful_existing_copies,
                       existing_domains,
                       have_local_copies);

  if (useful_existing_copies >= replication_) {
    // No new copies needed.
    return Result::SUCCESS;
  }

  size_t need_new_domains = secondary_replication_ -
      std::min((size_t)secondary_replication_,
               existing_domains.size() + have_local_copies);

  Result ret;
  size_t attempts = 0;
  SCOPE_EXIT {
    if (!retry) {
      // not updating stats twice
      return;
    }
    if (ret == Result::SUCCESS) {
      STAT_ADD(stats_, copyset_selection_attempts_rebuilding, attempts);
      STAT_INCR(stats_, copyset_selected_rebuilding);
      if (biased) {
        STAT_INCR(stats_, copyset_biased_rebuilding);
      }
    } else {
      STAT_INCR(stats_, copyset_selection_failed_rebuilding);
    }
  };

  while (true) {
    if (attempts >= MAX_BLACKLISTING_ITERATIONS) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Copyset selection for rebuilding for log %lu (R=%d) neither "
          "succeeded "
          "nor failed after %d attempts. Something's broken (or you have a "
          "really huge nodeset and most of it is unavailable). "
          "Useful existing copies: [%s]. Nodeset: %s",
          logid_.val_,
          (int)replication_,
          MAX_BLACKLISTING_ITERATIONS,
          rangeToString(inout_copyset, inout_copyset + useful_existing_copies)
              .c_str(),
          toString(nodeset_indices_).c_str());
      return ret = Result::FAILED;
    }
    ++attempts;

    size_t out_size = useful_existing_copies;

    // 1. Pick as many as allowed from my (local) domain.
    if (have_local_copies && replication_ - out_size > need_new_domains) {
      ld_check(my_domain_idx_.hasValue());
      out_size +=
          selectFlat(replication_ - out_size - need_new_domains,
                     hierarchy.getRoot().getSubdomain(my_domain_idx_.value()),
                     inout_copyset + out_size,
                     &biased,
                     rng);
    }

    // 2. Pick as many as possible from domains that don't appear
    //    in existing_copyset. This is rather arbitrary, could as well do
    //    step 3 before step 2.
    if (out_size < replication_) {
      size_t picked_from_new_domains =
          selectCrossDomain(need_new_domains,
                            replication_ - out_size,
                            hierarchy.getRoot(),
                            inout_copyset + out_size,
                            &biased,
                            rng);
      out_size += picked_from_new_domains;
      if (need_new_domains > 0 && picked_from_new_domains == 0) {
        RATELIMIT_LEVEL(
            retry ? dbg::Level::SPEW : dbg::Level::ERROR,
            std::chrono::seconds(10),
            2,
            "Failed to select %d nodes for rebuilding for log %lu because too "
            "many whole %ss are unavailable. Copyset: [%s]. Useful existing "
            "copies: %lu. Nodeset: %s. Unavailable nodes: %s. Secondary "
            "replication: %d. %s weights: %s",
            (int)replication_,
            logid_.val_,
            NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
            rangeToString(inout_copyset, inout_copyset + out_size).c_str(),
            useful_existing_copies,
            toString(nodeset_indices_).c_str(),
            toString(cache.unavailable_nodes).c_str(),
            (int)secondary_replication_,
            NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
            hierarchy.getRoot().getWeights().toString().c_str());
        return ret = retry_on_copyset_failure();
      }
    }

    // 3. Pick the remaining copies from existing_domains.
    if (out_size < replication_) {
      // This case is not expected to be reached often. That's why the shuffle
      // is here, not outside the while (true) loop.
      simple_shuffle(existing_domains.begin(), existing_domains.end(), rng);
      for (size_t domain_idx : existing_domains) {
        out_size += selectFlat(replication_ - out_size,
                               hierarchy.getRoot().getSubdomain(domain_idx),
                               inout_copyset + out_size,
                               &biased,
                               rng);
        if (out_size >= replication_) {
          break;
        }
      }
    }

    ld_check(out_size <= replication_);
    if (out_size < replication_) {
      RATELIMIT_LEVEL(
          retry ? dbg::Level::SPEW : dbg::Level::ERROR,
          std::chrono::seconds(10),
          2,
          "Failed to select %d nodes for rebuilding for log %lu because too "
          "many nodes are unavailable. Copyset: %s. Useful existing copies: "
          "%lu. "
          "Nodeset: %s",
          (int)replication_,
          logid_.val_,
          rangeToString(inout_copyset, inout_copyset + out_size).c_str(),
          useful_existing_copies,
          toString(nodeset_indices_).c_str());
      return ret = retry_on_copyset_failure();
    }

    if (folly::kIsDebug) {
      // Check that we didn't pick any duplicates.
      auto size = (out_full_size ? *out_full_size : replication_);
      std::vector<ShardID> e(size);
      std::transform(
          inout_copyset,
          inout_copyset + size,
          e.begin(),
          [](const StoreChainLink& link) { return link.destination; });
      std::sort(e.begin(), e.end());
      ld_assert(std::unique(e.begin(), e.end()) == e.end() &&
                "existing copyset cannot contain duplicate indices");
    }

    // Check if all selected nodes are available. If not, blacklist and retry.
    if (checkAvailabilityAndBlacklist(inout_copyset + useful_existing_copies,
                                      replication_ - useful_existing_copies,
                                      hierarchy,
                                      cache,
                                      &biased)) {
      // Leave the existing copies at the beginning of the copyset.
      // It's important for rebuilding.
      simple_shuffle(inout_copyset + useful_existing_copies,
                     inout_copyset + replication_,
                     rng);
      return ret = Result::SUCCESS;
    }
  }
}

size_t WeightedCopySetSelector::selectCrossDomain(size_t num_domains,
                                                  size_t replication,
                                                  AdjustedDomain domain,
                                                  StoreChainLink out[],
                                                  bool* out_biased,
                                                  RNG& rng) const {
  // Limitations of the current implementation
  // (assuming replication = 3, secondary_replication = 2,
  // secondary_replication_scope = rack)
  //  1. Unnecessarily biased when some rack has fewer than
  //     replication - 1 nodes.
  //  2. Unnecessarily biased when some rack weight is between 1/2 and 2/3 of
  //     total weight. (More generally, between 1/secondary_replication and
  //     1 - (secondary_replication - 1)/replication).
  // It's possible to remove both of these limitations (for 1. can just group
  // small racks together when constructing the hierarchy; for 2. the
  // explanation is a bit involved but implementation would be reasonably
  // simple; need to sort domains by weight, iteratively remove overweight
  // domains, do an AT_MOST_ONCE sampling to select among non-overweight
  // domains, then do an AT_LEAST_ONCE sampling to decide how many copies to
  // pick from each selected domain).

  ld_check(out_biased != nullptr);
  ld_check(replication >= num_domains);
  if (replication == 0) {
    return 0;
  }
  if (num_domains == 0) {
    num_domains = 1;
  }

  // Pick `num_domains` secondary domains (of level 1).
  folly::small_vector<size_t, 3> subdomains(num_domains);
  size_t overweight_idx;
  auto rv = Sampling::sampleMulti(domain.getWeights(),
                                  num_domains,
                                  Sampling::Mode::AT_MOST_ONCE,
                                  &subdomains[0],
                                  rng,
                                  nullptr,
                                  &overweight_idx);
  if (rv == Sampling::Result::IMPOSSIBLE) {
    // Cannot pick the requested number of domains.
    *out_biased = true;
    return 0;
  }
  if (rv == Sampling::Result::BIASED && !*out_biased) {
    if (print_bias_warnings_) {
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Copyset for log %lu is biased because %s containing %s is too "
          "heavy: "
          "%.3f when selecting %lu %ss from %s",
          logid_.val_,
          NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
          domain.getSubdomain(overweight_idx)
              .getSubdomain(0)
              .getShardID(0)
              .toString()
              .c_str(),
          domain.getWeights().weight(overweight_idx),
          num_domains,
          NodeLocation::scopeNames()[replication_scope_].c_str(),
          domain.getWeights().toString().c_str());
    }
    *out_biased = true;
  }
  simple_shuffle(subdomains.begin(), subdomains.end(), rng);

  // Select nodes in the picked domains. How to distribute the `replication`
  // copies across the `num_domains` domains? It doesn't matter since we
  // shuffled `subdomains`. For simplicity of implementation, we select as many
  // nodes as possible from the first subdomain, then as many of the remaining
  // from the second and so on.
  size_t out_size = 0;
  ShardID shard_in_overly_small_domain;
  for (size_t i = 0; i < subdomains.size(); ++i) {
    auto subdomain = domain.getSubdomain(subdomains[i]);
    size_t to_select = replication - out_size - (subdomains.size() - i - 1);
    ld_check(to_select > 0);
    size_t selected =
        selectFlat(to_select, subdomain, out + out_size, out_biased, rng);
    ld_check(selected > 0);
    if (selected < to_select) {
      shard_in_overly_small_domain = out[out_size].destination;
    }
    out_size += selected;
  }

  SCOPE_EXIT {
    // This reverse() is needed to balance leaders for rebuilding of a single
    // node. Example:
    // Consider replication property {rack: 2, node: 3}. If we don't reverse,
    // most copysets will look like this: [rack1, rack1, rack2]. Suppose we're
    // rebuilding one node, say Nr. There are 3 kinds of copysets with respect
    // to Nr: [Nr, X, Z], [X, Nr, Z], [Y, Z, Nr], where X is from the same rack
    // as Nr. All 3 are equally likely. In the first two cases, X will be the
    // rebuilding leader (as the first non-Nr node in copyset). So, 2/3 of the
    // rebuilding work will be done by one rack - the one in which Nr lives.
    // This is unacceptable.
    // The reverse() changes the picture to: [Nr, Y, Z], [Z, Nr, X], [Z, X, Nr],
    // so rebuilding leaders are evenly distributed across non-Nr racks;
    // Nr's rack doesn't do any rebuilding work. This is unfair as well, but
    // good enough for now.
    std::reverse(out, out + out_size);
  };

  if (out_size >= replication) {
    ld_check(out_size == replication);
    // Normal path - picked enough nodes.
    return out_size;
  }

  // Selected all nodes in picked domains, but it's less than replication
  // factor. Expand the search to other domains.
  ld_check(shard_in_overly_small_domain.isValid());
  if (!*out_biased) {
    if (print_bias_warnings_) {
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Copyset for log %lu is biased because %s containing %s contains "
          "fewer "
          "than replication - secondary_replication + 1 = %d %ss.",
          logid_.val_,
          NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
          shard_in_overly_small_domain.toString().c_str(),
          (int)(replication_ - secondary_replication_ + 1),
          NodeLocation::scopeNames()[replication_scope_].c_str());
    }
    *out_biased = true;
  }
  size_t all_subdomains = domain.getWeights().size();
  size_t offset = rng() % all_subdomains;
  for (size_t i = 0; i < all_subdomains && out_size < replication; ++i) {
    size_t subdomain_idx = (offset + i) % all_subdomains;
    if (std::find(subdomains.begin(), subdomains.end(), subdomain_idx) !=
        subdomains.end()) {
      // One of the initially picked domains. We already took all nodes from it.
      continue;
    }
    if (domain.getWeights().weight(subdomain_idx) <
        Sampling::EPSILON * hierarchy_.root.weights.totalWeight()) {
      // Nothing pickable here.
      continue;
    }
    auto subdomain = domain.getSubdomain(subdomain_idx);
    size_t to_select = replication - out_size;
    ld_check(to_select > 0);
    size_t selected =
        selectFlat(to_select, subdomain, out + out_size, out_biased, rng);
    out_size += selected;
  }

  ld_check(out_size <= replication);
  return out_size;
}

size_t WeightedCopySetSelector::selectFlat(size_t replication,
                                           AdjustedDomain domain,
                                           StoreChainLink out[],
                                           bool* out_biased,
                                           RNG& rng) const {
  if (replication == 0) {
    return 0;
  }

  folly::small_vector<size_t, 3> samples(replication);
  size_t size;
  size_t overweight_idx;

  auto rv = Sampling::sampleMulti(domain.getWeights(),
                                  replication,
                                  Sampling::Mode::AT_MOST_ONCE,
                                  &samples[0],
                                  rng,
                                  &size,
                                  &overweight_idx);

  if (rv != Sampling::Result::OK && !*out_biased) {
    ld_check(rv == Sampling::Result::BIASED ||
             rv == Sampling::Result::IMPOSSIBLE);
    if (print_bias_warnings_) {
      if (rv == Sampling::Result::BIASED) {
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            1,
            "Copyset for log %lu is biased because %s containing %s is too "
            "heavy: %.3f when selecting %lu shards from %s with weights %s",
            logid_.val_,
            NodeLocation::scopeNames()[replication_scope_].c_str(),
            domain.getSubdomain(overweight_idx)
                .getShardID(0)
                .toString()
                .c_str(),
            domain.getWeights().weight(overweight_idx),
            replication,
            NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
            domain.getWeights().toString().c_str());
      } else {
        ld_check(rv == Sampling::Result::IMPOSSIBLE);
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            1,
            "Copyset for log %lu is biased because %s containing %s is too "
            "small: got %lu shards when selecting %lu shards.",
            logid_.val_,
            NodeLocation::scopeNames()[secondary_replication_scope_].c_str(),
            domain.getSubdomain(0).getShardID(0).toString().c_str(),
            size,
            replication);
      }
    }
    *out_biased = true;
  }

  ld_check(!domain.isLeaf());

  for (size_t i = 0; i < size; ++i) {
    AdjustedDomain subdomain = domain.getSubdomain(samples[i]);
    ld_check(subdomain.isLeaf());
    size_t idx;
    rv = Sampling::sampleOne(subdomain.getWeights(), &idx, rng);
    // The subdomain must have at least one nonzero-weight node. Otherwise it
    // would have zero weight and wouldn't be selected by sampleMulti().
    ld_check(rv == Sampling::Result::OK);
    out[i].destination = subdomain.getShardID(idx);
  }

  // Shuffle the copyset to more evenly distribute
  // chain-sending and rebuilding work.
  simple_shuffle(out, out + size, rng);

  return size;
}

bool WeightedCopySetSelector::AdjustedHierarchy::setDomainDetached(
    const Hierarchy::Path& path,
    size_t level,
    bool detach) {
  if (level == std::numeric_limits<size_t>::max()) {
    level = path.size();
  }
  ld_check(level <= path.size());
  if (level == 0) {
    return false; // Not much point in detaching the root.
  }
  const double total_weight = base_->root.weights.totalWeight();

  // The part of the tree that we'll need to update is the path from our domain
  // to its lowest detached ancestor. If none of the ancestors are detached, to
  // the root. to_adjust is the list of edges on that path, ordered from bigger
  // to smaller domains.
  folly::small_vector<std::tuple<const Domain*, DomainAdjustment*, size_t>, 4>
      to_adjust;

  // If we're detaching our domain, all weights on that path need to be
  // decreased by the weight of our domain; if we're attaching - increased.
  // weight_delta is the signed value that needs to be added to each weight on
  // the path (so it's negative iff detach is true).
  double weight_delta = 1e200;

  // If some domain's weight (adjustment) became zero, we should remove
  // that domain from the tree (of adjustments) to keep the tree small and
  // efficient. More precisely, we maintain the invariant that a
  // DomainAdjustment exists in the tree iff it has at least one detached
  // descendant, or if it's itself detached. If we're re-attaching a domain with
  // no detached descendants, we should remove it from the tree, and potentially
  // remove some of its ancestors as well (a suffix of to_adjust).
  // In this case `removed` is set to true.
  bool removed = false;

  const Domain* domain = &base_->root;
  DomainAdjustment* adj = &root_diff_;
  // Go from root to our grandparent, creating missing domains along the way.
  for (size_t i = 0; i + 1 < level; ++i) {
    if (adj->is_detached) {
      // If our domain is inside a detached subtree, only update weights within
      // this subtree.
      to_adjust.clear();
    }

    size_t idx = path[i];
    to_adjust.emplace_back(domain, adj, idx);
    domain = &domain->subdomains[idx];
    adj = &adj->subdomains[idx];
  }

  // Look at our parent.

  size_t idx = path[level - 1];

  if (adj->is_detached) {
    to_adjust.clear();
  }

  if (domain->subdomains.empty()) {
    // We're disabling/enabling a node.

    bool is_detached = adj->weights.isWeightUpdated(idx);
    if (is_detached == detach) {
      return false;
    }

    if (detach) {
      // If we're disabling a node, subtract its current (adjusted)
      // weight from its weight and weight of all its ancestor domains.
      weight_delta =
          -AdjustedProbabilityDistribution(&domain->weights, &adj->weights)
               .weight(idx);
      to_adjust.emplace_back(domain, adj, idx);
    } else {
      // If we're enabling a node, revert its current weight adjustment
      // and subtract the same adjustment from weight of all its ancestors.
      weight_delta = -adj->weights.revert(idx);
      // If there are no detached nodes left in this domain, remove it from
      // the parent domain.
      removed = adj->weights.numUpdatedWeights() == 0;
    }
  } else {
    // We're detaching/attaching a non-leaf domain.

    if (folly::kIsDebug) { // assert some consistency of weights
      double w =
          AdjustedProbabilityDistribution(&domain->weights, &adj->weights)
              .weight(idx);
      if (adj->subdomains[idx].is_detached) {
        // Detached domain must have zero weight in parent.
        ld_check(fabs(w) < Sampling::EPSILON * total_weight);
      } else {
        // Non-detached domain must have weight equal to sum of weights of
        // its children.
        double w2 =
            AdjustedProbabilityDistribution(
                &domain->subdomains[idx].weights, &adj->subdomains[idx].weights)
                .totalWeight();
        ld_check(fabs(w - w2) < Sampling::EPSILON * total_weight);
      }
    }

    to_adjust.emplace_back(domain, adj, idx);

    // Go to the domain we're detaching/attaching.
    domain = &domain->subdomains[idx];
    adj = &adj->subdomains[idx];

    if (adj->is_detached == detach) {
      return false;
    }
    adj->is_detached = detach;

    weight_delta =
        AdjustedProbabilityDistribution(&domain->weights, &adj->weights)
            .totalWeight();
    if (detach) {
      // If we're detaching this subtree, subtract its weight from all
      // ancestors, if attaching - add.
      weight_delta = -weight_delta;
    }
    if (!detach && adj->subdomains.empty()) {
      // We're reattaching a domain that doesn't have any weight adjustments
      // in its whole subtree. Remove the domain from the tree of adjustments.
      removed = true;
    }
  }

  ld_check(weight_delta < 1e150); // must have been assigned

  // Update the weights of ancestor domains, from smaller to bigger.
  std::reverse(to_adjust.begin(), to_adjust.end());
  bool correcting_accumulated_error = false;
  for (const auto& tup : to_adjust) {
    std::tie(domain, adj, idx) = tup;
    if (removed) {
      double former_weight = adj->weights.revert(idx);
      ld_check(fabs(former_weight + weight_delta) <
               Sampling::EPSILON * total_weight);
      size_t erased = adj->subdomains.erase(idx);
      ld_check(erased);
      removed = adj->subdomains.empty();
    } else {
      adj->weights.addWeight(idx, weight_delta);
    }

    // If this domain's `weights` went through a lot of incremental adjustments,
    // refresh them for better floating point precision.
    if (!removed &&
        (correcting_accumulated_error ||
         ++adj->numerical_error_accumulated > 100)) {
      adj->correctAccumulatedNumericalError(domain);
      correcting_accumulated_error = true;
      adj->numerical_error_accumulated = 0;
    }
  }

  return true;
}

void WeightedCopySetSelector::DomainAdjustment::
    correctAccumulatedNumericalError(const Domain* domain) {
  if (domain->subdomains.empty()) {
    // Leaf.
    return;
  }
  ld_check_eq(weights.numUpdatedWeights(), subdomains.size());

  double former_total_weight = weights.totalAddedWeight();
  weights.clear();
  for (auto& p : subdomains) {
    double w;
    if (p.second.is_detached) {
      w = -domain->weights.weight(p.first);
    } else {
      w = p.second.weights.totalAddedWeight();
    }

    // This insert takes O(1) time because we're inserting in order of
    // increasing key.
    weights.addWeight(p.first, w);
  }
  double new_total_weight = weights.totalAddedWeight();
  ld_check(fabs(new_total_weight - former_total_weight) <=
           Sampling::EPSILON * domain->weights.totalWeight());
}

std::string WeightedCopySetSelector::Domain::toString() const {
  std::string res = "(weights: " + weights.toString();
  if (!node_ids.empty()) {
    res += ", nodes: " + logdevice::toString(node_ids);
  }
  if (!subdomains.empty()) {
    res += ", sub: " + logdevice::toString(subdomains);
  }
  res += ")";
  return res;
}

std::string WeightedCopySetSelector::Hierarchy::toString() const {
  return root.toString();
}

}} // namespace facebook::logdevice
