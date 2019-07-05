/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/CopySetSelectorDependencies.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/Sampling.h"
#include "logdevice/common/SmallMap.h"
#include "logdevice/common/configuration/ReplicationProperty.h"

namespace facebook { namespace logdevice {

class StatsHolder;

class WeightedCopySetSelector : public CopySetSelector {
 public:
  // @param print_bias_warnings
  //   If true, the copyset selector may print rate-limited log messages if
  //   it can't distribute the load evenly across shards (e.g. if replicating
  //   to 2 racks, and some rack is bigger than all other racks combined,
  //   we're forced to underutilize the nodes of the big rack).
  //   Set to false if load balancing is not important, e.g. for internal logs.
  //   Other, non-load-balancing-related, warnings are printed regardless of
  //   this setting.
  WeightedCopySetSelector(
      logid_t logid,
      const EpochMetaData& epoch_metadata,
      std::shared_ptr<NodeSetState> nodeset_state,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      folly::Optional<NodeID> my_node_id,
      const logsconfig::LogAttributes* log_attrs,
      bool locality_enabled,
      StatsHolder* stats = nullptr,
      RNG& init_rng = DefaultRNG::get(),
      bool print_bias_warnings = true,
      const CopySetSelectorDependencies* deps =
          CopySetSelectorDependencies::instance());

  std::string getName() const override;

  Result select(copyset_size_t extras,
                StoreChainLink copyset_out[],
                copyset_size_t* copyset_size_out,
                bool* chain_out = nullptr,
                State* selector_state = nullptr,
                RNG& rng = DefaultRNG::get(),
                bool retry = true) const override;

  Result augment(ShardID inout_copyset[],
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
    return replication_;
  }

 private:
  // These classes represent a tree of failure domains and nodes.
  // Nodes are only attached to leaf domains.
  // Currently the tree has exactly three levels: root,
  // secondary_replication_scope and replication_scope.

  // A vertex in the tree.
  struct Domain {
    // Exactly one of these two vectors is empty.
    // I.e. nodes are attached only to leaf domains, and every leaf domain
    // has at least one node attached.
    std::vector<ShardID> node_ids;
    std::vector<Domain> subdomains;

    ProbabilityDistribution weights;

    std::string toString() const;
  };

  // A sparse adjustment to a subtree. See comment near
  // ProbabilityDistributionAdjustment for the explanation of this
  // "XAdjustment + AdjustedX" pattern.
  // The kinds of adjustment supported:
  //  1. increase/decrease weight of a subdomain,
  //  2. "detach" a subtree from the rest of the tree (which really just
  //     changes some weights without changing the tree topology).
  struct DomainAdjustment {
    ProbabilityDistributionAdjustment weights;
    SmallRecursiveUnorderedMap<size_t, DomainAdjustment> subdomains;

    // Number of adjustments to `weights` since the last call to
    // correctAccumulatedNumericalError().
    int numerical_error_accumulated = 0;

    // true if this domain is a root of some detached subtree.
    // I.e. if you detach some domain X, only X gets is_detached=true, not
    // the whole subtree of X.
    bool is_detached = false;

    // When a domain is detached/attached (e.g. a node goes down/up), its
    // floating-point weight is subtracted/added to the corresponding
    // (floating-point) element of `weights` of the parent domain(s).
    // If this happens lots of times, numerical error can accumulate in the
    // value of `weights` of the parent domains.
    // This method, if called on a non-leaf domain, clears and reconstructs
    // `weights` based on subdomains' weights.
    // Called every once in a while (after a certain number of `weights`
    // updates, counted by numerical_error_accumulated).
    // @param domain  The Domain corresponding to this DomainAdjustment.
    void correctAccumulatedNumericalError(const Domain* domain);
  };

  class AdjustedDomain {
   public:
    AdjustedDomain(const Domain* base, const DomainAdjustment* diff)
        : base_(base), diff_(diff) {}

    AdjustedProbabilityDistribution getWeights() const {
      return AdjustedProbabilityDistribution(
          &base_->weights, diff_ ? &diff_->weights : nullptr);
    }
    AdjustedDomain getSubdomain(size_t i) const {
      ld_check(i < base_->subdomains.size());
      if (diff_) {
        auto it = diff_->subdomains.find(i);
        if (it != diff_->subdomains.end()) {
          return AdjustedDomain(&base_->subdomains[i], &it->second);
        }
      }
      return AdjustedDomain(&base_->subdomains[i], nullptr);
    }
    bool isLeaf() const {
      return !base_->node_ids.empty();
    }
    ShardID getShardID(int i) const {
      return base_->node_ids[i];
    }

   private:
    const Domain* base_;
    const DomainAdjustment* diff_;
  };

  struct Hierarchy {
    // Path from root to some domain or node.
    // Each element is an index in `subdomains` or `node_ids` vector of
    // corresponding Domain.
    using Path = folly::small_vector<size_t, 4>;

    Domain root;
    std::unordered_map<ShardID, Path, ShardID::Hash> node_paths;

    std::string toString() const;
  };
  // This breaks the "XAdjustment + AdjustedX" pattern: AdjustedHierarchy
  // plays the roles of both the "Adjustment" and "Adjusted" classes.
  // The only kind of adjustment is detaching/attaching a subtree.
  class AdjustedHierarchy {
   public:
    explicit AdjustedHierarchy(const Hierarchy* base) : base_(base) {}

    AdjustedDomain getRoot() const {
      return AdjustedDomain(&base_->root, &root_diff_);
    }

    // "Detaches" or "attaches" the given domain from/to the rest of the tree.
    // Imagine it as cutting the edge between this domain and its parent.
    //
    // This is implemented by setting the weight of this domain to zero in its
    // parent, as well as subtracting its weight from all ancestors. So
    // from ancestors' point of view this domain is dead (has zero weight).
    // However, the subtree of the domain remains intact; in particular, you
    // can use `selectFlat()` to pick nodes from that domain.
    //
    // It is allowed to detach a domain that's itself contained in the
    // subtree of another detached domain; e.g. you can detach the local rack
    // to process it separately, then blacklist some node in that rack by
    // detaching the node.
    //
    // Detaching is used for:
    //  * blacklisting unavailable nodes/domains by detaching them,
    //  * cutting out the local domain (my_domain_idx_) to pick its copies
    //    separately from the other domains,
    //  * when selecting copysets for rebuilding, cutting the domains that
    //    already have copies; these domains are processed separately.
    //
    // @param path identifies a domain. Only the first `level` elements
    // of `path` will be used; it's a microoptimization to avoid slicing
    // the vector: detachDomain(path, level) is the same as
    // detachDomain(path[0..level-1]). In effect, `level` is the distance from
    // root to the domain (level 0 is root, level 1 is secondary
    // replication scope, level 2 is replication scope, level 3 is node;
    // at least that's how this hierarchy is currently constructed).
    //
    // @return  false if no change was needed, i.e. the domain/node was
    //          already detached/attached.
    //          Subtlety: what will be returned if the domain/node wasn't
    //          detached/attached (setDomainDetached() wasn't called for it),
    //          but had zero weight? The answer is `false` if `path` leads to a
    //          node, and `true` if `path` leads to a Domain. (This contract is
    //          awkward, but it was easier to implement this way, and this
    //          behavior is just what the exising callers need).
    bool detachDomain(const Hierarchy::Path& path,
                      size_t level = std::numeric_limits<size_t>::max()) {
      return setDomainDetached(path, level, true);
    }
    bool attachDomain(const Hierarchy::Path& path,
                      size_t level = std::numeric_limits<size_t>::max()) {
      return setDomainDetached(path, level, false);
    }

    // Convenience aliases for detaching individual nodes.
    bool detachNode(ShardID node,
                    size_t level = std::numeric_limits<size_t>::max()) {
      return setDomainDetached(base_->node_paths.at(node), level, true);
    }
    bool attachNode(ShardID node,
                    size_t level = std::numeric_limits<size_t>::max()) {
      return setDomainDetached(base_->node_paths.at(node), level, false);
    }

   private:
    const Hierarchy* base_;
    DomainAdjustment root_diff_;

    // @param level   only use the first so many elements of @param path
    //                if numeric_limits<size_t>::max(), use all;
    // @param detach  if  true, detach the domain if it's attached;
    //                if false, attach the domain if it's detached.
    bool setDomainDetached(const Hierarchy::Path& path,
                           size_t level,
                           bool detach);
  };

  // Per-thread list of nodes known to be unavailable recently. Their
  // availability is re-checked before every copyset selection. This caching
  // is needed to avoid bias; see comment in select() for explanation.
  struct NodeAvailabilityCache {
    std::vector<ShardID> unavailable_nodes;
    // folly::Optional is used for deferred initialization.
    folly::Optional<AdjustedHierarchy> adjusted_hierarchy;

    // When locality is enabled, select() will usually detach the local domain
    // from the hierarchy and process it separately. But sometimes this leads
    // to bias. Example: rack weights {3, 2, 2, 2}, replicating to 3 racks,
    // my rack has weight 2. Suppose we detach the local rack, and:
    //  - With probability 2/3 pick 1 node from local rack and 2 nodes from the
    //    3 other racks.
    //  - With probability 1/3 pick all 3 nodes from the 3 other racks.
    //    This is impossible to do in an unbiased way since one of the 3 racks
    //    is heavier (weight 3) than 1/3 of the total weight of the 3 racks
    //    (3+2+2).
    // To avoid such situations, whenever we notice a bias we set
    // avoid_detaching_my_domain to true to inform the future calls to select()
    // that detaching the local domain is probably not a good idea.
    // This is reset back to false every time `unavailable_nodes` changes.
    bool avoid_detaching_my_domain = false;
  };

  const logid_t logid_;
  const CopySetSelectorDependencies* deps_;
  const std::shared_ptr<NodeSetState> nodeset_state_;
  const bool print_bias_warnings_;
  copyset_size_t replication_;
  copyset_size_t secondary_replication_;

  // Used only when building hierarchy, and for logging.
  NodeLocationScope replication_scope_;
  NodeLocationScope secondary_replication_scope_;

  // If true, sequencer's domain will get special treatment, with the goal of
  // reducing the amount of cross-domain traffic from sequencers.
  //  1. Weights will be adjusted on a per-log basis, so that:
  //      (a) The weight of each node, averaged over all logs, is unchanged.
  //          This ensures that the weight adjustment doesn't bias data
  //          distribution.
  //      (b) For each log, the weight of its sequencer domain is
  //          close to 1/replication_. This allows most copysets to contain at
  //          least one node from the local domain, saving a cross-domain hop
  //          when sending the copies.
  //  2. When selecting copyset, copies from the local domain (my_domain_idx_)
  //     will be picked separately from the rest of the copies, in a way that
  //     maximizes the probability that a copyset will contain at least one node
  //     from the local domain (without biasing the overall distribution).
  // Note that weights are adjusted based on primary sequencer domain, as
  // decided by sequencer placement. Our local domain may be different, e.g.
  // if the primary sequencer node is unavailable or if we're a rebuilding
  // donor.
  bool locality_enabled_;

  StatsHolder* stats_;

  StorageSet nodeset_indices_;

  Hierarchy hierarchy_;

  // Index of the local domain in `hierarchy_.root.subdomains`.
  // folly::none if my domain doesn't intersect the nodeset.
  folly::Optional<size_t> my_domain_idx_;

  mutable folly::ThreadLocal<NodeAvailabilityCache> node_availability_cache_;

  // If there are no weights in epoch metadata, this method is used to take
  // weights from config, transforming them to compensate for different-sized
  // domains.
  std::vector<double> calculateWeightsBasedOnConfig(
      const EpochMetaData& epoch_metadata,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const logsconfig::LogAttributes* log_attrs);

  // Part of calculateWeightsBasedOnConfig(). Shifts some weight between
  // domains to make sequencer's domain's effective weight close
  // to 1/replication_factor. This only really makes sense when weights are
  // taken from config. When weights are in EpochMetaData, this weight
  // adjustment should be nodeset selector's job. Note that this whole
  // procedure doesn't depend on the nodeset, so it would make more sense to do
  // it before selecting nodeset, especially if nodeset selection uses weights.
  // @param domain_target_weight
  //   In and out parameter. Before the call it must contain the total weights
  //   of domains in config. After the call it will contain the adjusted
  //   effective weights of domains.
  void optimizeWeightsForLocality(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const logsconfig::LogAttributes* log_attrs,
      std::unordered_map<std::string, double>& domain_target_weight);

  // Returns the thread-local instance of NodeAvailabilityCache, after
  // initializing it if needed and re-checking the cached blacklist of nodes.
  NodeAvailabilityCache& prepareCachedNodeAvailability() const;

  bool checkAvailabilityAndBlacklist(const StoreChainLink copyset[],
                                     size_t copyset_size,
                                     AdjustedHierarchy& hierarchy,
                                     NodeAvailabilityCache& cache,
                                     bool* out_biased,
                                     StoreChainLink* out_chain_links = nullptr,
                                     bool* out_chain = nullptr) const;

  // Selects `replication` nodes from the `domain`, according to weights.
  // `domain`'s immediate children must be leaves. If it fails to draw
  // exactly according to weights, sets *out_biased = true.
  // If there are fewer than `replication` nonzero-weight leaves, selects all
  // of them.
  // @return  Number of selected nodes.
  size_t selectFlat(size_t replication,
                    AdjustedDomain domain,
                    StoreChainLink out[],
                    bool* out_biased,
                    RNG& rng) const;
  // Selects `replication` nodes from at least `num_domains` different
  // subdomains of `domain`, according to weights.
  // `domain`'s children's children must be leaves. If it fails to draw
  // exactly according to weights, sets *out_biased = true.
  // If there are fewer than `num_domains` nonzero-weight subdomains, returns 0.
  // If are enough subdomains but fewer than `replication` nodes, selects
  // all nodes.
  // @return  Number of selected nodes.
  size_t selectCrossDomain(size_t num_domains,
                           size_t replication,
                           AdjustedDomain domain,
                           StoreChainLink out[],
                           bool* out_biased,
                           RNG& rng) const;

  // This is the first step of augment(). It separates the nodes of existing
  // copyset into "useful" and "redundant" (see below). Also fills out some
  // data structures that augment() will be using.
  //
  // The output copyset of augment() is a concatenation of three parts,
  // in this order:
  //  * useful existing copies,
  //  * new copies,
  //  * redundant existing copies.
  // The number of useful existing + new copies is equal to replication_.
  //
  // E.g. if inout_nodeset is [A0, A1, B0, B1] (where the first letter
  // identifies the a domain of scope secondary_replication_scope),
  // secondary_replication_ = replication_ = 3, then the output copyset will be
  // [A0, B0, X, A1, B1]. [A0, B0] are useful existing copies, [A1, B1] are
  // redundant existing copies, and X is a new copy that we'll pick.
  //
  // Obviously there's some freedom in deciding which copies to consider useful,
  // as well as permuting them; e.g. we could as well return
  // [B1, A1, X, A0, B0]. But this method doesn't exercise this freedom and
  // always picks the leftmost possible nodes as useful, and preserves order;
  // so in this example it'll return [A0, B0, X, A1, B1] and not some other
  // permutation.
  //
  // This method:
  //  * moves useful existing copies to the beginning of the copyset and sets
  //    useful_existing_copies to their count,
  //  * moves redundant existing copies to the end of the copyset and
  //    sets *out_full_size accordingly,
  //  * detaches the existing nodes and domains in hierarchy; assigns
  //    existing_domains and have_local_copies accordingly.
  // The rest of augment() needs to fill
  // inout_copyset[useful_existing_copies ... replication_ - 1].
  void splitExistingCopySet(
      StoreChainLink inout_copyset[],
      copyset_size_t existing_copyset_size,
      copyset_size_t* out_full_size,
      AdjustedHierarchy& hierarchy,
      size_t& useful_existing_copies,
      folly::small_vector<size_t, 4>& existing_domains, // except my domain
      bool& have_local_copies) const;
};

}} // namespace facebook::logdevice
