/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CrossDomainCopySetSelector.h"

#include <unordered_set>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/LinearCopySetSelector.h"
#include "logdevice/common/RandomLinearIteratorBase.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using NodeStatus = NodeAvailabilityChecker::NodeStatus;
using Domain = NodeLocationHierarchy::Domain;

// base Iterator class for iterating over items in a vector
template <typename Item>
class CrossDomainCopySetSelector::Iterator
    : public RandomLinearIteratorBase<Item, std::vector> {
  using Parent = RandomLinearIteratorBase<Item, std::vector>;

 public:
  explicit Iterator(const CrossDomainCopySetSelector* selector, RNG& rng)
      : Parent(rng) {}
};

CrossDomainCopySetSelector::CrossDomainCopySetSelector(
    logid_t logid,
    StorageSet storage_set,
    std::shared_ptr<NodeSetState> nodeset_state,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    NodeID my_node_id,
    copyset_size_t replication_factor,
    NodeLocationScope sync_replication_scope,
    const CopySetSelectorDependencies* deps)
    : deps_(deps),
      logid_(logid),
      nodeset_state_(nodeset_state),
      replication_factor_(replication_factor),
      sync_replication_scope_(sync_replication_scope),
      hierarchy_(nodes_configuration, storage_set),
      domains_in_scope_(hierarchy_.domainsInScope(sync_replication_scope_)) {
  ld_check(logid != LOGID_INVALID);
  ld_check(nodeset_state_ != nullptr);
  ld_check(sync_replication_scope_ != NodeLocationScope::ROOT &&
           sync_replication_scope_ != NodeLocationScope::NODE);
  ld_check(replication_factor_ <= COPYSET_SIZE_MAX);

  // get the node location information for the local sequencer node (myself)
  local_domains_.fill(nullptr);
  const node_index_t my_index = my_node_id.index();

  const auto* my_sd = nodes_configuration->getNodeServiceDiscovery(my_index);
  if (my_sd == nullptr) {
    ld_error("This sequencer node (index %hd) for log %lu is no "
             "longer in config!",
             my_index,
             logid_.val_);
    return;
  }

  if (!my_sd->location.hasValue()) {
    ld_error("This sequencer node %s for log %lu does not have location "
             "specified in config! Cannot exploit location locality to save "
             "cross-domain bandwith.",
             my_sd->address.toString().c_str(),
             logid_.val_);
    return;
  }

  const Domain* domain = hierarchy_.searchSubdomain(my_sd->location.value());
  // _domain_ is the smallest location domain in the hierarchy that contains
  // the sequencer node, it must not be nullptr since the ROOT domain exists
  ld_check(domain != nullptr);

  // populate all local subdomains
  while (domain != nullptr) {
    ld_check(domain->scope_ <= NodeLocationScope::ROOT);
    ld_check(domain->scope_ >= NodeLocationScope::NODE);

    local_domains_[static_cast<size_t>(domain->scope_)] = domain;
    domain = domain->parent_;
  }

  ld_check(local_domains_[static_cast<size_t>(NodeLocationScope::NODE)] ==
           nullptr);
  ld_check(local_domains_[static_cast<size_t>(NodeLocationScope::ROOT)] ==
           hierarchy_.getRoot());
}

std::string CrossDomainCopySetSelector::getName() const {
  return "CrossDomainCopySetSelector";
}

CopySetSelector::Result
CrossDomainCopySetSelector::select(copyset_size_t extras,
                                   StoreChainLink copyset_out[],
                                   copyset_size_t* copyset_size_out,
                                   bool* chain_out,
                                   CopySetSelector::State* selector_state,
                                   RNG& rng,
                                   bool retry) const {
  ld_check(copyset_out != nullptr);
  ld_check(copyset_size_out != nullptr);
  ld_check(extras <= COPYSET_SIZE_MAX - replication_factor_);

  // IMPORTANT Note: for the current implementation we do not consider
  // extras and expect logs using this selection scheme to have extras
  // set to 0. If extras is not 0, the selector will still try to find
  // _replication_ number of storage nodes to store copies of record,
  // and return CopySetSelector::Result::PARTIAL (instead of SUCCESS)
  // if _replication_ nodes are found.
  // TODO #8329263: support extras in Appender
  const copyset_size_t ncopies = replication_factor_;

  const NodeLocationHierarchy::Domain* primary_domain;
  copyset_size_t primary_nodes;

  // use the caller provided chain settings if possible, otherwise assume
  // chaining is disabled.
  bool chain = chain_out ? *chain_out : false;
  copyset_size_t nodes_selected = selectImpl(
      ncopies, copyset_out, &primary_domain, &primary_nodes, &chain, rng);

  ld_check(nodes_selected <= ncopies);
  if (nodes_selected < replication_factor_) {
    nodeset_state_->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }

    if (retry) {
      return select(extras,
                    copyset_out,
                    copyset_size_out,
                    chain_out,
                    selector_state,
                    rng,
                    false /* retry */);
    }
    return CopySetSelector::Result::FAILED;
  }

  if (chain_out) {
    *chain_out = chain;
  }

  *copyset_size_out = nodes_selected;
  return nodes_selected < replication_factor_ + extras
      ? CopySetSelector::Result::PARTIAL
      : CopySetSelector::Result::SUCCESS;
}

copyset_size_t CrossDomainCopySetSelector::nodesInPrimaryDomain(
    NodeLocationScope sync_replication_scope,
    copyset_size_t copies,
    bool chain_enabled) {
  if (copies < 2) {
    return copies;
  }

  if (sync_replication_scope == NodeLocationScope::REGION) {
    // for cross-region replication, place r-1 copies in the primary
    // region, and 1 copy in the remote region
    return copies - 1;
  }

  if (chain_enabled) {
    // if chaining is enabled, place one copy on the primary domain and
    // the rest on other racks
    return 1;
  }

  // otherwise, always try to place 2 copies on the primary domain
  return copies <= 2 ? 1 : 2;
}

std::string CrossDomainCopySetSelector::printChainLink(StoreChainLink* copyset,
                                                       size_t size) const {
  std::string copyset_str;
  for (int i = 0; i < size; ++i) {
    copyset_str += copyset[i].destination.toString();
    if (i < size - 1) {
      copyset_str += ", ";
    }
  }

  return copyset_str;
}

// The selection procedure begins with finding a primary domain in all domains
// in sync_replication_scope_ to try to place _n0_ copies (_n0_ is returned by
// NodesInPrimaryDomain()). The local domain, if exists, is always first
// attempted as the primary domain. If there are 0 available nodes in the
// attempted domain, other domains will be attempted until a primary domain is
// selected. Once a primary domain with non-zero available nodes is found, the
// selector selects the remaining nodes from other domains, and finally
// revists the primary domain if needed. For replication factor >= 2, the
// selection requires that at least 1 copy must be from domains other than the
// primary domain.
copyset_size_t
CrossDomainCopySetSelector::selectImpl(copyset_size_t ncopies,
                                       StoreChainLink copyset_out[],
                                       const Domain** primary_domain_out,
                                       copyset_size_t* primary_nodes_out,
                                       bool* chain_out,
                                       RNG& rng) const {
  ld_check(copyset_out != nullptr);
  ld_check(ncopies <= COPYSET_SIZE_MAX);

  // To meet the failure domain requirements, the copyset must span across
  // at least two location domains in the sync_replication_scope_
  if (ncopies >= 2 && domains_in_scope_.size() < 2) {
    // this is an permenant error, spam the log so that it can be easily
    // detected
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "ERROR: there is only %lu location domains in failure "
                    "domain scope: %s for log %lu. Copyset cannot be selected.",
                    domains_in_scope_.size(),
                    NodeLocation::scopeNames()[sync_replication_scope_].c_str(),
                    logid_.val_);
    return 0;
  }

  if (ncopies > hierarchy_.numClusterNodes()) {
    // not enough nodes with location info specified, another permanent error
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "ERROR: there are only %hu eligible nodes in the nodeset "
                    "but need to store %hhu copies for log %lu. Copyset cannot "
                    "be selected.",
                    hierarchy_.numClusterNodes(),
                    ncopies,
                    logid_.val_);
    return 0;
  }

  copyset_size_t nodes_selected = 0;
  size_t domains_selected = 0;
  // iterator binds to all domains in sync_replication_scope_
  DomainIterator domain_it(this, rng);
  domain_it.setContainerAndReset(&domains_in_scope_);

  const Domain* primary_domain =
      local_domains_[static_cast<size_t>(sync_replication_scope_)];
  // num of nodes in the primary domain
  copyset_size_t primary_nodes = 0;
  bool use_local_domain = primary_domain != nullptr;

  // Step 1: find the primary domain
  while (primary_nodes == 0) {
    if (!use_local_domain) {
      if (domain_it.next(&primary_domain) != 0) {
        // searched all domains
        break;
      }
    } else {
      // Using the local domain - seeking the iterator to it
      bool seek_res = domain_it.seekToValue(primary_domain);
      ld_check(seek_res);
      use_local_domain = false;
    }
    ld_check(domain_it.valid());

    ld_check(primary_domain != nullptr);

    const copyset_size_t primary_nodes_expected = nodesInPrimaryDomain(
        sync_replication_scope_, ncopies, chain_out ? *chain_out : false);
    ld_check(primary_nodes_expected < ncopies || ncopies < 2);

    primary_nodes = selectNodesInDomain(primary_domain,
                                        primary_nodes_expected,
                                        copyset_out,
                                        chain_out,
                                        rng,
                                        nullptr);

    domain_it.blacklistCurrentAndReset();
  }

  if (primary_nodes == 0) {
    // cannot find a primary domain to store a copy
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Cannot select a copyset for log %lu: unable to find "
                      "a primary domain to select nodes.",
                      logid_.val_);
    return 0;
  }

  ld_check(primary_domain != nullptr);
  ++domains_selected;
  nodes_selected += primary_nodes;

  // Step 2: we found at least 1 node in the primary domain, now find the
  //         ncopies - primary_nodes from the rest of domains
  while (nodes_selected < ncopies) {
    const Domain* domain;
    if (domain_it.next(&domain) != 0) {
      break;
    }

    ld_check(domain != nullptr);
    ld_check(domain != primary_domain);

    auto selected = selectNodesInDomain(domain,
                                        ncopies - nodes_selected,
                                        copyset_out + nodes_selected,
                                        chain_out,
                                        rng,
                                        nullptr);
    nodes_selected += selected;
    if (nodes_selected < ncopies) {
      domain_it.blacklistCurrentAndReset();
    }

    if (selected > 0) {
      ++domains_selected;
    }
  }

  if (nodes_selected < ncopies) {
    if (nodes_selected == primary_nodes) {
      // found 0 node in other domains in step 2, cannot meet the failure
      // domain requirements
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        2,
                        "Cannot select a copyset for log:%lu, unable to find "
                        "any domain other than the primary domain to store at "
                        "least one copy of record. nodes_selected=%d"
                        ", ncopies=%d, copyset_so_far[%s]",
                        logid_.val_,
                        nodes_selected,
                        ncopies,
                        printChainLink(copyset_out, nodes_selected).c_str());
      return 0;
    }

    // Step 3: if there are still not enough nodes, and we have selected from
    //         at least one domain aside from the primary domain, try selecting
    //         from the primary domain again. This time a predicate function is
    //         used to exclude nodes already selected from the primay domain
    ld_check(domains_selected >= 2);
    auto selected = selectNodesInDomain(
        primary_domain,
        ncopies - nodes_selected,
        copyset_out + nodes_selected,
        chain_out,
        rng,
        [copyset_out, primary_nodes](ShardID index) {
          return std::find_if(copyset_out,
                              copyset_out + primary_nodes,
                              [index](const StoreChainLink& c) {
                                return c.destination == index;
                              }) == copyset_out + primary_nodes;
        });

    if (selected > 0) {
      // move the newly selected primary nodes to be next to existing primary
      // nodes to form a partition
      std::rotate(copyset_out + primary_nodes,
                  copyset_out + nodes_selected,
                  copyset_out + nodes_selected + selected);
    }

    nodes_selected += selected;
    primary_nodes += selected;
  }

  ld_spew("Selected %hhu nodes (%d requested) for log %lu, nodes in primary "
          "domain: %hhu, nodes in other domains %d, total domains "
          "selected: %lu, ",
          nodes_selected,
          ncopies,
          logid_.val_,
          primary_nodes,
          nodes_selected - primary_nodes,
          domains_selected);

  if (primary_domain_out) {
    *primary_domain_out = primary_domain;
  }
  if (primary_nodes_out) {
    *primary_nodes_out = primary_nodes;
  }

  if (folly::kIsDebug) {
    ld_check(primary_domain->scope_ == sync_replication_scope_);

    // predicate function used to partition the copyset
    auto partition_fn = [this, primary_domain](StoreChainLink c) {
      return getDomainForNode(c.destination) == primary_domain;
    };

    // check the nodes are partitioned in the copyset with primary_nodes
    // at the beginning
    ld_assert(std::is_partitioned(
        copyset_out, copyset_out + nodes_selected, partition_fn));
    ld_assert(std::partition_point(
                  copyset_out, copyset_out + nodes_selected, partition_fn) ==
              copyset_out + primary_nodes);
  }
  return nodes_selected;
}

bool CrossDomainCopySetSelector::isLocalDomain(const Domain* domain) const {
  ld_check(domain != nullptr);
  return local_domains_[static_cast<size_t>(domain->scope_)] == domain;
}

// The selection uses a depth-first-search (with a twist) on the hierarchy tree
// so that nodes selected are close to each other to minimize cross-domain
// traffic. The twist is on the first domain to search among all children
// domains of a domain: if applicable, it always starts with the subdomain
// that contains the sequencer node, otherwise, the search starts with a
// _random_ subdomain.
copyset_size_t
CrossDomainCopySetSelector::selectNodesInDomain(const Domain* domain,
                                                copyset_size_t num_copies,
                                                StoreChainLink* copyset_out,
                                                bool* chain_out,
                                                RNG& rng,
                                                pred_func_t pred) const {
  ld_check(domain != nullptr);

  // number of nodes selected so far
  copyset_size_t nodes_selected = 0;

  // Step 1:
  // if the domain is a _local_ domain (domain that contains the sequencer node)
  // and it also has a _local_ subdomain, first select nodes from the local
  // subdomain to take advantage of locality
  const auto& subdomains = domain->subdomain_list_;
  const Domain* local_subdomain = nullptr;

  if (isLocalDomain(domain) && !subdomains.empty()) {
    ld_check(domain->scope_ != NodeLocationScope::NODE);
    local_subdomain = local_domains_[static_cast<size_t>(
        NodeLocation::nextSmallerScope(domain->scope_))];

    if (local_subdomain != nullptr) {
      ld_assert(std::find(subdomains.begin(),
                          subdomains.end(),
                          local_subdomain) != subdomains.end());

      // recursively select nodes from the local subdomain
      nodes_selected += selectNodesInDomain(local_subdomain,
                                            num_copies - nodes_selected,
                                            copyset_out + nodes_selected,
                                            chain_out,
                                            rng,
                                            pred);
    }
  }

  // Step 2:
  // if there are still nodes need to be selected, select them from subdomains,
  // starting from a random subdomain
  if (nodes_selected < num_copies && !subdomains.empty()) {
    DomainIterator domain_it(this, rng);
    domain_it.setContainer(&subdomains);
    if (local_subdomain) {
      domain_it.seekToValue(local_subdomain);
      domain_it.blacklistCurrentAndReset();
    }

    while (nodes_selected < num_copies) {
      const Domain* dom;
      if (domain_it.next(&dom) != 0) {
        break;
      }

      ld_check(dom != nullptr);
      ld_check(dom != local_subdomain);

      // recursively select nodes from the subdomain
      nodes_selected += selectNodesInDomain(dom,
                                            num_copies - nodes_selected,
                                            copyset_out + nodes_selected,
                                            chain_out,
                                            rng,
                                            pred);
    }
  }

  // Step 3:
  // we have searched all subdomains but there are still more nodes to
  // be selected, select them from cluster nodes that are _directly_
  // attached to the domain
  if (nodes_selected < num_copies && !domain->direct_nodes_.empty()) {
    NodeIterator node_it(this, rng);
    node_it.setContainerAndReset(&domain->direct_nodes_);
    nodes_selected += selectNodes(&node_it,
                                  num_copies - nodes_selected,
                                  copyset_out + nodes_selected,
                                  chain_out,
                                  pred);
  }

  ld_check(nodes_selected <= num_copies);
  return nodes_selected;
}

copyset_size_t
CrossDomainCopySetSelector::selectNodes(NodeIterator* start,
                                        copyset_size_t num_copies,
                                        StoreChainLink* copyset_out,
                                        bool* chain_out,
                                        pred_func_t pred) const {
  ld_check(num_copies > 0);
  ld_check(start != nullptr);
  ld_check(copyset_out != nullptr);

  copyset_size_t picked_so_far = 0;
  size_t first_node_retries = 0;
  while (picked_so_far < num_copies) {
    ShardID dest_idx;
    if (start->next(&dest_idx) != 0) {
      break;
    }

    if (pred != nullptr && !pred(dest_idx)) {
      // filtered out by the pred function
      continue;
    }

    StoreChainLink destination;
    auto node_status = deps_->getNodeAvailability()->checkNode(
        nodeset_state_.get(), dest_idx, &destination);
    switch (node_status) {
      case NodeStatus::AVAILABLE_NOCHAIN:
        if (chain_out) {
          *chain_out = false;
        }
        break;
      case NodeStatus::AVAILABLE:
        break;
      case NodeStatus::NOT_AVAILABLE:
        // See LinearCopySetSelector.cpp for explanation
        if (picked_so_far == 0 &&
            (first_node_retries++ <
             LinearCopySetSelector::MAX_FIRST_NODE_RETRIES)) {
          start->reset();
        }
        continue;
    }

    *copyset_out = destination;
    ++picked_so_far, ++copyset_out;
  }

  return picked_so_far;
}

const Domain*
CrossDomainCopySetSelector::getDomainForNode(ShardID index) const {
  const Domain* p_domain = hierarchy_.findDomainForShard(index);
  while (p_domain != nullptr && p_domain->scope_ < sync_replication_scope_) {
    p_domain = p_domain->parent_;
  }
  return p_domain;
}

CopySetSelector::Result
CrossDomainCopySetSelector::augment(ShardID inout_copyset[],
                                    copyset_size_t existing_copyset_size,
                                    copyset_size_t* out_full_size,
                                    RNG& rng,
                                    bool retry) const {
  ld_check(inout_copyset != nullptr);
  ld_check(replication_factor_ >= 1);

  copyset_size_t full_size;
  StoreChainLink inout_copyset_chain[COPYSET_SIZE_MAX];
  std::transform(inout_copyset,
                 inout_copyset + existing_copyset_size,
                 inout_copyset_chain,
                 [](const ShardID& destination) {
                   return StoreChainLink{destination, ClientID::INVALID};
                 });
  auto result = augment(inout_copyset_chain,
                        existing_copyset_size,
                        &full_size,
                        /* fill_client_id */ false,
                        nullptr,
                        rng,
                        retry);

  if (result != CopySetSelector::Result::SUCCESS) {
    return result;
  }

  std::transform(inout_copyset_chain,
                 inout_copyset_chain + full_size,
                 inout_copyset,
                 [](const StoreChainLink& c) { return c.destination; });

  if (out_full_size) {
    *out_full_size = full_size;
  }
  return CopySetSelector::Result::SUCCESS;
}

CopySetSelector::Result
CrossDomainCopySetSelector::augment(StoreChainLink inout_copyset[],
                                    copyset_size_t existing_copyset_size,
                                    copyset_size_t* out_full_size,
                                    bool fill_client_id,
                                    bool* chain_out,
                                    RNG& rng,
                                    bool retry) const {
  ld_check(inout_copyset != nullptr);
  ld_check(replication_factor_ >= 1);

  copyset_size_t full_size;
  StoreChainLink new_copyset_chain[COPYSET_SIZE_MAX];
  const Domain* primary_domain = nullptr;
  copyset_size_t primary_nodes_new = 0;
  copyset_size_t copyset_size_new = 0;

  if (folly::kIsDebug) {
    std::vector<ShardID> e(existing_copyset_size);
    std::transform(inout_copyset,
                   inout_copyset + existing_copyset_size,
                   e.begin(),
                   [](const StoreChainLink& link) { return link.destination; });
    std::sort(e.begin(), e.end());
    ld_assert(std::unique(e.begin(), e.end()) == e.end() &&
              "existing copyset cannot contain duplicate indices");
  }

  if (existing_copyset_size < replication_factor_) {
    // We'll pick a normal-sized copyset.
    full_size = replication_factor_;
  } else {
    // Existing copyset is already big enough.
    // If existing copies span at least two domains, we don't need to pick any
    // new copies; just reorder the existing ones so that there are at least
    // two domains among the first replciation_factor_ copies.
    // If all existing copies are in the same domain, leave the first
    // replication_factor_ - 1 of them and one new copy from a different domain.

    bool cross_domain = false;
    const Domain* first_domain = getDomainForNode(inout_copyset[0].destination);
    for (copyset_size_t i = 1; i < existing_copyset_size; ++i) {
      if (getDomainForNode(inout_copyset[i].destination) != first_domain) {
        cross_domain = true;
        if (i >= replication_factor_) {
          std::swap(inout_copyset[i], inout_copyset[replication_factor_ - 1]);
        }
        break;
      }
    }

    full_size = existing_copyset_size + (cross_domain ? 0 : 1);
    if (cross_domain) {
      if (out_full_size) {
        *out_full_size = full_size;
      }
      return Result::SUCCESS;
    } else {
      // Make room for a new node that we're going to pick.
      inout_copyset[existing_copyset_size] =
          inout_copyset[replication_factor_ - 1];
      existing_copyset_size = replication_factor_ - 1;
    }
  }

  ld_check_lt(existing_copyset_size, replication_factor_);

  // step 1: generate a new copyset
  copyset_size_new = selectImpl(replication_factor_,
                                new_copyset_chain,
                                &primary_domain,
                                &primary_nodes_new,
                                chain_out,
                                rng);

  if (copyset_size_new < replication_factor_) {
    nodeset_state_->resetGrayList(
        NodeSetState::GrayListResetReason::CANT_PICK_COPYSET);
    auto worker = Worker::onThisThread(false);
    if (worker) {
      worker->resetGraylist();
    }
    if (retry) {
      return augment(inout_copyset,
                     existing_copyset_size,
                     out_full_size,
                     fill_client_id,
                     chain_out,
                     rng,
                     false /* retry */);
    }

    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "ERROR: unable to pick a new copyset for rebuilding "
                    "an existing copyset for log %lu: can only select %hhu "
                    "nodes, replication: %hhu.",
                    logid_.val_,
                    copyset_size_new,
                    replication_factor_);
    return CopySetSelector::Result::FAILED;
  }

  ld_check_eq(copyset_size_new, replication_factor_);
  ld_check_ne(primary_domain, nullptr);
  ld_check(primary_domain->scope_ == sync_replication_scope_);
  ld_check_gt(primary_nodes_new, 0);

  // step 2: count the number of primary nodes in the existing copyset
  const size_t primary_nodes_existing = std::count_if(
      inout_copyset,
      inout_copyset + existing_copyset_size,
      [this, primary_domain](const StoreChainLink& idx) {
        return getDomainForNode(idx.destination) == primary_domain;
      });

  ld_check_le(primary_nodes_existing, existing_copyset_size);

  // step 3: remove nodes appeared in both copysets by replacing them with an
  // invalid index
  ssize_t primary_remaining = primary_nodes_existing;
  ssize_t other_remaining = existing_copyset_size - primary_nodes_existing;

  auto remove_pred = [=, &primary_remaining, &other_remaining](
                         const StoreChainLink& idx) {
    // Note that we only compare ShardIDs and not ClientIDs here.
    if (std::find_if(inout_copyset,
                     inout_copyset + existing_copyset_size,
                     [&idx](const StoreChainLink& c) {
                       return c.destination == idx.destination;
                     }) != inout_copyset + existing_copyset_size) {
      getDomainForNode(idx.destination) == primary_domain ? --primary_remaining
                                                          : --other_remaining;
      return true;
    }
    return false;
  };

  auto new_copyset_end = std::remove_if(
      new_copyset_chain, new_copyset_chain + copyset_size_new, remove_pred);

  ld_check_ge(primary_remaining, 0);
  ld_check_ge(other_remaining, 0);
  ld_check_ge(new_copyset_end, new_copyset_chain);

  // step 4: perform substitution by removing nodes from the new copyset if a
  // similar node can be found in the existing copyset. Nodes in the primary
  // domain and other nodes are subsituted separately at first, and
  // cross-substitution only happens when there is not enough nodes in the new
  // copyset.
  // Note that the approach relies on the fact that the generated copyset are
  // partitioned so that the nodes from the primary domain are in the beginning
  // of new_copyset

  // The remaining nodes in the generated copyset (C1) after substitution,
  // along with nodes in the existing copyset (C2), constitute a combined
  // copyset (C0). C0 should always be a cross-domain copyset if r >= 2.
  //
  // A brief proof: let us assume there are _p1_ primary nodes and _o1_
  // non-primary nodes in C1, _p2_ primary nodes and _o2_ non-primary nodes in
  // C2. From the requirements and property of C1 we know:
  //    i) _p1_ + _o1_ > _p2_ + _o2_
  //    ii) _p1_ > 0 and _o1_ > 0, _p2_ >=0  and _o2_ >= 0
  //
  // case I: _p1_ <= _p2_, after substituion C0 will contain _p2_ >= _p1_ > 0
  //         primary nodes and _o2_ + (_o1_ - (_p2_ - _p1_))
  //         = _p1_ + _o1_ - _p2_ > _p2_ + _o2_ - _p2_ = _o2_ >= 0 non-primary
  //         nodes. Therefore C0 is a cross-domain copyset
  //
  // case II:  _p1_ > _p2_ and _o1_ >= _o2_, after substitution C0 will contain
  //           _p1_ primary nodes and _o1_ non-primary nodes, and it is a
  //           cross-domain copyset
  //
  // case III: _p1_ > _p2_ and _o1_ < _o2_, after substitution C0 will contain
  //           _p1_ + _o1_ - _o2_ > _p2_ + _o2_ - _o2_ = _o2_ >= 0 primary nodes
  //           and _o2_ > _o1_ > 0 non-primary nodes. Therefore C0 is still a
  //           cross-domain copyset.
  //
  // From I, II, II, C0 is a cross-domain copyset if r >= 2.

  ld_check_eq(new_copyset_end - new_copyset_chain,
              primary_remaining + other_remaining + replication_factor_ -
                  existing_copyset_size);

  std::copy(new_copyset_chain + primary_remaining,
            new_copyset_end - other_remaining,
            inout_copyset + existing_copyset_size);

  if (out_full_size) {
    *out_full_size = full_size;
  }
  return CopySetSelector::Result::SUCCESS;
}

}} // namespace facebook::logdevice
