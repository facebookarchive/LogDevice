/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeLocationHierarchy.h"

#include <algorithm>
#include <queue>

#include <folly/Memory.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

NodeLocationHierarchy::Domain::Domain(NodeLocationScope scope, Domain* parent)
    : scope_(scope), parent_(parent) {
  ld_check(scope_ != NodeLocationScope::NODE);
}

NodeLocationHierarchy::NodeLocationHierarchy(
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_configuration,
    const StorageSet& indices)
    : root_(NodeLocationScope::ROOT, nullptr) {
  for (const ShardID i : indices) {
    const auto* node_sd =
        nodes_configuration->getNodeServiceDiscovery(i.node());
    if (node_sd && node_sd->location.hasValue()) {
      const auto& location = node_sd->location.value();
      insertShardWithLocation(i, location);
    }
  }

  checkConsistency();
}

void NodeLocationHierarchy::insertShardWithLocation(
    ShardID shard,
    const NodeLocation& location) {
  // starting from the root domain
  Domain* domain = &root_;
  ++domain->total_nodes_;
  ld_check(location.numScopes() <= NodeLocation::NUM_SCOPES);

  // descending along the hierarchy to the last specified scope of
  // the location
  NodeLocationScope scope = NodeLocationScope::ROOT;
  for (size_t i = 0; i < location.numScopes(); ++i) {
    scope = NodeLocation::nextSmallerScope(scope);
    ld_check(location.scopeSpecified(scope));
    const std::string& label = location.getLabel(scope);
    ld_check(!label.empty());

    Domain* next_domain = nullptr;
    // create or get the domain in the next scope
    auto result = domain->subdomains_.insert(std::make_pair(label, nullptr));

    if (result.second) {
      // insertion took place, create the domain
      result.first->second = std::make_unique<Domain>(scope, domain);
      next_domain = result.first->second.get();
      // also insert into the subdomain_list
      domain->subdomain_list_.push_back(next_domain);
    } else {
      next_domain = result.first->second.get();
      ld_check(next_domain != nullptr);
      ld_check(next_domain->scope_ == scope);
    }

    // subdomain_list_ should contain no duplicates and should have
    // the same number of items in subdomains_ map
    ld_check(domain->subdomains_.size() == domain->subdomain_list_.size());

    domain = next_domain;
    ++domain->total_nodes_;
  }

  // update the node->domain mapping and insert the cluster node index into
  // domain's direct_nodes_
  auto result = node_domain_map_.insert(std::make_pair(shard, domain));
  if (!result.second) {
    ld_critical("Duplicated node shard %s when creating "
                "NodeLocationHierarchy.",
                shard.toString().c_str());
    ld_check(false);
    return;
  }

  domain->direct_nodes_.push_back(shard);
}

// domains in the given scope are collected via breadth-first search
std::vector<const NodeLocationHierarchy::Domain*>
NodeLocationHierarchy::domainsInScope(NodeLocationScope scope) const {
  ld_check(scope != NodeLocationScope::NODE);
  std::vector<const Domain*> result;
  std::queue<const Domain*> queue;
  queue.push(&root_);
  while (!queue.empty()) {
    const Domain* domain = queue.front();
    queue.pop();
    if (domain->scope_ == scope) {
      result.push_back(domain);
    } else if (static_cast<size_t>(domain->scope_) >
               static_cast<size_t>(scope)) {
      for (auto sub_domain : domain->subdomain_list_) {
        queue.push(sub_domain);
      }
    } else {
      ld_check(false);
    }
  }

  return result;
}

const NodeLocationHierarchy::Domain*
NodeLocationHierarchy::searchSubdomain(const NodeLocation& location) const {
  const Domain* domain = &root_;
  NodeLocationScope scope = NodeLocationScope::ROOT;
  for (size_t i = 0; i < location.numScopes(); ++i) {
    scope = NodeLocation::nextSmallerScope(scope);
    ld_check(location.scopeSpecified(scope));
    const std::string& label = location.getLabel(scope);
    ld_check(!label.empty());
    auto it = domain->subdomains_.find(label);
    if (it == domain->subdomains_.end()) {
      // domain not found in the hierarchy, return what we have
      // so far
      return domain;
    }
    domain = it->second.get();
    ld_check(domain != nullptr);
    ld_check(domain->scope_ == scope);
  }

  return domain;
}

const NodeLocationHierarchy::Domain*
NodeLocationHierarchy::findDomain(const NodeLocation& location) const {
  const Domain* domain = searchSubdomain(location);
  ld_check(domain != nullptr);

  const size_t node_effective_scopes =
      static_cast<size_t>(NodeLocationScope::ROOT) -
      static_cast<size_t>(domain->scope_);

  ld_check(location.numScopes() >= node_effective_scopes);

  return location.numScopes() == node_effective_scopes ? domain : nullptr;
}

const NodeLocationHierarchy::Domain*
NodeLocationHierarchy::findDomainForShard(ShardID shard) const {
  auto it = node_domain_map_.find(shard);
  return it == node_domain_map_.end() ? nullptr : it->second;
}

void NodeLocationHierarchy::checkConsistency() const {
  if (folly::kIsDebug) {
    ld_check(root_.parent_ == nullptr);
    std::unordered_set<ShardID, ShardID::Hash> nodes;
    checkConsistencyRecursive(&root_, 0, &nodes);
  }
}

size_t NodeLocationHierarchy::checkConsistencyRecursive(
    const Domain* root,
    size_t level,
    std::unordered_set<ShardID, ShardID::Hash>* nodes) const {
  ld_check(root != nullptr);
  ld_check(nodes != nullptr);
  ld_check(static_cast<size_t>(root->scope_) ==
           static_cast<size_t>(NodeLocationScope::ROOT) - level);

  size_t total_cluster_nodes = 0;

  std::vector<const Domain*> sub_domains_in_map;
  for (const auto& kv : root->subdomains_) {
    const Domain* child = kv.second.get();
    ld_check(child != nullptr);
    ld_check(child->parent_ == root);
    total_cluster_nodes += checkConsistencyRecursive(child, level + 1, nodes);
    sub_domains_in_map.push_back(child);
  }

  // check subdomain map and list, they should contain same elements and no
  // duplicates
  std::vector<const Domain*> sub_domains_in_list(root->subdomain_list_);
  std::sort(sub_domains_in_map.begin(), sub_domains_in_map.end());
  std::sort(sub_domains_in_list.begin(), sub_domains_in_list.end());
  ld_assert(std::unique(sub_domains_in_map.begin(), sub_domains_in_map.end()) ==
            sub_domains_in_map.end());
  ld_assert(std::equal(sub_domains_in_map.begin(),
                       sub_domains_in_map.end(),
                       sub_domains_in_list.begin()));

  // cluster nodes associated with the domain must not be visited before
  for (const auto index : root->direct_nodes_) {
    auto result = nodes->insert(index);
    ld_check(result.second);
  }

  // check total nodes
  ld_check(root->total_nodes_ ==
           total_cluster_nodes + root->direct_nodes_.size());
  return root->total_nodes_;
}

std::string NodeLocationHierarchy::Domain::toString() const {
  if (parent_ == nullptr) {
    return "";
  }
  std::string res = parent_->toString() + ".";
  for (const auto& p : parent_->subdomains_) {
    if (p.second.get() == this) {
      return res += p.first;
    }
  }
  ld_check(false);
  return "INTERNAL-ERROR";
}

}} // namespace facebook::logdevice
