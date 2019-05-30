/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
// override-include-guard

namespace facebook { namespace logdevice {

template <typename AttrType, typename HashFn>
FailureDomainNodeSet<AttrType, HashFn>::FailureDomainNodeSet(
    const StorageSet& storage_set,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const ReplicationProperty& rep) {
  // in debug build, assert indices in _storage_set_ are unique
  if (folly::kIsDebug) {
    StorageSet copy(storage_set);
    std::sort(copy.begin(), copy.end());
    ld_assert(std::unique(copy.begin(), copy.end()) == copy.end() &&
              "shards in storage_set must be unique.");
  }

  // The user is expected to pass a valid ReplicationProperty object.
  ld_check(rep.isValid());
  size_t shard_replication = 0;
  auto scope = NodeLocation::nextSmallerScope(NodeLocationScope::ROOT);
  while (scope != NodeLocationScope::INVALID) {
    size_t replication = rep.getReplication(scope);
    // Do not consider the scope if the user did not specify a replication
    // factor for it, or if the replication factor for it is <= than the
    // replication factor at larger scopes.
    if (replication != 0 && replication > shard_replication) {
      ScopeState state{};
      state.replication = replication;
      scopes_[scope] = state;
      shard_replication = replication;
    }
    scope = NodeLocation::nextSmallerScope(scope);
  }

  // replication at implicit SHARD scope is equal to the replication at the
  // lowest scope defined by the user (usually NODE).
  shard_scope_.replication = shard_replication;
  ld_check(shard_scope_.replication > 0);

  // cluster should have some nodes.
  ld_check(nodes_configuration.clusterSize() > 0);

  // only include storage shards in nodes configuration and in reader's view
  // (e.g., exclude shards in "none" state)
  for (const ShardID shard :
       nodes_configuration.getStorageMembership()->readerView(storage_set)) {
    addShard(shard, nodes_configuration);
  }

  ld_check(numShards() <= storage_set.size());
  checkConsistency();
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::addShard(
    ShardID shard,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* service_disc =
      nodes_configuration.getNodeServiceDiscovery(shard.node());
  // shard came from the membership reader view of the node configuration
  ld_check(service_disc != nullptr);

  for (auto it = scopes_.begin(); it != scopes_.end(); ++it) {
    const NodeLocationScope scope = it->first;
    std::string domain_name;
    if (scope == NodeLocationScope::NODE) {
      domain_name = std::to_string(shard.node());
    } else if (!service_disc->location.hasValue() ||
               !service_disc->location.value().scopeSpecified(scope)) {
      ld_error("Node %d (%s) in the storage_set does not have location "
               "information in location scope: %s.",
               shard.node(),
               service_disc->address.toString().c_str(),
               NodeLocation::scopeNames()[scope].c_str());
      continue;
    } else {
      domain_name = service_disc->location.value().getDomain(scope);
    }

    ScopeState& state = it->second;
    FailureDomainState& fd = state.domains[domain_name];
    state.shard_map[shard] = &fd;
    ++fd.n_shards;
  }

  // Register the shard in the implicit SHARD scope.
  FailureDomainState& shard_fd = shard_scope_.domains[shard.toString()];
  shard_scope_.shard_map[shard] = &shard_fd;
  // There can only be one shard in a domain at scope SHARD.
  ld_check(shard_fd.n_shards == 0);
  ++shard_fd.n_shards;

  // initialize every shard to FULLY_AUTHORITATIVE
  ++authoritative_count_[static_cast<size_t>(
      AuthoritativeStatus::FULLY_AUTHORITATIVE)];
  shard_authoritative_[shard] = AuthoritativeStatus::FULLY_AUTHORITATIVE;
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::setShardAttribute(ShardID shard,
                                                               AttrType attr) {
  SCOPE_EXIT {
    checkConsistency();
  };

  auto status_it = shard_authoritative_.find(shard);
  if (status_it == shard_authoritative_.end()) {
    // shard is not in the storage set, ignore
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "%s is not in the storage set",
                    shard.toString().c_str());
    return;
  }

  const AuthoritativeStatus status = status_it->second;

  auto result = shard_attribute_.insert(std::make_pair(shard, attr));
  if (!result.second) {
    AttrType old_attr = result.first->second;
    if (old_attr == attr) {
      // shard is already of the same attribute, nothing to do.
      return;
    } else {
      ld_check(attribute_count_[old_attr] > 0);
      if (--attribute_count_[old_attr] == 0) {
        attribute_count_.erase(old_attr);
      }
      result.first->second = attr;
      // remove the old attribute
      removeAttr(shard, old_attr, status);
    }
  }

  ++attribute_count_[attr];
  addAttr(shard, attr, status);
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::removeAttr(
    ShardID shard,
    AttrType attr,
    AuthoritativeStatus status) {
  forEachScope([&](ScopeState& scope) {
    auto it_fd = scope.shard_map.find(shard);
    if (it_fd == scope.shard_map.end()) {
      return;
    }
    FailureDomainState* fd = it_fd->second;
    Count& attr_count = fd->shards_attr[attr];
    ld_check(attr_count.all > 0);

    if (isFully(status)) {
      ld_check(attr_count.full_only > 0);
      if (domainIsComplete(*fd, attr)) {
        ld_check(scope.n_full[attr] > 0);
        if (--scope.n_full[attr] == 0) {
          scope.n_full.erase(attr);
        }
      }
      --attr_count.full_only;
    }

    --attr_count.all;
    if (attr_count.all == 0) {
      auto& replicate_set = scope.replicate_set[attr];
      ld_assert(replicate_set.count(fd));
      replicate_set.erase(fd);
      if (replicate_set.empty()) {
        scope.replicate_set.erase(attr);
      }
      ld_check(attr_count.full_only == 0);
      fd->shards_attr.erase(attr);
    }
  });
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::addAttr(
    ShardID shard,
    AttrType attr,
    AuthoritativeStatus status) {
  // Add the new attribute.
  forEachScope([&](ScopeState& scope) {
    auto it_fd = scope.shard_map.find(shard);
    if (it_fd == scope.shard_map.end()) {
      return;
    }
    FailureDomainState* fd = it_fd->second;
    Count& attr_count = fd->shards_attr[attr];

    ++attr_count.all;
    if (attr_count.all == 1) {
      ld_assert(!scope.replicate_set[attr].count(fd));
      scope.replicate_set[attr].insert(fd);
    }

    if (isFully(status)) {
      ++attr_count.full_only;
      ld_check(attr_count.full_only <= fd->n_shards);
      if (domainIsComplete(*fd, attr)) {
        auto& n_full = scope.n_full[attr];
        ++n_full;
        ld_check(n_full <= scope.domains.size());
      }
    }
  });
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::setShardAuthoritativeStatus(
    ShardID shard,
    AuthoritativeStatus status) {
  SCOPE_EXIT {
    checkConsistency();
  };

  auto it = shard_authoritative_.find(shard);
  if (it == shard_authoritative_.end()) {
    // shard is not in the storage set, ignore.
    ld_check(false);
    return;
  }

  const AuthoritativeStatus prev = it->second;
  if (prev == status) {
    // already the same status, nothing to do
    return;
  }

  it->second = status;

  folly::Optional<AttrType> attr;
  auto attr_it = shard_attribute_.find(shard);
  if (attr_it != shard_attribute_.end()) {
    attr.assign(attr_it->second);
  }

  ld_check(authoritative_count_[static_cast<size_t>(prev)] > 0);
  --authoritative_count_[static_cast<size_t>(prev)];
  ++authoritative_count_[static_cast<size_t>(status)];

  forEachScope([&](ScopeState& scope) {
    setShardAuthoritativeStatusAtScope(shard, scope, prev, status, attr);
  });
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::setShardAuthoritativeStatusAtScope(
    ShardID shard,
    ScopeState& scope,
    AuthoritativeStatus prev,
    AuthoritativeStatus status,
    folly::Optional<AttrType> attr) {
  auto it_fd = scope.shard_map.find(shard);
  if (it_fd == scope.shard_map.end()) {
    return;
  }
  FailureDomainState* fd = it_fd->second;

  // Changing the authoritative status may make this domain complete for some
  // attributes. Record which ones are currently complete so we can assess what
  // changed later. Note: there are probably more efficient ways to do this but
  // it's not worth the added complexity.
  folly::F14FastMap<AttrType, bool, HashFn> was_complete;
  for (auto& it_attr : fd->shards_attr) {
    const auto a = it_attr.first;
    was_complete[a] = domainIsComplete(*fd, a);
  }

  if (status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    // The shard is now empty.
    ++fd->n_empty;
    ld_check(fd->n_empty <= fd->n_shards);
    if (fd->n_empty == fd->n_shards) {
      // All shards are now empty, the whole domain is empty.
      ++scope.n_empty;
      ld_check(scope.n_empty <= scope.domains.size());
    }
  } else if (prev == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    // The shard is now not empty.
    ld_check(fd->n_empty > 0);
    --fd->n_empty;
    if (fd->n_empty == fd->n_shards - 1) {
      // The whole domain is now not empty.
      ld_check(scope.n_empty > 0);
      --scope.n_empty;
    }
  }

  // full_only needs to be changed if we switch between not fully authoritative
  // and fully authoritative.
  if (attr.hasValue()) {
    auto a = attr.value();
    if (isFully(prev) && !isFully(status)) {
      ld_check(fd->shards_attr[a].full_only > 0);
      --fd->shards_attr[a].full_only;
    } else if (!isFully(prev) && isFully(status)) {
      ++fd->shards_attr[a].full_only;
      ld_check(fd->shards_attr[a].full_only <= fd->n_shards);
    }
  }

  // Now that we made some changes, find out if there are attributes for which
  // we became complete or became not complete.
  for (auto it_attr : fd->shards_attr) {
    const auto a = it_attr.first;
    const bool is_complete = domainIsComplete(*fd, a);
    if (was_complete[a] && !is_complete) {
      ld_check(scope.n_full[a] > 0);
      if (--scope.n_full[a] == 0) {
        scope.n_full.erase(a);
      }
    } else if (!was_complete[a] && is_complete) {
      ++scope.n_full[a];
      ld_check(scope.n_full[a] <= scope.domains.size());
    }
  }
}

template <typename AttrType, typename HashFn>
int FailureDomainNodeSet<AttrType, HashFn>::getShardAttribute(
    ShardID shard,
    AttrType* attr_out) const {
  ld_check(attr_out != nullptr);
  auto it = shard_attribute_.find(shard);
  if (it == shard_attribute_.end()) {
    return -1;
  }

  *attr_out = it->second;
  return 0;
}

template <typename AttrType, typename HashFn>
AuthoritativeStatus
FailureDomainNodeSet<AttrType, HashFn>::getShardAuthoritativeStatus(
    ShardID shard) const {
  auto it = shard_authoritative_.find(shard);
  // if the shard is in the storage set, it must have an authoritative status
  // value.
  ld_check(it != shard_authoritative_.end());
  return it->second;
}

template <typename AttrType, typename HashFn>
size_t FailureDomainNodeSet<AttrType, HashFn>::countCompleteDomains(
    attr_pred_t pred,
    const ScopeState& scope) const {
  size_t num_complete_domains = 0;

  // iterate all failure domains within the scope; for each domain
  // check if it is a complete domain wrt to the predicate given
  for (const auto& domain_kv : scope.domains) {
    size_t num_shards = 0;
    const FailureDomainState& domain = domain_kv.second;
    for (const auto& att_kv : domain.shards_attr) {
      if (pred(att_kv.first)) {
        num_shards += att_kv.second.full_only;
      }
    }

    ld_check(domain.n_shards >= domain.n_empty);
    if (num_shards > 0 && num_shards >= domain.n_shards - domain.n_empty) {
      // num_shards > 0 is required to ensure the domain is not empty
      ++num_complete_domains;
    }
  }

  return num_complete_domains;
}

template <typename AttrType, typename HashFn>
size_t FailureDomainNodeSet<AttrType, HashFn>::countCompleteDomains(
    AttrType attribute,
    const ScopeState& scope) const {
  auto it = scope.n_full.find(attribute);
  return it != scope.n_full.end() ? it->second : 0;
}

template <typename AttrType, typename HashFn>
storage_set_size_t
FailureDomainNodeSet<AttrType, HashFn>::countShards(attr_pred_t pred) const {
  storage_set_size_t res = 0;
  for (const auto& kv : attribute_count_) {
    if (pred(kv.first)) {
      res += kv.second;
    }
  }
  return res;
}

template <typename AttrType, typename HashFn>
storage_set_size_t
FailureDomainNodeSet<AttrType, HashFn>::countShards(AttrType attr) const {
  auto it = attribute_count_.find(attr);
  return it != attribute_count_.end() ? it->second : 0;
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::resetAttributeCounters() {
  forEachScope([](ScopeState& scope) {
    scope.n_full.clear();
    scope.replicate_set.clear();
    for (auto& it_fd : scope.domains) {
      it_fd.second.shards_attr.clear();
    }
  });
  shard_attribute_.clear();
  attribute_count_.clear();
}

template <typename AttrType, typename HashFn>
bool FailureDomainNodeSet<AttrType, HashFn>::domainIsComplete(
    FailureDomainState& fd,
    AttrType attr) const {
  // This domain is complete if all the non empty shards have the attribute.
  // An empty domain is not complete, hence the check for full_only > 0.
  return fd.shards_attr[attr].full_only > 0 &&
      fd.n_shards == fd.shards_attr[attr].full_only + fd.n_empty;
}

template <typename AttrType, typename HashFn>
template <typename AttrPred>
FmajorityResult
FailureDomainNodeSet<AttrType, HashFn>::isFmajority(AttrPred pred) const {
  // We have an f-majority if we have a f-majority for at least one scope.
  const bool f_maj = anyScope([&](const ScopeState& scope) {
    ld_check(scope.domains.size() >= scope.n_empty);
    const size_t n_non_empty = scope.domains.size() - scope.n_empty;
    const size_t f_majority = n_non_empty >= scope.replication - 1
        ? n_non_empty - scope.replication + 1
        : 0;
    return countCompleteDomains(pred, scope) >= f_majority;
  });

  if (f_maj) {
    return isCompleteSet(pred) ? FmajorityResult::AUTHORITATIVE_COMPLETE
                               : FmajorityResult::AUTHORITATIVE_INCOMPLETE;
  } else {
    return isCompleteSet(pred) ? FmajorityResult::NON_AUTHORITATIVE
                               : FmajorityResult::NONE;
  }
}

template <typename AttrType, typename HashFn>
bool FailureDomainNodeSet<AttrType, HashFn>::canReplicateAtScope(
    attr_pred_t pred,
    const ScopeState& scope) const {
  // For each attribute, merge the set of FailureDomainState* that have at least
  // one shard with the attribute here. We stop merging immediately after the
  // set's size becomes >= replication, making canReplicate O(replication *
  // num_attrs * num_scopes) in worst case.
  typename ScopeState::FDSet set;
  for (const auto& kv : scope.replicate_set) {
    if (pred(kv.first)) {
      for (const FailureDomainState* fd : kv.second) {
        set.insert(fd);
        if (set.size() >= scope.replication) {
          return true;
        }
      }
    }
  }
  return false;
}

template <typename AttrType, typename HashFn>
bool FailureDomainNodeSet<AttrType, HashFn>::canReplicateAtScope(
    AttrType attribute,
    const ScopeState& scope) const {
  auto it = scope.replicate_set.find(attribute);
  return it != scope.replicate_set.end()
      ? it->second.size() >= scope.replication
      : false;
}

template <typename AttrType, typename HashFn>
template <typename AttrPred>
bool FailureDomainNodeSet<AttrType, HashFn>::canReplicate(
    AttrPred pred,
    NodeLocationScope* fail_scope) const {
  // We can replicate if there are enough available domains at all scopes.
  return allScopes(
      [&](const ScopeState& scope) { return canReplicateAtScope(pred, scope); },
      fail_scope);
}

template <typename AttrType, typename HashFn>
template <typename AttrPred>
bool FailureDomainNodeSet<AttrType, HashFn>::isCompleteSet(
    AttrPred pred) const {
  // n_shards is the number of fully authoritative shards that have the
  // attribute(s) set.
  const size_t n_shards = countCompleteDomains(pred, shard_scope_);
  const size_t n_fully = numShards(AuthoritativeStatus::FULLY_AUTHORITATIVE) +
      numShards(AuthoritativeStatus::UNAVAILABLE);
  ld_check(n_shards <= n_fully);
  return n_shards == n_fully;
}

// used in debug build, asserts internally
template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::checkConsistency() const {
  if (!folly::kIsDebug) {
    return;
  }
  // checkConsistency() runs in O(total_shards * total_scopes), this is quite
  // expensive even for debug builds. Amortize the cost by not running the
  // function for every single change.
  const size_t total = shard_authoritative_.size() * scopes_.size();
  if (total > 0 && consistency_check_counter_++ % total != 0) {
    return;
  }

  checkConsistencyAtScope(shard_scope_);
  for (auto it = scopes_.begin(); it != scopes_.end(); ++it) {
    checkConsistencyAtScope(it->second);
  };
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::checkConsistencyAtScope(
    const ScopeState& scope) const {
  // 1/ Build a list of FailureDomainState objects by looking at the state of
  // each shard.
  folly::F14FastMap<const FailureDomainState*, FailureDomainState> domains;
  for (auto it_shard : scope.shard_map) {
    const FailureDomainState* fd = it_shard.second;
    FailureDomainState* fd_expected = &domains[fd];
    const ShardID shard = it_shard.first;
    auto it_status = shard_authoritative_.find(shard);
    ld_check(it_status != shard_authoritative_.end());
    auto status = it_status->second;
    folly::Optional<AttrType> attr;
    auto it_attr = shard_attribute_.find(shard);
    if (it_attr != shard_attribute_.end()) {
      attr = it_attr->second;
    }
    ++fd_expected->n_shards;
    if (status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
      ++fd_expected->n_empty;
    }
    if (attr.hasValue()) {
      ++fd_expected->shards_attr[attr.value()].all;
      if (isFully(status)) {
        ++fd_expected->shards_attr[attr.value()].full_only;
      }
    }
  }

  // 2/ Aggregate that data...
  size_t expected_n_empty = 0;
  folly::F14FastMap<AttrType, size_t, HashFn> expected_n_full;
  folly::F14FastMap<AttrType, typename ScopeState::FDSet, HashFn>
      expected_replicate_set;
  for (auto it : domains) {
    const FailureDomainState* fd_expected = &it.second;
    if (fd_expected->n_empty == fd_expected->n_shards) {
      ++expected_n_empty;
    }
    for (auto it_attr : fd_expected->shards_attr) {
      AttrType attr = it_attr.first;
      const Count& count = it_attr.second;
      if (count.all > 0) {
        expected_replicate_set[attr].insert(it.first);
      }
      if (count.full_only > 0 &&
          fd_expected->n_shards == count.full_only + fd_expected->n_empty) {
        ++expected_n_full[attr];
      }
    }
  }

  // 3/ Now compare this to the data that was aggregated on the fly in scope.
  ld_check(scope.n_empty == expected_n_empty);
  ld_check(scope.n_full == expected_n_full);
  ld_check(scope.replicate_set == expected_replicate_set);
}

template <typename AttrType, typename HashFn>
void FailureDomainNodeSet<AttrType, HashFn>::forEachScope(
    std::function<void(ScopeState&)> fn) {
  fn(shard_scope_);
  for (auto it = scopes_.begin(); it != scopes_.end(); ++it) {
    fn(it->second);
  }
}

template <typename AttrType, typename HashFn>
bool FailureDomainNodeSet<AttrType, HashFn>::anyScope(
    std::function<bool(const ScopeState&)> fn) const {
  if (fn(shard_scope_)) {
    return true;
  }
  for (auto it = scopes_.begin(); it != scopes_.end(); ++it) {
    if (fn(it->second)) {
      return true;
    }
  }

  return false;
}

template <typename AttrType, typename HashFn>
bool FailureDomainNodeSet<AttrType, HashFn>::allScopes(
    std::function<bool(const ScopeState&)> fn,
    NodeLocationScope* fail_scope) const {
  if (fail_scope) {
    *fail_scope = NodeLocationScope::INVALID;
  }
  if (!fn(shard_scope_)) {
    return false;
  }
  for (auto it = scopes_.begin(); it != scopes_.end(); ++it) {
    if (!fn(it->second)) {
      if (fail_scope) {
        *fail_scope = it->first;
      }
      return false;
    }
  }

  return true;
}

}} // namespace facebook::logdevice
