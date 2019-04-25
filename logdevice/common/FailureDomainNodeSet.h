/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <array>
#include <unordered_map>

#include <folly/container/F14Map.h>

#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

/**
 * @file FailureDomainNodeSet maintains a list of shards that can be tagged with
 * an attribute, and provides an API for querying the replication properties of
 * the subset of shards with a certain values for this attribute, given a
 * replication requirement across several failure domains.
 *
 * This utility provides 2 important methods:
 *
 * - canReplicate   Used to check if it would be possible to replicate a record
 *                  on the subset of shards that have certain values for the
 *                  attribute. For instance, if the replication requirements
 *                  include 3-way replication across the NODE scope and 2-way
 *                  replication across the RACK scope, this function will return
 *                  true if 3 shards have the required value for the attribute,
 *                  and these 3 shards span 2 racks and 3 nodes.
 *
 * - isFmajority    Used to check if enough shards have certain values for the
 *                  attribute so that that set of shards is an f-majority for at
 *                  least one of the scope for which there is a replication
 *                  requirement. For instance, with 3-way(NODE) / 2-way(RACK)
 *                  replication requirements, this function would return true if
 *                  there are only 2 nodes that don't have all their shards
 *                  tagged with the required value, or if there is only 1 rack
 *                  that does not have all its shards tagged with the required
 *                  value.
 *                  An example usage of this method is EpochRecovery which tags
 *                  each shard with "SEALED" if that shard has been sealed.
 *                  EpochRecovery is able to now it can proceed past the sealing
 *                  stage if it has sealed an f-majority of shards.
 *                  Another example usage is ClientReadStream which uses this
 *                  method to determine if a record with a given LSN exists or
 *                  not. When a storage shard indicates that it does not have a
 *                  copy of that record, it is tagged with a value. Given that
 *                  a record is considered to not exist if it is not properly
 *                  replicated, ClientReadStream will keep waiting for more
 *                  shards to chime in until it gets an f-majority of shards
 *                  tagged with the value.
 *
 * In addition to being able to tag a shard with a value, it is possible to tag
 * it with an AuthoritativeStatus, which is a special attribute that indicates
 * what is expected of this shard regarding its replication status. If the shard
 * is tagged with AUTHORITATIVE_EMPTY for instance, this means it is known to
 * not have data. If the set of shards spans 4 racks and the replication
 * at RACK level is 2, we usually need to have all shards in (4-2+1)=3 racks to
 * be tagged in order to have an f-majority. However, if all shards in one of
 * the racks are AUTHORITATIVE_EMPTY, we only need 2 racks.
 * @see logdevice/common/AuthoritativeStatus.h for more information on
 * AuthoritativeStatus.
 *
 * Internally, this class maintains one object `ScopeState` for replication
 * scope. This object keeps track of the list of domains at that scope as well
 * as a mapping from ShardID to the domain it belongs to. This object also
 * contains some metadata computed on the fly as we tag shards with new
 * attributes or change their authoritative status. For instance, we keep track
 * of:
 * - how many domains in that scope are empty, ie all their shards are
 *   AUTHORITATIVE_EMPTY;
 * - Which domains in that scope have at least one shard tagged with each
 *   value (useful for `canReplicate`);
 * - How many domains in that scope have all their shards tagged with a value
 *   (useful for `isFmajority`).
 *
 * Complexities:
 * - Tagging a shard with an attribute is linear to the number of replication
 *   scopes;
 * - Changing the authoritative status of a shard consists of a number of
 *   operations equal to the number of replication scopes times the number of
 *   different values for the attribute currently in use;
 * - isFmajority is linear to the number of replication scopes;
 * - canReplicate is linear to the number of replication scopes when used with
 *   an attribute value, or O(num attrs * replication * num scopes) in the worst
 *   case if used with an attribute predicate.
 */

/**
 * Enumerate the possible results of f-majority checks for a subset of the
 * storage_set.
 */
enum class FmajorityResult {
  // the subset of shards neither satisfies the authoritative f-majority
  // property, nor does it contain all fully authoritative shards.
  NONE,
  // the subset of shards satisfy the authoritative f-majority property, and it
  // has _all_ fully authoritative shards in the storage_set.
  AUTHORITATIVE_COMPLETE,
  // the subset of shards satisfies the authoritative f-majority property, and
  // it has suffcient but _not_ all fully authoritative shards in the
  // storage_set.
  AUTHORITATIVE_INCOMPLETE,
  // there are enough shards in UNDERREPLICATION that prevent us from
  // authoritatively deciding the replication state of records. Because of this,
  // the subset of shards does not satisfy the authoritative f-majority property
  // However, the subset of shards already contain all fully authoritative
  // shards in the storage_set. As a best effort, the subset of shards is
  // considered to be non-authoritative f-majority.
  NON_AUTHORITATIVE
};

const char* fmajorityResultString(FmajorityResult result);

template <typename AttrType, typename HashFn = std::hash<AttrType>>
class FailureDomainNodeSet {
 public:
  // a predicate function used to select a subset of shards based on their
  // attribute. Returning true for an attribute value means that shards with
  // such attribute value will be selected.
  using attr_pred_t = std::function<bool(AttrType)>;

  // TODO(T40776059) re-implement ShardAuthoritativeStatusMap to encapsulate
  // the below.
  typedef std::unordered_map<ShardID, AuthoritativeStatus, ShardID::Hash>
      AuthoritativeStatusMap;

  /**
   * Construct a FailureDomainNodeSet object.
   * @param storage_set              the entire set of shards to keep track of
   * @param nodes_configuration      cluster nodes configuration that contains
   *                                 storage node membership
   * @param rep                      replication requirement
   */
  FailureDomainNodeSet(
      const StorageSet& storage_set,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const ReplicationProperty& rep);

  ~FailureDomainNodeSet() {
    checkConsistency();
  }

  /**
   * Use this if you wish to always run consistency checks after each operation.
   * Used by tests.
   */
  void fullConsistencyCheck() {
    full_consistency_check_ = true;
  }

  /**
   * Set the attribute value of a shard. Note that a shard can only be
   * associated with one attribute value at a time, so if the shard has an
   * existing attribute value, the value will be cleared.
   */
  void setShardAttribute(ShardID shard, AttrType attribute);

  /**
   * Get the current attribute value of a shard indexed by @param index.
   *
   * @return 0   if the shard has an attribute and the attribute is written to
   *             @param attr_out.
   *        -1   if the shard is not part of the storage_set or there is no
   *             attribute for it.
   */
  int getShardAttribute(ShardID shard, AttrType* attr_out) const;

  /**
   * Set the AuthoritativeStatus value of a shard.
   * Note that a shard can only be associated with one attribute value at a
   * time, so if the shard has an existing AuthoritativeStatus value, the value
   * will be cleared.
   *
   * Requires: containsShard(shard) == true.
   */
  void setShardAuthoritativeStatus(ShardID shard, AuthoritativeStatus st);

  /**
   * @return AuthoritativeStatus value of a shard.
   *
   * @param shard Shard for which to get the AuthoritativeStatus value. This
   *               function asserts that the shard belongs to the storage_set.
   *
   * Requires: conainsShard(shard) == true.
   */
  AuthoritativeStatus getShardAuthoritativeStatus(ShardID shard) const;

  ////// convenient functions for checking a property

  /**
   * check if the subset of shards selected by _pred_ is a f-majority subset,
   * shard authoritative status is always considered
   */
  template <typename AttrPred>
  FmajorityResult isFmajority(AttrPred pred) const;

  /**
   * check if the subset of shards selected by _pred_ satisfies the replication
   * requirements.
   */
  template <typename AttrPred>
  bool canReplicate(AttrPred pred,
                    NodeLocationScope* fail_scope = nullptr) const;

  /**
   * check if the subset of shards selected by _pred_ satisfies the COMPLETE
   * property, ie all the fully authoritative shards are tagged with values that
   * match the provided predicate.
   */
  template <typename AttrPred>
  bool isCompleteSet(AttrPred pred) const;

  /**
   * total number of shards in the storage_set, regardless of their
   * authoritative status.
   */
  storage_set_size_t numShards() const {
    return shard_authoritative_.size();
  }

  /**
   * Count the number of shards with the given authoritative status
   */
  storage_set_size_t numShards(AuthoritativeStatus status) const {
    ld_check(status < AuthoritativeStatus::Count);
    return authoritative_count_[static_cast<size_t>(status)];
  }

  /**
   * Count the number of shards whose attribute satisfies _pred_ in the
   * storage_set. Shard authoritative status is ignored.
   */
  storage_set_size_t countShards(attr_pred_t pred) const;
  storage_set_size_t countShards(AttrType attr) const;

  // reset attributes counters for all shards
  void resetAttributeCounters();

  bool containsShard(ShardID shard) const {
    return shard_authoritative_.count(shard);
  }

  const AuthoritativeStatusMap getShardAuthoritativeStatusMap() {
    return shard_authoritative_;
  }

 private:
  // Store shard count for each attribute value in the failure domain
  struct Count {
    // Total shards for the attribute
    size_t all{0};
    // How many shards that are in FULLY_AUTHORITATIVE or UNAVAILABLE status.
    // Note that it is not necessary to track the number of shards for other
    // status.
    size_t full_only{0};
    bool operator==(const Count& rhs) const {
      return all == rhs.all && full_only == rhs.full_only;
    }
  };

  // Represent the state of a failure domain at a scope.
  struct FailureDomainState {
    // Number of shards in this domain.
    size_t n_shards{0};
    // Number of shards that are AUTHORITATIVE_EMPTY in this domain.
    size_t n_empty{0};
    // For each attribute, count the number of shards that have it.
    folly::F14FastMap<AttrType, Count, HashFn> shards_attr;
  };

  // Aggregated data for all domains at the same scope.
  struct ScopeState {
    // The domains at that scope.
    folly::F14NodeMap<std::string, FailureDomainState> domains;
    // A mapping between a shard and the domain it belongs to at that scope for
    // fast lookup.
    folly::F14FastMap<ShardID, FailureDomainState*, ShardID::Hash> shard_map;
    // Replication factor at that scope.
    size_t replication{0};
    // Number of the domains that have all their shards AUTHORITATIVE_EMPTY.
    size_t n_empty{0};
    // For each AttrType, number of domains at that scope that have all their
    // shards with the attribute. Used by isFmajority and isCompleteSet.
    folly::F14FastMap<AttrType, size_t, HashFn> n_full;
    // For each AttrType, set of domains at that scope that have at least one
    // shard with the attribute. canReplicate merges these sets to figure out if
    // we can replicate at each scope.
    using FDSet = std::unordered_set<const FailureDomainState*>;
    folly::F14FastMap<AttrType, FDSet, HashFn> replicate_set;
  };

  struct ScopeHash {
    size_t operator()(const NodeLocationScope& s) const {
      return std::hash<int>()(static_cast<int>(s));
    }
  };

  // Contains information about all the domains at all scopes.
  folly::F14FastMap<NodeLocationScope, ScopeState, ScopeHash> scopes_;
  // Accounting for the SHARD scope, which is an implicit scope for which the
  // replication factor is equal to the replication factor of the lower scope
  // defined by the user. For instance, if the user wants to replicate across 3
  // nodes, we have to replicate across 3 shards. we can extend this class very
  // easily in the future if we wish to have a higher replication factor at the
  // SHARD scope than NODE scope (ie replicate across 2 nodes but 3 shards). The
  // easiest way to do that may be to add SHARD in the NodeLocationScope enum
  // and remove `shard_scope_`.
  ScopeState shard_scope_;

  // Execute fn for all scopes.
  void forEachScope(std::function<void(ScopeState&)> fn);
  // Return true if the predicate is true for any scope.
  bool anyScope(std::function<bool(const ScopeState&)> pred) const;
  // Return true if the predicate is true for all scopes.
  // fail_scope is assigned to the first scope with predicate == false
  // (except if it is SHARD scope, which doesn't have value in enum)
  bool allScopes(std::function<bool(const ScopeState&)> pred,
                 NodeLocationScope* fail_scope = nullptr) const;

  // A mapping between ShardID and its attribute value.
  folly::F14FastMap<ShardID, AttrType, ShardID::Hash> shard_attribute_;

  // A mapping between a shard and its authoritative status.
 AuthoritativeStatusMap shard_authoritative_;

  // keeping track of how many shards are in each AuthoritativeStatus.
  std::array<size_t, static_cast<size_t>(AuthoritativeStatus::Count)>
      authoritative_count_{};

  folly::F14FastMap<AttrType, size_t, HashFn> attribute_count_;

  // For performance, even in debug builds, checkConsistency() is not called
  // every time in setShardAttribute() or setShardAuthoritativeStatus(); this
  // counter is used for deciding when to call it
  mutable size_t consistency_check_counter_ = 0;
  bool full_consistency_check_{false};

  static bool isFully(AuthoritativeStatus st) {
    return st == AuthoritativeStatus::FULLY_AUTHORITATIVE ||
        st == AuthoritativeStatus::UNAVAILABLE;
  }

  void
  addShard(ShardID shard,
           const configuration::nodes::NodesConfiguration& nodes_configuration);

  // Untag a shard with an attribute. Status is the current authoritative status
  // of that shard.
  void removeAttr(ShardID shard, AttrType attr, AuthoritativeStatus status);

  // Tag a shard with an attribute. Status is the current authoritative status
  // of that shard.
  void addAttr(ShardID shard, AttrType attr, AuthoritativeStatus status);

  // Count the number of domains at a scope that have at least one shard with
  // the attribute(s) and verify that number is greater than the replication
  // factor at the scope.
  bool canReplicateAtScope(attr_pred_t pred, const ScopeState& scope) const;
  bool canReplicateAtScope(AttrType attr, const ScopeState& scope) const;

  // Count the number of domains at a scope that have all their shards with the
  // attribute(s).
  // Note: the predicate version has a complexity of O(N_attrs * N_domains),
  // while
  //       the attr version's complexity is O(N_attrs)
  size_t countCompleteDomains(attr_pred_t pred, const ScopeState& scope) const;
  size_t countCompleteDomains(AttrType attr, const ScopeState& scope) const;

  // Check if a domain is complete (all non empty shards are tagged with the
  // value).
  bool domainIsComplete(FailureDomainState& fd, AttrType attr) const;

  // Make the necessary changes for a scope when a shard had its authoritative
  // status changed. Attr is the value (if any) that this shard is tagged with.
  void setShardAuthoritativeStatusAtScope(ShardID shard,
                                          ScopeState& scope,
                                          AuthoritativeStatus prev,
                                          AuthoritativeStatus status,
                                          folly::Optional<AttrType> attr);

  // used in debug build, asserts internally
  void checkConsistency() const;
  void checkConsistencyAtScope(const ScopeState& scope) const;
};

}} // namespace facebook::logdevice

#include "logdevice/common/FailureDomainNodeSet-inl.h"
