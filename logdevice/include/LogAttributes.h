/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include <folly/Optional.h>
#include <folly/container/F14Map.h>

#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/PermissionActions.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace logsconfig {

constexpr char const* REPLICATION_FACTOR = "replication_factor";
constexpr char const* EXTRA_COPIES = "extra_copies";
constexpr char const* SYNCED_COPIES = "synced_copies";
constexpr char const* MAX_WRITES_IN_FLIGHT = "max_writes_in_flight";
constexpr char const* SINGLE_WRITER = "single_writer";
constexpr char const* SYNC_REPLICATION_SCOPE = "sync_replicate_across";
constexpr char const* REPLICATE_ACROSS = "replicate_across";
constexpr char const* NODESET_SIZE = "nodeset_size";
constexpr char const* BACKLOG = "backlog";
constexpr char const* DELIVERY_LATENCY = "delivery_latency";
constexpr char const* SCD_ENABLED = "scd_enabled";
constexpr char const* LOCAL_SCD_ENABLED = "local_scd_enabled";
constexpr char const* WRITE_TOKEN = "write_token";
constexpr char const* PERMISSIONS = "permissions";
constexpr char const* ACLS = "acls";
constexpr char const* ACLS_SHADOW = "acls_shadow";
constexpr char const* STICKY_COPYSETS = "sticky_copysets";
constexpr char const* MUTABLE_PER_EPOCH_LOG_METADATA_ENABLED =
    "mutable_per_epoch_log_metadata_enabled";
constexpr char const* SEQUENCER_AFFINITY = "sequencer_affinity";
constexpr char const* SEQUENCER_BATCHING = "sequencer_batching";
constexpr char const* SEQUENCER_BATCHING_TIME_TRIGGER =
    "sequencer_batching_time_trigger";
constexpr char const* SEQUENCER_BATCHING_SIZE_TRIGGER =
    "sequencer_batching_size_trigger";
constexpr char const* SEQUENCER_BATCHING_COMPRESSION =
    "sequencer_batching_compression";
constexpr char const* SEQUENCER_BATCHING_PASSTHRU_THRESHOLD =
    "sequencer_batching_passthru_threshold";
constexpr char const* SHADOW = "shadow";
constexpr char const* SHADOW_DEST = "destination";
constexpr char const* SHADOW_RATIO = "ratio";
constexpr char const* TAIL_OPTIMIZED = "tail_optimized";

constexpr char const* EXTRAS = "extra_attributes";

/**
 * Attribute is a type wrapper class that denotes the existance or non-existance
 * or a value (like Optional) but also denotes whether the value of this
 * Attribute<T> was inherited or not.
 *
 * Attribute<T> is immutable and Thread-safe.
 */
template <typename Type>
class Attribute {
 public:
  /**
   * Merges a parent with attrs and produces a new Attribue
   * with the parent attributes applied (with inherited = true)
   */
  Attribute(const Attribute<Type>& attr, const Attribute<Type>& parent) {
    if (attr.isInherited() || !attr.hasValue()) {
      // check if the parent has a value and update our inherited
      // if not, then we reset (unset)
      if (parent.hasValue()) {
        value_ = parent.value_;
        inherited_ = true;
      } else {
        reset();
      }
    } else {
      value_ = attr.value_;
      inherited_ = attr.inherited_;
    }
  }

  // This is a shortcut constructor to construct an attribute from a value
  /* implicit */ Attribute(const Type& value)
      : value_(value), inherited_(false) {}
  Attribute(const Type& value, bool inherited)
      : value_(value), inherited_(inherited) {}
  // A shortcut constructor to construct attribute from folly::Optional
  /* implicit */ Attribute(const folly::Optional<Type>& value)
      : value_(value), inherited_(false) {}
  Attribute() {
    reset();
  }

  bool isInherited() const {
    return inherited_;
  }

  /**
   * Create a new Attribute<T> that replaces the current parent (if any) with
   * the supplied parent Attribute<T>
   */
  Attribute<Type> withParent(const Attribute<Type>& parent) const {
    return Attribute<Type>{*this, parent};
  }

  bool hasValue() const {
    return value_.hasValue();
  }

  /*
   * Returns the underlying folly::Optional by value
   */
  folly::Optional<Type> asOptional() const {
    return value_;
  }

  const Type& getValue(const Type& defaultValue) const {
    if (hasValue()) {
      return value_.value();
    }

    return defaultValue;
  }

  /**
   * returns true if we hold a value or we inherited one
   */
  explicit operator bool() const {
    return hasValue();
  }

  const Type& value() const& {
    return value_.value();
  }

  Type value() && {
    return std::move(value_.value());
  }

  const Type& operator*() const& {
    return value();
  }

  Type operator*() && {
    return std::move(value());
  }

  // a textual representation of the Attribute<T>, usually used for debugging
  std::string describe() const {
    std::string desc;
    if (hasValue()) {
      desc += std::to_string(value_.value());
    } else {
      desc += "<Undefined>";
    }
    if (isInherited()) {
      desc += " // Inherited";
    }
    return desc;
  }

  Attribute<Type>& operator=(Type&& arg) {
    inherited_ = false;
    value_ = std::move(arg);
    return *this;
  }

  Attribute<Type>& operator=(const Type& arg) {
    inherited_ = false;
    value_ = arg;
    return *this;
  }

 protected:
  void reset() {
    value_ = folly::none;
    inherited_ = false;
  }
  folly::Optional<Type> value_;
  bool inherited_;
};

template <typename Type>
bool operator==(const Attribute<Type>& a, const Attribute<Type>& b) {
  if (a.hasValue() != b.hasValue()) {
    return false;
  }
  if (a.hasValue()) {
    return (a.value() == b.value()) && (a.isInherited() == b.isInherited());
  }
  return true;
}

template <typename Type>
bool operator==(const Attribute<Type>& a, const Type& b) {
  return a.hasValue() && a.value() == b;
}

template <typename Type>
bool operator==(const Type& b, const Attribute<Type>& a) {
  return a.hasValue() && a.value() == b;
}

#define ACCESSOR(name)                                              \
  const decltype(name##_)& name() const {                           \
    return name##_;                                                 \
  }                                                                 \
  LogAttributes with_##name(const decltype(name##_)& name) const {  \
    LogAttributes copy = *this;                                     \
    copy.name##_ = name;                                            \
    return copy;                                                    \
  }                                                                 \
  LogAttributes with_##name(const decltype(name##_)&& name) const { \
    LogAttributes copy = *this;                                     \
    copy.name##_ = name;                                            \
    return copy;                                                    \
  }                                                                 \
  LogAttributes with_##name(decltype(name##_.value()) name) const { \
    LogAttributes copy = *this;                                     \
    copy.name##_ = name;                                            \
    return copy;                                                    \
  }                                                                 \
  /* ONLY FOR TEST CASES, DO NOT USE ANYWHERE ELSE. */              \
  void set_##name(decltype(name##_.value()) name) {                 \
    name##_ = name;                                                 \
  }

inline bool compareReplicateAcrossKeys(std::pair<NodeLocationScope, int>& lhs,
                                       std::pair<NodeLocationScope, int>& rhs) {
  return lhs.first < rhs.first;
}

inline bool
compareReplicateAcrossValues(std::pair<NodeLocationScope, int>& lhs,
                             std::pair<NodeLocationScope, int>& rhs) {
  return lhs.second < rhs.second;
}

/**
 * A container for a set of attributes for a log or a directory.
 *
 * LogAttributes instances are immutable and Thread-safe.
 */
class LogAttributes {
 public:
  using PermissionsMap =
      folly::F14FastMap<std::string,
                        std::array<bool, static_cast<int>(ACTION::MAX)>>;

  using ACLList = std::vector<std::string>;

  using ExtrasMap = folly::F14FastMap<std::string, std::string>;

  using ScopeReplicationFactors =
      std::vector<std::pair<NodeLocationScope, int>>;

  /**
   * Shadow is a small immutable class containing traffic shadowing information
   * for a log tree node.
   */
  class Shadow {
   public:
    Shadow(const std::string destination, const double ratio) noexcept
        : destination_(std::move(destination)), ratio_(ratio) {}

    const std::string& destination() const {
      return destination_;
    }

    double ratio() const {
      return ratio_;
    }

    bool operator==(const Shadow& other) const {
      return destination_ == other.destination_ &&
          std::fabs(ratio_ - other.ratio_) < EPSILON;
    }

   private:
    static constexpr double EPSILON = 0.001;

    std::string destination_;
    double ratio_;
  };

 protected:
  /**
   * This takes an set of attributes (LogAttributes) and a parent
   * (LogsAttributes) and apply the inheritance rules and the resulting set
   * which is a new (LogsAttributes) is *this*
   *
   * This is a C++ version of a function combinator.
   */
  void refreshAttributesInheritance(const LogAttributes& attrs,
                                    const LogAttributes& parent) {
#define MERGE_WITH_PARENT(source, attr) \
  attr##_ = source.attr##_.withParent(parent.attr##_);

    MERGE_WITH_PARENT(attrs, replicationFactor)
    MERGE_WITH_PARENT(attrs, extraCopies)
    MERGE_WITH_PARENT(attrs, syncedCopies)
    MERGE_WITH_PARENT(attrs, maxWritesInFlight)
    MERGE_WITH_PARENT(attrs, singleWriter)
    MERGE_WITH_PARENT(attrs, syncReplicationScope)
    MERGE_WITH_PARENT(attrs, replicateAcross)
    MERGE_WITH_PARENT(attrs, backlogDuration)
    MERGE_WITH_PARENT(attrs, nodeSetSize)
    MERGE_WITH_PARENT(attrs, deliveryLatency)
    MERGE_WITH_PARENT(attrs, scdEnabled)
    MERGE_WITH_PARENT(attrs, localScdEnabled)
    MERGE_WITH_PARENT(attrs, writeToken)
    MERGE_WITH_PARENT(attrs, stickyCopySets)
    MERGE_WITH_PARENT(attrs, mutablePerEpochLogMetadataEnabled)
    MERGE_WITH_PARENT(attrs, permissions)
    MERGE_WITH_PARENT(attrs, acls)
    MERGE_WITH_PARENT(attrs, aclsShadow)
    MERGE_WITH_PARENT(attrs, sequencerAffinity)
    MERGE_WITH_PARENT(attrs, sequencerBatching)
    MERGE_WITH_PARENT(attrs, sequencerBatchingTimeTrigger)
    MERGE_WITH_PARENT(attrs, sequencerBatchingSizeTrigger)
    MERGE_WITH_PARENT(attrs, sequencerBatchingCompression)
    MERGE_WITH_PARENT(attrs, sequencerBatchingPassthruThreshold)
    MERGE_WITH_PARENT(attrs, shadow)
    MERGE_WITH_PARENT(attrs, tailOptimized)

    MERGE_WITH_PARENT(attrs, extras)
#undef MERGE_WITH_PARENT
  }

  /**
   * Number of nodes on which to persist a record ('r' in the design doc).
   * Optional if replicateAcross_ is present.
   */
  Attribute<int> replicationFactor_;
  /**
   * Number of extra copies the sequencer sends out to storage nodes
   * ('x' in the design doc).  If x > 0, this is done to improve
   * latency and availability; the sequencer will try to delete extra
   * copies after the write is finalized.
   */
  Attribute<int> extraCopies_;
  /**
   * The number of copies that must be acknowledged by storage nodes
   * as synced to disk before the record is acknowledged to client as
   * fully appended. Can be 0. Capped at replicationFactor.
   */
  Attribute<int> syncedCopies_;
  /**
   * The largest number of records not released for delivery that the
   * sequencer allows to be outstanding ('z' in the design doc).
   */
  Attribute<int> maxWritesInFlight_;
  /**
   * Does LogDevice assume that there is a single writer for the log?
   */
  Attribute<bool> singleWriter_;
  /**
   * The location scope to enforce failure domain properties, by default
   * the scope is in the individual node level.
   * replicateAcross_ provides a more general way to do the same thing.
   */
  Attribute<NodeLocationScope> syncReplicationScope_;
  /**
   * Defines cross-domain replication. A vector of replication factors
   * at various scopes. When this option is given, replicationFactor_ is
   * optional. This option is best explained by examples:
   *  - "node: 3, rack: 2" means "replicate each record to at least 3 nodes
   *    in at least 2 different racks".
   *  - "rack: 2" with replicationFactor_ = 3 mean the same thing.
   *  - "rack: 3, region: 2" with replicationFactor_ = 4 mean "replicate
   *    each record to at least 4 nodes in at least 3 different racks in at
   *    least 2 different regions"
   *  - "rack: 3" means "replicate each record to at least 3 nodes in
   *    at least 3 different racks".
   *  - "rack: 3" with replicationFactor_ = 3 means the same thing.
   * Order of elements doesn't matter.
   */
  Attribute<ScopeReplicationFactors> replicateAcross_;
  /**
   * Duration that a record can exist in the log before it expires and
   * gets deleted. Valid value must be at least 1 second.
   */
  Attribute<folly::Optional<std::chrono::seconds>> backlogDuration_;
  /**
   * Size of the nodeset for the log. Optional. If value is not specified,
   * the nodeset for the log is considered to be all storage nodes in the
   * config.
   */
  Attribute<folly::Optional<int>> nodeSetSize_;
  /**
   * Maximum amount of time to artificially delay delivery of newly written
   * records (increases delivery latency but improves server and client
   * performance).
   */
  Attribute<folly::Optional<std::chrono::milliseconds>> deliveryLatency_;
  /**
   * Indicate whether or not the Single Copy Delivery optimization should be
   * used.
   */
  Attribute<bool> scdEnabled_;
  /**
   * Indicate whether or not to use Local Single Copy Delivery. This is ignored
   * if scdEnabled_ is false.
   */
  Attribute<bool> localScdEnabled_;
  /**
   * If this is nonempty, writes to the log group are only allowed if
   * Client::addWriteToken() was called with this string.
   */
  Attribute<folly::Optional<std::string>> writeToken_;
  /**
   * True if copysets on this log should be "sticky". See docblock in
   * StickyCopySetManager.h
   */
  Attribute<bool> stickyCopySets_;
  /**
   * If true, write mutable per-epoch metadata along with every data record.
   */
  Attribute<bool> mutablePerEpochLogMetadataEnabled_;
  /**
   * Maps a principal to a set of permissions. Used by ConfigPermissionChecker
   * and is populated when the permission_checker_type in the conifg file is
   * set to 'config'
   */
  Attribute<PermissionsMap> permissions_;
  /**
   * Vector of ACLs. Used by by permission checkers,
   * ro enforce permissions, which relay on
   * external store, i.e. HipsterPermissionChecker
   * and is populated when the permission_checker_type in the conifg file is
   * set to 'permission_store'
   */
  Attribute<ACLList> acls_;
  /**
   * Vector of ACLs. Used by by permission checkers
   * just for logging / debugging permissions.
   * Populated when the permission_checker_type in the conifg file is
   * set to 'permission_store'
   */
  Attribute<ACLList> aclsShadow_;
  /**
   * The location affinity of the sequencer. Sequencer routing will try to
   * find a sequencer in the given location first before looking elsewhere.
   */
  Attribute<folly::Optional<std::string>> sequencerAffinity_;

  /**
   * Enables or disables batching on sequencer.
   */
  Attribute<bool> sequencerBatching_;

  /**
   * Buffered writes for a log will be flushed when
   * the oldest of them has been buffered for this amount of time.
   */
  Attribute<std::chrono::milliseconds> sequencerBatchingTimeTrigger_;

  /**
   * Buffered writes for a log will be flushed as soon as this many payload
   * bytes are buffered.
   */
  Attribute<ssize_t> sequencerBatchingSizeTrigger_;

  /**
   * Compression codec.
   */
  Attribute<Compression> sequencerBatchingCompression_;

  /**
   * Writes with payload size greater than this value will not be batched.
   */
  Attribute<ssize_t> sequencerBatchingPassthruThreshold_;

  /**
   * Parameters for configuring traffic shadowing for a log group or directory.
   */
  Attribute<Shadow> shadow_;

  /**
   * If true, reading the tail of the log will be significantly more efficient.
   * The trade-off is more memory usage depending on the record size.
   */
  Attribute<bool> tailOptimized_;

  /**
   * Arbitrary fields that logdevice does not recognize
   */
  Attribute<ExtrasMap> extras_;

 public:
  friend class LogsConfigTree;
  LogAttributes(
      const Attribute<int>& replicationFactor,
      const Attribute<int>& extraCopies,
      const Attribute<int>& syncedCopies,
      const Attribute<int>& maxWritesInFlight,
      const Attribute<bool>& singleWriter,
      const Attribute<NodeLocationScope>& syncReplicationScope,
      const Attribute<ScopeReplicationFactors>& replicateAcross,
      const Attribute<folly::Optional<std::chrono::seconds>>& backlogDuration,
      const Attribute<folly::Optional<int>>& nodeSetSize,
      const Attribute<folly::Optional<std::chrono::milliseconds>>&
          deliveryLatency,
      const Attribute<bool>& scdEnabled,
      const Attribute<bool>& localScdEnabled,
      const Attribute<folly::Optional<std::string>>& writeToken,
      const Attribute<bool>& stickyCopySets,
      const Attribute<bool>& mutablePerEpochLogMetadataEnabled,
      const Attribute<PermissionsMap>& permissions,
      const Attribute<ACLList>& acls,
      const Attribute<ACLList>& aclsShadow,
      const Attribute<folly::Optional<std::string>>& sequencerAffinity,
      const Attribute<bool>& sequencerBatching,
      const Attribute<std::chrono::milliseconds>& sequencerBatchingTimeTrigger,
      const Attribute<ssize_t>& sequencerBatchingSizeTrigger,
      const Attribute<Compression>& sequencerBatchingCompression,
      const Attribute<ssize_t>& sequencerBatchingPassthruThreshold,
      const Attribute<Shadow>& shadow,
      const Attribute<bool>& tailOptimized,
      const Attribute<ExtrasMap>& extras)
      : replicationFactor_(replicationFactor),
        extraCopies_(extraCopies),
        syncedCopies_(syncedCopies),
        maxWritesInFlight_(maxWritesInFlight),
        singleWriter_(singleWriter),
        syncReplicationScope_(syncReplicationScope),
        replicateAcross_(replicateAcross),
        backlogDuration_(backlogDuration),
        nodeSetSize_(nodeSetSize),
        deliveryLatency_(deliveryLatency),
        scdEnabled_(scdEnabled),
        localScdEnabled_(localScdEnabled),
        writeToken_(writeToken),
        stickyCopySets_(stickyCopySets),
        mutablePerEpochLogMetadataEnabled_(mutablePerEpochLogMetadataEnabled),
        permissions_(permissions),
        acls_(acls),
        aclsShadow_(aclsShadow),
        sequencerAffinity_(sequencerAffinity),
        sequencerBatching_(sequencerBatching),
        sequencerBatchingTimeTrigger_(sequencerBatchingTimeTrigger),
        sequencerBatchingSizeTrigger_(sequencerBatchingSizeTrigger),
        sequencerBatchingCompression_(sequencerBatchingCompression),
        sequencerBatchingPassthruThreshold_(sequencerBatchingPassthruThreshold),
        shadow_(shadow),
        tailOptimized_(tailOptimized),
        extras_(extras) {}

  /**
   * Copies "attrs" while replacing its parent with the new "parent" argument.
   * This will re-apply the attribute inheritance from the parent.
   */
  LogAttributes(const LogAttributes& attrs, const LogAttributes& parent) {
    refreshAttributesInheritance(attrs, parent);
  }

  LogAttributes() {}

  // This creates a set of accessors for the attributes, the getter is the name
  // of the attribute defined as a function.
  ACCESSOR(replicationFactor)
  ACCESSOR(extraCopies)
  ACCESSOR(syncedCopies)
  ACCESSOR(maxWritesInFlight)
  ACCESSOR(singleWriter)
  ACCESSOR(syncReplicationScope)
  ACCESSOR(replicateAcross)
  ACCESSOR(backlogDuration)
  ACCESSOR(nodeSetSize)
  ACCESSOR(deliveryLatency)
  ACCESSOR(scdEnabled)
  ACCESSOR(localScdEnabled)
  ACCESSOR(writeToken)
  ACCESSOR(stickyCopySets)
  ACCESSOR(mutablePerEpochLogMetadataEnabled)
  ACCESSOR(permissions)
  ACCESSOR(acls)
  ACCESSOR(aclsShadow)
  ACCESSOR(sequencerAffinity)
  ACCESSOR(sequencerBatching)
  ACCESSOR(sequencerBatchingTimeTrigger)
  ACCESSOR(sequencerBatchingSizeTrigger)
  ACCESSOR(sequencerBatchingCompression)
  ACCESSOR(sequencerBatchingPassthruThreshold)
  ACCESSOR(shadow)
  ACCESSOR(tailOptimized)

  ACCESSOR(extras)

  bool operator==(const LogAttributes& other) const {
    auto as_tuple = [](const LogAttributes& l) {
      return std::tie(l.replicationFactor_,
                      l.extraCopies_,
                      l.syncedCopies_,
                      l.maxWritesInFlight_,
                      l.singleWriter_,
                      l.syncReplicationScope_,
                      l.replicateAcross_,
                      l.backlogDuration_,
                      l.nodeSetSize_,
                      l.deliveryLatency_,
                      l.scdEnabled_,
                      l.localScdEnabled_,
                      l.writeToken_,
                      l.stickyCopySets_,
                      l.mutablePerEpochLogMetadataEnabled_,
                      l.permissions_,
                      l.acls_,
                      l.aclsShadow_,
                      l.sequencerAffinity_,
                      l.sequencerBatching_,
                      l.sequencerBatchingTimeTrigger_,
                      l.sequencerBatchingSizeTrigger_,
                      l.sequencerBatchingCompression_,
                      l.sequencerBatchingPassthruThreshold_,
                      l.shadow_,
                      l.tailOptimized_,
                      l.extras_);
    };
    return as_tuple(*this) == as_tuple(other);
  }

  bool operator!=(const LogAttributes& other) const {
    return !(*this == other);
  }

  /**
   * Helpers and Shortcuts *
   */

  /***
   * Returns that replication factor defined for the biggest scope, this checks
   * both replicateAcross and syncReplicationScope.
   */
  int getReplicationFactorForBiggestScope() const {
    if (replicateAcross_ && replicateAcross_.value().size() > 0) {
      /*
       * The biggest scope necessarily has the minimum replication factor.
       * This method is not called "getSmallestReplicationFactor()" because
       * this could be understood as "replication factor of a log that has the
       * smallest replication factor".
       * we need the biggest replication
       */
      auto replicate_across = replicateAcross_.value();
      auto result_it = std::max_element(replicate_across.begin(),
                                        replicate_across.end(),
                                        compareReplicateAcrossKeys);
      return result_it->second;
    }
    assert(replicationFactor_.hasValue());
    return *replicationFactor_;
  }

  /***
   * Returns the biggest replication factor over all scopes. E.g. when using
   * replicateAcross {rack: 4, region: 2} this function would return 4
   */
  int getBiggestReplicationFactor() const {
    if (replicateAcross_ && replicateAcross_.value().size() > 0) {
      auto replicate_across = replicateAcross_.value();
      auto result_it = std::max_element(replicate_across.begin(),
                                        replicate_across.end(),
                                        compareReplicateAcrossValues);
      return result_it->second;
    }
    assert(replicationFactor_.hasValue());
    return *replicationFactor_;
  }

  NodeLocationScope getBiggestReplicationScope() const {
    NodeLocationScope def = NodeLocationScope::NODE;
    if (replicateAcross_ && replicateAcross_.value().size() > 0) {
      //  Return the scope with minimum replication factor.
      auto replicate_across = replicateAcross_.value();
      auto result_it = std::max_element(replicate_across.begin(),
                                        replicate_across.end(),
                                        compareReplicateAcrossKeys);
      return result_it->first;
    }
    if (syncReplicationScope_) {
      return *syncReplicationScope_;
    }
    return def;
  }

  NodeLocationScope getSmallestReplicationScope() const {
    NodeLocationScope def = NodeLocationScope::NODE;
    if (replicateAcross_ && replicateAcross_.value().size() > 0) {
      //  Return the scope with maximum replication factor.
      auto replicate_across = replicateAcross_.value();
      auto result_it = std::min_element(replicate_across.begin(),
                                        replicate_across.end(),
                                        compareReplicateAcrossKeys);
      return result_it->first;
    }
    if (syncReplicationScope_) {
      return *syncReplicationScope_;
    }
    return def;
  }
};
}}} // namespace facebook::logdevice::logsconfig
