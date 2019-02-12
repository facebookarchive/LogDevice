/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/ReplicationProperty.h"

#include "logdevice/include/Err.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice {

static ReplicationProperty
fromConfig(folly::Optional<int> replication_factor,
           folly::Optional<NodeLocationScope> sync_replication_scope,
           folly::Optional<logsconfig::LogAttributes::ScopeReplicationFactors>
               replicate_across) {
  ReplicationProperty prop;
  if (replicate_across.hasValue() && !replicate_across.value().empty()) {
    int rv = prop.assign(replicate_across.value());
    ld_check(rv == 0);
    ld_check(!sync_replication_scope.hasValue());
  }
  if (replication_factor.hasValue()) {
    ld_check(prop.getReplication(NodeLocationScope::NODE) == 0 ||
             prop.getReplication(NodeLocationScope::NODE) ==
                 replication_factor.value());
    prop.setReplication(NodeLocationScope::NODE, replication_factor.value());
  }
  if (sync_replication_scope.hasValue() &&
      sync_replication_scope.value() != NodeLocationScope::NODE &&
      replication_factor.value() > 1) {
    prop.setReplication(sync_replication_scope.value(), 2);
  }
  return prop;
}

ReplicationProperty
ReplicationProperty::fromLogAttributes(const logsconfig::LogAttributes& log) {
  return fromConfig(log.replicationFactor().asOptional(),
                    log.syncReplicationScope().asOptional(),
                    log.replicateAcross().asOptional().value_or(
                        logsconfig::LogAttributes::ScopeReplicationFactors()));
}

int ReplicationProperty::validateLogAttributes(
    const logsconfig::LogAttributes& log,
    std::string* out_error) {
#define ERR(x)        \
  do {                \
    if (out_error) {  \
      *out_error = x; \
    }                 \
    err = E::BADMSG;  \
    return -1;        \
  } while (false)

  if (log.replicateAcross().hasValue() &&
      log.syncReplicationScope().hasValue()) {
    ERR("Both syncReplicationScope and replicateAcross are set. "
        "They're incompatible.");
  }

  ReplicationProperty prop;

  if (log.replicateAcross().hasValue() &&
      !log.replicateAcross().value().empty()) {
    if (prop.assign(log.replicateAcross().value()) != 0) {
      ERR("Invalid replicateAcross. Replication factors must be positive and "
          "must be smaller for bigger scopes than for smaller scopes.");
    }

    if (log.replicationFactor().hasValue()) {
      if (log.replicationFactor().value() < prop.getReplicationFactor()) {
        ERR("replicationFactor (" +
            std::to_string(log.replicationFactor().value()) +
            ") is smaller "
            "than the biggest value in replicateAcross (" +
            std::to_string(prop.getReplicationFactor()) + ").");
      }

      if (prop.getReplication(NodeLocationScope::NODE) == 0) {
        prop.setReplication(
            NodeLocationScope::NODE, log.replicationFactor().value());
      } else {
        if (prop.getReplication(NodeLocationScope::NODE) !=
            log.replicationFactor().value()) {
          ERR("replicateAcross contains a replication factor for 'node' "
              "scope that is different from replicationFactor (" +
              std::to_string(prop.getReplication(NodeLocationScope::NODE)) +
              " vs " + std::to_string(log.replicationFactor().value()) + ").");
        }
      }
    }
  } else {
    if (!log.replicationFactor().hasValue()) {
      ERR("Neither replicationFactor nor replicateAcross is set. "
          "At least one of the two is required.");
    }
    if (log.replicationFactor().value() < 1) {
      ERR("replicationFactor is nonpositive (" +
          std::to_string(log.replicationFactor().value()) + ").");
    }

    prop.setReplication(
        NodeLocationScope::NODE, log.replicationFactor().value());

    if (log.syncReplicationScope().hasValue()) {
      NodeLocationScope scope = log.syncReplicationScope().value();
      static_assert(
          (int)NodeLocationScope::NODE == 0,
          "Did you add a location "
          "scope smaller than NODE? Update this validation code to allow it.");
      if (scope < NodeLocationScope::NODE || scope >= NodeLocationScope::ROOT) {
        ERR("syncReplicationScope out of range (" + std::to_string((int)scope) +
            ", " + NodeLocation::scopeNames()[scope] + ").");
      }

      if (scope > NodeLocationScope::NODE &&
          log.replicationFactor().value() > 1) {
        prop.setReplication(scope, 2);
      }
    }
  }

  ld_check(prop.isValid());
  ld_assert(prop == fromLogAttributes(log));

  if (log.extraCopies().hasValue() && log.extraCopies().value() < 0) {
    ERR("extraCopies is negative (" +
        std::to_string(log.extraCopies().value()) + ").");
  }
  if (log.syncedCopies().hasValue() && log.syncedCopies().value() < 0) {
    ERR("syncedCopies is negative (" +
        std::to_string(log.syncedCopies().value()) + ").");
  }
  // Allow syncedCopies to be greater than replication factor. This makes
  // inheritance easier: e.g. you can set syncedCopies to a big value for root
  // directory if you want all copies to be synced, regardless of
  // replication factors of particular log groups.

  int64_t copyset_size = (int64_t)prop.getReplicationFactor() +
      (int64_t)log.extraCopies().asOptional().value_or(0);
  if (copyset_size > COPYSET_SIZE_MAX) {
    ERR("The sum of replication factor and extraCopies is way too big (" +
        std::to_string(copyset_size) + ").");
  }

  if (prop.getDistinctReplicationFactors().size() > 2) {
    ERR("More than 2 different replication factors in replicateAcross and "
        "replicationFactor (" +
        prop.toString() +
        "). This is not supported at "
        "the moment.");
  }

  return 0;

#undef ERR
}

}} // namespace facebook::logdevice
