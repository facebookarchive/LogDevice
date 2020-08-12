/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/safety/SafetyAPI.h"

#include <folly/String.h>

#include "logdevice/admin/if/gen-cpp2/exceptions_types.h"
#include "logdevice/admin/if/gen-cpp2/safety_types.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

Impact mergeImpact(Impact i1, const Impact& i2, size_t error_sample_size) {
  const auto& i1_impact = i1.get_impact();
  const auto& i2_impact = i2.get_impact();
  std::set<thrift::OperationImpact> merge_impact(
      i1_impact.begin(), i1_impact.end());
  merge_impact.insert(i2_impact.begin(), i2_impact.end());
  i1.set_impact({merge_impact.begin(), merge_impact.end()});

  i1.set_total_logs_checked(i1.total_logs_checked_ref().value_or(0) +
                            i2.total_logs_checked_ref().value_or(0));

  if (!i2.logs_affected_ref().has_value()) {
    return i1;
  }
  if (!i1.logs_affected_ref().has_value()) {
    i1.logs_affected_ref() = {};
  }

  std::vector<thrift::ImpactOnEpoch> logs_affected;
  if (error_sample_size >= 0) {
    // How many can we accept? merge_limit can be negative if we are already
    // beyond capacity.
    size_t merge_limit = error_sample_size - i1.logs_affected_ref()->size();
    // Cannot copy more elements than the size of the source (impact)
    merge_limit = std::min(merge_limit, i2.logs_affected_ref()->size());

    std::copy_n(i2.logs_affected_ref()->begin(),
                merge_limit,
                std::back_inserter(*i1.logs_affected_ref()));
  } else {
    // Copy everything
    std::copy(i2.logs_affected_ref()->begin(),
              i2.logs_affected_ref()->end(),
              std::back_inserter(*i1.logs_affected_ref()));
  }
  return i1;
}

folly::Expected<Impact, Status>
mergeImpact(folly::Expected<Impact, Status> i,
            const folly::Expected<Impact, Status>& i2,
            size_t error_sample_size) {
  if (i.hasError()) {
    return i;
  }
  if (i2.hasError()) {
    return i2;
  }
  return mergeImpact(std::move(i.value()), i2.value(), error_sample_size);
}

std::string impactToString(const Impact& impact) {
#define MAP_ENTRY(e) \
  { thrift::OperationImpact::e, #e }
  static const std::map<thrift::OperationImpact, std::string> to_str{
      MAP_ENTRY(INVALID),
      MAP_ENTRY(REBUILDING_STALL),
      MAP_ENTRY(WRITE_AVAILABILITY_LOSS),
      MAP_ENTRY(READ_AVAILABILITY_LOSS),
      MAP_ENTRY(SEQUENCING_CAPACITY_LOSS),
      MAP_ENTRY(STORAGE_CAPACITY_LOSS)};
#undef MAP_ENTRY

  std::vector<std::string> impact_str;
  std::transform(impact.impact_ref()->begin(),
                 impact.impact_ref()->end(),
                 std::back_inserter(impact_str),
                 [](const auto& entry) { return to_str.at(entry); });
  return folly::join(",", impact_str);
}

int parseSafetyMargin(const std::string& descriptor, SafetyMargin& out) {
  if (descriptor.empty()) {
    return 0;
  }
  std::vector<std::string> domains;
  folly::split(',', descriptor, domains);
  for (const std::string& domain : domains) {
    if (domain.empty()) {
      continue;
    }
    std::vector<std::string> tokens;
    folly::split(":", domain, tokens, /* ignoreEmpty */ false);
    if (tokens.size() != 2) {
      return -1;
    }
    int margin = folly::to<int>(tokens[1]);

    std::string scope_str = tokens[0];
    std::transform(
        scope_str.begin(), scope_str.end(), scope_str.begin(), ::toupper);

    NodeLocationScope scope =
        NodeLocation::scopeNames().reverseLookup(scope_str);
    static_assert(
        (int)NodeLocationScope::NODE == 0,
        "Did you add a location "
        "scope smaller than NODE? Update this validation code to allow it.");
    if (scope < NodeLocationScope::NODE || scope >= NodeLocationScope::ROOT) {
      ld_error("Invalid scope in safety-margin %s ", scope_str.c_str());
      return false;
    }
    out.emplace(scope, margin);
  }
  return 0;
}

SafetyMargin
safetyMarginFromReplication(const ReplicationProperty& replication) {
  SafetyMargin output;
  for (const auto& scope_replication :
       replication.getDistinctReplicationFactors()) {
    output.insert(
        std::make_pair(scope_replication.first, scope_replication.second));
  }
  return output;
}

int parseSafetyMargin(const std::vector<std::string>& descriptors,
                      SafetyMargin& out) {
  for (const std::string& descriptor : descriptors) {
    if (parseSafetyMargin(descriptor, out) != 0) {
      return -1;
    }
  }
  return 0;
}
}} // namespace facebook::logdevice
