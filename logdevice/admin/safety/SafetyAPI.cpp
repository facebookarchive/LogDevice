/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/safety/SafetyAPI.h"

#include <folly/String.h>

#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

Impact::Impact(int result,
               std::vector<ImpactOnEpoch> logs_affected,
               bool internal_logs_affected,
               size_t total_logs_checked,
               std::chrono::seconds total_duration)
    : result(result),
      logs_affected(std::move(logs_affected)),
      internal_logs_affected(internal_logs_affected),
      total_logs_checked(total_logs_checked),
      total_duration(total_duration) {}

Impact::Impact() : result(ImpactResult::NONE), internal_logs_affected(false) {}

std::string Impact::toStringImpactResult(int result) {
  std::string s;

#define TO_STR(f)     \
  if (result & f) {   \
    if (!s.empty()) { \
      s += ", ";      \
    }                 \
    s += #f;          \
  }

  TO_STR(WRITE_AVAILABILITY_LOSS)
  TO_STR(READ_AVAILABILITY_LOSS)
  TO_STR(REBUILDING_STALL)
  TO_STR(INVALID)

#undef TO_STR

  if (s.empty()) {
    return "NONE";
  } else {
    return s;
  }
}

std::string Impact::toString() const {
  return toStringImpactResult(result);
}

Impact Impact::merge(Impact i1, const Impact& i2, size_t error_sample_size) {
  i1.result |= i2.result;
  i1.total_logs_checked += i2.total_logs_checked;
  if (error_sample_size >= 0) {
    // How many can we accept? merge_limit can be negative if we are already
    // beyond capacity.
    size_t merge_limit = error_sample_size - i1.logs_affected.size();
    // Cannot copy more elements than the size of the source (impact)
    merge_limit = std::min(merge_limit, i2.logs_affected.size());
    std::copy_n(i2.logs_affected.begin(),
                merge_limit,
                std::back_inserter(i1.logs_affected));
  } else {
    // Copy everything
    std::copy(i2.logs_affected.begin(),
              i2.logs_affected.end(),
              std::back_inserter(i1.logs_affected));
  }
  return i1;
}

folly::Expected<Impact, Status>
Impact::merge(folly::Expected<Impact, Status> i,
              const folly::Expected<Impact, Status>& i2,
              size_t error_sample_size) {
  if (i.hasError()) {
    return i;
  }
  if (i2.hasError()) {
    return i2;
  }
  return merge(std::move(i.value()), i2.value(), error_sample_size);
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
