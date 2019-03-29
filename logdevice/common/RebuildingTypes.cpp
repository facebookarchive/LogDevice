/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RebuildingTypes.h"

#include <algorithm>
#include <functional>
#include <vector>

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/hash/Hash.h>

namespace facebook { namespace logdevice {

std::string toString(const RebuildingMode& mode) {
  switch (mode) {
    case RebuildingMode::RELOCATE:
      return "RELOCATE";
    case RebuildingMode::RESTORE:
      return "RESTORE";
    case RebuildingMode::INVALID:
      return "INVALID";
  }
  return "UNKNOWN";
}

std::string toString(const PerDataClassTimeRanges& pdctr) {
  using namespace std::literals::string_literals;

  bool first_dc = true;
  std::string out("[");
  for (const auto& trs_kv : pdctr) {
    if (trs_kv.second.empty()) {
      continue;
    }

    if (!first_dc) {
      out += ",";
    }
    first_dc = false;

    out += dataClassPrefixes()[trs_kv.first] + ":{"s;
    out += toString(trs_kv.second);
    out += "}";
  }
  out += "]";

  return out;
}

bool RebuildingNodeInfo::operator==(const RebuildingNodeInfo& other) const {
  return mode == other.mode && dc_dirty_ranges == other.dc_dirty_ranges;
}

bool RebuildingNodeInfo::isUnderReplicated(DataClass dc,
                                           RecordTimestamp ts) const {
  if (!dc_dirty_ranges.empty()) {
    auto drs_kv = dc_dirty_ranges.find(dc);
    if (drs_kv == dc_dirty_ranges.end() ||
        drs_kv->second.find(ts) == drs_kv->second.end()) {
      return false;
    }
  }
  return true;
}

std::string RebuildingSet::describe(size_t max_size) const {
  std::vector<const decltype(shards)::value_type*> sorted_shards;
  for (auto& s : shards) {
    sorted_shards.push_back(&s);
  }
  std::sort(sorted_shards.begin(), sorted_shards.end(), [](auto l, auto r) {
    return l->first < r->first;
  });
  std::string res;
  bool first = true;
  for (auto s : sorted_shards) {
    if (res.size() >= max_size) {
      break;
    }
    if (!first) {
      res += ",";
    }
    res += s->first.toString();
    if (s->second.mode == RebuildingMode::RELOCATE) {
      res += "*";
    }
    res += toString(s->second.dc_dirty_ranges);
    first = false;
  }
  if (res.size() >= max_size) {
    res.resize(max_size);
  }
  return res;
}

bool RebuildingSet::operator==(const RebuildingSet& other) const {
  return shards == other.shards;
}

void RecordRebuildingInterface::getRNGSeedFromRecord(uint32_t (&out)[4],
                                                     const ShardID* copyset,
                                                     size_t copyset_size,
                                                     size_t salt) {
  folly::hash::SpookyHashV2 hash;
  hash.Init(0, 0);
  if (copyset_size > 0) {
    hash.Update((void*)copyset, sizeof(ShardID) * copyset_size);
  }
  hash.Update((void*)&salt, sizeof(salt));
  hash.Final(((uint64_t*)&out[0]), ((uint64_t*)&out[2]));
  if ((out[0] | out[1] | out[2] | out[3]) == 0) {
    // XorShiftRNG doesn't support seeds being all zeroes
    out[0] = 1;
  }
}

size_t LogRebuildingMap::KeyHasher::
operator()(const LogRebuildingMap::Key& k) const {
  return folly::hash::hash_combine(k.first.val_, k.second);
}

LogRebuildingInterface* LogRebuildingMap::find(logid_t logid,
                                               shard_index_t shard) {
  Key k(logid, shard);
  auto it = map.find(k);
  return it == map.end() ? nullptr : it->second.get();
}

void LogRebuildingMap::erase(logid_t logid, shard_index_t shard) {
  Key k(logid, shard);
  map.erase(k);
}

void LogRebuildingMap::insert(logid_t logid,
                              shard_index_t shard,
                              std::unique_ptr<LogRebuildingInterface> log) {
  Key k(logid, shard);
  map[k] = std::move(log);
}

}} // namespace facebook::logdevice
