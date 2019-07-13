/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/ShardToPathMapping.h"

#include <algorithm>
#include <cctype>

#include <boost/algorithm/string/predicate.hpp>
#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

namespace fs = boost::filesystem;

namespace {
bool isSlot(const std::string& filename) {
  static const std::string kSlot{"slot"};
  return filename.size() > kSlot.size() &&
      std::equal(
             filename.begin(), filename.begin() + kSlot.size(), kSlot.begin());
}

folly::Optional<shard_index_t> getShard(const std::string& filename) {
  static const std::string kShard{"shard"};
  if (filename.size() <= kShard.size() ||
      !std::equal(
          filename.begin(), filename.begin() + kShard.size(), kShard.begin())) {
    return folly::none;
  }
  // validate that the rest of the string consists entirely of digits
  auto expected = folly::tryTo<shard_index_t>(
      folly::StringPiece{filename, /* startFrom */ kShard.size()});
  if (expected.hasValue()) {
    return expected.value();
  } else {
    return folly::none;
  }
}
} // namespace

int ShardToPathMapping::get(std::vector<fs::path>* paths_out) {
  ld_check(fs::is_directory(root_));
  shard_paths_.clear();
  shard_paths_.resize(nshards_);

  if (!handleEmptyRoot()) {
    std::vector<fs::path> free_slots;
    findFixedAssignment();
    bool success =
        findSlots(&free_slots) && assignFreeSlots(std::move(free_slots));
    if (!success) {
      return -1;
    }
  }

  paths_out->clear();
  for (shard_index_t shard_idx = 0; shard_idx < nshards_; ++shard_idx) {
    paths_out->push_back(shard_paths_[shard_idx].value());
  }
  return 0;
}

bool ShardToPathMapping::handleEmptyRoot() {
  for (const fs::directory_entry& entry : fs::directory_iterator(root_)) {
    const std::string filename = entry.path().filename().string();
    // Consider the root to be nonempty if we find a slot or shard
    // subdirectory.
    if (isSlot(filename) || getShard(filename).hasValue()) {
      return false;
    }
  }
  ld_debug("Empty local log store directory, generating default mapping");
  for (int idx = 0; idx < nshards_; ++idx) {
    shard_paths_[idx] = root_ / fs::path("shard" + std::to_string(idx));
  }
  return true;
}

void ShardToPathMapping::findFixedAssignment() {
  boost::system::error_code code;
  for (shard_index_t shard_idx = 0; shard_idx < nshards_; ++shard_idx) {
    fs::path shard_path = root_ / fs::path("shard" + std::to_string(shard_idx));
    if (fs::exists(shard_path, code)) {
      ld_check(!shard_paths_[shard_idx].hasValue());
      shard_paths_[shard_idx] = shard_path;
      ld_debug("Found shard %d in \"%s\"", shard_idx, shard_path.c_str());
    }
  }
}

bool ShardToPathMapping::findSlots(std::vector<fs::path>* free_slots_out) {
  boost::system::error_code code;
  // Find any slot* subdirectories.
  for (const fs::directory_entry& slot_entry : fs::directory_iterator(root_)) {
    const fs::path& slot_path = slot_entry.path();
    if (!isSlot(slot_path.filename().string())) {
      // Not a slot, ignore.
      continue;
    }
    if (!fs::is_directory(slot_path, code)) {
      ld_error("Path \"%s\" exists but is not a directory.  Unexpected!",
               slot_path.c_str());
      return false;
    }

    // We have a slot_path.  Look for a shard under it.
    shard_index_t shards_found = 0;
    for (const auto& shard_entry : fs::directory_iterator(slot_path)) {
      const fs::path& shard_path = shard_entry.path();
      const auto fn = shard_path.filename();
      auto shard_idx_opt = getShard(fn.string());
      if (!shard_idx_opt) {
        // Not a shard, ignore.
        continue;
      }

      ++shards_found;
      shard_index_t shard_idx = shard_idx_opt.value();
      if (shard_paths_[shard_idx].hasValue()) {
        ld_error("Shard %d found in more than one location: \"%s\" and \"%s\"!",
                 shard_idx,
                 shard_paths_[shard_idx].value().c_str(),
                 shard_path.c_str());
        return false;
      }
      shard_paths_[shard_idx] = shard_path;
      ld_debug("Found shard %d in \"%s\"", shard_idx, shard_path.c_str());
    }

    if (shards_found == 0) {
      // This is a free slot.
      free_slots_out->push_back(slot_path);
      ld_debug("Free slot in \"%s\"", slot_path.c_str());
    } else if (shards_found > 1) {
      ld_error(
          "Path \"%s\" contains more than one shard* subdirectory which is "
          "unexpected since the server only creates one shard per slot.",
          slot_path.c_str());
      return false;
    }
  }
  return true;
}

bool ShardToPathMapping::assignFreeSlots(std::vector<fs::path> free_slots) {
  size_t unassigned_shards = std::count_if(
      shard_paths_.begin(),
      shard_paths_.end(),
      [&](const folly::Optional<fs::path>& opt) { return !opt.hasValue(); });
  if (free_slots.size() != unassigned_shards) {
    // We could technically proceed if free_slots > unassigned_shards but it
    // suggests a mismatch between the number of shards passed to the server
    // and the directory structure, so be paranoid and error out in that case
    // too.
    ld_error("Found %zu unassigned shards but %zu free slots under \"%s\"!",
             unassigned_shards,
             free_slots.size(),
             root_.c_str());
    return false;
  }

  // Slots will be assigned randomly
  std::shuffle(free_slots.begin(), free_slots.end(), folly::ThreadLocalPRNG());
  for (shard_index_t shard_idx = 0; shard_idx < nshards_; ++shard_idx) {
    folly::Optional<fs::path>& opt = shard_paths_[shard_idx];
    if (!opt.hasValue()) {
      ld_check(!free_slots.empty());
      fs::path path = free_slots.back() / ("shard" + std::to_string(shard_idx));
      free_slots.pop_back();
      opt = path;
      ld_info("Assigned shard %d to \"%s\"", shard_idx, path.c_str());
    }
  }
  ld_check(free_slots.empty());
  return true;
}

bool ShardToPathMapping::parseFilePath(const std::string& path,
                                       shard_index_t* out_shard,
                                       std::string* out_filename) {
  auto complainer = folly::makeGuard([&] {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Couldn't parse shard idx from path: %s",
                    path.c_str());
  });

  // Looking for "/shard42/".

  size_t p = path.find("shard");
  if (p == std::string::npos || (p != 0 && path[p - 1] != '/')) {
    return false;
  }

  size_t q = path.find_first_of('/', p);
  q = q == std::string::npos ? path.size() : q;
  p += strlen("shard");
  ld_check(q >= p);

  shard_index_t shard;
  try {
    shard = folly::to<shard_index_t>(path.substr(p, q - p));
  } catch (std::range_error&) {
    return false;
  }

  if (shard < 0 || shard >= MAX_SHARDS) {
    return false;
  }

  if (out_shard != nullptr) {
    *out_shard = shard;
  }
  if (out_filename != nullptr) {
    *out_filename = path.substr(q + (q < path.size() ? 1 : 0));
  }

  complainer.dismiss();
  return true;
}

}} // namespace facebook::logdevice
