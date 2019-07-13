/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/filesystem.hpp>
#include <folly/Optional.h>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Helper class for ShardedLocalLogStore constructors which determines
 * which shard should go where in the filesystem.  The input is a root
 * directory under which all shards will live.
 *
 * It supports two approaches to allocating locations for shards:
 * (1) shardX lives in <root>/shardX.
 * (2) shardX lives in <root>/slotY/shardX.
 *
 * Approach (2) is more flexible when <root>/slot* is precreated as symlinks
 * to different physical disks.  If the disks get renamed or reordered during
 * maintenance, that will not affect the shard mapping.
 *
 * The output of the class is a vector<path> for each index in [0..nshards).
 * If the root directory contains "shard[0-9]+" direct subdirectories, those
 * will be preserved per approach (1).  If the root directory contains "slot*"
 * subdirectories, this class will look for shards in them, or allocate shards
 * in them.
 */

class ShardToPathMapping {
 public:
  explicit ShardToPathMapping(boost::filesystem::path root,
                              shard_size_t nshards)
      : root_(root), nshards_(nshards) {}

  // Returns 0 on success, -1 on failure such as a mismatch between the
  // expected number of shards and the directory structure on disk
  int get(std::vector<boost::filesystem::path>* paths_out);

  // Parses path to a file in a shard. Expected format:
  // "<path>/shard<idx>/<filename>
  // If the given path is of that form, assigns <idx> and <filename> to
  // *out_shard and *out_filename, and returns true.
  // Otherwise returns false.
  static bool parseFilePath(const std::string& path,
                            shard_index_t* out_shard,
                            std::string* out_filename);

 private:
  const boost::filesystem::path root_;
  const shard_size_t nshards_;
  // This is the assignment, incrementally built up by the private methods below
  std::vector<folly::Optional<boost::filesystem::path>> shard_paths_;

  // If the root has no subdirectories, just generate a straightforward
  // assignment with shard* subdirectories.
  bool handleEmptyRoot();
  // Looks for shard* under the root directory.  This is the fixed assignment.
  // Populates `shard_paths_'.
  void findFixedAssignment();
  // Looks for slot* under the root directory, and assigned shards under them.
  // Populates `shard_paths_' and returns any free slots.
  bool findSlots(std::vector<boost::filesystem::path>* free_slots_out);
  bool assignFreeSlots(std::vector<boost::filesystem::path> free_slots);
};

}} // namespace facebook::logdevice
