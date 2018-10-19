/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/ShardToPathMapping.h"

#include <fstream>

#include <boost/filesystem.hpp>
#include <folly/Memory.h>
#include <folly/experimental/TestUtil.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;
namespace fs = boost::filesystem;

class ShardToPathMappingTest : public ::testing::Test {
 public:
  void SetUp() override {
    dbg::currentLevel = dbg::Level::DEBUG;
    const char* prefix = "ShardToPathMappingTest";
    root_pin_ = std::make_unique<folly::test::TemporaryDirectory>(prefix);
    root_ = root_pin_->path();
  }

  void createSubdirectories(const std::vector<std::string>& dirs) {
    for (const auto& dir : dirs) {
      fs::create_directories(root_ / dir);
    }
  }

  fs::path root_;

 private:
  std::unique_ptr<folly::test::TemporaryDirectory> root_pin_;
};

template <typename Container = std::vector<std::string>>
static std::vector<std::string> toStrings(const std::vector<fs::path>& paths) {
  std::vector<std::string> ret;
  for (const auto& path : paths) {
    ret.emplace_back(path.c_str());
  }
  return Container(ret.begin(), ret.end());
}

template <typename T>
static std::vector<T> sorted(std::vector<T> v) {
  std::sort(v.begin(), v.end());
  return v;
}

// If the root directory contains shard[0-9]+ directories, the numbers should
// be picked up.
TEST_F(ShardToPathMappingTest, Fixed) {
  createSubdirectories({"shard0", "shard1", "shard3", "shard2"});
  std::vector<fs::path> output;
  int rv = ShardToPathMapping(root_, 4).get(&output);
  ASSERT_EQ(0, rv);
  // Order matters
  std::vector<fs::path> expected{
      root_ / "shard0",
      root_ / "shard1",
      root_ / "shard2",
      root_ / "shard3",
  };
  // Need to convert to strings when comparing because gtest fails to print
  // fs::paths with a stack overflow.
  ASSERT_EQ(toStrings(expected), toStrings(output));
}

// If the root directory contains slot* directories without shard
// subdirectories, the class should assign them arbitrarily.
TEST_F(ShardToPathMappingTest, AllSlotsFree) {
  std::vector<std::string> slots{"slot0", "slot1", "slot2", "slot3"};
  createSubdirectories(slots);
  std::vector<fs::path> output;
  int rv = ShardToPathMapping(root_, 4).get(&output);
  ASSERT_EQ(0, rv);

  std::vector<std::string> slots_used;
  std::vector<std::string> shards_assigned;
  for (fs::path path : output) {
    shards_assigned.push_back(path.filename().c_str());
    slots_used.push_back(path.parent_path().filename().c_str());
  }

  // All slots should have been used exactly once
  std::sort(slots_used.begin(), slots_used.end());
  ASSERT_EQ(slots, sorted(slots_used));
  std::vector<std::string> expected{"shard0", "shard1", "shard2", "shard3"};
  ASSERT_EQ(expected, sorted(shards_assigned));
}

// With some slots full but some free, must preserve the assignment of full
// slots and allocate others arbitrarily
TEST_F(ShardToPathMappingTest, SomeSlotsFull) {
  std::vector<std::string> slots{
      "slot0/shard4",
      "slot1",
      "slot2",
      "slot3/shard1",
      "slot4/shard0",
  };
  createSubdirectories(slots);
  std::vector<fs::path> output;
  int rv = ShardToPathMapping(root_, 5).get(&output);
  ASSERT_EQ(0, rv);

  ASSERT_STREQ((root_ / slots[0]).c_str(), output[4].c_str());
  ASSERT_STREQ((root_ / slots[3]).c_str(), output[1].c_str());
  ASSERT_STREQ((root_ / slots[4]).c_str(), output[0].c_str());
  // Shards 2 and 3 should go into slots 1 and 2.
  ASSERT_TRUE(output[2] == (root_ / slots[1] / "shard2") ||
              output[2] == (root_ / slots[2] / "shard2"));
  ASSERT_TRUE(output[3] == (root_ / slots[1] / "shard3") ||
              output[3] == (root_ / slots[2] / "shard3"));
  ASSERT_STRNE(output[2].c_str(), output[3].c_str());
}

// Files named "shard0" etc should be allowed same as directories (for
// operational reasons we may want to put a placeholder file instead of a
// directory)
TEST_F(ShardToPathMappingTest, ShardFiles) {
  createSubdirectories({"shard0"});
  std::ofstream((root_ / "shard1").string());
  std::vector<fs::path> output;
  int rv = ShardToPathMapping(root_, 2).get(&output);
  ASSERT_EQ(0, rv);
  // Order matters
  std::vector<fs::path> expected{
      root_ / "shard0",
      root_ / "shard1",
  };
  ASSERT_EQ(toStrings(expected), toStrings(output));
}
