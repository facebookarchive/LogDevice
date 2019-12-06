/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/FileBasedStatsStore.h"

#include <stdio.h>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "logdevice/test/ldbench/worker/StatsStore.h"

namespace facebook { namespace logdevice { namespace ldbench {

class FileBasedStatsStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tmp_file_name = std::tmpnam(nullptr);
    store = std::make_unique<FileBasedStatsStore>(tmp_file_name);
  }
  void TearDown() override {
    std::remove(tmp_file_name.c_str());
  }
  std::unique_ptr<FileBasedStatsStore> store;
  std::string tmp_file_name;
};

TEST_F(FileBasedStatsStoreTest, CreateFileStatsStore) {
  EXPECT_TRUE(store->isReady());
}

TEST_F(FileBasedStatsStoreTest, WriteStatsToFile) {
  folly::dynamic stats_obj =
      folly::dynamic::object("timestamp", 123)("succeed", 1000)("failed", 200);
  store->writeCurrentStats(stats_obj);
  folly::json::serialization_opts opts;
  std::string write_line = folly::json::serialize(stats_obj, opts);
  std::fstream ifs(tmp_file_name, std::ifstream::in);
  std::string out_str;
  ifs >> out_str;
  ASSERT_GT(out_str.size(), 0);
  EXPECT_EQ(write_line, out_str);
  ifs.close();
}
}}} // namespace facebook::logdevice::ldbench
