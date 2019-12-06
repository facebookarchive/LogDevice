/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/FileBasedEventStore.h"

#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "logdevice/test/ldbench/worker/BenchTracer.h"

namespace facebook { namespace logdevice { namespace ldbench {
class FileBasedEventStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tmp_file_name = std::tmpnam(nullptr);
    store = std::make_shared<FileBasedEventStore>(tmp_file_name);
  }
  void TearDown() override {
    std::remove(tmp_file_name.c_str());
  }
  std::shared_ptr<FileBasedEventStore> store;
  std::unique_ptr<BenchTracer> bench_tracer;
  std::string tmp_file_name;
};

TEST_F(FileBasedEventStoreTest, CreateFileEventStore) {
  EXPECT_TRUE(store->isReady());
}

TEST_F(FileBasedEventStoreTest, writeFileEventStore) {
  folly::dynamic event_obj = folly::dynamic::object("timestamp", 123)(
      "eventid", 123661)("type", "wlat")("blob", 2);
  std::vector<std::thread> test_threads;
  for (int i = 0; i < 10; i++) {
    test_threads.push_back(
        std::thread(&FileBasedEventStore::logEvent, store.get(), event_obj));
  }
  for (int i = 0; i < 10; i++) {
    test_threads[i].join();
  }
  folly::json::serialization_opts opts;
  std::string write_line = folly::json::serialize(event_obj, opts);
  std::fstream ifs(tmp_file_name, std::ifstream::in);
  std::string out_str;
  std::vector<std::string> all_strs;
  while (ifs >> out_str) {
    EXPECT_EQ(write_line, out_str);
    all_strs.push_back(out_str);
  }
  EXPECT_EQ(all_strs.size(), 10);
  ifs.close();
}

TEST_F(FileBasedEventStoreTest, eventByTracer) {
  bench_tracer = std::make_unique<BenchTracer>(store, 101);
  EventRecord event_record(123, "122", "wlat", 1111);
  std::vector<std::thread> test_threads;
  for (int i = 0; i < 10; i++) {
    test_threads.push_back(
        std::thread(&BenchTracer::doSample, bench_tracer.get(), event_record));
  }
  for (int i = 0; i < 10; i++) {
    test_threads[i].join();
  }
  std::fstream ifs(tmp_file_name, std::ifstream::in);
  std::string out_str;
  std::vector<std::string> all_strs;
  bench_tracer.reset();
  while (ifs >> out_str) {
    auto out_obj = folly::parseJson(out_str);
    EXPECT_EQ(out_obj["timestamp"], event_record.time_stamp_);
    all_strs.push_back(out_str);
  }
  EXPECT_EQ(all_strs.size(), 10);
  ifs.close();
}
}}} // namespace facebook::logdevice::ldbench
