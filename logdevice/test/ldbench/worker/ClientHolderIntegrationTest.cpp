/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <mutex>
#include <thread>
#include <vector>

#include <folly/dynamic.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/test/ldbench/worker/BenchStats.h"
#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace ldbench {

class ClientHolderIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster_ = IntegrationTestUtils::ClusterFactory().setNumLogs(10).create(4);
    temp_dir_ = std::make_unique<TemporaryDirectory>("clientHolderTest");
    options.bench_name = "write";
    options.sys_name = "logdevice";
    options.record_writer_info = true;
    options.config_path = cluster_->getConfigPath();
    options.log_range_names.clear();
    publish_dir_ = temp_dir_->path().generic_string();
    options.publish_dir = publish_dir_;
    options.stats_interval = 1; // keep writing stats
    options.event_ratio = 1;    // make sure record every event
    stats_file_name_ = folly::to<std::string>(options.publish_dir,
                                              "/stats_",
                                              options.bench_name,
                                              options.worker_id_index,
                                              "_.csv");
    event_file_name_ = folly::to<std::string>(options.publish_dir,
                                              "/event_",
                                              options.bench_name,
                                              options.worker_id_index,
                                              "_.csv");
    options.buffered_writer_options.time_trigger =
        std::chrono::milliseconds(100);
  }
  void TearDown() override {}

  void appendByHolder() {
    client_holder_ = std::make_unique<LogStoreClientHolder>();
    auto worker_cb = [this](LogIDType, bool, bool, uint64_t, uint64_t) {
      stop_ = true;
      cv_.notify_all();
    };
    client_holder_->setWorkerCallBack(worker_cb);
    std::string payload_buf;
    RecordWriterInfo info;
    info.client_timestamp =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    uint64_t header = (info.serializedSize() + 3) / 4;
    uint64_t size = 128;
    uint64_t len = (size + 3) / 4;
    len = std::max(len, header) - header;
    payload_buf.resize(header + len);
    record_size_ = header + len;
    info.serialize(reinterpret_cast<char*>(payload_buf.data()));
    EXPECT_TRUE(client_holder_->append(1, std::move(payload_buf), nullptr));
    while (true) {
      auto next_tick =
          std::chrono::steady_clock::now() + std::chrono::seconds(2);
      std::unique_lock<std::mutex> lock(file_mutex_);
      cv_.wait_until(lock, next_tick);
      if (stop_) {
        break;
      }
    }
    folly::dynamic base_stats_obj = client_holder_->getAllStats();
    EXPECT_EQ(base_stats_obj["success"].asInt(), 1);
    EXPECT_EQ(base_stats_obj["success_byte"].asInt(), record_size_);
    return;
  }

  void checkStats() {
    // read and verify stats
    std::fstream stats_ifs(stats_file_name_, std::ifstream::in);
    EXPECT_TRUE(stats_ifs);
    std::vector<std::string> all_stats;
    std::string stat_str;
    while (stats_ifs >> stat_str) {
      all_stats.push_back(stat_str);
    }
    stats_ifs.close();
    EXPECT_GT(all_stats.size(), 0);
    if (all_stats.size() > 0) {
      folly::dynamic stat_obj = folly::parseJson(all_stats.back());
      EXPECT_EQ(stat_obj["success"].asInt(), 1);
      EXPECT_EQ(stat_obj["success_byte"].asInt(), record_size_);
    }
    return;
  }

  void checkEvents() {
    // read and verify event
    std::fstream event_ifs(event_file_name_, std::ifstream::in);
    EXPECT_TRUE(event_ifs);
    std::vector<std::string> all_events;
    std::string event_str;
    std::vector<uint64_t> count(10, 0);
    while (event_ifs >> event_str) {
      all_events.push_back(event_str);
      folly::dynamic event_obj = folly::parseJson(event_str);
      std::string eventid = event_obj["eventid"].asString();
      std::vector<folly::StringPiece> s;
      folly::splitTo<folly::StringPiece>(
          "-", eventid, std::inserter(s, s.begin()));
      EXPECT_EQ(2, s.size());
      uint64_t logid = folly::prettyToDouble(
          s.front(), folly::PrettyType::PRETTY_UNITS_METRIC);
      EXPECT_EQ(count[logid - 1], 0);
      count[logid - 1]++;
    }
    event_ifs.close();
    EXPECT_EQ(all_events.size(), 1);
    return;
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::unique_ptr<LogStoreClientHolder> client_holder_;
  std::string publish_dir_;
  std::string stats_file_name_;
  std::string event_file_name_;
  std::mutex file_mutex_;
  std::condition_variable cv_;
  std::atomic<bool> stop_{false};
  std::unique_ptr<TemporaryDirectory> temp_dir_;
  uint64_t record_size_;
};

TEST_F(ClientHolderIntegrationTest, appendByHolder) {
  options.use_buffered_writer = false;
  appendByHolder();
  client_holder_.reset();
  checkStats();
  checkEvents();
  std::remove(stats_file_name_.c_str());
  std::remove(event_file_name_.c_str());
  options.use_buffered_writer = true;
  appendByHolder();
  client_holder_.reset();
  checkStats();
  checkEvents();
}

}}} // namespace facebook::logdevice::ldbench
