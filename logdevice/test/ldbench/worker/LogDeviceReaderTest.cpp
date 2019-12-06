/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogDeviceReader.h"

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/ldbench/worker/LogStoreClientHolder.h"
#include "logdevice/test/ldbench/worker/Options.h"
#include "logdevice/test/ldbench/worker/RecordWriterInfo.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogStoreReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster_ = IntegrationTestUtils::ClusterFactory().setNumLogs(10).create(4);
    client_ = cluster_->createClient();
    temp_dir_ = std::make_unique<TemporaryDirectory>("clientHolderTestForRead");
    options.bench_name = "reader";
    options.sys_name = "logdevice";
    options.record_writer_info = true;
    options.config_path = cluster_->getConfigPath();
    options.log_range_names.clear();
    publish_dir_ = temp_dir_->path().generic_string();
    options.publish_dir = publish_dir_;
    options.stats_interval = 0;          // keep writing stats
    options.event_ratio = 1;             // make sure record every event
    options.use_buffered_writer = false; // only append 1 do not buffer
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

    client_holder_ = std::make_unique<LogStoreClientHolder>();
    reader_ = client_holder_->createReader();
    auto record_cb = [this](LogIDType,
                            LogPositionType,
                            std::chrono::milliseconds,
                            std::string) {
      ++read_records_;
      stop_ = true;
      cv_.notify_one();
      ld_check(read_records_.load() > 0);
      return true;
    };
    auto gap_cb =
        [this](LogStoreGapType, LogIDType, LogPositionType, LogPositionType) {
          ++failed_records_;
          return true;
        };
    auto stop_cb = [this](LogIDType) {
      stop_ = true;
      cv_.notify_one();
    };
    reader_->setWorkerRecordCallback(std::move(record_cb));
    reader_->setWorkerGapCallback(std::move(gap_cb));
    reader_->setWorkerDoneCallback(std::move(stop_cb));
  }
  void TearDown() override {}

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
    }
    return;
  }

  void checkEvents() {
    // read and verify event
    std::fstream event_ifs(event_file_name_, std::ifstream::in);
    EXPECT_TRUE(event_ifs);
    std::vector<std::string> all_events;
    std::string event_str;
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
      EXPECT_EQ(logid, 1);
    }
    event_ifs.close();
    EXPECT_EQ(all_events.size(), 1);
    return;
  }

  void waitStop() {
    while (true) {
      if (stop_) {
        break;
      }
      {
        auto next_tick =
            std::chrono::system_clock::now() + std::chrono::milliseconds(200);
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_until(lock, next_tick);
      }
    }
    stop_ = false;
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
  std::unique_ptr<LogStoreClientHolder> client_holder_;
  std::string publish_dir_;
  std::string stats_file_name_;
  std::string event_file_name_;
  uint64_t start_;
  std::unique_ptr<LogStoreReader> reader_;
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> read_records_{0};
  std::atomic<uint64_t> failed_records_{0};
  std::atomic<uint64_t> append_records_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<TemporaryDirectory> temp_dir_;
};

TEST_F(LogStoreReaderTest, readerTest) {
  start_ = 0;
  stop_ = false;
  auto get_cb = [this](bool successful, uint64_t lsn) {
    EXPECT_TRUE(successful);
    start_ = lsn;
    stop_ = true;
    cv_.notify_one();
  };
  auto now = std::chrono::system_clock().now();
  auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
  auto epoch = now_ms.time_since_epoch();
  auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
  EXPECT_TRUE(client_holder_->findTime(1, ts, get_cb));
  waitStop();
  EXPECT_GT(start_, 0);
  start_ = 0;
  EXPECT_TRUE(client_holder_->getTail(1, get_cb));
  waitStop();
  EXPECT_GT(start_, 0);
  EXPECT_TRUE(reader_->startReading(
      1, start_, std::numeric_limits<LogPositionType>::max()));
  std::string payload_buf;
  RecordWriterInfo info;
  info.client_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  uint64_t header = (info.serializedSize() + 3) / 4;
  uint64_t size = 128;
  uint64_t len = (size + 3) / 4;
  len = std::max(len, header) - header;
  payload_buf.resize(header + len);
  info.serialize(reinterpret_cast<char*>(payload_buf.data()));
  client_->appendSync(logid_t(1), std::move(payload_buf));
  waitStop();
  EXPECT_EQ(read_records_.load(), 1);
  EXPECT_TRUE(reader_->stopReading(1));
  auto all_stats = client_holder_->getAllStats();
  EXPECT_EQ(all_stats["success"].asInt(), 1);
  // make sure everything has been write down
  client_holder_.reset();
  checkStats();
  checkEvents();
}
}}} // namespace facebook::logdevice::ldbench
