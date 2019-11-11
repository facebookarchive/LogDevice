/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/CheckpointStoreFactory.h"
#include "logdevice/include/CheckpointedReaderFactory.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/SyncCheckpointedReader.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice {

class CheckpointingIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster_ = IntegrationTestUtils::ClusterFactory().setNumLogs(3).create(4);
    client_ = cluster_->createClient();
    temp_dir_ = std::make_unique<TemporaryDirectory>("checkpointTestForStore");
    store_path_ = temp_dir_->path().generic_string();
  }
  void TearDown() override {}

  void appendSampleRecords(logid_t log_id, uint32_t records_total) {
    for (int i = 0; i < records_total; ++i) {
      ASSERT_NE(LSN_INVALID, client_->appendSync(log_id, "payload"));
    }
  }

  void runSyncReader(
      folly::Function<std::unique_ptr<CheckpointStore>()> create_store) {
    cluster_->waitForRecovery();
    auto reader = client_->createReader(2);

    uint32_t records_total = 100;
    uint32_t records_break = 40;
    appendSampleRecords(logid_t(1), records_break);
    appendSampleRecords(logid_t(2), records_break);

    auto store = create_store();
    reader_ = CheckpointedReaderFactory().createSyncCheckpointedReader(
        "customer", std::move(reader), std::move(store), {});
    ASSERT_EQ(0,
              reader_->startReadingFromCheckpoint(
                  logid_t(1), client_->getTailLSNSync(logid_t(1))));
    ASSERT_EQ(0,
              reader_->startReadingFromCheckpoint(
                  logid_t(2), client_->getTailLSNSync(logid_t(2))));

    ssize_t total_read = 0;
    auto read_until_no_data = [this, &total_read, records_total]() {
      std::vector<std::unique_ptr<DataRecord>> data_out;
      GapRecord gap_out;
      while (true) {
        data_out.clear();
        auto nread = reader_->read(records_total, &data_out, &gap_out);
        if (nread == -1 && err == E::GAP) {
          continue;
        }
        ASSERT_NE(-1, nread);
        if (nread == 0) {
          break;
        }
        total_read += nread;
      }
    };

    read_until_no_data();

    // We multiply by 2 because while read from two logs.
    EXPECT_EQ(2 * records_break, total_read);

    EXPECT_EQ(Status::OK, reader_->syncWriteCheckpoints());

    appendSampleRecords(logid_t(1), records_total - records_break);
    appendSampleRecords(logid_t(2), records_total - records_break);

    reader = client_->createReader(2);
    store = create_store();
    reader_ = CheckpointedReaderFactory().createSyncCheckpointedReader(
        "customer", std::move(reader), std::move(store), {});

    ASSERT_EQ(0,
              reader_->startReadingFromCheckpoint(
                  logid_t(1), client_->getTailLSNSync(logid_t(1))));
    ASSERT_EQ(0,
              reader_->startReadingFromCheckpoint(
                  logid_t(2), client_->getTailLSNSync(logid_t(2))));

    read_until_no_data();

    EXPECT_EQ(2 * records_total, total_read);
  }

  void runAsyncReader(
      folly::Function<std::unique_ptr<CheckpointStore>()> create_store) {
    cluster_->waitForRecovery();

    uint32_t records_total = 100;
    uint32_t records_break = 40;

    appendSampleRecords(logid_t(1), records_break);
    appendSampleRecords(logid_t(2), records_break);

    auto reader = client_->createAsyncReader(2);
    auto store = create_store();
    async_reader_ = CheckpointedReaderFactory().createAsyncCheckpointedReader(
        "customer", std::move(reader), std::move(store), {});

    std::atomic_int32_t total_read = 0;
    folly::Baton<> done_1, done_2;

    auto record_cb = [&total_read](std::unique_ptr<DataRecord>&) {
      total_read++;
      return true;
    };

    auto done_cb = [&done_1, &done_2](logid_t log_id) {
      if (log_id == logid_t(1)) {
        done_1.post();
      } else {
        done_2.post();
      }
    };

    async_reader_->setRecordCallback(record_cb);
    async_reader_->setDoneCallback(done_cb);

    async_reader_->startReadingFromCheckpoint(
        logid_t(1),
        [](Status status) { ASSERT_EQ(Status::OK, status); },
        client_->getTailLSNSync(logid_t(1)));

    async_reader_->startReadingFromCheckpoint(
        logid_t(2),
        [](Status status) { ASSERT_EQ(Status::OK, status); },
        client_->getTailLSNSync(logid_t(2)));

    done_1.wait();
    done_1.reset();
    done_2.wait();
    done_2.reset();

    // We multiply by 2 because while read from two logs.
    EXPECT_EQ(2 * records_break, total_read);
    EXPECT_EQ(Status::OK, async_reader_->syncWriteCheckpoints());

    appendSampleRecords(logid_t(1), records_total - records_break);
    appendSampleRecords(logid_t(2), records_total - records_break);

    reader = client_->createAsyncReader(2);
    store = create_store();
    async_reader_ = CheckpointedReaderFactory().createAsyncCheckpointedReader(
        "customer", std::move(reader), std::move(store), {});

    async_reader_->setRecordCallback(record_cb);
    async_reader_->setDoneCallback(done_cb);

    async_reader_->startReadingFromCheckpoint(
        logid_t(1),
        [](Status status) { ASSERT_EQ(Status::OK, status); },
        client_->getTailLSNSync(logid_t(1)));
    async_reader_->startReadingFromCheckpoint(
        logid_t(2),
        [](Status status) { ASSERT_EQ(Status::OK, status); },
        client_->getTailLSNSync(logid_t(2)));

    done_1.wait();
    done_2.wait();
    EXPECT_EQ(2 * records_total, total_read);
  }

  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
  std::shared_ptr<Client> client_;
  std::unique_ptr<SyncCheckpointedReader> reader_;
  std::unique_ptr<AsyncCheckpointedReader> async_reader_;
  std::unique_ptr<TemporaryDirectory> temp_dir_;
  std::string store_path_;
};

TEST_F(CheckpointingIntegrationTest, FileBasedSyncReaderTest) {
  auto create_store = [this]() {
    return CheckpointStoreFactory().createFileBasedCheckpointStore(store_path_);
  };
  runSyncReader(std::move(create_store));
}

TEST_F(CheckpointingIntegrationTest, RSMBasedSyncReaderTest) {
  auto create_store = [this]() {
    return CheckpointStoreFactory().createRSMBasedCheckpointStore(
        client_, logid_t(3), std::chrono::milliseconds(500));
  };
  runSyncReader(std::move(create_store));
}

TEST_F(CheckpointingIntegrationTest, FileBasedAsyncReaderTest) {
  auto create_store = [this]() {
    return CheckpointStoreFactory().createFileBasedCheckpointStore(store_path_);
  };
  runAsyncReader(std::move(create_store));
}

TEST_F(CheckpointingIntegrationTest, RSMBasedAsyncReaderTest) {
  auto create_store = [this]() {
    return CheckpointStoreFactory().createRSMBasedCheckpointStore(
        client_, logid_t(3), std::chrono::milliseconds(500));
  };
  runAsyncReader(std::move(create_store));
}

}} // namespace facebook::logdevice
