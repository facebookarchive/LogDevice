#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/include/AsyncCheckpointedReader.h"
#include "logdevice/include/CheckpointedReaderFactory.h"
#include "logdevice/include/SyncCheckpointedReader.h"
#include "logdevice/lib/checkpointing/test/MockCheckpointStore.h"
#include "logdevice/lib/test/MockAsyncReader.h"
#include "logdevice/lib/test/MockReader.h"

using namespace facebook::logdevice;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class CheckpointedReaderTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_checkpoint_store_ = std::make_unique<MockCheckpointStore>();
  }

  std::unique_ptr<MockCheckpointStore> mock_checkpoint_store_;
};

TEST_F(CheckpointedReaderTest, SyncStartReading) {
  auto log = logid_t(5);
  auto checkpoint_lsn = 10;
  EXPECT_CALL(*mock_checkpoint_store_, getLSNSync("customer", log, _))
      .Times(1)
      .WillOnce(Invoke([checkpoint_lsn](auto, auto, auto* value) {
        *value = checkpoint_lsn;
        return Status::OK;
      }));

  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, startReading(log, checkpoint_lsn + 1, _, _))
      .Times(1)
      .WillOnce(Return(0));

  auto sync_reader = CheckpointedReaderFactory().createSyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  sync_reader->startReadingFromCheckpoint(log);
}

TEST_F(CheckpointedReaderTest, SyncStartReadingCreatesNew) {
  auto log = logid_t(5);
  EXPECT_CALL(*mock_checkpoint_store_, getLSNSync("customer", log, _))
      .Times(1)
      .WillOnce(Return(Status::NOTFOUND));

  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, startReading(log, LSN_OLDEST, _, _))
      .Times(1)
      .WillOnce(Return(0));

  auto sync_reader = CheckpointedReaderFactory().createSyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  sync_reader->startReadingFromCheckpoint(log);
}

TEST_F(CheckpointedReaderTest, SyncStartReadingFailedCheckpointGet) {
  auto log = logid_t(5);
  EXPECT_CALL(*mock_checkpoint_store_, getLSNSync("customer", log, _))
      .Times(1)
      .WillOnce(Return(Status::SHUTDOWN));

  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, startReading(_, _, _, _)).Times(0);

  auto sync_reader = CheckpointedReaderFactory().createSyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  EXPECT_EQ(-1, sync_reader->startReadingFromCheckpoint(log));
  EXPECT_EQ(Status::SHUTDOWN, err);
}

TEST_F(CheckpointedReaderTest, SyncStartReadingFailed) {
  auto log = logid_t(5);
  EXPECT_CALL(*mock_checkpoint_store_, getLSNSync("customer", log, _))
      .Times(1)
      .WillOnce(Return(Status::NOTFOUND));

  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, startReading(log, LSN_OLDEST, _, _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto, auto, auto) {
        err = Status::NOTFOUND;
        return -1;
      }));

  auto sync_reader = CheckpointedReaderFactory().createSyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  EXPECT_EQ(-1, sync_reader->startReadingFromCheckpoint(log));
  EXPECT_EQ(Status::NOTFOUND, err);
}

TEST_F(CheckpointedReaderTest, AsyncStartReading) {
  auto log = logid_t(5);
  auto checkpoint_lsn = 10;
  folly::Baton<> call_baton;
  auto scb = [&call_baton](Status status) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, getLSN("customer", log, _))
      .Times(1)
      .WillOnce(Invoke([checkpoint_lsn](auto, auto, auto cb) {
        cb(Status::OK, checkpoint_lsn);
      }));

  auto mock_reader = std::make_unique<MockAsyncReader>();
  EXPECT_CALL(*mock_reader, startReading(log, checkpoint_lsn + 1, _, _))
      .Times(1)
      .WillOnce(Return(0));

  auto async_reader = CheckpointedReaderFactory().createAsyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  async_reader->startReadingFromCheckpoint(log, std::move(scb));
  call_baton.wait();
}

TEST_F(CheckpointedReaderTest, AsyncStartReadingCreatesNew) {
  auto log = logid_t(5);
  folly::Baton<> call_baton;
  auto scb = [&call_baton](Status status) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, getLSN("customer", log, _))
      .Times(1)
      .WillOnce(Invoke(
          [](auto, auto, auto cb) { cb(Status::NOTFOUND, LSN_INVALID); }));

  auto mock_reader = std::make_unique<MockAsyncReader>();
  EXPECT_CALL(*mock_reader, startReading(log, LSN_OLDEST, _, _))
      .Times(1)
      .WillOnce(Return(0));

  auto async_reader = CheckpointedReaderFactory().createAsyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  async_reader->startReadingFromCheckpoint(log, std::move(scb));
  call_baton.wait();
}

TEST_F(CheckpointedReaderTest, AsyncStartReadingFailedCheckpointGet) {
  auto log = logid_t(5);
  folly::Baton<> call_baton;
  auto scb = [&call_baton](Status status) {
    EXPECT_EQ(Status::SHUTDOWN, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, getLSN("customer", log, _))
      .Times(1)
      .WillOnce(Invoke(
          [](auto, auto, auto cb) { cb(Status::SHUTDOWN, LSN_INVALID); }));

  auto mock_reader = std::make_unique<MockAsyncReader>();
  EXPECT_CALL(*mock_reader, startReading(log, _, _, _)).Times(0);

  auto async_reader = CheckpointedReaderFactory().createAsyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  async_reader->startReadingFromCheckpoint(log, std::move(scb));
  call_baton.wait();
}

TEST_F(CheckpointedReaderTest, AsyncStartReadingFailed) {
  auto log = logid_t(5);
  folly::Baton<> call_baton;
  auto scb = [&call_baton](Status status) {
    EXPECT_EQ(Status::NOTFOUND, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, getLSN("customer", log, _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto, auto cb) { cb(Status::OK, 39); }));

  auto mock_reader = std::make_unique<MockAsyncReader>();
  EXPECT_CALL(*mock_reader, startReading(log, 40, _, _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto, auto, auto) {
        err = Status::NOTFOUND;
        return -1;
      }));

  auto async_reader = CheckpointedReaderFactory().createAsyncCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  async_reader->startReadingFromCheckpoint(log, std::move(scb));
  call_baton.wait();
}
