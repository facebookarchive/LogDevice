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

  std::unique_ptr<DataRecord> simpleDataRecord(logid_t log_id, lsn_t lsn) {
    auto record = std::make_unique<DataRecord>();
    record->logid = log_id;
    record->attrs.lsn = lsn;
    return record;
  }

  GapRecord simpleGapRecord(logid_t log_id, lsn_t lsn) {
    GapRecord record;
    record.logid = log_id;
    record.hi = lsn;
    return record;
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

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  EXPECT_EQ(-1, sync_reader->startReadingFromCheckpoint(log));
  EXPECT_EQ(Status::NOTFOUND, err);
}

TEST_F(CheckpointedReaderTest, SyncReadSavesLSN) {
  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, read(_, _, _))
      .Times(1)
      .WillOnce(Invoke([&](auto, auto* records, auto) {
        records->push_back(std::move(simpleDataRecord(logid_t(3), 7)));
        records->push_back(std::move(simpleDataRecord(logid_t(4), 6)));
        records->push_back(std::move(simpleDataRecord(logid_t(3), 5)));
        records->push_back(std::move(simpleDataRecord(logid_t(4), 9)));
        return 0;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(2, checkpoints.size());
        EXPECT_EQ(7, checkpoints[logid_t(3)]);
        EXPECT_EQ(9, checkpoints[logid_t(4)]);
        return Status::OK;
      }));

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  std::vector<std::unique_ptr<DataRecord>> records;
  sync_reader->read(100, &records, nullptr);
  sync_reader->syncWriteCheckpoints();
  EXPECT_EQ(4, records.size());
}

TEST_F(CheckpointedReaderTest, SyncReadSavesLSNForGaps) {
  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, read(_, _, _))
      .Times(1)
      .WillOnce(Invoke([&](auto, auto, auto* gap) {
        *gap = simpleGapRecord(logid_t(3), 5);
        return -1;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(1, checkpoints.size());
        EXPECT_EQ(5, checkpoints[logid_t(3)]);
        return Status::OK;
      }));

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  GapRecord gap;
  sync_reader->read(100, {}, &gap);
  sync_reader->syncWriteCheckpoints();
}

TEST_F(CheckpointedReaderTest, SyncReadDoesntSaveLSNMAXForGaps) {
  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, read(_, _, _))
      .Times(1)
      .WillOnce(Invoke([&](auto, auto, auto* gap) {
        *gap = simpleGapRecord(logid_t(3), LSN_MAX);
        return -1;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(0, checkpoints.size());
        return Status::OK;
      }));

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  GapRecord gap;
  sync_reader->read(100, nullptr, &gap);
  sync_reader->syncWriteCheckpoints();
}

TEST_F(CheckpointedReaderTest, AsyncReadSavesLSN) {
  auto mock_reader = std::make_unique<MockAsyncReader>();
  std::function<bool(std::unique_ptr<DataRecord>&)> record_callback;
  EXPECT_CALL(*mock_reader, setRecordCallback(_))
      .Times(1)
      .WillOnce(Invoke([&](auto callback) {
        record_callback = callback;
        return true;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(2, checkpoints.size());
        EXPECT_EQ(7, checkpoints[logid_t(3)]);
        EXPECT_EQ(9, checkpoints[logid_t(4)]);
        return Status::OK;
      }));

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  auto cb = [](auto&) { return true; };
  async_reader->setRecordCallback(cb);
  auto record = simpleDataRecord(logid_t(3), 7);
  record_callback(record);
  record = simpleDataRecord(logid_t(4), 6);
  record_callback(record);
  record = simpleDataRecord(logid_t(3), 5);
  record_callback(record);
  record = simpleDataRecord(logid_t(4), 9);
  record_callback(record);
  async_reader->syncWriteCheckpoints(std::vector<logid_t>());
}

TEST_F(CheckpointedReaderTest, AsyncReadSavesLSNforGaps) {
  auto mock_reader = std::make_unique<MockAsyncReader>();
  std::function<bool(const GapRecord&)> gap_callback;
  EXPECT_CALL(*mock_reader, setGapCallback(_))
      .Times(1)
      .WillOnce(Invoke([&](auto callback) {
        gap_callback = callback;
        return true;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(2, checkpoints.size());
        EXPECT_EQ(6, checkpoints[logid_t(4)]);
        EXPECT_EQ(9, checkpoints[logid_t(5)]);
        return Status::OK;
      }));

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  auto cb = [](auto&) { return true; };
  async_reader->setGapCallback(cb);
  auto gap = simpleGapRecord(logid_t(3), LSN_MAX);
  gap_callback(gap);
  gap = simpleGapRecord(logid_t(4), 6);
  gap_callback(gap);
  gap = simpleGapRecord(logid_t(4), LSN_MAX);
  gap_callback(gap);
  gap = simpleGapRecord(logid_t(5), 9);
  gap_callback(gap);
  gap = simpleGapRecord(logid_t(5), 3);
  gap_callback(gap);
  async_reader->syncWriteCheckpoints(std::vector<logid_t>());
}

TEST_F(CheckpointedReaderTest, SyncErasesWhenStartReading) {
  auto mock_reader = std::make_unique<MockReader>();
  EXPECT_CALL(*mock_reader, read(_, _, _))
      .Times(1)
      .WillOnce(Invoke([&](auto, auto* records, auto) {
        records->push_back(std::move(simpleDataRecord(logid_t(3), 7)));
        records->push_back(std::move(simpleDataRecord(logid_t(4), 6)));
        records->push_back(std::move(simpleDataRecord(logid_t(3), 5)));
        records->push_back(std::move(simpleDataRecord(logid_t(4), 9)));
        return 0;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(1, checkpoints.size());
        EXPECT_EQ(7, checkpoints[logid_t(3)]);
        return Status::OK;
      }));

  auto sync_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  std::vector<std::unique_ptr<DataRecord>> records;
  sync_reader->read(100, &records, nullptr);
  sync_reader->startReading(logid_t(4), LSN_OLDEST);
  sync_reader->syncWriteCheckpoints(std::vector<logid_t>());
  EXPECT_EQ(4, records.size());
}

TEST_F(CheckpointedReaderTest, AsyncErasesWhenStartReading) {
  auto mock_reader = std::make_unique<MockAsyncReader>();
  EXPECT_CALL(*mock_reader, setRecordCallback(_))
      .Times(1)
      .WillOnce(Invoke([&](auto callback) {
        auto record = simpleDataRecord(logid_t(3), 7);
        callback(record);
        record = simpleDataRecord(logid_t(4), 6);
        callback(record);
        record = simpleDataRecord(logid_t(3), 5);
        callback(record);
        record = simpleDataRecord(logid_t(4), 9);
        callback(record);
        return true;
      }));

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto checkpoints) {
        EXPECT_EQ(1, checkpoints.size());
        EXPECT_EQ(7, checkpoints[logid_t(3)]);
        return Status::OK;
      }));

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  auto cb = [](auto&) { return true; };
  async_reader->setRecordCallback(cb);
  async_reader->startReading(logid_t(4), LSN_OLDEST);
  async_reader->syncWriteCheckpoints(std::vector<logid_t>());
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

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
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

  auto async_reader = CheckpointedReaderFactory().createCheckpointedReader(
      "customer",
      std::move(mock_reader),
      std::move(mock_checkpoint_store_),
      {});

  async_reader->startReadingFromCheckpoint(log, std::move(scb));
  call_baton.wait();
}
