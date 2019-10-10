/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/CheckpointedReaderBase.h"

#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/lib/checkpointing/test/MockCheckpointStore.h"
#include "logdevice/lib/checkpointing/test/MockCheckpointedReader.h"

using namespace facebook::logdevice;

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

class CheckpointedReaderBaseTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_checkpoint_store_ = std::make_unique<MockCheckpointStore>();
  }

  std::unique_ptr<MockCheckpointStore> mock_checkpoint_store_;
};

TEST_F(CheckpointedReaderBaseTest, AsyncWriteUsesCheckpointStore) {
  std::map<logid_t, lsn_t> checkpoints = {{logid_t(3), 4}, {logid_t(5), 2}};
  folly::Baton<> call_baton;
  auto callback = [&call_baton](Status status) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, updateLSN("customer", checkpoints, _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto, auto cb) {
        cb(Status::OK, CheckpointStore::Version(1), "");
      }));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  reader_base.asyncWriteCheckpoints(checkpoints, callback);
  call_baton.wait();
}

TEST_F(CheckpointedReaderBaseTest, SyncWriteRetries) {
  std::map<logid_t, lsn_t> checkpoints = {{logid_t(3), 4}, {logid_t(5), 2}};

  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", checkpoints))
      .Times(5)
      .WillRepeatedly(Return(Status::BADMSG));

  auto reader_base = MockCheckpointedReader(
      "customer", std::move(mock_checkpoint_store_), {5});
  auto status = reader_base.syncWriteCheckpoints(checkpoints);
  EXPECT_EQ(Status::BADMSG, status);
}

TEST_F(CheckpointedReaderBaseTest, SyncWriteStopsRetryingWhenOK) {
  std::map<logid_t, lsn_t> checkpoints = {{logid_t(3), 4}, {logid_t(5), 2}};

  InSequence seq;
  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", checkpoints))
      .Times(5)
      .WillRepeatedly(Return(Status::BADMSG));
  EXPECT_CALL(*mock_checkpoint_store_, updateLSNSync("customer", checkpoints))
      .Times(1)
      .WillOnce(Return(Status::OK));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  auto status = reader_base.syncWriteCheckpoints(checkpoints);
  EXPECT_EQ(Status::OK, status);
}

TEST_F(CheckpointedReaderBaseTest, AsyncRemoveCheckpointsUsesCheckpointStore) {
  std::vector<logid_t> checkpoints = {logid_t(3), logid_t(5)};
  folly::Baton<> call_baton;
  auto callback = [&call_baton](Status status) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  EXPECT_CALL(
      *mock_checkpoint_store_, removeCheckpoints("customer", checkpoints, _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto, auto cb) {
        cb(Status::OK, CheckpointStore::Version(1), "");
      }));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  reader_base.asyncRemoveCheckpoints(checkpoints, callback);
  call_baton.wait();
}

TEST_F(CheckpointedReaderBaseTest,
       AsyncRemoveAllCheckpointsUsesCheckpointStore) {
  folly::Baton<> call_baton;
  auto callback = [&call_baton](Status status) {
    EXPECT_EQ(Status::OK, status);
    call_baton.post();
  };
  EXPECT_CALL(*mock_checkpoint_store_, removeAllCheckpoints("customer", _))
      .Times(1)
      .WillOnce(Invoke([](auto, auto cb) {
        cb(Status::OK, CheckpointStore::Version(1), "");
      }));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  reader_base.asyncRemoveAllCheckpoints(callback);
  call_baton.wait();
}

TEST_F(CheckpointedReaderBaseTest, SyncRemoveCheckpointsUsesCheckpointStore) {
  std::vector<logid_t> checkpoints = {logid_t(3), logid_t(5)};
  EXPECT_CALL(
      *mock_checkpoint_store_, removeCheckpointsSync("customer", checkpoints))
      .Times(1)
      .WillOnce(Return(Status::OK));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  auto status = reader_base.syncRemoveCheckpoints(checkpoints);
  EXPECT_EQ(Status::OK, status);
}

TEST_F(CheckpointedReaderBaseTest,
       SyncRemoveAllCheckpointsUsesCheckpointStore) {
  EXPECT_CALL(*mock_checkpoint_store_, removeAllCheckpointsSync("customer"))
      .Times(1)
      .WillOnce(Return(Status::OK));

  auto reader_base =
      MockCheckpointedReader("customer", std::move(mock_checkpoint_store_), {});
  auto status = reader_base.syncRemoveAllCheckpoints();
  EXPECT_EQ(Status::OK, status);
}
