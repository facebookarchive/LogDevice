/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/common/test/InMemVersionedConfigStore.h"
#include "logdevice/common/test/MockVersionedConfigStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/checkpointing/if/gen-cpp2/Checkpoint_types.h"

using namespace facebook::logdevice;

using apache::thrift::BinarySerializer;
using checkpointing::thrift::Checkpoint;
using ::testing::_;
using ::testing::Invoke;

class CheckpointStoreImplTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_versioned_config_store_ = std::make_unique<MockVersionedConfigStore>(
        CheckpointStoreImpl::extractVersion);
    in_mem_versioned_config_store_ =
        std::make_unique<InMemVersionedConfigStore>(
            CheckpointStoreImpl::extractVersion);
    cb_ = [](Status) {};
  }

  std::unique_ptr<MockVersionedConfigStore> mock_versioned_config_store_;
  std::unique_ptr<InMemVersionedConfigStore> in_mem_versioned_config_store_;
  CheckpointStore::StatusCallback cb_;
};

TEST_F(CheckpointStoreImplTest, GetLSN) {
  Checkpoint checkpoint;
  checkpoint.log_lsn_map = {{1, 5}, {2, 7}, {5, 9}};
  EXPECT_CALL(*mock_versioned_config_store_, getLatestConfig("customer", _))
      .Times(6)
      .WillRepeatedly(Invoke([checkpoint](auto, auto cb) {
        cb(Status::OK, ThriftCodec::serialize<BinarySerializer>(checkpoint));
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  for (auto [log_id, lsn] : checkpoint.log_lsn_map) {
    lsn_t value_out;
    auto status =
        checkpointStore->getLSNSync("customer", logid_t(log_id), &value_out);
    EXPECT_EQ(Status::OK, status);
    EXPECT_EQ(lsn, value_out);

    folly::Baton<> call_baton;
    auto cb = [lsn = lsn, &call_baton](Status status, lsn_t value) {
      EXPECT_EQ(Status::OK, status);
      EXPECT_EQ(lsn, value);
      call_baton.post();
    };
    checkpointStore->getLSN("customer", logid_t(log_id), cb);
    call_baton.wait();
  }
}

TEST_F(CheckpointStoreImplTest, GetHandleInvalidCheckpoint) {
  Checkpoint checkpoint;
  checkpoint.log_lsn_map = {{1, 5}, {2, 7}, {5, 9}};
  EXPECT_CALL(*mock_versioned_config_store_, getLatestConfig("customer", _))
      .Times(2)
      .WillRepeatedly(Invoke([checkpoint](auto, auto cb) {
        cb(Status::OK, "IncorrectSerializedThrift");
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  lsn_t value_out;
  auto status = checkpointStore->getLSNSync("customer", logid_t(1), &value_out);
  EXPECT_EQ(Status::BADMSG, status);

  folly::Baton<> call_baton;
  auto cb = [&call_baton](Status status, lsn_t) {
    EXPECT_EQ(Status::BADMSG, status);
    call_baton.post();
  };
  checkpointStore->getLSN("customer", logid_t(4), cb);
  call_baton.wait();
}

TEST_F(CheckpointStoreImplTest, GetHandleMissingLog) {
  Checkpoint checkpoint;
  checkpoint.log_lsn_map = {{1, 5}, {2, 7}, {5, 9}};
  EXPECT_CALL(*mock_versioned_config_store_, getLatestConfig("customer", _))
      .Times(2)
      .WillRepeatedly(Invoke([checkpoint](auto, auto cb) {
        cb(Status::OK, ThriftCodec::serialize<BinarySerializer>(checkpoint));
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  lsn_t value_out;
  auto status = checkpointStore->getLSNSync("customer", logid_t(3), &value_out);
  EXPECT_EQ(Status::NOTFOUND, status);

  auto cb = [](Status status, lsn_t) { EXPECT_EQ(Status::NOTFOUND, status); };
  checkpointStore->getLSN("customer", logid_t(3), cb);
}

TEST_F(CheckpointStoreImplTest, UpdateEmptyStore) {
  Checkpoint correct;
  correct.log_lsn_map[1] = 2;
  correct.version = 1;

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) {
        auto [status, value] = mcb(folly::none);
        EXPECT_EQ(status, Status::OK);
        auto value_thrift =
            ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
                Slice::fromString(value));
        ASSERT_NE(nullptr, value_thrift);
        EXPECT_EQ(correct, *value_thrift);
        cb(status, CheckpointStore::Version(1), "");
      }));
  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  auto status = checkpointStore->updateLSNSync("customer", logid_t(1), 2);
  EXPECT_EQ(Status::OK, status);
  checkpointStore->updateLSN("customer", logid_t(1), 2, std::move(cb_));
}

TEST_F(CheckpointStoreImplTest, UpdateWhenMultipleValues) {
  Checkpoint correct;
  correct.log_lsn_map = {{1, 2}, {2, 5}, {3, 7}, {2, 3}};
  correct.version = 4;

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer2", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) mutable {
        auto before_update = correct;
        before_update.log_lsn_map[3] = 9;
        before_update.version = 3;
        auto [status, value] =
            mcb(ThriftCodec::serialize<BinarySerializer>(before_update));
        EXPECT_EQ(status, Status::OK);
        auto value_thrift =
            ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
                Slice::fromString(value));
        ASSERT_NE(nullptr, value_thrift);
        EXPECT_EQ(correct, *value_thrift);
        cb(status, CheckpointStore::Version(1), "");
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  auto status = checkpointStore->updateLSNSync("customer2", logid_t(3), 7);
  EXPECT_EQ(Status::OK, status);
  checkpointStore->updateLSN("customer2", logid_t(3), 7, std::move(cb_));
}

TEST_F(CheckpointStoreImplTest, UpdateHandleIncorrectValue) {
  Checkpoint correct;

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer3", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) {
        auto [status, value] = mcb(std::string("IncorrectSerializedThrift"));
        EXPECT_EQ(Status::BADMSG, status);
        cb(status, CheckpointStore::Version(1), "");
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  auto status = checkpointStore->updateLSNSync("customer3", logid_t(3), 7);
  EXPECT_EQ(Status::BADMSG, status);
  checkpointStore->updateLSN("customer3", logid_t(3), 7, std::move(cb_));
}

TEST_F(CheckpointStoreImplTest, UpdateAndGetWithInMemVersionedConfigStore) {
  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(in_mem_versioned_config_store_));

  checkpointStore->updateLSNSync(("customer1"), logid_t(1), 2);
  checkpointStore->updateLSNSync(("customer1"), logid_t(2), 5);
  checkpointStore->updateLSNSync(("customer1"), logid_t(3), 2);
  checkpointStore->updateLSNSync(("customer1"), logid_t(1), 1);
  checkpointStore->updateLSNSync(("customer1"), logid_t(1), 4);
  checkpointStore->updateLSNSync(("customer2"), logid_t(2), 2);
  checkpointStore->updateLSNSync(("customer2"), logid_t(3), 3);
  checkpointStore->updateLSNSync(("customer2"), logid_t(3), 4);
  checkpointStore->updateLSNSync(("customer2"), logid_t(4), 1);

  lsn_t value;
  auto status = checkpointStore->getLSNSync("customer1", logid_t(1), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(4, value);

  status = checkpointStore->getLSNSync("customer1", logid_t(2), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(5, value);

  status = checkpointStore->getLSNSync("customer1", logid_t(3), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(2, value);

  status = checkpointStore->getLSNSync("customer2", logid_t(2), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(2, value);

  status = checkpointStore->getLSNSync("customer2", logid_t(3), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(4, value);

  status = checkpointStore->getLSNSync("customer2", logid_t(4), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(1, value);

  std::map<logid_t, lsn_t> new_entries = {
      {logid_t(1), 5}, {logid_t(2), 3}, {logid_t(4), 6}};
  checkpointStore->updateLSNSync("customer1", new_entries);
  status = checkpointStore->getLSNSync("customer1", logid_t(1), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(5, value);

  status = checkpointStore->getLSNSync("customer1", logid_t(2), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(3, value);

  status = checkpointStore->getLSNSync("customer1", logid_t(3), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(2, value);

  status = checkpointStore->getLSNSync("customer1", logid_t(4), &value);
  ASSERT_EQ(Status::OK, status);
  ASSERT_EQ(6, value);

  checkpointStore->removeCheckpointsSync("customer1", {logid_t(2), logid_t(4)});

  status = checkpointStore->getLSNSync("customer1", logid_t(1), &value);
  ASSERT_EQ(Status::OK, status);
  status = checkpointStore->getLSNSync("customer1", logid_t(2), &value);
  ASSERT_EQ(Status::NOTFOUND, status);
  status = checkpointStore->getLSNSync("customer1", logid_t(3), &value);
  ASSERT_EQ(Status::OK, status);
  status = checkpointStore->getLSNSync("customer1", logid_t(4), &value);
  ASSERT_EQ(Status::NOTFOUND, status);

  checkpointStore->removeAllCheckpointsSync("customer2");
  status = checkpointStore->getLSNSync("customer2", logid_t(2), &value);
  ASSERT_EQ(Status::NOTFOUND, status);
  status = checkpointStore->getLSNSync("customer2", logid_t(3), &value);
  ASSERT_EQ(Status::NOTFOUND, status);
  status = checkpointStore->getLSNSync("customer2", logid_t(4), &value);
  ASSERT_EQ(Status::NOTFOUND, status);
}

TEST_F(CheckpointStoreImplTest, RemoveSomeCheckpoints) {
  Checkpoint correct;
  correct.log_lsn_map = {{1, 2}, {2, 5}, {3, 7}, {2, 3}};
  correct.version = 2;

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) {
        auto before_remove = correct;
        before_remove.log_lsn_map[5] = 8;
        before_remove.log_lsn_map[7] = 1;
        before_remove.version = 1;
        auto [status, value] =
            mcb(ThriftCodec::serialize<BinarySerializer>(before_remove));
        EXPECT_EQ(status, Status::OK);
        auto value_thrift =
            ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
                Slice::fromString(value));
        ASSERT_NE(nullptr, value_thrift);
        EXPECT_EQ(correct, *value_thrift);
        cb(status, CheckpointStore::Version(1), "");
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  checkpointStore->removeCheckpoints(
      "customer", {logid_t(5), logid_t(7)}, std::move(cb_));
  auto status = checkpointStore->removeCheckpointsSync(
      "customer", {logid_t(5), logid_t(7)});
  EXPECT_EQ(Status::OK, status);
}

TEST_F(CheckpointStoreImplTest, RemoveAllCheckpoints) {
  Checkpoint correct;
  correct.version = 4;

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) {
        Checkpoint before_remove;
        before_remove.log_lsn_map = {{1, 2}, {2, 5}, {3, 7}, {2, 3}};
        before_remove.version = 3;
        auto [status, value] =
            mcb(ThriftCodec::serialize<BinarySerializer>(before_remove));
        EXPECT_EQ(status, Status::OK);
        auto value_thrift =
            ThriftCodec::deserialize<BinarySerializer, Checkpoint>(
                Slice::fromString(value));
        ASSERT_NE(nullptr, value_thrift);
        EXPECT_EQ(correct, *value_thrift);
        cb(status, CheckpointStore::Version(1), "");
      }));

  auto checkpointStore = std::make_unique<CheckpointStoreImpl>(
      std::move(mock_versioned_config_store_));

  checkpointStore->removeAllCheckpoints("customer", std::move(cb_));
  auto status = checkpointStore->removeAllCheckpointsSync("customer");
  EXPECT_EQ(Status::OK, status);
}

TEST_F(CheckpointStoreImplTest, ExtractVersion) {
  auto version = CheckpointStoreImpl::extractVersion("Incorrect thrift");
  EXPECT_EQ(folly::none, version);

  Checkpoint checkpoint;
  checkpoint.version = 5;
  auto serialized_thrift = ThriftCodec::serialize<BinarySerializer>(checkpoint);
  version = CheckpointStoreImpl::extractVersion(serialized_thrift);
  ASSERT_NE(folly::none, version);
  EXPECT_EQ(CheckpointStore::Version(5), version.value());
}
