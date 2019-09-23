/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/VersionedConfigStore.h"
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
    // TODO: Change the extract version function when proper versioning is
    // added.
    auto extract_version_function = [](folly::StringPiece) {
      return VersionedConfigStore::version_t(1);
    };

    mock_versioned_config_store_ =
        std::make_unique<MockVersionedConfigStore>(extract_version_function);
    cb_ = [](Status, CheckpointStore::Version, std::string) {};
  }

  std::unique_ptr<MockVersionedConfigStore> mock_versioned_config_store_;
  CheckpointStore::UpdateCallback cb_;
};

TEST_F(CheckpointStoreImplTest, UpdateEmptyStore) {
  Checkpoint correct;
  correct.log_lsn_map[1] = 2;

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

  EXPECT_CALL(
      *mock_versioned_config_store_, readModifyWriteConfig("customer2", _, _))
      .Times(2)
      .WillRepeatedly(Invoke([correct](auto, auto mcb, auto cb) mutable {
        auto before_update = correct;
        before_update.log_lsn_map[3] = 9;
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
