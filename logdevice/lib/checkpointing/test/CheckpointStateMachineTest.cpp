/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/CheckpointStateMachine.h"

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"

using namespace facebook::logdevice;

using apache::thrift::BinarySerializer;
using checkpointing::thrift::Checkpoint;
using checkpointing::thrift::CheckpointDelta;
using checkpointing::thrift::CheckpointState;
using checkpointing::thrift::RemoveCheckpoint;
using checkpointing::thrift::UpdateCheckpoint;

class CheckpointStateMachineTest : public ::testing::Test {
 public:
  void SetUp() override {
    state_machine_ = std::make_unique<CheckpointStateMachine>(logid_t(1));
  }

  std::unique_ptr<CheckpointStateMachine> state_machine_;
};

TEST_F(CheckpointStateMachineTest, DeserializeStateError) {
  std::string serialized_state("Incorrect state");
  Payload payload(serialized_state.c_str(), serialized_state.size());
  EXPECT_EQ(nullptr,
            state_machine_->deserializeState(
                payload, 1, std::chrono::milliseconds(0)));
}

TEST_F(CheckpointStateMachineTest, DeserializeDeltaError) {
  std::string serialized_delta("Incorrect delta");
  Payload payload(serialized_delta.c_str(), serialized_delta.size());
  EXPECT_EQ(nullptr, state_machine_->deserializeDelta(payload));
}

TEST_F(CheckpointStateMachineTest, SerializeStateHandleNullBuf) {
  CheckpointState checkpoint_state;
  auto serialized_state =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(
          checkpoint_state);
  EXPECT_EQ(serialized_state.size(),
            state_machine_->serializeState(checkpoint_state, nullptr, 0));
}

TEST_F(CheckpointStateMachineTest, SerializeDeserializeState) {
  Checkpoint checkpoint_1;
  checkpoint_1.log_lsn_map = {{4, 3}, {3, 2}};
  checkpoint_1.version = 1;
  Checkpoint checkpoint_2;
  checkpoint_2.log_lsn_map = {{2, 3}, {1, 9}};
  CheckpointState checkpoint_state;
  checkpoint_state.checkpoints = std::map<std::string, Checkpoint>(
      {{"customer1", checkpoint_1}, {"customer2", checkpoint_2}});
  checkpoint_state.version = 5;
  size_t len = state_machine_->serializeState(checkpoint_state, nullptr, 0);
  void* buf = malloc(len);
  int rv = state_machine_->serializeState(checkpoint_state, buf, len);
  EXPECT_EQ(len, rv);

  Payload payload(buf, len);
  auto ptr = state_machine_->deserializeState(
      payload, 1, std::chrono::milliseconds(0));
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(checkpoint_state, *ptr);
}

TEST_F(CheckpointStateMachineTest, DeserializeDeltaUpdate) {
  Checkpoint checkpoint;
  checkpoint.log_lsn_map = {{4, 3}, {3, 2}};
  checkpoint.version = 1;
  UpdateCheckpoint update;
  update.customer_id = "customer1";
  update.checkpoint = checkpoint;
  CheckpointDelta checkpoint_delta;
  checkpoint_delta.set_update_checkpoint(update);

  auto serialized_delta =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(
          checkpoint_delta);

  Payload payload(serialized_delta.data(), serialized_delta.size());
  auto ptr = state_machine_->deserializeDelta(payload);
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(checkpoint_delta, *ptr);
}

TEST_F(CheckpointStateMachineTest, DeserializeDeltaRemove) {
  RemoveCheckpoint remove_checkpoint;
  remove_checkpoint.customer_id = "customer1";
  CheckpointDelta checkpoint_delta;
  checkpoint_delta.set_remove_checkpoint(remove_checkpoint);

  auto serialized_delta =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(
          checkpoint_delta);

  Payload payload(serialized_delta.data(), serialized_delta.size());
  auto ptr = state_machine_->deserializeDelta(payload);
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(checkpoint_delta, *ptr);
}

TEST_F(CheckpointStateMachineTest, AppliesDeltaForUpdate) {
  Checkpoint checkpoint_1;
  checkpoint_1.log_lsn_map = {{4, 3}, {3, 2}};
  checkpoint_1.version = 1;
  Checkpoint checkpoint_2;
  checkpoint_2.log_lsn_map = {{2, 3}, {1, 9}};
  CheckpointState checkpoint_state;
  checkpoint_state.checkpoints = std::map<std::string, Checkpoint>(
      {{"customer1", checkpoint_1}, {"customer2", checkpoint_2}});
  checkpoint_state.version = 5;

  Checkpoint checkpoint;
  checkpoint.log_lsn_map = {{1, 5}, {6, 7}};
  checkpoint.version = 1;
  UpdateCheckpoint update;
  update.customer_id = "customer1";
  update.checkpoint = checkpoint;
  CheckpointDelta checkpoint_delta;
  checkpoint_delta.set_update_checkpoint(update);

  std::string failure_reason;
  int rv = state_machine_->applyDelta(checkpoint_delta,
                                      checkpoint_state,
                                      60,
                                      std::chrono::milliseconds(0),
                                      failure_reason);
  EXPECT_EQ(0, rv);
  EXPECT_EQ("", failure_reason);

  EXPECT_EQ(2, checkpoint_state.checkpoints.size());
  EXPECT_EQ(checkpoint, checkpoint_state.checkpoints["customer1"]);
  EXPECT_EQ(checkpoint_2, checkpoint_state.checkpoints["customer2"]);
  EXPECT_EQ(60, checkpoint_state.version);
}

TEST_F(CheckpointStateMachineTest, AppliesDeltaWrongDeltaType) {
  Checkpoint checkpoint_1;
  checkpoint_1.log_lsn_map = {{4, 3}, {3, 2}};
  checkpoint_1.version = 1;
  Checkpoint checkpoint_2;
  checkpoint_2.log_lsn_map = {{2, 3}, {1, 9}};
  CheckpointState checkpoint_state;
  checkpoint_state.checkpoints = std::map<std::string, Checkpoint>(
      {{"customer1", checkpoint_1}, {"customer2", checkpoint_2}});
  checkpoint_state.version = 5;

  CheckpointDelta checkpoint_delta;

  std::string failure_reason;
  int rv = state_machine_->applyDelta(checkpoint_delta,
                                      checkpoint_state,
                                      60,
                                      std::chrono::milliseconds(0),
                                      failure_reason);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ("Unknown type", failure_reason);
}

TEST_F(CheckpointStateMachineTest, DefaultStateHasVersion) {
  auto default_state = state_machine_->makeDefaultState(50);
  EXPECT_TRUE(default_state->checkpoints.empty());
  EXPECT_EQ(50, default_state->version);
}
