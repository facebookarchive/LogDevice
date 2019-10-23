/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/replicated_state_machine/KeyValueStoreStateMachine.h"

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "logdevice/common/ThriftCodec.h"

using namespace facebook::logdevice;

using apache::thrift::BinarySerializer;
using replicated_state_machine::thrift::KeyValueStoreDelta;
using replicated_state_machine::thrift::KeyValueStoreState;
using replicated_state_machine::thrift::RemoveValue;
using replicated_state_machine::thrift::UpdateValue;

class KeyValueStoreStateMachineTest : public ::testing::Test {
 public:
  void SetUp() override {
    state_machine_ = std::make_unique<KeyValueStoreStateMachine>(logid_t(1));
  }

  std::unique_ptr<KeyValueStoreStateMachine> state_machine_;
};

TEST_F(KeyValueStoreStateMachineTest, DeserializeStateError) {
  std::string serialized_state("Incorrect state");
  Payload payload(serialized_state.c_str(), serialized_state.size());
  EXPECT_EQ(nullptr,
            state_machine_->deserializeState(
                payload, 1, std::chrono::milliseconds(0)));
}

TEST_F(KeyValueStoreStateMachineTest, DeserializeDeltaError) {
  std::string serialized_delta("Incorrect delta");
  Payload payload(serialized_delta.c_str(), serialized_delta.size());
  EXPECT_EQ(nullptr, state_machine_->deserializeDelta(payload));
}

TEST_F(KeyValueStoreStateMachineTest, SerializeStateHandleNullBuf) {
  KeyValueStoreState state;
  auto serialized_state =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(state);
  EXPECT_EQ(serialized_state.size(),
            state_machine_->serializeState(state, nullptr, 0));
}

TEST_F(KeyValueStoreStateMachineTest, SerializeDeserializeState) {
  std::string value_1 = "abc";
  std::string value_2 = "cde";
  KeyValueStoreState state;
  state.store = std::map<std::string, std::string>(
      {{"customer1", value_1}, {"customer2", value_2}});
  state.version = 5;
  size_t len = state_machine_->serializeState(state, nullptr, 0);
  void* buf = malloc(len);
  int rv = state_machine_->serializeState(state, buf, len);
  EXPECT_EQ(len, rv);

  Payload payload(buf, len);
  auto ptr = state_machine_->deserializeState(
      payload, 1, std::chrono::milliseconds(0));
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(state, *ptr);
}

TEST_F(KeyValueStoreStateMachineTest, DeserializeDeltaUpdate) {
  UpdateValue update;
  update.key = "customer1";
  update.value = "abc";
  KeyValueStoreDelta delta;
  delta.set_update_value(update);

  auto serialized_delta =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

  Payload payload(serialized_delta.data(), serialized_delta.size());
  auto ptr = state_machine_->deserializeDelta(payload);
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(delta, *ptr);
}

TEST_F(KeyValueStoreStateMachineTest, DeserializeDeltaRemove) {
  RemoveValue remove_value;
  remove_value.key = "customer1";
  KeyValueStoreDelta delta;
  delta.set_remove_value(remove_value);

  auto serialized_delta =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(delta);

  Payload payload(serialized_delta.data(), serialized_delta.size());
  auto ptr = state_machine_->deserializeDelta(payload);
  ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(delta, *ptr);
}

TEST_F(KeyValueStoreStateMachineTest, AppliesDeltaForUpdate) {
  std::string value_1 = "abc";
  std::string value_2 = "def";
  KeyValueStoreState state;
  state.store = std::map<std::string, std::string>(
      {{"customer1", value_1}, {"customer2", value_2}});
  state.version = 5;

  std::string value_3 = "ghi";
  UpdateValue update;
  update.key = "customer1";
  update.value = value_3;
  KeyValueStoreDelta delta;
  delta.set_update_value(update);

  std::string failure_reason;
  int rv = state_machine_->applyDelta(
      delta, state, 60, std::chrono::milliseconds(0), failure_reason);
  EXPECT_EQ(0, rv);
  EXPECT_EQ("", failure_reason);

  EXPECT_EQ(2, state.store.size());
  EXPECT_EQ(value_3, state.store["customer1"]);
  EXPECT_EQ(value_2, state.store["customer2"]);
  EXPECT_EQ(60, state.version);
}

TEST_F(KeyValueStoreStateMachineTest, AppliesDeltaWrongDeltaType) {
  std::string value_1 = "abc";
  std::string value_2 = "def";
  KeyValueStoreState state;
  state.store = std::map<std::string, std::string>(
      {{"customer1", value_1}, {"customer2", value_2}});
  state.version = 5;

  KeyValueStoreDelta delta;

  std::string failure_reason;
  int rv = state_machine_->applyDelta(
      delta, state, 60, std::chrono::milliseconds(0), failure_reason);
  EXPECT_EQ(-1, rv);
  EXPECT_EQ("Unknown type", failure_reason);
}

TEST_F(KeyValueStoreStateMachineTest, DefaultStateHasVersion) {
  auto default_state = state_machine_->makeDefaultState(50);
  EXPECT_TRUE(default_state->store.empty());
  EXPECT_EQ(50, default_state->version);
}
