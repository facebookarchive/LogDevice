/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/NodeSetStateTest.h"

namespace facebook { namespace logdevice {

TEST_F(NodeSetStateTest, Basic) {
  storage_set_ = {N0, N1, N2, N3};
  auto storage_set_state = std::make_unique<MyNodeSetState>(
      storage_set_, LOG_ID, NodeSetState::HealthCheck::ENABLED);
  for (auto index : storage_set_) {
    EXPECT_EQ(NodeSetState::NotAvailableReason::NONE,
              storage_set_state->getNotAvailableReason(index));
  }

  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::STORE_DISABLED);

  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_LT(storage_set_state->getNotAvailableUntil(storage_set_[0]),
            std::chrono::steady_clock::now());
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::STORE_DISABLED),
            1);
}

TEST_F(NodeSetStateTest, StateTransitionTestWithProbingEnabled) {
  storage_set_ = {N0, N1, N2, N3};
  auto storage_set_state = std::make_unique<MyNodeSetState>(
      storage_set_, LOG_ID, NodeSetState::HealthCheck::ENABLED);

  // NONE -> NO_SPC
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::NO_SPC),
            1);

  auto until_time = storage_set_state->getNotAvailableUntil(storage_set_[0]);

  // NO_SPC -> STORE_DISABLED - Not possible
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(
      storage_set_state->getNotAvailableUntil(storage_set_[0]), until_time);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::NO_SPC),
            1);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::STORE_DISABLED),
            0);

  auto prev_time = storage_set_state->getNotAvailableUntil(storage_set_[0]);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // PROBING -> PROBING (but previous deadline has expired)
  storage_set_state->checkNotAvailableUntil(
      storage_set_[0], std::chrono::steady_clock::now());
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::PROBING);
  EXPECT_NE(
      storage_set_state->getNotAvailableUntil(storage_set_[0]), prev_time);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::PROBING),
            1);

  // PROBING -> STORE_DISABLED
  until_time = std::chrono::steady_clock::now();
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      until_time,
      NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::STORE_DISABLED),
            1);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::PROBING),
            0);

  storage_set_state->setNotAvailableUntil(
      storage_set_[1],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::OVERLOADED);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[1]),
            NodeSetState::NotAvailableReason::OVERLOADED);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::OVERLOADED),
            1);

  // Clear the state
  storage_set_state->clearNotAvailableUntil(storage_set_[0]);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NONE);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::PROBING),
            0);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::OVERLOADED),
            1);
}

TEST_F(NodeSetStateTest, StateTransitionTestWithProbingDisabled) {
  storage_set_ = {N0, N1, N2, N3};
  auto storage_set_state = std::make_unique<MyNodeSetState>(
      storage_set_, LOG_ID, NodeSetState::HealthCheck::DISABLED);

  // NONE -> NO_SPC
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::NO_SPC),
            1);

  auto until_time = storage_set_state->getNotAvailableUntil(storage_set_[0]);

  // NO_SPC -> STORE_DISABLED - Not possible
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NO_SPC);
  EXPECT_EQ(
      storage_set_state->getNotAvailableUntil(storage_set_[0]), until_time);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::NO_SPC),
            1);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::STORE_DISABLED),
            0);

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  storage_set_state->checkNotAvailableUntil(
      storage_set_[0], std::chrono::steady_clock::now());
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::NONE);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::NO_SPC),
            0);

  storage_set_state->setNotAvailableUntil(
      storage_set_[1],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::OVERLOADED);
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[1]),
            NodeSetState::NotAvailableReason::OVERLOADED);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::OVERLOADED),
            1);

  // Clear the state for index 0
  EXPECT_EQ(storage_set_state->checkNotAvailableUntil(
                storage_set_[1], std::chrono::steady_clock::now()),
            std::chrono::steady_clock::time_point::min());
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[1]),
            NodeSetState::NotAvailableReason::NONE);
  EXPECT_EQ(storage_set_state->numNotAvailableShards(
                NodeSetState::NotAvailableReason::OVERLOADED),
            0);
}

TEST_F(NodeSetStateTest, SlowStateTransitionTestWithProbingEnabled) {
  storage_set_ = {N0, N1, N2, N3};
  auto storage_set_state = std::make_unique<MyNodeSetState>(
      storage_set_, LOG_ID, NodeSetState::HealthCheck::ENABLED);

  // 1. Reach PROBING state via NONE->OVERLOADED->PROBING
  storage_set_state->setNotAvailableUntil(
      storage_set_[0],
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::OVERLOADED);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  storage_set_state->checkNotAvailableUntil(
      storage_set_[0], std::chrono::steady_clock::now());
  EXPECT_EQ(storage_set_state->getNotAvailableReason(storage_set_[0]),
            NodeSetState::NotAvailableReason::PROBING);

  // 2. now interact with SLOW state
  std::vector<std::tuple<NodeSetState::NotAvailableReason, // old
                         NodeSetState::NotAvailableReason, // new
                         bool                              // allowed?
                         >>
      T;

  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::PROBING,
                              NodeSetState::NotAvailableReason::SLOW,
                              true));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::NONE,
                              true));

  T.push_back(
      std::make_tuple(NodeSetState::NotAvailableReason::NONE,
                      NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC,
                      true));
  T.push_back(
      std::make_tuple(NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC,
                      NodeSetState::NotAvailableReason::SLOW,
                      true));
  T.push_back(
      std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                      NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC,
                      false));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::OVERLOADED,
                              false));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::NO_SPC,
                              false));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::UNROUTABLE,
                              false));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::STORE_DISABLED,
                              false));
  T.push_back(std::make_tuple(NodeSetState::NotAvailableReason::SLOW,
                              NodeSetState::NotAvailableReason::SLOW,
                              true));

  for (auto& t : T) {
    NodeSetState::NotAvailableReason old_state = std::get<0>(t);
    NodeSetState::NotAvailableReason new_state = std::get<1>(t);
    bool transition_allowed = std::get<2>(t);

    ld_info("Attempting transition from %s to %s",
            storage_set_state->reasonString(old_state),
            storage_set_state->reasonString(new_state));
    storage_set_state->setNotAvailableUntil(
        storage_set_[0], std::chrono::steady_clock::now(), new_state);
    if (transition_allowed) {
      EXPECT_EQ(
          storage_set_state->getNotAvailableReason(storage_set_[0]), new_state);
      if (new_state != NodeSetState::NotAvailableReason::NONE) {
        EXPECT_EQ(storage_set_state->numNotAvailableShards(new_state), 1);
      }

      if (old_state != NodeSetState::NotAvailableReason::NONE &&
          old_state != new_state) {
        EXPECT_EQ(storage_set_state->numNotAvailableShards(old_state), 0);
      }
    } else {
      EXPECT_EQ(
          storage_set_state->getNotAvailableReason(storage_set_[0]), old_state);
      EXPECT_EQ(storage_set_state->numNotAvailableShards(old_state), 1);

      if (old_state != NodeSetState::NotAvailableReason::NONE) {
        EXPECT_EQ(storage_set_state->numNotAvailableShards(old_state), 1);
      }
    }
  }
}

}} // namespace facebook::logdevice
