/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RetryHandler.h"

#include <gtest/gtest.h>

#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

TEST(RetryHandlerTest, testFirstTimeSuccess) {
  int num_calls = 0;

  EXPECT_EQ(std::make_pair(Status::OK, false),
            RetryHandler<Status>::syncRun(
                [&](size_t) {
                  num_calls++;
                  return Status::OK;
                },
                [](const Status& st) { return st != Status::OK; },
                /* max_retries= */ 10,
                /* backoff_min= */ std::chrono::milliseconds(1),
                /* backoff_max= */ std::chrono::milliseconds(10),
                /* jitter_param= */ 0));
  EXPECT_EQ(1, num_calls);
}

TEST(RetryHandlerTest, testEventualSuccess) {
  int num_calls = 0;

  EXPECT_EQ(std::make_pair(Status::OK, false),
            RetryHandler<Status>::syncRun(
                [&](size_t trial) {
                  num_calls++;
                  // Succeed in the 4th trial (zero indexed)
                  if (trial == 3) {
                    return Status::OK;
                  }
                  return Status::UNKNOWN;
                },
                [](const Status& st) { return st != Status::OK; },
                /* max_retries= */ 10,
                /* backoff_min= */ std::chrono::milliseconds(1),
                /* backoff_max= */ std::chrono::milliseconds(10),
                /* jitter_param= */ 0));
  EXPECT_EQ(4, num_calls);
}

TEST(RetryHandlerTest, testFailure) {
  int num_calls = 0;

  EXPECT_EQ(std::make_pair(Status::UNKNOWN, true),
            RetryHandler<Status>::syncRun(
                [&](size_t) {
                  num_calls++;
                  // Never succeed
                  return Status::UNKNOWN;
                },
                [](const Status& st) { return st != Status::OK; },
                /* max_retries= */ 10,
                /* backoff_min= */ std::chrono::milliseconds(1),
                /* backoff_max= */ std::chrono::milliseconds(10),
                /* jitter_param= */ 0));
  EXPECT_EQ(10, num_calls);
}

}} // namespace facebook::logdevice
