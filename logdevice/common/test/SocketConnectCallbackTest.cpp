/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/SocketConnectCallback.h"

#include <gtest/gtest.h>

#include "logdevice/common/test/MockSocketAdapter.h"

using namespace ::testing;
using ::testing::_;
using namespace facebook::logdevice;

TEST(SocketConnectCallbackTest, SimpleTest) {
  SocketConnectCallback cb;
  auto fut = cb.getConnectStatus();
  cb.connectSuccess();
  EXPECT_EQ(std::move(fut).get().getType(),
            folly::AsyncSocketException::ALREADY_OPEN);
}

TEST(SocketConnectCallbackTest, ErroredOut) {
  SocketConnectCallback cb;
  auto fut = cb.getConnectStatus();
  folly::AsyncSocketException ex(
      folly::AsyncSocketException::UNKNOWN, "Unknown error occured");
  cb.connectErr(ex);
  EXPECT_EQ(
      std::move(fut).get().getType(), folly::AsyncSocketException::UNKNOWN);
}
