/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CircularBuffer.h"

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

struct CircularBufferTest : public ::testing::Test {
  CircularBufferTest() : buf(BUF_SIZE) {
    for (int i = 0; i < BUF_SIZE; ++i) {
      buf[i] = i;
    }
  }

  static const int BUF_SIZE = 16;
  CircularBuffer<int> buf;
};

TEST_F(CircularBufferTest, RotateByOne) {
  EXPECT_EQ(0, buf.front());
  buf.rotate();
  EXPECT_EQ(1, buf.front());
  EXPECT_EQ(2, buf[1]);
}

TEST_F(CircularBufferTest, RotateByN) {
  for (int i = 1; i < buf.size(); ++i) {
    buf.rotate(i);
    EXPECT_EQ(i, buf.front());
    buf.rotate(-i + buf.size());
    EXPECT_EQ(0, buf.front());
  }
}

TEST_F(CircularBufferTest, RotateBySize) {
  buf.rotate(buf.size());
  for (int i = 0; i < buf.size(); ++i) {
    EXPECT_EQ(i, buf[i]);
  }
}

TEST_F(CircularBufferTest, ResizeAndRotate) {
  for (int i = 0; i < buf.size() / 2; ++i) {
    EXPECT_EQ(i, buf.front());
    buf.rotate();
  }

  buf.assign(5);
  EXPECT_EQ(5, buf.size());
  for (int i = 0; i < 5; ++i) {
    buf[i] = i + 10;
  }

  EXPECT_EQ(10, buf.front());
  for (int i = 1; i < 5; ++i) {
    buf.rotate(i);
    EXPECT_EQ(i + 10, buf.front());
    buf.rotate(-i + buf.size());
    EXPECT_EQ(10, buf.front());
  }
}

}} // namespace facebook::logdevice
