/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <memory>

#include "logdevice/common/OffsetMap.h"

namespace {

using namespace facebook::logdevice;

class OffsetMapTest : public ::testing::Test {
 public:
  OffsetMapTest() {
    dbg::assertOnData = true;
  }
};

TEST(OffsetMapTest, BasicSerialization) {
  const size_t n_counters = 99;
  const size_t max_len = 1024 * 1024;
  std::unique_ptr<char[]> buf1(new char[max_len]);
  std::vector<size_t> counter_size(n_counters, 0);
  size_t written = 0;

  for (int i = 0; i < n_counters; ++i) {
    OffsetMap offset_map_writer;
    offset_map_writer.setCounter(
        CounterType::BYTE_OFFSET, static_cast<uint64_t>(i % 10));
    counter_size[i] =
        offset_map_writer.serialize(buf1.get() + written, max_len - written);
    ASSERT_GT(counter_size[i], 0);
    written += counter_size[i];
  }

  ld_info("Wrote %lu records of %lu bytes.", n_counters, written);

  size_t n_read = 0;
  for (int i = 0; i < n_counters; ++i) {
    OffsetMap offset_map_reader;
    int nbytes =
        offset_map_reader.deserialize({buf1.get() + n_read, max_len - n_read});
    ASSERT_EQ(offset_map_reader.getCounter(CounterType::BYTE_OFFSET),
              (uint64_t)i % 10);
    ASSERT_EQ(counter_size[i], nbytes);
    n_read += nbytes;
  }
  ASSERT_EQ(written, n_read);
}

TEST(OffsetMapTest, Operators) {
  OffsetMap om1, om2, result, result_test;
  result_test.setCounter(CounterType::BYTE_OFFSET, 8);
  result = om1 + om2;
  ASSERT_EQ(result == om1, true);
  om1.setCounter(CounterType::BYTE_OFFSET, 4);
  om2.setCounter(CounterType::BYTE_OFFSET, 4);
  result = om1 + om2;
  ASSERT_EQ(result == result_test, true);
  result += om1;
  ASSERT_NE(result == result_test, true);
}

} // namespace
