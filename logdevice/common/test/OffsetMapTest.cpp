/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/OffsetMap.h"

#include <cstring>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/RecordOffset.h"

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
    offset_map_writer.setCounter(BYTE_OFFSET, i % 10);
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
    ASSERT_EQ(offset_map_reader.getCounter(BYTE_OFFSET), i % 10);
    ASSERT_EQ(counter_size[i], nbytes);
    n_read += nbytes;
  }
  ASSERT_EQ(written, n_read);
}

TEST(OffsetMapTest, Operators) {
  OffsetMap om1, om2, result, result_test;
  result_test.setCounter(BYTE_OFFSET, 8);
  result = OffsetMap::mergeOffsets(om1, om2);
  ASSERT_EQ(result == om1, true);
  om1.setCounter(BYTE_OFFSET, 4);
  om2.setCounter(BYTE_OFFSET, 4);
  result = OffsetMap::mergeOffsets(om1, om2);
  ASSERT_EQ(result == result_test, true);
  result = OffsetMap::mergeOffsets(result, om1);
  ASSERT_NE(result == result_test, true);
}

TEST(OffsetMapTest, AtomicTest) {
  AtomicOffsetMap atomic_offset_map;
  OffsetMap offset_map_1, offset_map_2;
  offset_map_1.setCounter(1, 10);
  offset_map_1.setCounter(2, 20);
  offset_map_2.setCounter(3, 30);

  int n_loop = 10;

  auto add_offsets = [&atomic_offset_map,
                      &n_loop](const OffsetMap& offset_map) {
    for (int i = 0; i < n_loop; ++i) {
      atomic_offset_map.fetchAdd(offset_map);
    }
  };

  std::thread thread1(add_offsets, offset_map_1);
  std::thread thread2(add_offsets, offset_map_1);
  std::thread thread3(add_offsets, offset_map_2);
  std::thread thread4(add_offsets, offset_map_2);

  thread1.join();
  thread2.join();
  thread3.join();
  thread4.join();

  OffsetMap result =
      OffsetMap::mergeOffsets(offset_map_1 * 2, offset_map_2 * 2) * n_loop;

  ASSERT_EQ(atomic_offset_map.load(), result);
}

} // namespace
