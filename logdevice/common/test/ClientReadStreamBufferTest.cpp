/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/client_read_stream/ClientReadStreamBuffer.h"

#include <gtest/gtest.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/client_read_stream/ClientReadStreamCircularBuffer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamOrderedMapBuffer.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

struct Collector {
  bool operator()(lsn_t lsn, ClientReadStreamRecordState& /*record*/) {
    ld_info("%s", lsn_to_string(lsn).c_str());
    collected.push_back(lsn);
    return true;
  }
  std::vector<lsn_t> collected;
};

#define ASSERT_COLLECTED(_collector_, ...)                                 \
  {                                                                        \
    ASSERT_EQ(std::vector<lsn_t>({__VA_ARGS__}), (_collector_).collected); \
    (_collector_).collected.clear();                                       \
  }

class ClientReadStreamBufferTest
    : public ::testing::TestWithParam<ClientReadStreamBufferType> {
 public:
  void SetUp() override {
    if (GetParam() == ClientReadStreamBufferType::CIRCULAR) {
      buf = std::make_unique<ClientReadStreamCircularBuffer>(10, 100);
    } else {
      buf = std::make_unique<ClientReadStreamOrderedMapBuffer>(10, 100);
    }
  }
  std::unique_ptr<ClientReadStreamBuffer> buf;
};

TEST_P(ClientReadStreamBufferTest, ForEachForward) {
  buf->createOrGet(lsn_t{100});
  buf->createOrGet(lsn_t{101});
  buf->createOrGet(lsn_t{102});
  Collector collector;
  buf->forEach(100, 102, std::ref(collector));
  ASSERT_COLLECTED(collector, 100, 101, 102);
  ASSERT_TRUE(true);
}

TEST_P(ClientReadStreamBufferTest, ForEachWithHoles) {
  // holes are only supported in ordered map
  if (GetParam() != ClientReadStreamBufferType::ORDERED_MAP) {
    return;
  }
  auto reset_buffer = [&]() {
    buf->createOrGet(lsn_t{100});
    buf->createOrGet(lsn_t{101});
    buf->createOrGet(lsn_t{102});
    buf->createOrGet(lsn_t{105});
    buf->createOrGet(lsn_t{106});
  };
  Collector collector;
  reset_buffer();
  buf->forEach(100, 102, std::ref(collector));
  ASSERT_COLLECTED(collector, 100, 101, 102);
  reset_buffer();
  buf->forEach(103, 106, std::ref(collector));
  ASSERT_COLLECTED(collector, 105, 106);
  reset_buffer();
  buf->forEach(106, 103, std::ref(collector));
  ASSERT_COLLECTED(collector, 106, 105);
  reset_buffer();
  buf->forEach(103, 105, std::ref(collector));
  ASSERT_COLLECTED(collector, 105);
  reset_buffer();
  buf->forEach(102, 108, std::ref(collector));
  ASSERT_COLLECTED(collector, 102, 105, 106);
  reset_buffer();
  buf->forEach(108, 102, std::ref(collector));
  ASSERT_COLLECTED(collector, 106, 105, 102);
  reset_buffer();
  buf->forEach(107, 109, std::ref(collector));
  ASSERT_COLLECTED(collector);
  reset_buffer();
  buf->forEach(109, 107, std::ref(collector));
  ASSERT_COLLECTED(collector);
  ASSERT_TRUE(true);
}

TEST_P(ClientReadStreamBufferTest, ForEachBackward) {
  buf->createOrGet(lsn_t{100});
  buf->createOrGet(lsn_t{101});
  buf->createOrGet(lsn_t{102});
  Collector collector;
  buf->forEach(102, 100, std::ref(collector));
  ASSERT_COLLECTED(collector, 102, 101, 100);
  ASSERT_TRUE(true);
}

TEST_P(ClientReadStreamBufferTest, ForEachBackward2) {
  buf->createOrGet(lsn_t{100});
  buf->createOrGet(lsn_t{101});
  buf->createOrGet(lsn_t{102});
  buf->createOrGet(lsn_t{103});
  Collector collector;
  buf->forEach(103, 101, std::ref(collector));
  ASSERT_COLLECTED(collector, 103, 102, 101);
  ASSERT_TRUE(true);
}

TEST_P(ClientReadStreamBufferTest, ForEachForwardEmpty) {
  Collector collector;
  buf->forEach(100, 102, std::ref(collector));
  if (GetParam() == ClientReadStreamBufferType::ORDERED_MAP) {
    // Ordered map only returns actual entries
    // but since the map is empty, it should not return anything
    ASSERT_COLLECTED(collector);
  } else {
    ASSERT_COLLECTED(collector, 100, 101, 102);
  }
  ASSERT_TRUE(true);
}

TEST_P(ClientReadStreamBufferTest, ForEachBackwardEmpty) {
  Collector collector;
  buf->forEach(102, 100, std::ref(collector));
  if (GetParam() == ClientReadStreamBufferType::ORDERED_MAP) {
    // Ordered map only returns actual entries
    // but since the map is empty, it should not return anything
    ASSERT_COLLECTED(collector);
  } else {
    ASSERT_COLLECTED(collector, 102, 101, 100);
  }
  ASSERT_TRUE(true);
}

INSTANTIATE_TEST_CASE_P(
    ClientReadStreamBufferTest,
    ClientReadStreamBufferTest,
    ::testing::Values(ClientReadStreamBufferType::CIRCULAR,
                      ClientReadStreamBufferType::ORDERED_MAP));

}} // namespace facebook::logdevice
