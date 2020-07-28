/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/buffered_writer/BufferedWriteCodec.h"

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

class BufferedWriteEstimatorTest
    : public ::testing::TestWithParam<
          std::pair<int, std::vector<std::string>>> {};

TEST_P(BufferedWriteEstimatorTest, EstimateMatch) {
  const auto& [checksum_bits, payloads] = GetParam();

  BufferedWriteCodec::Estimator estimator;
  for (const auto& payload : payloads) {
    estimator.append(payload);
  }
  const size_t size = estimator.calculateSize(checksum_bits);

  BufferedWriteCodec::Encoder encoder(checksum_bits, payloads.size(), size);
  for (const auto& payload : payloads) {
    encoder.append(payload);
  }
  folly::IOBufQueue queue;
  encoder.encode(queue);
  auto payload = queue.move();

  EXPECT_EQ(size, payload->computeChainDataLength());
}

namespace {

const std::vector<std::pair<int, std::vector<std::string>>> payloads{
    // {checksum_bits, payloads}
    {0, {}},
    {32, {}},
    {64, {}},
    {0, {""}},
    {32, {"", ""}},
    {64, {"payload"}},
    {0, {"", "payload"}},
    {32, {"payload", ""}},
    {64, {std::string(1024, '\0'), "()", std::string(1234, '?')}},
    {32, std::vector<std::string>(10, "-")},
    {32, std::vector<std::string>(100, "-")},
    {32, std::vector<std::string>(1000, "-")},
    {32, std::vector<std::string>(10000, "-")},
    {32, std::vector<std::string>(100000, "-")},
};

INSTANTIATE_TEST_CASE_P(EstimateMatch,
                        BufferedWriteEstimatorTest,
                        ::testing::ValuesIn(payloads));
} // namespace

}} // namespace facebook::logdevice
