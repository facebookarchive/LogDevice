/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/OverloadDetector.h"

#include <chrono>

#include <gtest/gtest.h>

namespace facebook::logdevice {

constexpr uint32_t kDefaultOverloadThreshold = 90;
constexpr uint32_t kDefaultPercentile = 90;
constexpr size_t kRecvBufSize{100};

class MockOverloadDetectorDependencies : public OverloadDetectorDependencies {
 public:
  struct Options {
    std::chrono::milliseconds loop_period{std::chrono::seconds{60}};
    uint32_t overload_threshold{kDefaultOverloadThreshold};
    uint32_t percentile{kDefaultPercentile};
    double min_buf_lengths_read{1.0};
  };

  MockOverloadDetectorDependencies(Options options,
                                   size_t recv_buf_size,
                                   std::vector<ssize_t>& occupancies,
                                   std::vector<uint64_t>& rcvd_bytes)
      : options_(options),
        recv_buf_size_(recv_buf_size),
        occupancies_{occupancies},
        rcvd_bytes_{rcvd_bytes} {}

  ~MockOverloadDetectorDependencies() override {}

  std::chrono::milliseconds getLoopPeriod() const override {
    return options_.loop_period;
  }
  uint32_t getOverloadThreshold() const override {
    return options_.overload_threshold;
  }
  uint32_t getPercentile() const override {
    return options_.percentile;
  }
  double getMinBufLengthsRead() const override {
    return options_.min_buf_lengths_read;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() override {
    return nullptr;
  }

  size_t getTcpRecvBufSize(node_index_t) override {
    return recv_buf_size_;
  }
  ssize_t getTcpRecvBufOccupancy(node_index_t nid) override {
    return occupancies_[nid];
  }
  uint64_t getNumBytesReceived(node_index_t nid) override {
    return rcvd_bytes_[nid];
  }

 private:
  Options options_;
  size_t recv_buf_size_;
  std::vector<ssize_t>& occupancies_;
  std::vector<uint64_t>& rcvd_bytes_;
};

class OverloadDetectorTest : public ::testing::Test {
 public:
  void SetUp() override {
    occupancies.resize(kNumberOfNodes);
    rcvd_bytes.resize(kNumberOfNodes);
  }

  std::unique_ptr<OverloadDetector>
  BuildDetector(MockOverloadDetectorDependencies::Options options,
                size_t recv_buf_size,
                std::vector<ssize_t>& occupancies,
                std::vector<uint64_t>& rcvd_bytes) {
    auto deps = std::make_unique<MockOverloadDetectorDependencies>(
        options, recv_buf_size, occupancies, rcvd_bytes);
    return std::make_unique<OverloadDetector>(
        static_cast<std::unique_ptr<OverloadDetectorDependencies>>(
            std::move(deps)));
  }

  std::vector<ssize_t> occupancies;
  std::vector<uint64_t> rcvd_bytes;
  constexpr static size_t kNumberOfNodes = 100;
};

TEST_F(OverloadDetectorTest, NothingOnRecvBufs) {
  auto detector = BuildDetector(MockOverloadDetectorDependencies::Options(),
                                kRecvBufSize,
                                occupancies,
                                rcvd_bytes);

  for (node_index_t nid = 0; nid < kNumberOfNodes; ++nid) {
    detector->updateSampleFor(nid);
  }
  detector->updateOverloaded();

  ASSERT_FALSE(detector->overloaded())
      << "We detected overload of empty recv-q!";
}

TEST_F(OverloadDetectorTest, FullRecvBufs) {
  auto detector = BuildDetector(MockOverloadDetectorDependencies::Options(),
                                kRecvBufSize,
                                occupancies,
                                rcvd_bytes);

  for (node_index_t nid = 0; nid < kNumberOfNodes; ++nid) {
    occupancies[nid] = kRecvBufSize; // full
    detector->updateSampleFor(nid);
  }
  detector->updateOverloaded();

  ASSERT_TRUE(detector->overloaded())
      << "We should have been overloaded with all recv-q full";
}

TEST_F(OverloadDetectorTest, OnlyConsiderFreshData) {
  MockOverloadDetectorDependencies::Options options;
  options.percentile = 50;
  auto detector = BuildDetector(options, kRecvBufSize, occupancies, rcvd_bytes);

  for (node_index_t nid = 0; nid < kNumberOfNodes; ++nid) {
    if (nid > 0) {
      // update sample twice so we see delta as 0
      detector->updateSampleFor(nid);
      detector->updateSampleFor(nid);
    } else /* nid == 0 */ {
      // update sample twice so we see delta as > 0
      detector->updateSampleFor(nid);
      rcvd_bytes[nid] += kRecvBufSize + 1;
      occupancies[nid] = kRecvBufSize;
      detector->updateSampleFor(nid);
    }
  }
  detector->updateOverloaded();

  ASSERT_TRUE(detector->overloaded())
      << "We should discard samples below threshold if they are not fresh";
}

TEST_F(OverloadDetectorTest, DontDiscardSamplesWhenOverloaded) {
  MockOverloadDetectorDependencies::Options options;
  options.percentile = 50;
  auto detector = BuildDetector(options, kRecvBufSize, occupancies, rcvd_bytes);

  for (node_index_t nid = 0; nid < kNumberOfNodes; ++nid) {
    if (nid > 0) {
      // update sample twice so we see delta as 0
      detector->updateSampleFor(nid);
      occupancies[nid] = kRecvBufSize;
      detector->updateSampleFor(nid);
    } else /* nid == 0 */ {
      // update sample twice so we see delta as > 0
      detector->updateSampleFor(nid);
      rcvd_bytes[nid] += kRecvBufSize + 1;
      detector->updateSampleFor(nid);
    }
  }
  detector->updateOverloaded();

  ASSERT_TRUE(detector->overloaded())
      << "We should not discard samples >= threshold even when they are not "
         "fresh";
}

} // namespace facebook::logdevice
