/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DataSizeRequest.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockNodeSetAccessor.h"
#include "logdevice/common/test/MockNodeSetFinder.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

namespace {

class Callback {
 public:
  void operator()(Status status, size_t size) {
    called_ = true;
    size_ = size;
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, size_t expected_size) {
    assertCalled(expected_status, {expected_size, expected_size});
  }

  void assertCalled(Status expected_status,
                    std::vector<size_t> expected_size_range = {}) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_status, status_);
    if (!expected_size_range.empty()) {
      ld_check_eq(expected_size_range.size(), 2);
      ASSERT_LE(expected_size_range[0], size_);
      ASSERT_GE(expected_size_range[1], size_);
    }
  }

 private:
  bool called_ = false;
  size_t size_;
  Status status_;
};

ShardAuthoritativeStatusMap starting_map_;
void changeShardStartingAuthStatus(ShardID shard, AuthoritativeStatus st) {
  starting_map_.setShardStatus(shard.node(), shard.shard(), st);
}

static_assert(
    static_cast<int>(DataSizeAccuracy::COUNT) == 1,
    "Some cases of DataSizeAccuracy not covered by integration tests!");

class MockDataSizeRequest : public DataSizeRequest {
 public:
  MockDataSizeRequest(int storage_set_size,
                      ReplicationProperty replication,
                      Callback& callback,
                      std::chrono::milliseconds start,
                      std::chrono::milliseconds end,
                      folly::Optional<Configuration::NodesConfig>
                          nodes_config_override = folly::none)
      : DataSizeRequest(logid_t(1),
                        start,
                        end,
                        DataSizeAccuracy::APPROXIMATE,
                        std::ref(callback),
                        std::chrono::seconds(1)),
        replication_(replication) {
    map_ = starting_map_;

    Configuration::NodesConfig nodes_config = nodes_config_override.hasValue()
        ? std::move(nodes_config_override.value())
        : createSimpleNodesConfig(storage_set_size);

    Configuration::MetaDataLogsConfig meta_config =
        createMetaDataLogsConfig(nodes_config,
                                 nodes_config.getNodes().size(),
                                 replication.getReplicationFactor());

    config_ = ServerConfig::fromDataTest(
        __FILE__, std::move(nodes_config), std::move(meta_config));

    storage_set_.reserve(storage_set_size);
    for (node_index_t nid = 0; nid < storage_set_size; ++nid) {
      storage_set_.emplace_back(ShardID(nid, 1));
    }

    initNodeSetFinder();
  }

  bool isMockJobTimerActive() {
    return getMockStorageSetAccessor()->isJobTimerActive();
  }
  bool isMockGracePeriodTimerActive() {
    return getMockStorageSetAccessor()->isGracePeriodTimerActive();
  }
  void mockJobTimeout() {
    getMockStorageSetAccessor()->mockJobTimeout();
  }

  void mockShardStatusChanged(ShardID shard, AuthoritativeStatus auth_st) {
    map_.setShardStatus(shard.node(), shard.shard(), auth_st);
    onShardStatusChanged();
  }

  bool isShardRebuilding(ShardID shard) {
    shard_status_t st;
    int rv = failure_domain_->getShardAttribute(shard, &st);
    ld_check_eq(rv, 0);
    return st & SHARD_IS_REBUILDING;
  }

  bool haveSufficientResult() {
    return failure_domain_->isFmajority(node_has_result_filter) !=
        FmajorityResult::NONE;
  }
  bool haveDeadEnd() {
    return DataSizeRequest::haveDeadEnd();
  }

 protected: // mock stuff that communicates externally
  void deleteThis() override {}

  StorageSetAccessor::SendResult sendTo(ShardID) override {
    return {StorageSetAccessor::Result::SUCCESS, Status::OK};
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion) override {
    return std::make_unique<MockStorageSetAccessor>(
        logid_t(1),
        shards,
        nodes_configuration,
        minRep,
        shard_access,
        completion,
        StorageSetAccessor::Property::FMAJORITY,
        std::chrono::seconds(1));
  }

  std::unique_ptr<NodeSetFinder> makeNodeSetFinder() override {
    return std::make_unique<MockNodeSetFinder>(
        storage_set_, replication_, [this](Status status) {
          this->start(status);
        });
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return map_;
  }

 private:
  MockStorageSetAccessor* getMockStorageSetAccessor() {
    ld_check(nodeset_accessor_);
    return (MockStorageSetAccessor*)nodeset_accessor_.get();
  }

  StorageSet storage_set_;
  ReplicationProperty replication_;
  std::shared_ptr<ServerConfig> config_;
  ShardAuthoritativeStatusMap map_;
};

ShardID node(node_index_t index) {
  return ShardID(index, 1);
}

} // namespace

TEST(DataSizeRequestTest, AllEmpty) {
  Callback cb;
  MockDataSizeRequest req(5,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, 0);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, 0);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.haveSufficientResult());
  req.onReply(node(2), E::OK, 0);
  cb.assertCalled(E::OK, 0);
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.haveSufficientResult());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(DataSizeRequestTest, EmptyAfterFmajority) {
  Callback cb;
  MockDataSizeRequest req(5,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, 0);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(1), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, 0);
  cb.assertNotCalled();
  // Still possible to get enough actual responses, shouldn't finish yet
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.haveSufficientResult());
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  // Still possible to get enough actual responses, shouldn't finish yet
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.haveSufficientResult());
  req.onReply(node(3), E::OK, 0);
  cb.assertCalled(E::OK, 0);
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.haveSufficientResult());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(DataSizeRequestTest, NotEmpty) {
  Callback cb;
  MockDataSizeRequest req(8,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(2), E::OK, 0);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, 777000);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, 10100100100);
  cb.assertNotCalled();
  req.onReply(node(7), E::OK, 777000);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, 10122122122);
  cb.assertNotCalled();
  // Duplicate answer sholdn't be counted
  req.onReply(node(2), E::OK, 99999999999);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_FALSE(req.haveDeadEnd());
  req.onReply(node(4), E::OK, 8055055055);
  // Size is ~28.3 GB / replication factor of 3 = ~9.4 GB.
  // However, we only have the results of 6/8 of the nodes, so our estimate
  // should be 9.4 GB * (8/6) = ~12.5GB.
  // Let's expect something in [12.4,12.6] GB.
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.haveSufficientResult());
  cb.assertCalled(E::OK, {12400000000, 12600000000});
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.haveSufficientResult());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(DataSizeRequestTest, NotEmptyMixedResponses) {
  Callback cb;
  // Have N7's shard be underreplicated from the start
  changeShardStartingAuthStatus(node(7), AuthoritativeStatus::UNDERREPLICATION);
  MockDataSizeRequest req(8,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(2), E::OK, 0);
  cb.assertNotCalled();
  req.onReply(node(1), E::REBUILDING, 0);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, 10100100100);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, 10122122122);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, 8055055055);
  cb.assertNotCalled();
  req.onReply(node(6), E::OK, 10122122122);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, 1337);
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.haveSufficientResult());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  // Answer should be ~12.8 GB, so the estimate incl the remaining node should
  // be 12.8 GB * 8/6 = ~17 GB, so let's expect a result in [16.9,17.1].
  cb.assertCalled(E::OK, {16900000000, 17100000000});
}

TEST(DataSizeRequestTest, DeadEnd1) {
  Callback cb;
  MockDataSizeRequest req(5,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.mockShardStatusChanged(node(2), AuthoritativeStatus::UNAVAILABLE);
  req.onReply(node(2), E::REBUILDING, 0);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(1), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, 10122122122);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_FALSE(req.haveDeadEnd());
  req.onReply(node(0), E::REBUILDING, 0);
  req.mockShardStatusChanged(node(0), AuthoritativeStatus::UNAVAILABLE);
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_TRUE(req.haveDeadEnd());
  // Should see 10122122122 * 5 nodes / replication factor 3 = ~16.87 GB.
  cb.assertCalled(E::PARTIAL, {16800000000, 16950000000});
}

TEST(DataSizeRequestTest, DeadEnd2) {
  Callback cb;
  // Set some initial shard authoritative states
  changeShardStartingAuthStatus(node(7), AuthoritativeStatus::UNDERREPLICATION);
  changeShardStartingAuthStatus(
      node(1), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  MockDataSizeRequest req(8,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::min(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(5), E::OK, 10122122122);
  cb.assertNotCalled();
  req.onReply(node(2), E::REBUILDING, 0);
  req.mockShardStatusChanged(node(2), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, 10100100100);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, 8055055055);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.haveSufficientResult());
  req.onReply(node(7), E::REBUILDING, 0);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.haveSufficientResult());
  req.mockShardStatusChanged(node(6), AuthoritativeStatus::UNAVAILABLE);
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_TRUE(req.haveDeadEnd());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  // Answer should be (SUM / 4 responses) * 8 nodes = ~18.85 GB.
  cb.assertCalled(E::PARTIAL, {18800000000, 18950000000});
}

TEST(DataSizeRequestTest, DeadEndWithPermanentErrors1) {
  Callback cb;
  // Set some initial shard authoritative states
  changeShardStartingAuthStatus(node(7), AuthoritativeStatus::UNDERREPLICATION);
  MockDataSizeRequest req(8,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::min(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(5), E::OK, 10122122122);
  cb.assertNotCalled();
  req.onReply(node(2), E::REBUILDING, 0);
  cb.assertNotCalled();
  req.onReply(node(3), E::SHUTDOWN, 0);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  req.mockShardStatusChanged(node(2), AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_TRUE(req.haveDeadEnd());
  // Answer should be ~27 GB.
  cb.assertCalled(E::PARTIAL, {26800000000, 27200000000});
}

TEST(DataSizeRequestTest, DeadEndWithPermanentErrors2) {
  Callback cb;
  MockDataSizeRequest req(8,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::min(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(5), E::OK, 10122122122);
  cb.assertNotCalled();
  req.onReply(node(3), E::NOTSUPPORTED, 0);
  ASSERT_FALSE(req.haveDeadEnd());
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, 0);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, 8055055055);
  cb.assertNotCalled();
  req.onReply(node(6), E::FAILED, 0);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  req.onReply(node(2), E::REBUILDING, 0);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(2), AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_FALSE(req.haveSufficientResult());
  ASSERT_TRUE(req.haveDeadEnd());
  // Answer should be ~16.2 GB.
  cb.assertCalled(E::PARTIAL, {16000000000, 16500000000});
}

TEST(DataSizeRequestTest, Failed) {
  Callback cb;
  MockDataSizeRequest req(5,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::FAILED, 0);
  cb.assertNotCalled();
  req.onReply(node(1), E::FAILED, 0);
  cb.assertNotCalled();
  req.onReply(node(2), E::FAILED, 0);
  cb.assertCalled(E::FAILED, 0);
}

TEST(DataSizeRequestTest, ClientTimeout) {
  Callback cb;
  MockDataSizeRequest req(5,
                          ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                          cb,
                          std::chrono::milliseconds::zero(),
                          std::chrono::milliseconds::max());
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::FAILED, 0);
  cb.assertNotCalled();
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.mockJobTimeout();
  cb.assertCalled(E::TIMEDOUT, 0);
}
