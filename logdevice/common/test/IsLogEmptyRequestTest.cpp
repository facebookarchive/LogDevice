/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/IsLogEmptyRequest.h"

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
  void operator()(Status status, bool empty) {
    called_ = true;
    empty_ = empty;
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, bool expected_empty) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_status, status_);
    ASSERT_EQ(expected_empty, empty_);
  }

 private:
  bool called_ = false;
  bool empty_;
  Status status_;
};

ShardAuthoritativeStatusMap starting_map_;
void changeShardStartingAuthStatus(ShardID shard, AuthoritativeStatus st) {
  starting_map_.setShardStatus(shard.node(), shard.shard(), st);
}

class MockIsLogEmptyRequest : public IsLogEmptyRequest {
 public:
  MockIsLogEmptyRequest(int storage_set_size,
                        ReplicationProperty replication,
                        Callback& callback,
                        std::chrono::milliseconds grace_period =
                            std::chrono::milliseconds::zero(),
                        folly::Optional<Configuration::NodesConfig>
                            nodes_config_override = folly::none,
                        std::chrono::milliseconds client_timeout =
                            std::chrono::milliseconds(1000))
      : IsLogEmptyRequest(logid_t(1),
                          client_timeout,
                          std::ref(callback),
                          grace_period),
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
  void mockGracePeriodTimedout() {
    getMockStorageSetAccessor()->mockGracePeriodTimedout();
  }

  bool completionConditionCalled() {
    return completion_cond_called_;
  }

  static chrono_interval_t<std::chrono::milliseconds>
  getWaveTimeoutInterval(std::chrono::milliseconds client_timeout) {
    return IsLogEmptyRequest::getWaveTimeoutInterval(client_timeout);
  }

  void mockShardStatusChanged(ShardID shard, AuthoritativeStatus auth_st) {
    map_.setShardStatus(shard.node(), shard.shard(), auth_st);
    getMockStorageSetAccessor()->mockShardStatusChanged(shard, auth_st);
    onShardStatusChanged();
  }

  bool isShardRebuilding(ShardID shard) {
    shard_status_t st;
    int rv = failure_domain_->getShardAttribute(shard, &st);
    ld_check_eq(rv, 0);
    return st & SHARD_IS_REBUILDING;
  }

  bool haveEmptyFMajority() {
    return IsLogEmptyRequest::haveEmptyFMajority();
  }
  bool haveNonEmptyCopyset() {
    return IsLogEmptyRequest::haveNonEmptyCopyset();
  }
  bool haveDeadEnd() {
    return IsLogEmptyRequest::haveDeadEnd();
  }

 protected: // mock stuff that communicates externally
  void deleteThis() override {}

  StorageSetAccessor::SendResult sendTo(ShardID) override {
    return StorageSetAccessor::SendResult::SUCCESS;
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      const std::shared_ptr<ServerConfig>& config,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion) override {
    auto res = std::make_unique<MockStorageSetAccessor>(
        logid_t(1),
        shards,
        config,
        minRep,
        shard_access,
        completion,
        StorageSetAccessor::Property::FMAJORITY,
        std::chrono::seconds(1));
    res->setInitialShardAuthStatusMap(map_);
    return std::move(res);
  }

  std::unique_ptr<NodeSetFinder> makeNodeSetFinder() override {
    return std::make_unique<MockNodeSetFinder>(
        storage_set_, replication_, [this](Status status) {
          this->start(status);
        });
  }

  std::shared_ptr<ServerConfig> getConfig() const override {
    return config_;
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

TEST(IsLogEmptyRequestTest, Empty) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, EmptyWithGracePeriod) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, NotEmpty) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(2), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, SomeNodesEmpty1) {
  Callback cb;
  MockIsLogEmptyRequest req(10,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(8), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, SomeNodesEmpty2) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  req.onReply(node(4), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, NotEmptyAndExceededGracePeriod) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  req.onReply(node(2), E::FAILED, false);
  cb.assertNotCalled();
  req.mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, MixedResponsesNoGracePeriod) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  cb.assertNotCalled();
  req.mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, CrossRackReplicated1) {
  Callback cb;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 9;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 9 nodes, 3 per rack, with 1 shard each.
  addNodes(&nodes, 3, 1, "test.test1.01.01A.aa"); // nodes 0-2
  addNodes(&nodes, 3, 1, "test.test1.02.02A.aa"); // nodes 3-5
  addNodes(&nodes, 3, 1, "test.test1.02.02A.ab"); // nodes 6-9
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  // Get "not empty" responses satisfying (node, 3) criteria but not (rack, 2).

  req.onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(0), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  // Get a couple "empty" responses.

  req.onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(8), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  // Finally, get "not empty" from different rack to satisfy all replication
  // criteria. Should cause isLogEmpty to say log is non-empty.

  req.onReply(node(4), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, CrossRackReplicated2) {
  Callback cb;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 3}, {NodeLocationScope::NODE, 4}});
  int storage_set_size = 9;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 9 nodes, 3 per rack, with 1 shard each.
  addNodes(&nodes, 3, 1, "test.test1.01.01A.aa"); // nodes 0-2
  addNodes(&nodes, 3, 1, "test.test1.02.02A.aa"); // nodes 3-5
  addNodes(&nodes, 3, 1, "test.test1.02.02A.ab"); // nodes 6-8
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  // Get "not empty" responses satisfying (rack, 3) criteria but not (node, 4).
  req.onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(8), E::OK, false);
  cb.assertNotCalled();

  // Get a couple "empty" responses.
  req.onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(7), E::OK, true);
  cb.assertNotCalled();

  // Finally, get "not empty" from another node to satisfy all replication
  // criteria. Should cause isLogEmpty to say log is non-empty.
  req.onReply(node(0), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, CrossRackUnderReplicated1) {
  Callback cb;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 1 shard each.
  addNodes(&nodes, 2, 1, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 1, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 1, "test.test1.02.02A.ab"); // nodes 4-5
  nodes_config.setNodes(std::move(nodes));

  // Have N0's shard be underreplicated from the start
  changeShardStartingAuthStatus(node(0), AuthoritativeStatus::UNDERREPLICATION);

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  ASSERT_TRUE(req.isShardRebuilding(node(0)));
  for (int i = 1; i < storage_set_size; i++) {
    ASSERT_FALSE(req.isShardRebuilding(node(i)));
  }

  // Get a couple empty responses, 1 non-empty, then empty f-majority.
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(0), E::REBUILDING, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  req.onReply(node(5), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, CrossRackUnderReplicated2) {
  Callback cb;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 1 shard each.
  addNodes(&nodes, 2, 1, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 1, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 1, "test.test1.02.02A.ab"); // nodes 4-5
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req.isShardRebuilding(node(i)));
  }

  // Get 1 empty response, 1 underreplicated, and finally 3 non-empty.
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(5), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, CrossRackUnderReplicated3) {
  Callback cb;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 1 shard each.
  addNodes(&nodes, 2, 1, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 1, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 1, "test.test1.02.02A.ab"); // nodes 4-5
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req.isShardRebuilding(node(i)));
  }

  // Get 1 non-empty response, 3 empty, and 2 in rebuliding
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req.mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  ASSERT_FALSE(req.haveDeadEnd());
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  // Given another underreplicated node, it's now clear that it's a dead end
  // since the remaining nodes are all underreplicated. Should finish early and
  // consider log non-empty.
  req.mockShardStatusChanged(node(3), AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_TRUE(req.haveDeadEnd());
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, EarlyDeadEnd) {
  Callback cb;
  ReplicationProperty replication({{NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes in a single rack with 1 shard each.
  addNodes(&nodes, 6, 1, "test.test1.01.01A.aa");
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req.isShardRebuilding(node(i)));
  }

  // Get 1 non-empty response, 2 empty, and 2 in rebuliding
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req.mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  // Another underreplicated node should make for a dead end, and although we
  // don't yet have an f-majority of responses, there's no point in waiting any
  // longer; we'd rather finish and say the log is non-empty.
  req.mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_TRUE(req.haveDeadEnd());
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
}

TEST(IsLogEmptyRequestTest, LateDeadEnd) {
  Callback cb;
  ReplicationProperty replication({{NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes in a single rack with 1 shard each.
  addNodes(&nodes, 6, 1, "test.test1.01.01A.aa");
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  MockIsLogEmptyRequest req(
      storage_set_size,
      replication,
      cb,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req.isShardRebuilding(node(i)));
  }

  // Get 2 non-empty responses, 2 empty, and 2 in rebuilding.
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty, with E::OK as we've got enough responses.
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req.mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req.haveDeadEnd());
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  // This shard in rebuilding should bring us to a dead end, and since we've
  // got an f-majority of responses, end with E::OK.
  req.mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  ASSERT_TRUE(req.haveDeadEnd());
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, Failed) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::FAILED, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::FAILED, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::FAILED, false);
  cb.assertCalled(E::FAILED, false);
}

TEST(IsLogEmptyRequestTest, ClientTimeout) {
  Callback cb;
  MockIsLogEmptyRequest req(5,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::FAILED, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.mockJobTimeout();
  cb.assertCalled(E::TIMEDOUT, false);
}

TEST(IsLogEmptyRequestTest, BasicNodeDown) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  // Node 4 is down, failed to send to it
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  // Node 4 is down, failed to send to it for the second time.
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, NodeDownNoGracePeriod) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  // Node 4 is down, failed to send to it
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  // Node 4 is down, failed to send to it for the second time.
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req.mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, BasicTransientError) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  // Node replies again that it is in rebuilding
  req.onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req.mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, TransientErrorNoGracePeriod) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(2), E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  cb.assertNotCalled();
  req.mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, NodeDisabled1) {
  Callback cb;
  // Have N0's shard be authoritative empty from the start
  changeShardStartingAuthStatus(
      node(0), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  cb.assertNotCalled();
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::CONNFAILED, false);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, NodeDisabled2) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req.onMessageSent(node(4), E::CONNFAILED);
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  req.onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  ASSERT_TRUE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, AuthEmptyFMajorityOnStart) {
  Callback cb;
  // Have N[0..3] be authoritative empty from the start
  for (int i = 0; i < 4; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, AllAuthEmptyOnStart) {
  Callback cb;
  // Have all nodes be authoritative empty from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, MostUnderreplicatedOnStart) {
  Callback cb;
  // Have most nodes be underreplicated from the start
  for (int i = 0; i < 5; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  ASSERT_FALSE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, MostUnderreplicatedOnStart2) {
  Callback cb;
  // Have most nodes be underreplicated from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(3), AuthoritativeStatus::UNAVAILABLE);
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, AllUnderreplicatedOnStart) {
  Callback cb;
  // Have all nodes be underreplicated from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  ASSERT_FALSE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertCalled(E::PARTIAL, false);
}

TEST(IsLogEmptyRequestTest, AllUnavailableOnStart) {
  Callback cb;
  // Have all nodes be unavailable from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(node(i), AuthoritativeStatus::UNAVAILABLE);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  ASSERT_FALSE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertCalled(E::PARTIAL, false);
}

// Verify that we're correctly handling the case where some permanent error
// causes finalizing from onMessageSent.
TEST(IsLogEmptyRequestTest, LegacyNodeCausesDeadEnd) {
  Callback cb;
  // Have a bunch of nodes be underreplicated from the start
  for (int i = 0; i < 3; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertNotCalled();
  req.onMessageSent(node(5), E::PROTONOSUPPORT);
  cb.assertCalled(E::FAILED, false);
}

// If we hit a dead end before getting proper responses from an f-majority o
// the nodes, we should finish with E::FAILED.
TEST(IsLogEmptyRequestTest, EarlyDeadEndFailed1) {
  Callback cb;
  // Have a bunch of nodes be underreplicated from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertNotCalled();
  req.onReply(node(5), E::SHUTDOWN, false);
  // Here, we didn't actually have a dead end: it's still possible to get a
  // non-empty copyset. NodeSetAccessor chooses for us and makes us finish,
  // since it's no longer possible to get responses from an f-majority of the
  // shards.
  ASSERT_FALSE(req.haveDeadEnd());
  cb.assertCalled(E::FAILED, false);
}

TEST(IsLogEmptyRequestTest, EarlyDeadEndFailed2) {
  Callback cb;
  // Have N0 be underreplicated from the start
  changeShardStartingAuthStatus(node(0), AuthoritativeStatus::UNDERREPLICATION);
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  ASSERT_FALSE(req.completionConditionCalled());
  cb.assertNotCalled();
  req.onReply(node(5), E::SHUTDOWN, false);
  cb.assertNotCalled();
  req.onReply(node(4), E::REBUILDING, false);
  // We don't check for dead end when receiving the above response, but if we
  // get two shard authoritative status updates indicating that rebuilding is
  // still going on, we should finish with E::FAILED at this point since we had
  // one node that was not rebuilding (shutting down).
  cb.assertNotCalled();
  req.onReply(node(1), E::REBUILDING, false);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  req.mockShardStatusChanged(node(1), AuthoritativeStatus::UNAVAILABLE);
  cb.assertCalled(E::FAILED, false);
}

TEST(IsLogEmptyRequestTest, SomeAuthEmpty1) {
  Callback cb;
  // Have N0,N1 be authoritative empty from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE to complete with 'empty'
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, SomeAuthEmpty2) {
  Callback cb;
  // Have N0, N1 be authoritative empty from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE, hit grace period
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req.onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertCalled(E::OK, true);
}

TEST(IsLogEmptyRequestTest, SomeAuthEmpty3) {
  Callback cb;
  // Have N0 be authoritative empty from the start
  changeShardStartingAuthStatus(
      node(0), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE, hit grace period
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  req.onReply(node(1), E::OK, false);
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertCalled(E::OK, false);
}

TEST(IsLogEmptyRequestTest, ResponseAfterAuthEmpty) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(1), E::OK, true);
  ASSERT_TRUE(req.completionConditionCalled());
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be equivalent to an 'empty' response
  // while this remains the case
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  // However, if node 4 were to somehow respond non-empty now, we should still
  // finish with the result accordingly. Let's try below.
  req.onReply(node(4), E::OK, false);
  cb.assertCalled(E::OK, false);
}

TEST(IsLogEmptyRequestTest, AuthEmptyThenPermanentError) {
  Callback cb;
  MockIsLogEmptyRequest req(6,
                            ReplicationProperty({{NodeLocationScope::NODE, 3}}),
                            cb,
                            /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req.isMockJobTimerActive());
  ASSERT_FALSE(req.isMockGracePeriodTimerActive());
  req.onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req.onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req.completionConditionCalled());
  req.onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req.completionConditionCalled());
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req.mockShardStatusChanged(node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  // Further responses from node 4 should be ignored since we already got an
  // answer per its relevant shard being authoritative empty. To test this,
  // let's say it now returns E::SHUTDOWN, a permanent error, then see if it
  // still finishes if another node join in and declares the log 'empty'.
  req.onReply(node(4), E::SHUTDOWN, false);
  cb.assertNotCalled();
  req.onReply(node(0), E::OK, true);
  cb.assertCalled(E::OK, true);
}

// Make sure wave timeout choice works as intended
TEST(IsLogEmptyRequestTest, WaveTimeoutInterval) {
  static_assert(IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MIN == 500 &&
                    IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MAX == 1500,
                "Range to which we clamp wave timeouts changed, please update "
                "tests accordingly.");

  // Short timeout -- should hit the minimum
  auto interval = MockIsLogEmptyRequest::getWaveTimeoutInterval(
      std::chrono::milliseconds(5000));
  ASSERT_EQ(
      interval.lo.count(), IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MIN);
  ASSERT_EQ(interval.hi.count(), 10000);

  // Long timeout -- should hit the maximum
  interval = MockIsLogEmptyRequest::getWaveTimeoutInterval(
      std::chrono::milliseconds(15000));
  ASSERT_EQ(
      interval.lo.count(), IsLogEmptyRequest::WAVE_TIMEOUT_LOWER_BOUND_MAX);
  ASSERT_EQ(interval.hi.count(), 10000);

  // This should give us a lower bound of 1000ms
  interval = MockIsLogEmptyRequest::getWaveTimeoutInterval(
      std::chrono::milliseconds(10000));
  ASSERT_EQ(interval.lo.count(), 1000);
  ASSERT_EQ(interval.hi.count(), 10000);
}

} // namespace
