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

class MockIsLogEmptyRequest;

class Callback {
 public:
  void operator()(MockIsLogEmptyRequest& req, Status status, bool empty);

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, bool expected_empty) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_status, status_);
    ASSERT_EQ(expected_empty, empty_);
  }

  bool completionConditionCalled() {
    return completion_cond_called_;
  }

  bool wasGracePeriodTimerActive() {
    return was_grace_period_timer_active_;
  }

  bool wasJobTimerActive() {
    return was_job_timer_active_;
  }

  bool hadDeadEnd() {
    return had_dead_end_;
  }

  void printShardStateComparison() {
    ld_info("%s", shard_state_comparison_.c_str());
  }

 private:
  bool called_ = false;
  bool empty_;
  Status status_;

  // Post-completion state for checks by tests:
  bool completion_cond_called_;
  bool was_job_timer_active_;
  bool was_grace_period_timer_active_;
  bool had_dead_end_;
  std::string shard_state_comparison_;
};

class IsLogEmptyRequestTest : public ::testing::Test {
 public:
  ShardAuthoritativeStatusMap starting_map_;
  void changeShardStartingAuthStatus(ShardID shard, AuthoritativeStatus st) {
    starting_map_.setShardStatus(shard.node(), shard.shard(), st);
  }
};

class MockIsLogEmptyRequest : public IsLogEmptyRequest {
 public:
  MockIsLogEmptyRequest(IsLogEmptyRequestTest* test,
                        int storage_set_size,
                        ReplicationProperty replication,
                        Callback& callback,
                        std::unique_ptr<MockIsLogEmptyRequest>& holder,
                        std::chrono::milliseconds grace_period =
                            std::chrono::milliseconds::zero(),
                        folly::Optional<Configuration::NodesConfig>
                            nodes_config_override = folly::none,
                        std::chrono::milliseconds client_timeout =
                            std::chrono::milliseconds(1000))
      : IsLogEmptyRequest(
            logid_t(1),
            client_timeout,
            [this](Status st, bool empty) { callbackFunc(st, empty); },
            grace_period),
        replication_(replication),
        callback_object_(callback),
        holder_(holder) {
    map_ = test->starting_map_;

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

  void callbackFunc(Status status, bool empty) {
    callback_object_(*this, status, empty);
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

  bool completionConditionCalled() const {
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

  void injectNAShardStatus(ShardID shard, AuthoritativeStatus auth_st) {
    getMockStorageSetAccessor()->mockShardStatusChanged(shard, auth_st);
    getMockStorageSetAccessor()->onShardStatusChanged();
  }

  void injectFDShardStatus(ShardID shard, AuthoritativeStatus auth_st) {
    map_.setShardStatus(shard.node(), shard.shard(), auth_st);
    IsLogEmptyRequest::applyShardStatus(/*initialize_unknown=*/false);
  }

  bool haveShardAuthoritativeStatusDifferences() {
    return IsLogEmptyRequest::haveShardAuthoritativeStatusDifferences();
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

  std::string getHumanReadableShardStatuses() {
    return IsLogEmptyRequest::getHumanReadableShardStatuses();
  }

  std::string getNonEmptyShardsList() {
    return IsLogEmptyRequest::getNonEmptyShardsList();
  }

  MockStorageSetAccessor* getMockStorageSetAccessor() {
    ld_check(nodeset_accessor_);
    return (MockStorageSetAccessor*)nodeset_accessor_.get();
  }

  // Provides debugging information in case of test failure, but also prevents
  // people from completely breaking the logging functions.
  std::string getShardStateComparison() {
    return folly::sformat(
        "Shard states according to\nNA: [{}],\nFD: [{}].\nNon-empty shards: {}",
        getMockStorageSetAccessor()->describeState().c_str(),
        getHumanReadableShardStatuses().c_str(),
        getNonEmptyShardsList().c_str());
  }

  void printShardStateComparison() {
    ld_info("%s", getShardStateComparison().c_str());
  }

 protected: // mock stuff that communicates externally
  void deleteThis() override {
    holder_.reset();
  }

  StorageSetAccessor::SendResult sendTo(ShardID) override {
    return {StorageSetAccessor::Result::SUCCESS, Status::OK};
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_configuration,
      StorageSet shards,
      ReplicationProperty minRep,
      StorageSetAccessor::ShardAccessFunc shard_access,
      StorageSetAccessor::CompletionFunc completion) override {
    auto res = std::make_unique<MockStorageSetAccessor>(
        logid_t(1),
        shards,
        nodes_configuration,
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

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return map_;
  }

 private:
  StorageSet storage_set_;
  ReplicationProperty replication_;
  std::shared_ptr<ServerConfig> config_;
  ShardAuthoritativeStatusMap map_;
  Callback& callback_object_;
  std::unique_ptr<MockIsLogEmptyRequest>& holder_;
};

ShardID node(node_index_t index) {
  return ShardID(index, 1);
}

void Callback::operator()(MockIsLogEmptyRequest& req,
                          Status status,
                          bool empty) {
  called_ = true;
  empty_ = empty;
  status_ = status;

  // Capture some final state of the request.
  completion_cond_called_ = req.completionConditionCalled();
  was_grace_period_timer_active_ = req.isMockGracePeriodTimerActive();
  had_dead_end_ = req.haveDeadEnd();
  shard_state_comparison_ = req.getShardStateComparison();
  was_job_timer_active_ = req.isMockJobTimerActive();
}

TEST_F(IsLogEmptyRequestTest, Empty) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, EmptyWithGracePeriod) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, NotEmpty) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(2), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, SomeNodesEmpty1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      10,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(8), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, SomeNodesEmpty2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  req->onReply(node(4), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST_F(IsLogEmptyRequestTest, NotEmptyAndExceededGracePeriod) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  req->onReply(node(2), E::FAILED, false);
  cb.assertNotCalled();
  req->mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, MixedResponsesNoGracePeriod) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  cb.assertNotCalled();
  req->mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, CrossRackReplicated1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 9;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 9 nodes, 3 per rack, with 2 shard each.
  addNodes(&nodes, 3, 2, "test.test1.01.01A.aa"); // nodes 0-2
  addNodes(&nodes, 3, 2, "test.test1.02.02A.aa"); // nodes 3-5
  addNodes(&nodes, 3, 2, "test.test1.02.02A.ab"); // nodes 6-9
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  // Get "not empty" responses satisfying (node, 3) criteria but not (rack, 2).

  req->onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(0), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  // Get a couple "empty" responses.

  req->onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(8), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  // Finally, get "not empty" from different rack to satisfy all replication
  // criteria. Should cause isLogEmpty to say log is non-empty.

  req->onReply(node(4), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, CrossRackReplicated2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 3}, {NodeLocationScope::NODE, 4}});
  int storage_set_size = 9;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 9 nodes, 3 per rack, with 2 shard each.
  addNodes(&nodes, 3, 2, "test.test1.01.01A.aa"); // nodes 0-2
  addNodes(&nodes, 3, 2, "test.test1.02.02A.aa"); // nodes 3-5
  addNodes(&nodes, 3, 2, "test.test1.02.02A.ab"); // nodes 6-8
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  // Get "not empty" responses satisfying (rack, 3) criteria but not (node, 4).
  req->onReply(node(2), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(8), E::OK, false);
  cb.assertNotCalled();

  // Get a couple "empty" responses.
  req->onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(7), E::OK, true);
  cb.assertNotCalled();

  // Finally, get "not empty" from another node to satisfy all replication
  // criteria. Should cause isLogEmpty to say log is non-empty.
  req->onReply(node(0), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, CrossRackUnderReplicated1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 2 shard each.
  addNodes(&nodes, 2, 2, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 2, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 2, "test.test1.02.02A.ab"); // nodes 4-5
  nodes_config.setNodes(std::move(nodes));

  // Have N0's shard be underreplicated from the start
  changeShardStartingAuthStatus(node(0), AuthoritativeStatus::UNDERREPLICATION);

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  ASSERT_TRUE(req->isShardRebuilding(node(0)));
  for (int i = 1; i < storage_set_size; i++) {
    ASSERT_FALSE(req->isShardRebuilding(node(i)));
  }

  // Get a couple empty responses, 1 non-empty, then empty f-majority.
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(0), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  req->onReply(node(5), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST_F(IsLogEmptyRequestTest, CrossRackUnderReplicated2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 2 shard each.
  addNodes(&nodes, 2, 2, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 2, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 2, "test.test1.02.02A.ab"); // nodes 4-5
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req->isShardRebuilding(node(i)));
  }

  // Get 1 empty response, 1 underreplicated, and finally 3 non-empty.
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->mockShardStatusChanged(node(5), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  ASSERT_FALSE(req->haveDeadEnd());
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.hadDeadEnd());
}

TEST_F(IsLogEmptyRequestTest, CrossRackUnderReplicated3) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication(
      {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes, 2 per rack, with 2 shard each.
  addNodes(&nodes, 2, 2, "test.test1.01.01A.aa"); // nodes 0-1
  addNodes(&nodes, 2, 2, "test.test1.02.02A.aa"); // nodes 2-3
  addNodes(&nodes, 2, 2, "test.test1.02.02A.ab"); // nodes 4-5
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req->isShardRebuilding(node(i)));
  }

  // Get 1 non-empty response, 3 empty, and 2 in rebuliding
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req->mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req->haveDeadEnd());
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  ASSERT_FALSE(req->haveDeadEnd());
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  // Given another underreplicated node, it's now clear that it's a dead end
  // since the remaining nodes are all underreplicated. Should finish early and
  // consider log non-empty.
  req->mockShardStatusChanged(node(3), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_TRUE(cb.hadDeadEnd());
}

TEST_F(IsLogEmptyRequestTest, EarlyDeadEnd) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication({{NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes in a single rack with 2 shard each.
  addNodes(&nodes, 6, 2, "test.test1.01.01A.aa");
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req->isShardRebuilding(node(i)));
  }

  // Get 1 non-empty response, 2 empty, and 2 in rebuliding
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req->mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req->haveDeadEnd());
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  // Another underreplicated node should make for a dead end, and although we
  // don't yet have an f-majority of responses, there's no point in waiting any
  // longer; we'd rather finish and say the log is non-empty.
  req->mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_TRUE(cb.hadDeadEnd());
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

TEST_F(IsLogEmptyRequestTest, LateDeadEnd) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  ReplicationProperty replication({{NodeLocationScope::NODE, 3}});
  int storage_set_size = 6;
  Configuration::Nodes nodes;
  Configuration::NodesConfig nodes_config =
      createSimpleNodesConfig(storage_set_size);

  // Use 6 nodes in a single rack with 2 shard each.
  addNodes(&nodes, 6, 2, "test.test1.01.01A.aa");
  ld_check_eq(nodes.size(), storage_set_size);
  nodes_config.setNodes(std::move(nodes));

  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      storage_set_size,
      replication,
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500),
      folly::Optional<Configuration::NodesConfig>(nodes_config));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  for (int i = 0; i < storage_set_size; i++) {
    ASSERT_FALSE(req->isShardRebuilding(node(i)));
  }

  // Get 2 non-empty responses, 2 empty, and 2 in rebuilding.
  // Since it's then no longer possible to get consensus, should finish with
  // log considered non-empty, with E::OK as we've got enough responses.
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  // Still possible to get consensus, shouldn't finish yet
  req->mockShardStatusChanged(node(5), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  ASSERT_FALSE(req->haveDeadEnd());
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  // This shard in rebuilding should bring us to a dead end, and since we've
  // got an f-majority of responses, end with E::OK.
  req->mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_TRUE(cb.hadDeadEnd());
}

TEST_F(IsLogEmptyRequestTest, Failed) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::FAILED, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::FAILED, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::FAILED, false);
  cb.assertCalled(E::FAILED, false);
}

TEST_F(IsLogEmptyRequestTest, ClientTimeout) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      5,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::FAILED, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->mockJobTimeout();
  cb.assertCalled(E::TIMEDOUT, false);
}

TEST_F(IsLogEmptyRequestTest, BasicNodeDown) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  // Node 4 is down, failed to send to it
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  // Node 4 is down, failed to send to it for the second time.
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST_F(IsLogEmptyRequestTest, NodeDownNoGracePeriod) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  // Node 4 is down, failed to send to it
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  // Node 4 is down, failed to send to it for the second time.
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  req->mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, BasicTransientError) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(2), E::OK, false);
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  cb.assertNotCalled();
  req->mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, TransientErrorNoGracePeriod) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(0));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(2), E::OK, true);
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  cb.assertNotCalled();
  req->mockGracePeriodTimedout();
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, NodeDisabled1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N0's shard be authoritative empty from the start
  changeShardStartingAuthStatus(
      node(0), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  cb.assertNotCalled();
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::CONNFAILED, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST_F(IsLogEmptyRequestTest, NodeDisabled2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req->onMessageSent(node(4), E::CONNFAILED);
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  ASSERT_TRUE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
}

TEST_F(IsLogEmptyRequestTest, AuthEmptyFMajorityOnStart) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N[0..3] be authoritative empty from the start
  for (int i = 0; i < 4; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.wasJobTimerActive());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_TRUE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, AllAuthEmptyOnStart) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have all nodes be authoritative empty from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.wasJobTimerActive());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_TRUE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, MostUnderreplicatedOnStart) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have most nodes be underreplicated from the start
  for (int i = 0; i < 5; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_FALSE(cb.wasJobTimerActive());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, MostUnderreplicatedOnStart2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have most nodes be underreplicated from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  ASSERT_FALSE(req->completionConditionCalled());
  cb.assertNotCalled();
  req->mockShardStatusChanged(node(4), AuthoritativeStatus::UNAVAILABLE);
  cb.assertNotCalled();
  req->mockShardStatusChanged(node(3), AuthoritativeStatus::UNAVAILABLE);
  cb.assertCalled(E::PARTIAL, false);
}

TEST_F(IsLogEmptyRequestTest, AllUnderreplicatedOnStart) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have all nodes be underreplicated from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_FALSE(cb.wasJobTimerActive());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, AllUnavailableOnStart) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have all nodes be unavailable from the start
  for (int i = 0; i < 6; i++) {
    changeShardStartingAuthStatus(node(i), AuthoritativeStatus::UNAVAILABLE);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  // Caught by haveDeadEnd, job won't even start
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_FALSE(cb.wasJobTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

// Verify that we're correctly handling the case where some permanent error
// causes finalizing from onMessageSent.
TEST_F(IsLogEmptyRequestTest, LegacyNodeCausesDeadEnd1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  ASSERT_FALSE(req->completionConditionCalled());
  req->mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(0), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onMessageSent(node(4), E::PROTONOSUPPORT);
  // Just 5 remain, so we can't reach consensus. Should reach a dead end, and
  // since the above is a non-rebuilding error, it should finish with 'FAILED'.
  cb.assertCalled(E::FAILED, false);
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

// Have some node fail due to not supporting this request type; reach dead end
// by non-empty node making consensus impossible.
TEST_F(IsLogEmptyRequestTest, LegacyNodeThenDeadEnd) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  ASSERT_FALSE(req->completionConditionCalled());
  req->mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(0), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onMessageSent(node(4), E::PROTONOSUPPORT);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  // Just 5 remain, so we can't reach consensus. Should reach a dead end, and
  // since the above is a non-rebuilding error, it should finish with 'FAILED'.
  cb.assertCalled(E::FAILED, false);
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

// If we hit a dead end before getting proper responses from an f-majority o
// the nodes, we should finish with E::FAILED.
TEST_F(IsLogEmptyRequestTest, EarlyDeadEndFailed1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have a bunch of nodes be underreplicated from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::UNDERREPLICATION);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  ASSERT_FALSE(req->completionConditionCalled());
  cb.assertNotCalled();
  req->onReply(node(5), E::SHUTDOWN, false);
  // Here, we didn't actually have a dead end: it's still possible to get a
  // non-empty copyset. NodeSetAccessor chooses for us and makes us finish,
  // since it's no longer possible to get responses from an f-majority of the
  // shards.
  cb.assertCalled(E::FAILED, false);
  ASSERT_FALSE(cb.hadDeadEnd());
}

TEST_F(IsLogEmptyRequestTest, EarlyDeadEndFailed2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  ASSERT_FALSE(req->completionConditionCalled());
  req->mockShardStatusChanged(node(0), AuthoritativeStatus::UNDERREPLICATION);
  cb.assertNotCalled();
  req->onReply(node(5), E::SHUTDOWN, false);
  cb.assertNotCalled();
  // One more node in mini-rebuilding will cause us to hit a dead end, and
  // since this is before getting responses from an f-majority of the nodes,
  // and some failures were not due to rebuilding, the result should be FAILED.
  req->onReply(node(4), E::REBUILDING, false);
  cb.assertCalled(E::FAILED, false);
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
  ASSERT_FALSE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, SomeAuthEmpty1) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N0,N1 be authoritative empty from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE to complete with 'empty'
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, SomeAuthEmpty2) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N0, N1 be authoritative empty from the start
  for (int i = 0; i < 2; i++) {
    changeShardStartingAuthStatus(
        node(i), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE, hit grace period
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  // Node 4 is down, failed to send to it
  req->onMessageSent(node(4), E::CONNFAILED);
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, SomeAuthEmpty3) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N0 be authoritative empty from the start
  changeShardStartingAuthStatus(
      node(0), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));

  // Get some mixed responses, another node becomes AE, hit grace period
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  req->onReply(node(1), E::OK, false);
  cb.assertCalled(E::OK, false);
  ASSERT_TRUE(cb.completionConditionCalled());
}

TEST_F(IsLogEmptyRequestTest, ResponseAfterAuthEmpty) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(1), E::OK, true);
  ASSERT_TRUE(req->completionConditionCalled());
  cb.assertNotCalled();
  // Node 4 changes to auth empty; should be equivalent to an 'empty' response
  // while this remains the case
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  // However, if node 4 were to somehow respond non-empty now, we should still
  // finish with the result accordingly. Let's try below.
  req->onReply(node(4), E::OK, false);
  cb.assertCalled(E::OK, false);
}

TEST_F(IsLogEmptyRequestTest, AuthEmptyThenPermanentError) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(5), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::OK, true);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  req->onReply(node(1), E::OK, true);
  cb.assertNotCalled();
  ASSERT_TRUE(req->completionConditionCalled());
  // Node 4 changes to auth empty; should be considered an 'empty' response
  req->mockShardStatusChanged(
      node(4), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  cb.assertNotCalled();
  // Further responses from node 4 should be ignored since we already got an
  // answer per its relevant shard being authoritative empty. To test this,
  // let's say it now returns E::SHUTDOWN, a permanent error, then see if it
  // still finishes if another node join in and declares the log 'empty'.
  req->onReply(node(4), E::SHUTDOWN, false);
  cb.assertNotCalled();
  req->onReply(node(0), E::OK, true);
  cb.assertCalled(E::OK, true);
}

// Make sure wave timeout choice works as intended
TEST_F(IsLogEmptyRequestTest, WaveTimeoutInterval) {
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

// Make sure that when too many nodes are unable to respond due to mini
// rebuilding being slow or stuck, we quit early with PARTIAL result, rather
// than retrying until timeout.
TEST_F(IsLogEmptyRequestTest, StuckMiniRebuilding) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this, 6, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb, req);
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::REBUILDING, false);
  cb.assertNotCalled();

  // Make the last node say it is in mini rebuilding. This should make us hit a
  // dead end and end the request with a partial result.
  req->onReply(node(3), E::REBUILDING, false);
  // Should be counted a dead end, and finish with a partial result.
  cb.assertCalled(E::PARTIAL, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

// Two nodes with stuck mini-rebuilding, one unresponsive node.
TEST_F(IsLogEmptyRequestTest, StuckMiniRebuildingAndOneSlowNode) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this, 20, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb, req);
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::REBUILDING, false);
  cb.assertNotCalled();
  for (int i = 2; i < 18; i++) {
    req->onReply(node(i), E::OK, true);
    cb.assertNotCalled();
  }
  req->onReply(node(18), E::REBUILDING, false);
  cb.assertNotCalled();
  ASSERT_FALSE(req->completionConditionCalled());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());

  // Hit timeout while waiting for the last node, which is unresponsive for
  // some reason.
  req->mockJobTimeout();
  cb.assertCalled(E::TIMEDOUT, false);
}

// A node finishes mini-rebuilding, makes us reach empty f-majority.
TEST_F(IsLogEmptyRequestTest, MiniRebuildingFinishesEmpty) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this, 6, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb, req);
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(1), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, true);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, true);
  cb.assertNotCalled();

  // Now we're just one more 'empty' away from an empty f-majority. Let's
  // imagine that N2's mini-rebuilding finishes, and it now tells us it doesn't
  // have any records for this log.
  req->onReply(node(2), E::OK, true);
  // Should be counted a dead end, and finish with a partial result.
  cb.assertCalled(E::OK, true);
  ASSERT_TRUE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

// A node finishes mini-rebuilding, makes us reach non-empty copyset.
TEST_F(IsLogEmptyRequestTest, MiniRebuildingFinishesNonEmpty) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  req = std::make_unique<MockIsLogEmptyRequest>(
      this, 6, ReplicationProperty({{NodeLocationScope::NODE, 3}}), cb, req);
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, true);
  cb.assertNotCalled();

  // Now we're just one more 'empty' away from an empty f-majority. Let's
  // imagine that N2's mini-rebuilding finishes, and it now tells us it doesn't
  // have any records for this log.
  req->onReply(node(2), E::OK, false);
  // Should be counted a dead end, and finish with a partial result.
  cb.assertCalled(E::OK, false);
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

// Check that haveShardAuthoritativeStatusDifferences() works.
TEST_F(IsLogEmptyRequestTest, ShardAuthStatusDifferenceCheck) {
  Callback cb;
  std::unique_ptr<MockIsLogEmptyRequest> req;
  // Have N0's shard be authoritative empty from the start
  changeShardStartingAuthStatus(
      node(0), AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  req = std::make_unique<MockIsLogEmptyRequest>(
      this,
      6,
      ReplicationProperty({{NodeLocationScope::NODE, 3}}),
      cb,
      req,
      /*grace_period=*/std::chrono::milliseconds(500));
  cb.assertNotCalled();
  ASSERT_TRUE(req->isMockJobTimerActive());
  ASSERT_FALSE(req->isMockGracePeriodTimerActive());
  req->onReply(node(0), E::CONNFAILED, false);
  cb.assertNotCalled();
  req->onReply(node(1), E::OK, false);
  cb.assertNotCalled();

  // Inject authoritative status differences, verify that check catches them.
  // First with NA, then with FD.
  ASSERT_FALSE(req->haveShardAuthoritativeStatusDifferences());
  req->printShardStateComparison();
  req->injectNAShardStatus(node(2), AuthoritativeStatus::UNDERREPLICATION);
  req->printShardStateComparison();
  ASSERT_TRUE(req->haveShardAuthoritativeStatusDifferences());
  req->injectFDShardStatus(node(2), AuthoritativeStatus::UNDERREPLICATION);
  req->printShardStateComparison();
  ASSERT_FALSE(req->haveShardAuthoritativeStatusDifferences());
  req->injectFDShardStatus(node(5), AuthoritativeStatus::UNAVAILABLE);
  req->printShardStateComparison();
  ASSERT_TRUE(req->haveShardAuthoritativeStatusDifferences());
  req->injectNAShardStatus(node(5), AuthoritativeStatus::UNAVAILABLE);
  ASSERT_FALSE(req->haveShardAuthoritativeStatusDifferences());
  req->printShardStateComparison();

  // Make request finish non-empty.
  req->onReply(node(5), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(2), E::REBUILDING, false);
  cb.assertNotCalled();
  req->onReply(node(3), E::OK, false);
  cb.assertNotCalled();
  req->onReply(node(4), E::OK, false);
  cb.assertCalled(E::OK, false);
  cb.printShardStateComparison();
  ASSERT_FALSE(cb.completionConditionCalled());
  ASSERT_FALSE(cb.wasGracePeriodTimerActive());
}

} // namespace
