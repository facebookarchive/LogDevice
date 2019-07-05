/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"

#include <gtest/gtest.h>

#include "logdevice/common/LinearCopySetSelector.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

static constexpr logid_t kLogID{1};
static constexpr shard_index_t kShard{0};
static constexpr epoch_t kPurgeTo{10};

#define N0S0 ShardID(0, 0)
#define N1S0 ShardID(1, 0)
#define N2S0 ShardID(2, 0)
#define N3S0 ShardID(3, 0)
#define N4S0 ShardID(4, 0)
#define N5S0 ShardID(5, 0)
#define N6S0 ShardID(6, 0)
#define N7S0 ShardID(7, 0)
#define N8S0 ShardID(8, 0)
#define N9S0 ShardID(9, 0)
#define N10S0 ShardID(10, 0)
#define MY_NODE_ID NodeID(0, 1)

class MyCopySetSelectorDeps;
class MockGetEpochRecoveryMetadataRequest;
class MockedNodeSetAccessor;

class GetEpochRecoveryMetadataRequestCallback {
 public:
  void operator()(Status status, std::unique_ptr<EpochRecoveryStateMap> map) {
    called_ = true;
    result_ = *map;
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, EpochRecoveryStateMap map) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_status, status_);
    ASSERT_EQ(map.size(), result_.size());
    for (auto& entry : map) {
      ASSERT_TRUE(result_.count(entry.first));
      ASSERT_EQ(result_.at(entry.first), map.at(entry.first));
      result_.erase(entry.first);
    }
    ASSERT_TRUE(result_.empty());
  }

 private:
  bool called_ = false;
  EpochRecoveryStateMap result_;
  Status status_;
};

class GetEpochRecoveryMetadataRequestTest : public ::testing::Test {
 public:
  void init(int nodeSetSize = 10) {
    dbg::currentLevel = dbg::Level::DEBUG;
    dbg::assertOnData = true;

    Configuration::Nodes nodes;
    NodeSetTestUtil::addNodes(&nodes, 10, 1, "rg0.dc0.cl0.ro0.rk0", 1);
    Configuration::NodesConfig nodes_config(std::move(nodes));
    auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
    NodeSetTestUtil::addLog(logs_config.get(), kLogID, 1, 0, 1, {});
    config_ = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(
            "GetEpochRecoveryMetadataRequestTest", std::move(nodes_config)),
        std::move(logs_config));
    int i = 0;
    while (i < nodeSetSize) {
      nodes_.push_back(ShardID(node_index_t(i), shard_index_t(0)));
      i++;
    }
    epoch_metadata_ = std::make_shared<EpochMetaData>(nodes_, replication_);
    nodeset_state_ = std::make_shared<NodeSetState>(
        nodes_, kLogID, NodeSetState::HealthCheck::DISABLED);

    deps_ = std::make_unique<MyCopySetSelectorDeps>();
    rng_wrapper_ = std::make_unique<DefaultRNG>();
  }

  std::unique_ptr<MockGetEpochRecoveryMetadataRequest>
  createRequest(epoch_t start, epoch_t end) {
    auto request =
        std::make_unique<MockGetEpochRecoveryMetadataRequest>(this,
                                                              worker_id_t(0),
                                                              kLogID,
                                                              kShard,
                                                              start,
                                                              end,
                                                              kPurgeTo,
                                                              epoch_metadata_,
                                                              std::ref(cb_));
    request_ = request.get();
    return request;
  }

  void fireDeferredCompleteTimer();

  void sendNotReadyReplyFromSelf();

  void verifyShardsStateInError(StorageSet set);

  MockGetEpochRecoveryMetadataRequest* request_;
  MockedNodeSetAccessor* accessor_;
  bool requestSendSuccess_{true};
  std::shared_ptr<EpochMetaData> epoch_metadata_;
  GetEpochRecoveryMetadataRequestCallback cb_;
  std::vector<node_index_t> destination_;
  std::shared_ptr<Configuration> config_;
  Settings settings_ = create_default_settings<Settings>();
  BackoffTimer* wave_timer_{nullptr};
  std::shared_ptr<NodeSetState> nodeset_state_;
  std::unique_ptr<MyCopySetSelectorDeps> deps_;
  ReplicationProperty replication_{{NodeLocationScope::NODE, 3}};
  std::unique_ptr<RNG> rng_wrapper_;
  StorageSet nodes_;
  node_index_t destinationRunningOldProto = -1;
  node_index_t destinationUnroutable = -1;
  bool deferredCompleteTimerCreated_{false};
  bool deferredCompleteCalled_{false};
  bool deferredCompleteTimerActivated_{false};
  TailRecord tail_record;
  OffsetMap epoch_size_map;
  OffsetMap epoch_end_offsets;
};

class MyCopySetSelectorDeps : public CopySetSelectorDependencies,
                              public NodeAvailabilityChecker {
 public:
  NodeStatus checkNode(NodeSetState*,
                       ShardID shard,
                       StoreChainLink* destination_out,
                       bool /*ignore_nodeset_state*/,
                       bool /*allow_unencrypted_conections*/) const override {
    *destination_out = {shard, ClientID()};
    return NodeAvailabilityChecker::NodeStatus::AVAILABLE;
  }

  const NodeAvailabilityChecker* getNodeAvailability() const override {
    return this;
  }
};

class MyLinearCopySetSelector : public LinearCopySetSelector {
 public:
  explicit MyLinearCopySetSelector(GetEpochRecoveryMetadataRequestTest* test)
      : LinearCopySetSelector(test->replication_.getReplicationFactor(),
                              test->nodes_,
                              test->nodeset_state_,
                              test->deps_.get()) {}
};

class MockedNodeSetAccessor : public StorageSetAccessor {
 public:
  explicit MockedNodeSetAccessor(
      GetEpochRecoveryMetadataRequestTest* test,
      logid_t logid,
      EpochMetaData epoch_metadata,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion,
      StorageSetAccessor::Property property,
      Settings settings)
      : StorageSetAccessor(logid,
                           epoch_metadata,
                           nodes_configuration,
                           node_access,
                           completion,
                           property),
        test_(test),
        settings_(settings) {
    rng_ = test->rng_wrapper_.get();
  }

  std::unique_ptr<Timer>
  createJobTimer(std::function<void()> /*callback*/) override {
    return nullptr;
  }

  void cancelJobTimer() override {}
  void activateJobTimer() override {}

  std::unique_ptr<CopySetSelector> createCopySetSelector(
      logid_t /*unused*/,
      const EpochMetaData& /*unused*/,
      std::shared_ptr<NodeSetState> /*unused*/,
      const std::shared_ptr<
          const configuration::nodes::NodesConfiguration>& /*unused*/)
      override {
    return std::make_unique<MyLinearCopySetSelector>(test_);
  }

  std::unique_ptr<BackoffTimer>
  createWaveTimer(std::function<void()> callback) override {
    auto timer = std::make_unique<MockBackoffTimer>();
    timer->setCallback(callback);
    test_->wave_timer_ = timer.get();
    return std::move(timer);
  }

  ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return empty_map_;
  }

 private:
  ShardAuthoritativeStatusMap empty_map_;
  GetEpochRecoveryMetadataRequestTest* const test_;
  Settings settings_;
};

class MockGetEpochRecoveryMetadataRequest
    : public GetEpochRecoveryMetadataRequest {
  using MockSender = SenderTestProxy<MockGetEpochRecoveryMetadataRequest>;

 public:
  MockGetEpochRecoveryMetadataRequest(GetEpochRecoveryMetadataRequestTest* test,
                                      worker_id_t worker_id,
                                      logid_t log_id,
                                      shard_index_t shard,
                                      epoch_t start,
                                      epoch_t end,
                                      epoch_t purge_to,
                                      std::shared_ptr<EpochMetaData> metadata,
                                      CompletionCallback cb)
      : GetEpochRecoveryMetadataRequest(worker_id,
                                        log_id,
                                        shard,
                                        start,
                                        end,
                                        purge_to,
                                        metadata,
                                        cb) {
    test_ = test;
    sender_ = std::make_unique<MockSender>(this);
  }

  void registerWithWorker() override {}

  void deleteThis() override {}

  void fireDeferredCompleteTimer() {
    static_cast<MockTimer*>(deferredCompleteTimer_.get())->trigger();
  }

  std::unique_ptr<StorageSetAccessor> makeStorageSetAccessor(
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion) override {
    auto accessor = std::make_unique<MockedNodeSetAccessor>(
        test_,
        getLogId(),
        *epoch_metadata_,
        getNodesConfiguration(),
        node_access,
        completion,
        StorageSetAccessor::Property::REPLICATION,
        getSettings());
    test_->accessor_ = accessor.get();
    return accessor;
  }

  std::unique_ptr<BackoffTimer> createRetryTimer() override {
    return std::make_unique<MockBackoffTimer>();
  }

  std::unique_ptr<Timer>
  createDeferredCompleteTimer(std::function<void()> cb) override {
    ld_check(!test_->deferredCompleteTimerCreated_);
    test_->deferredCompleteTimerCreated_ = true;
    return std::make_unique<MockTimer>(cb);
  }

  void activateDeferredCompleteTimer() override {
    ASSERT_FALSE(test_->deferredCompleteTimerActivated_);
    test_->deferredCompleteTimerActivated_ = true;
    dynamic_cast<MockTimer*>(deferredCompleteTimer_.get())
        ->activate(std::chrono::microseconds(0));
  }

  void deferredComplete() override {
    ld_check(!test_->deferredCompleteCalled_);
    test_->deferredCompleteCalled_ = true;
    GetEpochRecoveryMetadataRequest::deferredComplete();
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const {
    return test_->config_->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  const Settings& getSettings() const override {
    return test_->settings_;
  }

  const ShardAuthoritativeStatusMap& getShardAuthoritativeStatusMap() override {
    return shard_authoritative_status_map_;
  }

  NodeID getMyNodeID() override {
    return MY_NODE_ID;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& /*msg*/,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback* /*unused*/) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }

    NodeID destination = addr.id_.node_;
    EXPECT_TRUE(destination.isNodeID());

    if (test_->destinationUnroutable == destination.index()) {
      err = E::UNROUTABLE;
      return -1;
    }
    if (test_->destinationRunningOldProto == destination.index()) {
      err = E::PROTONOSUPPORT;
      return -1;
    }
    test_->destination_.push_back(destination.index());
    return 0;
  }

  ShardAuthoritativeStatusMap shard_authoritative_status_map_;
  GetEpochRecoveryMetadataRequestTest* test_;
};

void GetEpochRecoveryMetadataRequestTest::fireDeferredCompleteTimer() {
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  ASSERT_TRUE(deferredCompleteTimerActivated_);
  request_->fireDeferredCompleteTimer();
}

void GetEpochRecoveryMetadataRequestTest::sendNotReadyReplyFromSelf() {
  auto result = std::find(
      std::begin(destination_), std::end(destination_), node_index_t(0));
  if (result != std::end(destination_)) {
    destination_.erase(result);
    request_->onReply(N0S0, E::NOTREADY, nullptr);
    verifyShardsStateInError({ShardID(0, 0)});
  }
}

void GetEpochRecoveryMetadataRequestTest::verifyShardsStateInError(
    StorageSet set) {
  ASSERT_EQ(set,
            accessor_->getShardsInState(
                StorageSetAccessor::ShardState::PERMANENT_ERROR));
}

TEST_F(GetEpochRecoveryMetadataRequestTest, BasicTest) {
  init();
  auto request = createRequest(epoch_t(1), epoch_t(1));
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  EpochRecoveryMetadata md(epoch_t(2),
                           esn_t(10),
                           esn_t(10),
                           0,
                           tail_record,
                           epoch_size_map,
                           epoch_end_offsets);
  EpochRecoveryStateMap map{{1, {E::OK, md}}};
  request->execute();
  int i = 0;
  // All but one reply with E:NOTREADY
  while (i < destination_.size() - 1) {
    request->onReply(ShardID(destination_[i], 0), E::NOTREADY, nullptr);
    ASSERT_FALSE(deferredCompleteCalled_);
    cb_.assertNotCalled();
    i++;
  }
  request->onReply(ShardID(destination_[i], 0),
                   E::OK,
                   std::make_unique<EpochRecoveryStateMap>(map));
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, map);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, BasicTest2) {
  init();
  auto request = createRequest(epoch_t(1), epoch_t(2));
  epoch_end_offsets.setCounter(BYTE_OFFSET, 0);
  epoch_size_map.setCounter(BYTE_OFFSET, 0);
  EpochRecoveryMetadata md1(epoch_t(2),
                            esn_t(10),
                            esn_t(10),
                            0,
                            tail_record,
                            epoch_size_map,
                            epoch_end_offsets);
  EpochRecoveryStateMap map1{
      {1, {E::OK, md1}}, {2, {E::EMPTY, EpochRecoveryMetadata()}}};
  request->execute();
  sendNotReadyReplyFromSelf();
  request->onReply(ShardID(destination_[0], 0),
                   E::OK,
                   std::make_unique<EpochRecoveryStateMap>(map1));
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, map1);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, AllFullyAuthoritativeNodesRespond) {
  init(4);
  auto request = createRequest(epoch_t(1), epoch_t(3));
  EpochRecoveryStateMap map{{1, {E::EMPTY, EpochRecoveryMetadata()}},
                            {2, {E::EMPTY, EpochRecoveryMetadata()}},
                            {3, {E::NOTREADY, EpochRecoveryMetadata()}}};

  EpochRecoveryStateMap result{{1, {E::EMPTY, EpochRecoveryMetadata()}},
                               {2, {E::EMPTY, EpochRecoveryMetadata()}},
                               {3, {E::UNKNOWN, EpochRecoveryMetadata()}}};
  // N0,N3 is FULLY_AUTHORITATIVE

  // Set N1 as AUTHORITATIVE_EMPTY
  request->shard_authoritative_status_map_.setShardStatus(
      node_index_t(1),
      shard_index_t(0),
      AuthoritativeStatus::AUTHORITATIVE_EMPTY);

  // Set N2 as UNAVAILABLE
  request->shard_authoritative_status_map_.setShardStatus(
      node_index_t(2), shard_index_t(0), AuthoritativeStatus::UNAVAILABLE);

  request->execute();
  request->onReply(N3S0, E::OK, std::make_unique<EpochRecoveryStateMap>(map));
  ASSERT_FALSE(deferredCompleteCalled_);
  cb_.assertNotCalled();

  sendNotReadyReplyFromSelf();
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, result);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, SingleEmpty) {
  init(4);
  auto request = createRequest(epoch_t(1), epoch_t(2));
  EpochRecoveryStateMap map{{1, {E::EMPTY, EpochRecoveryMetadata()}},
                            {2, {E::EMPTY, EpochRecoveryMetadata()}}};
  // Set N0 - N2 as AUTHORITATIVE_EMPTY
  for (int i = 0; i < 3; i++) {
    request->shard_authoritative_status_map_.setShardStatus(
        node_index_t(i),
        shard_index_t(0),
        AuthoritativeStatus::AUTHORITATIVE_EMPTY);
  }
  request->execute();
  // Node\Epoch
  //    Epoch 1   Epoch 2
  // 0  NOTREADY  NOTREADY
  // 1  NOTREADY  NOTREADY
  // 2  NOTREADY  NOTREADY
  // 3  EMPTY     EMPTY
  sendNotReadyReplyFromSelf();

  request->onReply(N1S0, E::NOTREADY, nullptr);
  ASSERT_FALSE(deferredCompleteCalled_);
  cb_.assertNotCalled();
  request->onReply(N3S0, E::EMPTY, nullptr);
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, map);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, SingleEmpty2) {
  init(4);
  auto request = createRequest(epoch_t(1), epoch_t(2));
  EpochRecoveryStateMap map1{{1, {E::EMPTY, EpochRecoveryMetadata()}},
                             {2, {E::NOTREADY, EpochRecoveryMetadata()}}};
  EpochRecoveryStateMap map2{{1, {E::EMPTY, EpochRecoveryMetadata()}},
                             {2, {E::EMPTY, EpochRecoveryMetadata()}}};

  request->shard_authoritative_status_map_.setShardStatus(
      node_index_t(1),
      shard_index_t(0),
      AuthoritativeStatus::AUTHORITATIVE_EMPTY);

  request->execute();
  request->onReply(N1S0, E::OK, std::make_unique<EpochRecoveryStateMap>(map1));
  ASSERT_FALSE(deferredCompleteCalled_);
  cb_.assertNotCalled();
  sendNotReadyReplyFromSelf();
  ASSERT_FALSE(deferredCompleteCalled_);
  cb_.assertNotCalled();

  request->onReply(N3S0, E::OK, std::make_unique<EpochRecoveryStateMap>(map2));
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, map2);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, NodeWithOldProtocol_Single) {
  init(4);
  destinationRunningOldProto = node_index_t(3);
  auto request = createRequest(epoch_t(1), epoch_t(1));
  request->execute();
  ASSERT_FALSE(deferredCompleteCalled_);
  cb_.assertNotCalled();
  EpochRecoveryStateMap map{{1, {E::EMPTY, EpochRecoveryMetadata()}}};
  request->onReply(N3S0, E::OK, std::make_unique<EpochRecoveryStateMap>(map));
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::OK, map);
}

TEST_F(GetEpochRecoveryMetadataRequestTest, NodeWithOldProtocol_Range) {
  init(4);
  destinationRunningOldProto = node_index_t(3);
  auto request = createRequest(epoch_t(1), epoch_t(2));
  request->execute();
  EpochRecoveryStateMap map{{1, {E::UNKNOWN, EpochRecoveryMetadata()}},
                            {2, {E::UNKNOWN, EpochRecoveryMetadata()}}};
  ASSERT_TRUE(deferredCompleteCalled_);
  ASSERT_TRUE(deferredCompleteTimerCreated_);
  fireDeferredCompleteTimer();
  cb_.assertCalled(E::ABORTED, map);
}

}} // namespace facebook::logdevice
