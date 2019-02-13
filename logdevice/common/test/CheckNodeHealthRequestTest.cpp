/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CheckNodeHealthRequest.h"

#include <gtest/gtest.h>

#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"

#define N1 ShardID(1, 0)

namespace facebook { namespace logdevice {

class MockNodeSetState : public NodeSetState {
 public:
  MockNodeSetState(const StorageSet& shards,
                   logid_t log_id,
                   HealthCheck healthCheck)
      : NodeSetState(shards, log_id, healthCheck) {}

  void postHealthCheckRequest(ShardID, bool /* unused */) override {}

  std::chrono::steady_clock::time_point
  getDeadline(NotAvailableReason /* unused */) const override {
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  }

  bool shouldPerformSpaceBasedRetention() const override {
    return true;
  }
};

class CheckNodeHealthRequestTest : public ::testing::Test {
 public:
  void init(size_t nnodes) {
    int i = 0;
    while (i < nnodes) {
      storage_set_.push_back(ShardID(i, 0));
      i++;
    }
    storage_set_state_ = std::make_shared<MockNodeSetState>(
        storage_set_, logid_t(1), NodeSetState::HealthCheck::ENABLED);
    // All Available
    for (auto shard : storage_set_) {
      EXPECT_EQ(NodeSetState::NotAvailableReason::NONE,
                storage_set_state_->getNotAvailableReason(shard));
    }
  }

  void sendResponse(Status st, request_id_t request_id) {
    const auto& index =
        request_set_.set.get<CheckNodeHealthRequestSet::RequestIndex>();
    auto it = index.find(request_id);
    ASSERT_TRUE(it != index.end());
    (*it)->onReply(st);
  }

  void onConnectionClosed(node_index_t index) {
    auto* callback = socket_callbacks_[index];
    ASSERT_NE(nullptr, callback);
    (*callback)(E::PEER_CLOSED, Address(NodeID(index, 1)));
  }

  void makeShardUnavailable(ShardID shard) {
    auto reason = NodeSetState::NotAvailableReason::UNROUTABLE;
    std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() - std::chrono::seconds(10);
    storage_set_state_->setNotAvailableUntil(shard, deadline, reason);
    EXPECT_EQ(reason, storage_set_state_->getNotAvailableReason(shard));

    storage_set_state_->checkNotAvailableUntil(
        shard, std::chrono::steady_clock::now());
    EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
              storage_set_state_->getNotAvailableReason(shard));
  }

  StorageSet storage_set_;
  bool healthRequestSendSuccess_;
  CheckNodeHealthRequestSet request_set_;
  std::shared_ptr<NodeSetState> storage_set_state_;
  std::unordered_map<node_index_t, SocketCallback*> socket_callbacks_;
};

class MockCheckNodeHealthRequest : public CheckNodeHealthRequest {
 public:
  using MockSender = SenderTestProxy<MockCheckNodeHealthRequest>;

  MockCheckNodeHealthRequest(
      CheckNodeHealthRequestTest* test,
      NodeID node_id,
      std::shared_ptr<NodeSetState> storage_set_state_,
      logid_t log_id,
      nodeset_state_id_t storage_set_state_id,
      std::chrono::seconds request_timeout,
      const Settings& settings = create_default_settings<Settings>())
      : CheckNodeHealthRequest(ShardID(node_id.index(), 0),
                               storage_set_state_,
                               log_id,
                               storage_set_state_id,
                               request_timeout,
                               false),
        settings_(settings),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  CheckNodeHealthRequestSet& pendingHealthChecks() override {
    return test_->request_set_;
  }

  const Settings& getSettings() override {
    return settings_;
  }

  void activateRequestTimeoutTimer() override {
    EXPECT_FALSE(timer_active_);
    timer_active_ = true;
  }

  void fireTimer() {
    EXPECT_TRUE(timer_active_);
    timer_active_ = false;
    noReply();
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& /*msg*/,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback* callback) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }
    if (!test_->healthRequestSendSuccess_) {
      err = E::UNROUTABLE;
      return -1;
    }
    NodeID destination = addr.id_.node_;
    EXPECT_TRUE(destination.isNodeID());
    test_->socket_callbacks_[destination.index()] = callback;
    return 0;
  }

  Settings settings_;
  CheckNodeHealthRequestTest* test_;
  bool timer_active_ = false;
  const int NUM_SHARDS = 15;
};

TEST_F(CheckNodeHealthRequestTest, BasicTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  request_id_t req_id = req->id_;
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  // When execute function returns CONTINUE, the request is
  // inserted to map and ownership transferred. Release ownership here
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // Make N1 available
  sendResponse(Status::OK, req_id);
  EXPECT_EQ(NodeSetState::NotAvailableReason::NONE,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, NodeUnhealthyTest) {
  init(4);
  // Make N1 unavailable
  makeShardUnavailable(N1);

  storage_set_state_->checkNotAvailableUntil(
      N1, std::chrono::steady_clock::now());
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  auto req_id = req->id_;
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // N1 still unavailable but for a different reason
  sendResponse(Status::DISABLED, req_id);
  EXPECT_EQ(NodeSetState::NotAvailableReason::STORE_DISABLED,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, NotAStorageNode) {
  init(4);
  // Make N1 unavailable
  makeShardUnavailable(N1);

  storage_set_state_->checkNotAvailableUntil(
      N1, std::chrono::steady_clock::now());
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  auto req_id = req->id_;
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // N1 still unavailable but for a different reason
  sendResponse(Status::NOTSTORAGE, req_id);
  EXPECT_EQ(NodeSetState::NotAvailableReason::STORE_DISABLED,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, HealthCheckDelayedReplyTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  auto raw_req = req.get();
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // Reponse delayed.
  raw_req->fireTimer();

  EXPECT_EQ(NodeSetState::NotAvailableReason::UNROUTABLE,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, HealthCheckSendFailedTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = false;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});

  EXPECT_EQ(Request::Execution::COMPLETE, req->execute());
  EXPECT_EQ(NodeSetState::NotAvailableReason::UNROUTABLE,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, DuplicateHealthCheckTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  auto req2 =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  auto req_id_2 = req2->id_;

  // should erase previous request
  EXPECT_EQ(Request::Execution::CONTINUE, req2->execute());
  req2.release();

  auto key = boost::make_tuple(storage_set_state_->id_, ShardID(1, 0));
  auto& index =
      request_set_.set
          .get<CheckNodeHealthRequestSet::NodeSetStateIdShardIDIndex>();
  auto it = index.find(key);

  ASSERT_TRUE(it != index.end());
  EXPECT_EQ((*it)->id_, req_id_2);

  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_FALSE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, StoreOverridesHealthCheckTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  auto req_id = req->id_;

  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // Simulate a store response setting an unavailable state
  storage_set_state_->setNotAvailableUntil(
      N1,
      std::chrono::steady_clock::now(),
      NodeSetState::NotAvailableReason::STORE_DISABLED);
  EXPECT_EQ(NodeSetState::NotAvailableReason::STORE_DISABLED,
            storage_set_state_->getNotAvailableReason(N1));

  // now we get a response for health check.
  sendResponse(Status::OVERLOADED, req_id);
  // Should not affect the status since the state already changed from PROBING
  EXPECT_EQ(NodeSetState::NotAvailableReason::STORE_DISABLED,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

TEST_F(CheckNodeHealthRequestTest, ConnectionClosedWithPendingProbeTest) {
  init(4);

  // Make N1 unavailable
  makeShardUnavailable(N1);

  // Create a health check request
  healthRequestSendSuccess_ = true;
  auto req =
      std::make_unique<MockCheckNodeHealthRequest>(this,
                                                   NodeID(1, 1),
                                                   storage_set_state_,
                                                   logid_t(1),
                                                   storage_set_state_->id_,
                                                   std::chrono::seconds{10});
  EXPECT_EQ(Request::Execution::CONTINUE, req->execute());
  req.release();
  EXPECT_EQ(NodeSetState::NotAvailableReason::PROBING,
            storage_set_state_->getNotAvailableReason(N1));

  // simulate connection getting closed.
  onConnectionClosed(1);

  EXPECT_EQ(NodeSetState::NotAvailableReason::UNROUTABLE,
            storage_set_state_->getNotAvailableReason(N1));
  ASSERT_TRUE(request_set_.set.empty());
}

}} // namespace facebook::logdevice
