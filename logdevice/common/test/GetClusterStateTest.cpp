/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockGetClusterStateRequest : public GetClusterStateRequest {
 public:
  MockGetClusterStateRequest(Settings settings,
                             std::shared_ptr<ServerConfig> config,
                             ClusterState* cluster_state,
                             get_cs_callback_t cb,
                             folly::Optional<NodeID> dest = folly::none)
      : GetClusterStateRequest(settings.get_cluster_state_timeout,
                               settings.get_cluster_state_wave_timeout,
                               cb,
                               dest),
        settings_(settings),
        config_(config),
        cluster_state_(cluster_state) {}

 protected:
  void initTimers() override {}
  void activateWaveTimer() override {}

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  ClusterState* getClusterState() const override {
    return cluster_state_;
  }

  bool sendTo(NodeID to) override {
    recipients_.push_back(to);
    return false;
  }

  const Settings& getSettings() const override {
    return settings_;
  }

  void attachToWorker() override {}
  void destroyRequest() override {}

 public:
  std::vector<NodeID> recipients_;
  Settings settings_;
  std::shared_ptr<ServerConfig> config_;
  ClusterState* cluster_state_;
};

class GetClusterStateTest : public ::testing::Test {
 public:
  void init(size_t nnodes, size_t nlogs) {
    config_ = createSimpleConfig(nnodes, nlogs);
    cluster_state_ = std::make_unique<MockClusterState>(nnodes);
  }

  std::unique_ptr<MockGetClusterStateRequest>
  create(folly::Optional<NodeID> dest,
         std::chrono::milliseconds timeout = std::chrono::seconds(1),
         std::chrono::milliseconds wave_timeout = std::chrono::seconds(1)) {
    callback_called_ = false;
    auto cb = [&](Status st,
                  std::vector<uint8_t> nodes_state,
                  std::vector<node_index_t> boycotted_nodes) {
      ASSERT_FALSE(callback_called_);
      callback_called_ = true;
      status_ = st;
      nodes_state_ = nodes_state;
      boycotted_nodes_ = boycotted_nodes;
    };

    Settings settings = create_default_settings<Settings>();
    settings.get_cluster_state_timeout = timeout;
    settings.get_cluster_state_wave_timeout = wave_timeout;
    return std::make_unique<MockGetClusterStateRequest>(
        settings, config_->serverConfig(), cluster_state_.get(), cb, dest);
  }

  void checkStatus(Status st) {
    ASSERT_TRUE(callback_called_);
    ASSERT_EQ(status_, st);
  }

  void checkNotDone() {
    ASSERT_FALSE(callback_called_);
  }

 private:
  std::shared_ptr<Configuration> config_;
  std::unique_ptr<ClusterState> cluster_state_;

  bool callback_called_{false};
  Status status_;
  std::vector<uint8_t> nodes_state_;
  std::vector<node_index_t> boycotted_nodes_;
};

TEST_F(GetClusterStateTest, SendToExplicitDest) {
  init(4, 1);

  NodeID dest(0, 1);
  auto req = create(dest);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  ASSERT_EQ(req->recipients_.size(), 1);
  ASSERT_EQ(req->recipients_[0], dest);

  ASSERT_TRUE(req->onReply(Address(dest), E::OK, {0, 0, 0, 0}, {}));
  checkStatus(E::OK);
}

TEST_F(GetClusterStateTest, SendToExplicitDestFailed) {
  init(4, 1);

  NodeID dest(0, 1);
  for (auto e : {E::PROTONOSUPPORT, E::NOTSUPPORTED, E::FAILED}) {
    auto req = create(dest);
    ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

    ASSERT_EQ(req->recipients_.size(), 1);
    ASSERT_EQ(req->recipients_[0], dest);
    // Clear the vector to make sure that the next reply (with error) does not
    // trigger a new wave.
    req->recipients_.clear();

    ASSERT_TRUE(req->onReply(Address(dest), e, {0, 0, 0, 0}, {}));
    ASSERT_TRUE(req->recipients_.empty());
    checkStatus(e);
  }
}

TEST_F(GetClusterStateTest, SendToExplicitDestTimeout) {
  init(4, 1);

  NodeID dest(0, 1);
  auto req = create(dest);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  ASSERT_EQ(req->recipients_.size(), 1);
  ASSERT_EQ(req->recipients_[0], dest);

  ASSERT_TRUE(req->onTimeout());
  checkStatus(E::TIMEDOUT);
}

TEST_F(GetClusterStateTest, SendToExplicitDestWaveTimeout) {
  init(4, 1);

  NodeID dest(0, 1);
  auto req = create(dest);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  ASSERT_EQ(req->recipients_.size(), 1);
  ASSERT_EQ(req->recipients_[0], dest);

  ASSERT_TRUE(req->onWaveTimeout());
  checkStatus(E::TIMEDOUT);
}

TEST_F(GetClusterStateTest, Simple) {
  init(4, 1);

  auto req = create(folly::none);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  // make sure it sent a wave of 2 messages
  ASSERT_EQ(req->recipients_.size(), 2);
  auto dest = req->recipients_[0];

  ASSERT_TRUE(req->onReply(Address(dest), E::OK, {0, 0, 0, 0}, {}));
  checkStatus(E::OK);
}

TEST_F(GetClusterStateTest, Timeout) {
  init(4, 1);

  auto req = create(folly::none);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  // make sure it sent a wave of 2 messages
  ASSERT_EQ(req->recipients_.size(), 2);

  ASSERT_TRUE(req->onTimeout());
  checkStatus(E::TIMEDOUT);
}

TEST_F(GetClusterStateTest, WaveTimeout) {
  init(4, 1);

  auto req = create(folly::none);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  // make sure it sent a wave of 2 messages
  ASSERT_EQ(req->recipients_.size(), 2);

  ASSERT_FALSE(req->onWaveTimeout());

  // make sure it sent another wave
  ASSERT_EQ(req->recipients_.size(), 4);

  ASSERT_TRUE(req->onWaveTimeout());

  checkStatus(E::TIMEDOUT);
}

TEST_F(GetClusterStateTest, FirstWaveFailed) {
  init(4, 1);

  auto req = create(folly::none);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  // make sure it sent a wave of 2 messages
  ASSERT_EQ(req->recipients_.size(), 2);
  auto dest = req->recipients_[0];

  ASSERT_FALSE(req->onReply(Address(dest), E::FAILED, {0, 0, 0, 0}, {}));
  checkNotDone();
  ASSERT_EQ(req->recipients_.size(), 2);

  dest = req->recipients_[1];
  ASSERT_FALSE(req->onReply(Address(dest), E::FAILED, {0, 0, 0, 0}, {}));
  checkNotDone();

  // it should send another wave
  ASSERT_EQ(req->recipients_.size(), 4);

  dest = req->recipients_[2];
  ASSERT_TRUE(req->onReply(Address(dest), E::OK, {0, 0, 0, 0}, {}));

  checkStatus(E::OK);
}

}} // namespace facebook::logdevice
