/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/GetRsmSnapshotRequest.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class GetRsmSnapshotRequestTest;
class MockGetRsmSnapshotRequest : public GetRsmSnapshotRequest {
  using MockSender = SenderTestProxy<MockGetRsmSnapshotRequest>;

 public:
  MockGetRsmSnapshotRequest(
      Settings settings,
      std::shared_ptr<const NodesConfiguration> nodes_config,
      ClusterState* cluster_state,
      RSMSnapshotStore::snapshot_cb_t cb)
      : GetRsmSnapshotRequest(0, WorkerType::GENERAL, "1", LSN_OLDEST, cb),
        settings_(settings),
        nodes_config_(nodes_config),
        cluster_state_(cluster_state) {
    sender_ = std::make_unique<MockSender>(this);
  }

  std::unordered_set<node_index_t> getCandidates() {
    return candidates_;
  }

  size_t candidatesSize() {
    return candidates_.size();
  }

  NodeID getLastDest() {
    return last_dest_;
  }

  GET_RSM_SNAPSHOT_REPLY_Header&
  getHeader(GET_RSM_SNAPSHOT_REPLY_Message& msg) {
    return msg.header_;
  }

  GET_RSM_SNAPSHOT_flags_t getFlags() {
    return flags_;
  }

  void initTimers() override {}
  void activateWaveTimer() override {}

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return nodes_config_;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
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

    GET_RSM_SNAPSHOT_Message* rsm_msg =
        dynamic_cast<GET_RSM_SNAPSHOT_Message*>(msg.get());
    flags_ = rsm_msg->getHeader().flags;
    recipients_.push_back(destination);
    last_dest_ = destination;
    return 0;
  }

  ClusterState* getClusterState() const override {
    return cluster_state_;
  }

  void registerWithWorker() override {}
  void destroyRequest() override {}

  std::vector<NodeID> recipients_;
  Settings settings_;
  std::shared_ptr<const NodesConfiguration> nodes_config_;
  GetRsmSnapshotRequestTest* test_;
  ClusterState* cluster_state_;
};

class GetRsmSnapshotRequestTest : public ::testing::Test {
 public:
  void init(size_t nnodes, size_t nlogs) {
    nodes_config_ = createSimpleNodesConfig(nnodes);
    cluster_state_ = std::make_unique<MockClusterState>(nnodes);
  }

  std::unique_ptr<MockGetRsmSnapshotRequest>
  create(std::chrono::milliseconds timeout = std::chrono::seconds(1),
         std::chrono::milliseconds wave_timeout = std::chrono::seconds(1)) {
    callback_called_ = false;

    Settings settings = create_default_settings<Settings>();
    settings.get_cluster_state_timeout = timeout;
    settings.get_cluster_state_wave_timeout = wave_timeout;
    auto snapshot_cb = [&](Status st,
                           std::string /* unused */,
                           RSMSnapshotStore::SnapshotAttributes /* unused */) {
      ASSERT_FALSE(callback_called_);
      callback_called_ = true;
      status_ = st;
    };
    return std::make_unique<MockGetRsmSnapshotRequest>(
        settings, nodes_config_, cluster_state_.get(), snapshot_cb);
  }

  void checkStatus(Status st) {
    ASSERT_EQ(status_, st);
  }

  void callbackCalled(bool b) {
    ASSERT_EQ(callback_called_, b);
  }

 private:
  std::shared_ptr<const NodesConfiguration> nodes_config_;
  std::unique_ptr<ClusterState> cluster_state_;

  bool callback_called_{false};
  Status status_{E::INTERNAL};
  std::vector<uint8_t> nodes_state_;
  std::vector<node_index_t> boycotted_nodes_;
};

TEST_F(GetRsmSnapshotRequestTest, BasicRequestResponse) {
  init(4, 1);
  auto req = create();
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
  auto candidates = req->getCandidates();
  ASSERT_EQ(candidates.size(), 4); // all nodes are up

  ASSERT_EQ(req->recipients_.size(), 1);
  auto sent_to = req->recipients_[0];

  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr{
      E::OK, logid_t(1), req->id_, -1, LSN_OLDEST};
  GET_RSM_SNAPSHOT_REPLY_Message reply_msg(reply_hdr, "");
  req->onReply(Address(sent_to), reply_msg);
  checkStatus(E::OK);
  callbackCalled(true);
}

TEST_F(GetRsmSnapshotRequestTest, BasicErrors) {
  int num_nodes = 5;
  init(num_nodes, 1);
  auto rq = create();
  int available_nodes_left = num_nodes;
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  ASSERT_EQ(rq->candidatesSize(), available_nodes_left);

  Status st = E::SHUTDOWN;
  GET_RSM_SNAPSHOT_REPLY_Header hdr{st, logid_t(1), rq->id_, 0, lsn_t(1)};
  const GET_RSM_SNAPSHOT_REPLY_Message msg(hdr, "");
  auto dest = rq->recipients_[0];
  rq->onReply(Address(dest), msg); // this triggers another call to start()
  available_nodes_left--;
  ASSERT_EQ(rq->candidatesSize(), available_nodes_left);

  rq->onError(E::CONNFAILED,
              rq->getLastDest()); // this triggers another call to start()
  ASSERT_EQ(rq->getFlags(), 0);
  available_nodes_left--;
  ASSERT_EQ(rq->candidatesSize(), available_nodes_left);

  rq->onError(E::PROTONOSUPPORT,
              rq->getLastDest()); // this triggers another call to start()
  ASSERT_EQ(rq->getFlags(), 0);
  available_nodes_left--;
  ASSERT_EQ(rq->candidatesSize(), available_nodes_left);

  hdr.st = E::OK;
  const GET_RSM_SNAPSHOT_REPLY_Message msg2(hdr, "snapshot-blob");
  rq->onReply(Address(dest), msg2); // this will finalize the request
  ASSERT_EQ(rq->candidatesSize(), available_nodes_left);
}

TEST_F(GetRsmSnapshotRequestTest, WaveTimeout) {
  init(4, 1);
  auto req = create();
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
  ASSERT_EQ(req->recipients_.size(), 1);
  req->onWaveTimeout();

  // wave timeout would result in reaching out to another cluster node
  ASSERT_EQ(req->recipients_.size(), 2);
  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr{
      E::OK, logid_t(1), req->id_, -1, LSN_OLDEST};
  GET_RSM_SNAPSHOT_REPLY_Message reply_msg(reply_hdr, "");
  auto second_node = req->recipients_[1];
  req->onReply(Address(second_node), reply_msg);
  checkStatus(E::OK);
  callbackCalled(true);
}

TEST_F(GetRsmSnapshotRequestTest, RequestTimeout) {
  init(4, 1);
  auto req = create();
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
  ASSERT_EQ(req->recipients_.size(), 1);
  req->onRequestTimeout();

  checkStatus(E::TIMEDOUT);
  callbackCalled(true);
}

TEST_F(GetRsmSnapshotRequestTest, Redirect) {
  int num_nodes = 5;
  init(num_nodes, 1);
  auto req = create();
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
  ASSERT_EQ(req->recipients_.size(), 1);
  auto sent_to = req->recipients_[0];
  node_index_t expected_redirect_node{0};
  for (; expected_redirect_node < num_nodes &&
       expected_redirect_node == sent_to.index();
       expected_redirect_node++) {
  }

  // Got a REDIRECT
  GET_RSM_SNAPSHOT_REPLY_Header reply_hdr{
      E::REDIRECTED, logid_t(1), req->id_, expected_redirect_node, LSN_INVALID};
  GET_RSM_SNAPSHOT_REPLY_Message reply_msg(reply_hdr, "");
  req->onReply(Address(sent_to), reply_msg);
  ASSERT_EQ(req->recipients_.size(), 2); // we should've sent to 2 nodes by now
  auto actual_redirected_node = req->recipients_[1];
  // verify that the message was sent to REDIRECTED node
  ASSERT_EQ(actual_redirected_node.index(), expected_redirect_node);
  ASSERT_EQ(req->getFlags(), 1 /* force */);
  callbackCalled(false);

  auto& hdr = req->getHeader(reply_msg);
  hdr.st = E::OK;
  hdr.redirect_node = -1;
  req->onReply(Address(actual_redirected_node), reply_msg);
  checkStatus(E::OK);
  callbackCalled(true);
}

TEST_F(GetRsmSnapshotRequestTest, BlacklistOnErrors) {
  init(4, 1);
  for (auto e : {E::EMPTY, E::SHUTDOWN, E::NOTFOUND}) {
    auto req = create();
    ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
    ASSERT_EQ(req->recipients_.size(), 1);
    auto first_node = req->recipients_[0];

    GET_RSM_SNAPSHOT_REPLY_Header reply_hdr{
        e, logid_t(1), req->id_, -1, LSN_OLDEST};
    GET_RSM_SNAPSHOT_REPLY_Message reply_msg(reply_hdr, "");
    req->onReply(Address(first_node), reply_msg);
    ASSERT_EQ(req->recipients_.size(), 2);
    auto second_node = req->recipients_[1];
    ASSERT_NE(first_node, second_node);

    callbackCalled(false);
    auto& hdr = req->getHeader(reply_msg);
    hdr.st = E::OK;
    req->onReply(Address(second_node), reply_msg);
    checkStatus(E::OK);
    callbackCalled(true);
  }
}

TEST_F(GetRsmSnapshotRequestTest, ResetCandidateNodes) {
  int num_nodes = 5;
  init(num_nodes, 1);
  auto rq = create();
  int available_nodes_left = num_nodes;
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());

  for (int i = 0; i < num_nodes; i++, available_nodes_left--) {
    ASSERT_EQ(rq->candidatesSize(), available_nodes_left);
    rq->onError(E::CONNFAILED,
                rq->getLastDest()); // this triggers another call to start()
    ASSERT_EQ(rq->getFlags(), 0);
  }

  // Once all candidate nodes are removed, cluster state is used to refresh
  // the candidate list
  callbackCalled(false);
  ASSERT_EQ(rq->candidatesSize(), num_nodes);
}

TEST_F(GetRsmSnapshotRequestTest, RetryLimitTest) {
  int num_nodes = 5;
  int retry_limit{3};
  init(num_nodes, 1);
  auto rq = create();
  int available_nodes_left;
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());

  auto exhaust_candidates = [&]() {
    available_nodes_left = num_nodes;
    for (int i = 0; i < num_nodes; i++, available_nodes_left--) {
      ASSERT_EQ(rq->candidatesSize(), available_nodes_left);
      rq->onError(E::CONNFAILED,
                  rq->getLastDest()); // this triggers another call to start()
      ASSERT_EQ(rq->getFlags(), 0);
    }
  };

  while (retry_limit--) {
    exhaust_candidates();
    // Once all candidate nodes are removed, cluster state is used to refresh
    // the candidate list
    callbackCalled(false);
    ASSERT_EQ(rq->candidatesSize(), num_nodes);
  }

  // See what happens when we cross the limit
  exhaust_candidates();
  checkStatus(E::FAILED);
  callbackCalled(true);
  ASSERT_EQ(rq->candidatesSize(), 0);
}

TEST_F(GetRsmSnapshotRequestTest, WaveTimeoutsFollowedBySuccessfulReply) {
  int num_nodes = 5;
  init(num_nodes, 1);
  auto rq = create();
  int available_nodes_left = num_nodes;
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  ASSERT_EQ(rq->recipients_.size(), 1);

  // Hit N-1 wave timeouts before finding a successful node
  for (int i = 1; i <= num_nodes - 1; i++, available_nodes_left--) {
    ASSERT_EQ(rq->candidatesSize(), available_nodes_left);
    rq->onWaveTimeout(); // this triggers another call to start()
    callbackCalled(false);
    ASSERT_EQ(rq->recipients_.size(), i + 1);
  }

  // Only 1 node left for which start() will send out the
  // GET_RSM_SNAPSHOT_Message
  ASSERT_EQ(rq->candidatesSize(), 1);
  GET_RSM_SNAPSHOT_REPLY_Header hdr{E::OK, logid_t(1), rq->id_, 0, lsn_t(1)};
  const GET_RSM_SNAPSHOT_REPLY_Message msg(hdr, "snapshot-blob");
  auto dest = rq->recipients_[num_nodes - 1];
  rq->onReply(Address(dest), msg); // this should finalize the request
  ASSERT_EQ(rq->getFlags(), 0);
  checkStatus(E::OK);
  callbackCalled(true);

  // Verify the reply is from the only remaining node in the candidates_
  auto candidates = rq->getCandidates();
  ASSERT_EQ(candidates.size(), 1);
  auto node_left_it = candidates.begin();
  ASSERT_EQ(dest.index(), *node_left_it);
}

TEST_F(GetRsmSnapshotRequestTest, StaleReply) {
  int num_nodes = 5;
  init(num_nodes, 1);
  auto rq = create();
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  auto dest1 = rq->recipients_[0];
  GET_RSM_SNAPSHOT_REPLY_Header hdr{
      E::STALE, logid_t(1), rq->id_, 0, LSN_OLDEST};
  const GET_RSM_SNAPSHOT_REPLY_Message msg(hdr, "");
  rq->onReply(Address(dest1), msg);
  ASSERT_EQ(rq->getFlags(), 0);
  ASSERT_EQ(rq->candidatesSize(), num_nodes);
  callbackCalled(true);
  checkStatus(E::STALE);
}

}} // namespace facebook::logdevice
