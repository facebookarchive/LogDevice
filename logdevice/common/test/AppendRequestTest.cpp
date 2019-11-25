/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppendRequest.h"

#include <memory>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockAppendRequest : public AppendRequest {
 public:
  using MockSender = SenderTestProxy<MockAppendRequest>;
  MockAppendRequest(logid_t log_id,
                    std::shared_ptr<Configuration> configuration,
                    std::shared_ptr<SequencerLocator> locator,
                    ClusterState* cluster_state)
      : AppendRequest(
            nullptr,
            log_id,
            AppendAttributes(),
            Payload(),
            std::chrono::milliseconds(0),
            append_callback_t(),
            std::make_unique<MockSequencerRouter>(log_id,
                                                  this,
                                                  configuration->serverConfig(),
                                                  locator,
                                                  cluster_state)),
        settings_(create_default_settings<Settings>()) {
    dbg::assertOnData = true;

    sender_ = std::make_unique<MockSender>(this);

    // hack to prevent callback from being called by ~AppendRequest()
    failed_to_post_ = true;
    // Skip the check which requires a Client
    bypassWriteTokenCheck();
  }

  void registerRequest() override {}
  void setupTimer() override {}
  void resetServerSocketConnectThrottle(NodeID /*node_id*/) override {}
  const Settings& getSettings() const override {
    return settings_;
  }
  epoch_t getSeenEpoch(logid_t /*log*/) const override {
    return epoch_t(0);
  }
  void updateSeenEpoch(logid_t /*log_id*/, epoch_t /*seen_epoch*/) override {}

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& /*msg*/,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }
    dest_ = addr.id_.node_;
    return 0;
  }

  void destroy() override {}

  Status getStatus() const {
    return status_;
  }

  NodeID dest_;
  Settings settings_;
};

class AppendRequestTest : public ::testing::Test {
 public:
  enum class LocatorType {
    HASH_BASED, // uses consistent hashing
    STATIC,     // always picks N0
  };

  void init(size_t nnodes,
            size_t nlogs,
            LocatorType type = LocatorType::HASH_BASED) {
    auto simple_config = createSimpleConfig(nnodes, nlogs);
    config_ = UpdateableConfig::createEmpty();
    config_->updateableServerConfig()->update(simple_config->serverConfig());
    config_->updateableLogsConfig()->update(simple_config->logsConfig());
    cluster_state_ = std::make_unique<MockClusterState>(nnodes);
    switch (type) {
      case LocatorType::HASH_BASED:
        locator_ = std::make_unique<MockHashBasedSequencerLocator>(
            config_->updateableServerConfig(),
            cluster_state_.get(),
            simple_config);
        break;
      case LocatorType::STATIC:
        locator_ = std::make_unique<StaticSequencerLocator>(config_);
        break;
    }
  }

  std::unique_ptr<MockAppendRequest> create(logid_t log_id) {
    return std::make_unique<MockAppendRequest>(
        log_id, config_->get(), locator_, cluster_state_.get());
  }

  bool isNodeAlive(NodeID node_id) {
    return cluster_state_->isNodeAlive(node_id.index());
  }

  // use the fact that AppendRequestTest is a friend of AppendRequest
  bool shouldAddAppendSuccess(AppendRequest* req, Status status) {
    return req->shouldAddAppendSuccess(status);
  }

  bool shouldAddAppendFail(AppendRequest* req, Status status) {
    return req->shouldAddAppendFail(status);
  }

  void sendProbe(AppendRequest* req) {
    req->sendProbe();
  }

  std::shared_ptr<UpdateableConfig> config_;
  std::shared_ptr<SequencerLocator> locator_;
  std::unique_ptr<ClusterState> cluster_state_;
};

// Verifies that AppendRequest selects another node if it's unable to connect
// to the original one due to a timeout
TEST_F(AppendRequestTest, ConnectTimeout) {
  init(4, 1);

  auto rq = create(logid_t(1));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());

  auto dest = rq->dest_;
  ASSERT_EQ(E::UNKNOWN, rq->getStatus());
  ASSERT_NE(NodeID(), dest);

  // timeout while connecting to `dest'
  rq->noReply(E::TIMEDOUT, Address(dest), false);

  // request hasn't completed yet; another node was selected
  ASSERT_EQ(E::UNKNOWN, rq->getStatus());
  ASSERT_NE(dest, rq->dest_);
}

// This test verifies that internal errors codes (such as E::DISABLED) are not
// reported back to the caller of append() when SequencerLocator picks the same
// node twice; see #6629599 for details.
TEST_F(AppendRequestTest, InternalErrorsNotReportedT6629599) {
  const NodeID N0(0, 1);

  // use StaticSequencerLocator which'll always select N0
  init(4, 1, LocatorType::STATIC);

  auto rq = create(logid_t(1));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  ASSERT_EQ(N0, rq->dest_);

  rq->noReply(E::DISABLED, Address(N0), false);

  // Connections to N0 are disabled, but StaticSequencerLocator keeps selecting
  // it. Request should be aborted, and E::DISABLED translated to E::CONNFAILED.
  ASSERT_EQ(E::CONNFAILED, rq->getStatus());
}

// Verifies that AppendRequest selects same node if it's preempted by a dead
// node
TEST_F(AppendRequestTest, PreemptedByDeadNodeFailure) {
  int num_nodes = 4;
  init(num_nodes, 1);

  auto rq = create(logid_t(1));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());

  auto dest = rq->dest_;
  ASSERT_EQ(E::UNKNOWN, rq->getStatus());
  ASSERT_NE(NodeID(), dest);

  // make sure redirect != dest
  node_index_t redirect = (dest.index() + 1) % num_nodes;
  APPENDED_Header hdr{rq->id_,
                      LSN_INVALID,
                      RecordTimestamp::zero(),
                      NodeID(redirect, 1),
                      E::PREEMPTED,
                      APPENDED_Header::REDIRECT_NOT_ALIVE};

  rq->onReplyReceived(hdr, Address(dest));

  // request hasn't completed yet; same node was selected
  ASSERT_EQ(E::PREEMPTED, rq->getStatus());
  ASSERT_EQ(dest, rq->dest_);

  // if the node sends another redirect. request should fail
  rq->onReplyReceived(hdr, Address(dest));
  ASSERT_EQ(E::NOSEQUENCER, rq->getStatus());
}

// Verifies that AppendRequest selects same node if it's preempted by a dead
// node
TEST_F(AppendRequestTest, PreemptedByDeadNodeSuccess) {
  int num_nodes = 4;
  init(num_nodes, 1);

  auto rq = create(logid_t(1));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());

  auto dest = rq->dest_;
  ASSERT_EQ(E::UNKNOWN, rq->getStatus());
  ASSERT_NE(NodeID(), dest);

  // make sure redirect != dest
  node_index_t redirect = (dest.index() + 1) % num_nodes;
  APPENDED_Header hdr{rq->id_,
                      LSN_INVALID,
                      RecordTimestamp::zero(),
                      NodeID(redirect, 1),
                      E::PREEMPTED,
                      APPENDED_Header::REDIRECT_NOT_ALIVE};

  rq->onReplyReceived(hdr, Address(dest));

  // request hasn't completed yet; same node was selected
  ASSERT_EQ(E::PREEMPTED, rq->getStatus());
  ASSERT_EQ(dest, rq->dest_);

  APPENDED_Header hdr2{rq->id_,
                       lsn_t(5),
                       RecordTimestamp(std::chrono::milliseconds(10)),
                       NodeID(),
                       E::OK,
                       APPENDED_flags_t(0)};

  // if the node sends another redirect. request should fail
  rq->onReplyReceived(hdr2, Address(dest));
  ASSERT_EQ(E::OK, rq->getStatus());
}

// Tests if the node with the location matching the sequencerAffinity is chosen
// as the sequencer. If there are none, it makes sure the SequencerLocator
// still picks something.
TEST_F(AppendRequestTest, SequencerAffinityTest) {
  const NodeID N0(0, 1), N1(1, 1);
  auto settings = create_default_settings<Settings>();

  // Config with 2 regions each with 1 node
  config_ = std::make_shared<UpdateableConfig>(Configuration::fromJsonFile(
      TEST_CONFIG_FILE("sequencer_affinity_2nodes.conf")));

  cluster_state_ = std::make_unique<MockClusterState>(
      config_->getNodesConfigurationFromServerConfigSource()->clusterSize());

  settings.use_sequencer_affinity = true;
  locator_ = std::make_unique<MockHashBasedSequencerLocator>(
      config_->updateableServerConfig(),
      cluster_state_.get(),
      config_->get(),
      settings);

  // Log with id 1 prefers rgn1. N0 is the only node in that region.
  auto rq = create(logid_t(1));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  EXPECT_EQ(N0, rq->dest_);

  // Log with id 2 prefers rgn2. N1 is the only node in that region.
  rq = create(logid_t(2));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  EXPECT_EQ(N1, rq->dest_);

  // Log with id 3 prefers rgn3. No nodes are in that region so it chooses a
  // sequencer at random.
  rq = create(logid_t(3));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  EXPECT_TRUE(N0 == rq->dest_ || N1 == rq->dest_);

  // Tests that sequencer won't use sequencer affinity if
  // use-sequencer-affinity is false.
  settings.use_sequencer_affinity = false;
  locator_ = std::make_unique<MockHashBasedSequencerLocator>(
      config_->updateableServerConfig(),
      cluster_state_.get(),
      config_->get(),
      settings);

  rq = create(logid_t(2));
  ASSERT_EQ(Request::Execution::CONTINUE, rq->execute());
  EXPECT_NE(N1, rq->dest_);
}

TEST_F(AppendRequestTest, ShouldAddStatAppendSuccess) {
  init(1, 1);
  auto request = create(logid_t(1));

  for (int i = 0; i < static_cast<int>(Status::MAX); ++i) {
    const auto enum_val = static_cast<Status>(i);

    if (enum_val == Status::OK) {
      EXPECT_TRUE(shouldAddAppendSuccess(request.get(), enum_val))
          << "Enum val: " << enum_val;
    } else {
      EXPECT_FALSE(shouldAddAppendSuccess(request.get(), enum_val))
          << "Enum val: " << enum_val;
    }
  }
}

TEST_F(AppendRequestTest, ShouldAddStatAppendFail) {
  init(1, 1);
  auto request = create(logid_t(1));

  for (int i = 0; i < static_cast<int>(Status::MAX); ++i) {
    const auto enum_val = static_cast<Status>(i);

    switch (enum_val) {
      case Status::TIMEDOUT:
      case Status::PEER_CLOSED:
      case Status::SEQNOBUFS:
      case Status::CONNFAILED:
      case Status::DISABLED:
      case Status::SEQSYSLIMIT:
        EXPECT_TRUE(shouldAddAppendFail(request.get(), enum_val))
            << "Enum val: " << enum_val;
        break;
      default:
        EXPECT_FALSE(shouldAddAppendFail(request.get(), enum_val))
            << "Enum val: " << enum_val;
    }
  }
}

}} // namespace facebook::logdevice
