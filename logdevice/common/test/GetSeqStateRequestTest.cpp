/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetSeqStateRequest.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_REPLY_Message.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockGetSeqStateRequest : public GetSeqStateRequest {
 public:
  MockGetSeqStateRequest(logid_t log_id,
                         GetSeqStateRequest::Options options,
                         std::shared_ptr<const Configuration> config,
                         std::shared_ptr<SequencerLocator> locator,
                         ClusterState* cluster_state)
      : GetSeqStateRequest(
            log_id,
            GetSeqStateRequest::Context::UNKNOWN,
            std::move(options),
            std::make_unique<MockSequencerRouter>(log_id,
                                                  this,
                                                  config->serverConfig(),
                                                  locator,
                                                  cluster_state)) {}

  void onSequencerKnown(NodeID dest,
                        SequencerRouter::flags_t /*flags*/) override {
    dest_ = dest;
  }

  NodeID dest_{};

 private:
  void setupTimers() override {}
  void resetBackoffTimer() override {}
  bool shouldExecute() override {
    if (options_.on_complete) {
      callback_list_.push_back(options_.on_complete);
    }
    return true;
  }
  void destroy() override {
    executeCallbacks();
  }
  void activateReplyTimer() override {}
  void cancelReplyTimer() override {}
};

class GetSeqStateRequestTest : public ::testing::Test {
 public:
  void init(size_t nnodes, size_t nlogs) {
    auto simple_config = createSimpleConfig(nnodes, nlogs);
    config_ = std::make_shared<UpdateableConfig>(simple_config);
    cluster_state_ = std::make_unique<MockClusterState>(nnodes);
    locator_ = std::make_unique<MockHashBasedSequencerLocator>(
        config_->updateableServerConfig(), cluster_state_.get(), simple_config);
    dbg::assertOnData = true;
  }

  std::unique_ptr<MockGetSeqStateRequest>
  create(logid_t log_id, GetSeqStateRequest::Options options) {
    return std::make_unique<MockGetSeqStateRequest>(log_id,
                                                    std::move(options),
                                                    config_->get(),
                                                    locator_,
                                                    cluster_state_.get());
  }

  void onReplyTimeout(std::unique_ptr<MockGetSeqStateRequest>& req,
                      NodeID node) {
    req->onReplyTimeout(node);
  }

 private:
  std::shared_ptr<UpdateableConfig> config_;
  std::shared_ptr<SequencerLocator> locator_;
  std::unique_ptr<ClusterState> cluster_state_;
};

TEST_F(GetSeqStateRequestTest, SkipOnTimeout) {
  init(4, 1);

  NodeID N0;
  {
    GetSeqStateRequest::Options opts;
    auto req = create(logid_t(1), opts);

    ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
    N0 = req->dest_;
    ASSERT_TRUE(N0.isNodeID());

    onReplyTimeout(req, N0);
    ASSERT_NE(N0, req->dest_);
  }
}

TEST_F(GetSeqStateRequestTest, Callback) {
  init(4, 1);
  const NodeID seq_node = NodeID(2, 1);
  bool completed_ = false;

  GetSeqStateRequest::Options opts;
  opts.on_complete = [&](GetSeqStateRequest::Result res) {
    ASSERT_EQ(E::OK, res.status);
    ASSERT_EQ(seq_node, res.last_seq);
    completed_ = true;
  };

  auto req = create(logid_t(1), opts);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());

  {
    NodeID N0 = req->dest_;
    GET_SEQ_STATE_REPLY_Message reply{
        GET_SEQ_STATE_REPLY_Header{logid_t(1), LSN_INVALID, LSN_INVALID}};
    reply.status_ = E::REDIRECTED;
    reply.redirect_ = seq_node;

    req->onReply(N0, reply);
    ASSERT_EQ(seq_node, req->dest_);
  }
  {
    GET_SEQ_STATE_REPLY_Header hdr{logid_t(1),
                                   compose_lsn(epoch_t(2), esn_t(0)),
                                   compose_lsn(epoch_t(2), esn_t(1))};
    GET_SEQ_STATE_REPLY_Message reply(hdr);
    reply.status_ = E::OK;
    req->onReply(seq_node, reply);
  }

  ASSERT_TRUE(completed_);
}

TEST_F(GetSeqStateRequestTest, RedirectBack) {
  init(4, 1);
  bool callback_called_ = false;

  GetSeqStateRequest::Options opts;
  opts.on_complete = [&](GetSeqStateRequest::Result res) {
    callback_called_ = true;
    EXPECT_EQ(E::TIMEDOUT, res.status);
  };

  auto req = create(logid_t(1), opts);
  ASSERT_EQ(Request::Execution::CONTINUE, req->execute());
  auto N0 = req->dest_;

  // timeout when sending to N0
  onReplyTimeout(req, N0);

  // alternate node redirects back to N0
  auto N1 = req->dest_;
  ASSERT_NE(N0, N1);
  GET_SEQ_STATE_REPLY_Message reply{
      GET_SEQ_STATE_REPLY_Header{logid_t(1), LSN_INVALID}};
  reply.status_ = E::REDIRECTED;
  reply.redirect_ = N0;
  req->onReply(N1, reply);

  // another timeout
  ASSERT_EQ(N0, req->dest_);
  onReplyTimeout(req, N0);

  // request is expected to complete with an error
  ASSERT_TRUE(callback_called_);
}

TEST_F(GetSeqStateRequestTest, MatchOptions) {
  init(4, 1);
  {
    GetSeqStateRequest::Options opts1;
    auto req1 = create(logid_t(1), opts1);
    GetSeqStateRequest::Options opts2;
    ASSERT_TRUE(req1->matchOptions(opts2));
  }
  {
    GetSeqStateRequest::Options opts1;
    opts1.include_tail_attributes = true;
    auto req1 = create(logid_t(1), opts1);
    GetSeqStateRequest::Options opts2;
    opts2.include_tail_attributes = true;
    ASSERT_TRUE(req1->matchOptions(opts2));
  }
  {
    GetSeqStateRequest::Options opts1;
    opts1.include_epoch_offset = true;
    auto req1 = create(logid_t(1), opts1);
    GetSeqStateRequest::Options opts2;
    ASSERT_FALSE(req1->matchOptions(opts2));
  }
  {
    GetSeqStateRequest::Options opts1;
    opts1.include_tail_attributes = true;
    auto req1 = create(logid_t(1), opts1);
    GetSeqStateRequest::Options opts2;
    auto req2 = create(logid_t(1), opts2);
    ASSERT_FALSE(req1->matchOptions(opts2));
  }
}

}} // namespace facebook::logdevice
