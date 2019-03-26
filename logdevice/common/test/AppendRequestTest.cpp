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
#include <opentracing/mocktracer/in_memory_recorder.h>
#include <opentracing/mocktracer/tracer.h>
#include <opentracing/value.h>

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

  std::unique_ptr<MockAppendRequest>
  createWithMockE2ETracing(logid_t log_id,
                           std::shared_ptr<opentracing::Tracer> tracer) {
    auto mock_request = std::make_unique<MockAppendRequest>(
        log_id, config_->get(), locator_, cluster_state_.get());

    mock_request->e2e_tracer_ = tracer;

    return mock_request;
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

  std::string getTracingContext(AppendRequest* req) {
    return req->tracing_context_;
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

TEST_F(AppendRequestTest, E2ETracing) {
  init(1, 1);

  // simple request, no tracing by default
  {
    auto request = create(logid_t(1));

    // by default e2e tracing should not be on
    ASSERT_EQ(request->isE2ETracingOn(), false);

    request->setTracingContext();
    ASSERT_EQ(request->isE2ETracingOn(), true);
    // the way mocktracer is created results in a nullptr e2e_tracer_ object
    ASSERT_EQ(request->hasTracerObject(), false);
  }

  // we now create the tracer with a mock e2e_tracer_
  auto recorder = new opentracing::mocktracer::InMemoryRecorder{};

  opentracing::mocktracer::MockTracerOptions tracer_options;
  tracer_options.recorder.reset(recorder);

  auto tracer = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options)});

  {
    auto request = createWithMockE2ETracing(logid_t(1), tracer);
    ASSERT_EQ(request->isE2ETracingOn(), false);

    // no spans expected
    ASSERT_TRUE(recorder->spans().empty());

    request->setTracingContext();
    ASSERT_TRUE(request->isE2ETracingOn());

    // now we should expect a tracer object
    ASSERT_TRUE(request->hasTracerObject());

    request->execute();
  }

  auto spans = recorder->spans();

  // first span should be initialization one
  auto request_init_span = spans.front();
  auto request_span_id = request_init_span.span_context.span_id;
  ASSERT_EQ(request_init_span.operation_name, "AppendRequest_initiated");

  // last span that is finished should be the execution span
  auto execution_span = spans.back();
  ASSERT_EQ(execution_span.operation_name, "APPEND_execution");
  auto execution_span_id = execution_span.span_context.span_id;

  // check the reference between the append request initializaton span and the
  // execution span
  ASSERT_EQ(execution_span.references[0].reference_type,
            opentracing::SpanReferenceType::FollowsFromRef);
  ASSERT_EQ(execution_span.references[0].span_id, request_span_id);

  // all the others spans should have a childof reference with the execution one
  for (auto span = spans.begin() + 1; span != spans.end() - 1; span++) {
    ASSERT_EQ(span->references[0].reference_type,
              opentracing::SpanReferenceType::ChildOfRef);
    ASSERT_EQ(span->references[0].span_id, execution_span_id);
  }
}

TEST_F(AppendRequestTest, E2ETracingProbe) {
  init(1, 1);

  // create the tracer with a mock e2e_tracer_
  auto recorder = new opentracing::mocktracer::InMemoryRecorder{};

  opentracing::mocktracer::MockTracerOptions tracer_options;
  tracer_options.recorder.reset(recorder);

  auto tracer = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options)});

  {
    auto request = createWithMockE2ETracing(logid_t(1), tracer);

    // enable e2e tracing
    request->setTracingContext();
    request->execute();

    // send probe and check span created
    sendProbe(request.get());
    ASSERT_EQ(recorder->spans().back().operation_name, "PROBE_Message_send");

    // simple header, not used in this testing, but is necessary for the call
    APPENDED_Header hdr{request->id_,
                        lsn_t(5),
                        RecordTimestamp(std::chrono::milliseconds(10)),
                        NodeID(),
                        E::OK,
                        APPENDED_flags_t(0)};

    // call onReplyReceived, default source of reply (APPENDED)
    request->onReplyReceived(hdr, Address(request->dest_));
    ASSERT_EQ(
        recorder->spans().back().operation_name, "APPENDED_Message_receive");

    // try with probe source
    request->onReplyReceived(hdr, Address(request->dest_), ReplySource::PROBE);
    ASSERT_EQ(recorder->spans().back().operation_name, "PROBE_Reply_receive");
  }
}

TEST_F(AppendRequestTest, E2ETracingContextInject) {
  init(1, 1);

  auto recorder = new opentracing::mocktracer::InMemoryRecorder{};
  auto recorder2 = new opentracing::mocktracer::InMemoryRecorder{};

  opentracing::mocktracer::MockTracerOptions tracer_options, tracer_options2;
  tracer_options.recorder.reset(recorder);
  tracer_options2.recorder.reset(recorder2);

  // use two distict tracer to test Inject and Extract using different
  // tracer objects
  auto tracer = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options)});
  auto tracer2 = std::make_shared<opentracing::mocktracer::MockTracer>(
      opentracing::mocktracer::MockTracerOptions{std::move(tracer_options2)});

  // we want to record what is the encoding for the tracing context within the
  // request
  std::string tracing_context;

  {
    auto request = createWithMockE2ETracing(logid_t(1), tracer);
    request->setTracingContext();
    request->execute();

    // get the tracing encoding
    tracing_context = getTracingContext(request.get());
    ASSERT_FALSE(tracing_context.empty());
  }

  // use the other tracer to obtain the context
  std::stringstream out_ss(tracing_context, std::ios_base::in);
  auto new_span_context = tracer2->Extract(out_ss);

  // use the extracted reference to create a new span
  auto new_span = tracer2->StartSpan("New", {ChildOf(new_span_context->get())});
  new_span->Finish();

  // the trace id from the request_execution_span_
  auto id = recorder->spans().back().span_context.trace_id;
  // the trace id of the newly created span referencing the extracted context
  auto id2 = recorder2->spans().front().span_context.trace_id;
  // the initial span and the reconstructed one should have the same trace id
  ASSERT_EQ(id, id2);

  // The new span should have a ChildOf reference to the same span id as the
  // original one
  auto span_id = recorder->spans().back().span_context.span_id;
  auto span_id2 = recorder2->spans().front().references[0].span_id;
  ASSERT_EQ(span_id, span_id2);
}

}} // namespace facebook::logdevice
