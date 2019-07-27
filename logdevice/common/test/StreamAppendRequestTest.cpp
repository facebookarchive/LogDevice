/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/StreamAppendRequest.h"

#include <memory>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/MockSequencerRouter.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

class MockStreamAppendRequest : public StreamAppendRequest {
 public:
  // internally calls 'canSendToImpl' and 'sendMessageImpl'
  using MockSender = SenderTestProxy<MockStreamAppendRequest>;

  MockStreamAppendRequest(logid_t log_id,
                          std::shared_ptr<Configuration> configuration,
                          std::shared_ptr<SequencerLocator> locator,
                          ClusterState* cluster_state,
                          Payload payload,
                          write_stream_request_id_t stream_rqid,
                          bool stream_resume)
      : StreamAppendRequest(
            nullptr,
            log_id,
            AppendAttributes(),
            payload,
            std::chrono::milliseconds(0),
            append_callback_t(),
            std::make_unique<MockSequencerRouter>(log_id,
                                                  this,
                                                  configuration->serverConfig(),
                                                  locator,
                                                  cluster_state),
            stream_rqid,
            stream_resume),
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
  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback*) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }
    APPEND_Message* append_msg = reinterpret_cast<APPEND_Message*>(msg.get());
    checkAppendMessage(append_msg);
    dest_ = addr.id_.node_;
    return 0;
  }

  void destroy() override {}

  Status getStatus() const {
    return status_;
  }

  void checkAppendMessage(APPEND_Message* append_msg) {
    ASSERT_NE(nullptr, append_msg);
    std::string msg_type;
    uint64_t stream_id, seq_num;
    Payload payload = append_msg->payload_.getPayload();
    parsePayload(payload, stream_id, seq_num, msg_type);

    ASSERT_NE(
        0u, append_msg->header_.flags & APPEND_Header::WRITE_STREAM_REQUEST);
    ASSERT_EQ(append_msg->write_stream_request_id_.id.val(), stream_id);
    ASSERT_EQ(append_msg->write_stream_request_id_.seq_num.val(), seq_num);
    if (msg_type == "STREAM_REQUEST") {
      ASSERT_EQ(
          0u, (append_msg->header_.flags & APPEND_Header::WRITE_STREAM_RESUME));
    } else if (msg_type == "STREAM_RESUME") {
      ASSERT_NE(
          0u, (append_msg->header_.flags & APPEND_Header::WRITE_STREAM_RESUME));
    } else {
      ADD_FAILURE();
    }
  }
  static void parsePayload(Payload payload,
                           uint64_t& stream_id,
                           uint64_t& seq_num,
                           std::string& msg_type) {
    std::string stream_id_str, seq_num_str;
    folly::split(':', payload.toString(), stream_id_str, seq_num_str, msg_type);
    stream_id = std::stoull(stream_id_str);
    seq_num = std::stoull(seq_num_str);
  }
  static Payload makePayload(uint64_t write_stream_id,
                             uint64_t write_stream_seq_num,
                             bool stream_resume) {
    std::string* payload_string = new std::string();
    (*payload_string) += std::to_string(write_stream_id);
    (*payload_string) += ":";
    (*payload_string) += std::to_string(write_stream_seq_num);
    (*payload_string) += ":";
    (*payload_string) += stream_resume ? "STREAM_RESUME" : "STREAM_REQUEST";
    return Payload((void*)payload_string->c_str(), payload_string->length());
  }

  NodeID dest_;
  Settings settings_;
};

class StreamAppendRequestTest : public ::testing::Test {
 public:
  void init(size_t nnodes, size_t nlogs) {
    auto simple_config = createSimpleConfig(nnodes, nlogs);
    config_ = UpdateableConfig::createEmpty();
    config_->updateableServerConfig()->update(simple_config->serverConfig());
    config_->updateableLogsConfig()->update(simple_config->logsConfig());
    cluster_state_ = std::make_unique<MockClusterState>(nnodes);
    locator_ = std::make_unique<MockHashBasedSequencerLocator>(
        config_->updateableServerConfig(), cluster_state_.get(), simple_config);
  }

  std::unique_ptr<MockStreamAppendRequest> create(logid_t log_id,
                                                  uint64_t write_stream_id,
                                                  uint64_t write_stream_seq_num,
                                                  bool stream_resume) {
    write_stream_request_id_t stream_rqid = {
        write_stream_id_t(write_stream_id),
        write_stream_seq_num_t(write_stream_seq_num)};
    return std::make_unique<MockStreamAppendRequest>(
        log_id,
        config_->get(),
        locator_,
        cluster_state_.get(),
        MockStreamAppendRequest::makePayload(
            write_stream_id, write_stream_seq_num, stream_resume),
        stream_rqid,
        stream_resume);
  }

  std::shared_ptr<UpdateableConfig> config_;
  std::shared_ptr<SequencerLocator> locator_;
  std::unique_ptr<ClusterState> cluster_state_;
};

// Verifies if the APPEND_Message created is correct.
TEST_F(StreamAppendRequestTest, AppendMessageConstruction) {
  init(4, 1);
  auto rq1 = create(logid_t(1), 1UL, 0UL, false);
  ASSERT_EQ(Request::Execution::CONTINUE, rq1->execute());
  auto rq2 = create(logid_t(2), 123UL, 34UL, true);
  ASSERT_EQ(Request::Execution::CONTINUE, rq2->execute());
}

}} // namespace facebook::logdevice
