/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ConfigurationFetchRequest.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Err.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace std::literals::chrono_literals;

class MockConfigurationFetchRequest : public ConfigurationFetchRequest {
 public:
  using ConfigurationFetchRequest::ConfigurationFetchRequest;

  int sendMessageTo(std::unique_ptr<facebook::logdevice::Message> msg,
                    NodeID to) override {
    return sendMessageTo_(msg, to);
  }

  MOCK_METHOD2(sendMessageTo_,
               int(std::unique_ptr<facebook::logdevice::Message>& msg,
                   NodeID to));

  MOCK_METHOD0(deleteThis, void());
  MOCK_METHOD0(cancelTimeoutTimer, void());

  void unMockNonSendFunctions() {
    ON_CALL(*this, deleteThis()).WillByDefault(Invoke([this]() {
      ConfigurationFetchRequest::deleteThis();
    }));
    ON_CALL(*this, cancelTimeoutTimer()).WillByDefault(Invoke([this]() {
      ConfigurationFetchRequest::cancelTimeoutTimer();
    }));
  }

  FRIEND_TEST(ConfigurationFetchRequestTest, SuccessScenarioWithCallback);
  FRIEND_TEST(ConfigurationFetchRequestTest, FailureScenarioWithCallback);
  FRIEND_TEST(ConfigurationFetchRequestTest, SuccessScenarioWithoutCallback);
  FRIEND_TEST(ConfigurationFetchRequestTest, FailureScenarioWithoutCallback);
};

MATCHER_P(MessageMatcher, expected, "Message doesn't match") {
  auto got = static_cast<const CONFIG_FETCH_Message*>(arg.get());
  return got->getHeader().rid == expected.getHeader().rid &&
      got->getHeader().config_type == expected.getHeader().config_type;
}

TEST(ConfigurationFetchRequestTest, SuccessScenarioWithCallback) {
  bool cb_called = false;

  const request_id_t* rqid = nullptr;
  MockConfigurationFetchRequest rq{
      NodeID(10, 2),
      ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
      [&](Status status, CONFIG_CHANGED_Header header, std::string config) {
        cb_called = true;
        EXPECT_EQ(Status::OK, status);
        EXPECT_EQ(*rqid, header.rid);
        EXPECT_EQ("{}", config);
      },
      worker_id_t(4),
      5s};
  rqid = &rq.id_;

  auto fetch_message = CONFIG_FETCH_Message{
      CONFIG_FETCH_Header{*rqid, CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG},
  };

  EXPECT_CALL(rq, sendMessageTo_(MessageMatcher(fetch_message), NodeID(10, 2)))
      .WillOnce(Return(0));

  rq.sendConfigFetch();

  EXPECT_CALL(rq, deleteThis());
  EXPECT_CALL(rq, cancelTimeoutTimer());

  CONFIG_CHANGED_Header hdr;
  hdr.status = Status::OK;
  hdr.rid = *rqid;
  rq.onReply(hdr, "{}");

  EXPECT_TRUE(cb_called);
}

TEST(ConfigurationFetchRequestTest, FailureScenarioWithCallback) {
  bool cb_called = false;

  MockConfigurationFetchRequest rq{
      NodeID(10, 2),
      ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
      [&](Status status, CONFIG_CHANGED_Header, std::string config) {
        cb_called = true;
        EXPECT_EQ(Status::BADMSG, status);
        EXPECT_EQ("", config);
      },
      worker_id_t(4),
      5s};

  auto fetch_message = CONFIG_FETCH_Message{
      CONFIG_FETCH_Header{rq.id_, CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG},
  };

  EXPECT_CALL(rq, sendMessageTo_(MessageMatcher(fetch_message), NodeID(10, 2)))
      .WillOnce(InvokeWithoutArgs([]() {
        err = Status::BADMSG;
        return -1;
      }));

  EXPECT_CALL(rq, deleteThis());
  EXPECT_CALL(rq, cancelTimeoutTimer());

  rq.sendConfigFetch();

  EXPECT_TRUE(cb_called);
}

TEST(ConfigurationFetchRequestTest, SuccessScenarioWithoutCallback) {
  MockConfigurationFetchRequest rq{
      NodeID(10, 2),
      ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
  };

  auto fetch_message = CONFIG_FETCH_Message{
      CONFIG_FETCH_Header{
          REQUEST_ID_INVALID, CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG},
  };

  EXPECT_CALL(rq, sendMessageTo_(MessageMatcher(fetch_message), NodeID(10, 2)))
      .WillOnce(Return(0));

  EXPECT_CALL(rq, deleteThis());
  EXPECT_CALL(rq, cancelTimeoutTimer());

  rq.sendConfigFetch();
}

TEST(ConfigurationFetchRequestTest, FailureScenarioWithoutCallback) {
  MockConfigurationFetchRequest rq{
      NodeID(10, 2),
      ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
  };

  auto fetch_message = CONFIG_FETCH_Message{
      CONFIG_FETCH_Header{
          REQUEST_ID_INVALID, CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG},
  };

  EXPECT_CALL(rq, sendMessageTo_(MessageMatcher(fetch_message), NodeID(10, 2)))
      .WillOnce(InvokeWithoutArgs([]() {
        err = Status::BADMSG;
        return -1;
      }));

  EXPECT_CALL(rq, deleteThis());
  EXPECT_CALL(rq, cancelTimeoutTimer());

  rq.sendConfigFetch();
}

TEST(ConfigurationFetchRequestTest, GetThreadAffinity) {
  {
    MockConfigurationFetchRequest rq{
        NodeID(10, 2),
        ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
        [&](Status, CONFIG_CHANGED_Header, std::string) {},
        worker_id_t(4),
        5s};
    EXPECT_EQ(4, rq.getThreadAffinity(100));
  }

  {
    MockConfigurationFetchRequest rq{
        NodeID(10, 2), ConfigurationFetchRequest::ConfigType::LOGS_CONFIG};
    EXPECT_EQ(-1, rq.getThreadAffinity(100));
  }
}

TEST(ConfigurationFetchRequestTest, TimeoutTimer) {
  bool cb_called = false;
  Semaphore sem;
  auto mrq = std::make_unique<MockConfigurationFetchRequest>(
      NodeID(2, 0),
      ConfigurationFetchRequest::ConfigType::LOGS_CONFIG,
      [&](Status status, CONFIG_CHANGED_Header, std::string) {
        cb_called = true;
        EXPECT_EQ(Status::TIMEDOUT, status);
        EXPECT_EQ(worker_id_t(1), Worker::onThisThread()->idx_);
        sem.post();
      },
      worker_id_t(1),
      200ms);
  mrq->unMockNonSendFunctions();

  auto updatable_config =
      std::make_shared<UpdateableConfig>(createSimpleConfig(3, 0));
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;
  auto processor = make_test_processor(settings, std::move(updatable_config));

  std::unique_ptr<Request> rq = std::move(mrq);
  processor->postRequest(rq);

  sem.timedwait(std::chrono::seconds{10});

  EXPECT_TRUE(cb_called);

  // Make sure that the object gets destructed afterwards
  auto verify_empty_map = []() {
    auto& map = Worker::onThisThread()->runningConfigurationFetches().map;
    EXPECT_EQ(0, map.size());
    return 0;
  };
  run_on_worker(processor.get(), 1, verify_empty_map);
}

TEST(ConfigurationFetchRequestTest, NoCallbackObjectDestruction) {
  auto mrq = std::make_unique<MockConfigurationFetchRequest>(
      NodeID(2, 0), ConfigurationFetchRequest::ConfigType::LOGS_CONFIG);
  mrq->unMockNonSendFunctions();

  auto updatable_config =
      std::make_shared<UpdateableConfig>(createSimpleConfig(3, 0));
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;
  auto processor = make_test_processor(settings, std::move(updatable_config));
  std::unique_ptr<Request> rq = std::move(mrq);
  processor->blockingRequest(rq);

  // Make sure that the object gets destructed afterwards
  auto verify_empty_map = []() {
    auto& map = Worker::onThisThread()->runningConfigurationFetches().map;
    EXPECT_EQ(0, map.size());
    return 0;
  };
  run_on_all_workers(processor.get(), verify_empty_map);
}
