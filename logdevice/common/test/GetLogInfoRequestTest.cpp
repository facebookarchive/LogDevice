/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetLogInfoRequest.h"

#include <functional>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/test/MockBackoffTimer.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/types.h"

using namespace facebook::logdevice;

constexpr std::chrono::milliseconds
/* implicit */
operator"" _ms(unsigned long long val) {
  return std::chrono::milliseconds(val);
}

namespace {

class Callback {
 public:
  void operator()(Status status, const std::string& result) {
    called_ = true;
    result_ = result;
    status_ = status;
  }

  void assertNotCalled() {
    ASSERT_FALSE(called_);
  }

  void assertCalled(Status expected_status, std::string expected_result) {
    ASSERT_TRUE(called_);
    ASSERT_EQ(expected_result, result_);
    ASSERT_EQ(expected_status, status_);
  }

 private:
  bool called_ = false;
  std::string result_;
  Status status_;
};

class MockGetLogInfoFromNodeRequest : public GetLogInfoFromNodeRequest {
 public:
  explicit MockGetLogInfoFromNodeRequest(GetLogInfoRequest* parent)
      : GetLogInfoFromNodeRequest(parent), parent_(parent) {}

 private:
  void createRetryTimer() override {
    // no-op
  }

  void activateRetryTimer() override {
    // no-op
  }

  void deleteThis() override {}

  GetLogInfoRequest* findParent() override {
    return parent_;
  }

  GetLogInfoRequest* parent_;
};

class MockGetLogInfoRequest : public GetLogInfoRequest {
 public:
  MockGetLogInfoRequest(int nnodes,
                        LOGS_CONFIG_API_Header::Type request_type,
                        std::string identifier,
                        Callback& callback)
      : GetLogInfoRequest(request_type,
                          identifier,
                          100_ms,
                          std::make_shared<GetLogInfoRequestSharedState>(),
                          std::ref(callback),
                          worker_id_t(-1)) {
    Configuration::NodesConfig nodes_config = createSimpleNodesConfig(nnodes);
    // metadata stored on all nodes with max replication factor 3
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);
    config_ = std::make_shared<Configuration>(
        ServerConfig::fromDataTest(
            __FILE__, std::move(nodes_config), std::move(meta_config)),
        std::make_shared<configuration::LocalLogsConfig>());

    // Start immediately, every test needs this
    start();
  }

  bool config_reload_requested_{false};

  const std::vector<std::unique_ptr<MockGetLogInfoFromNodeRequest>>&
  getAllPerNodeRequests() {
    return per_node_reqs_;
  }

  MockGetLogInfoFromNodeRequest* getLastPerNodeRequest() {
    ld_check(per_node_reqs_.size() > 0);
    return per_node_reqs_.back().get();
  }

 protected: // mock stuff that communicates externally
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return config_->getNodesConfigurationFromServerConfigSource();
  }

  int reloadConfig() override {
    config_reload_requested_ = true;
    return 0;
  }

  void createTimers() override {
    // no-op
  }

  void activateTargetNodeChangeTimer() override {
    // no-op
  }

  bool isRetryTimerActive() override {
    // no-op
    return false;
  }

  void activateRetryTimer() override {
    // running a retry immediately
    attemptTargetNodeChange();
  }

  void postPerNodeRequest() override {
    per_node_reqs_.emplace_back(new MockGetLogInfoFromNodeRequest(this));
    per_node_reqs_.back()->start();
  }

  void deleteThis() override {}

 private:
  std::shared_ptr<Configuration> config_;
  std::vector<std::unique_ptr<MockGetLogInfoFromNodeRequest>> per_node_reqs_;
};

NodeID node(node_index_t index) {
  return NodeID(index, 1);
}

} // namespace

TEST(GetLogInfoRequestTest, Simple) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID, "1", cb);
  cb.assertNotCalled();
  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(0), E::OK, 1, "binary_payload", 14);
  cb.assertCalled(E::OK, "binary_payload");
}

TEST(GetLogInfoRequestTest, Failures) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID, "1", cb);
  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(0), E::SHUTDOWN, 1, "", 0);
  cb.assertNotCalled();
  req.getLastPerNodeRequest()->onReply(node(1), E::OK, 1, "binary_payload", 14);
  cb.assertCalled(E::OK, "binary_payload");
}

TEST(GetLogInfoRequestTest, ClientTimeout) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID, "1", cb);
  cb.assertNotCalled();
  ASSERT_FALSE(req.config_reload_requested_);

  req.onClientTimeout();
  cb.assertNotCalled();
  // timeout should not trigger a config reload if no replies have been received
  ASSERT_FALSE(req.config_reload_requested_);

  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(2), E::OK, 1, "binary_payload", 14);
  cb.assertCalled(E::OK, "binary_payload");

  req.onClientTimeout();
  // timeout should trigger a reload if replies have been received
  ASSERT_TRUE(req.config_reload_requested_);
}

TEST(GetLogInfoRequestTest, StaleResponses) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID, "1", cb);
  cb.assertNotCalled();
  ASSERT_FALSE(req.config_reload_requested_);

  req.onClientTimeout();
  cb.assertNotCalled();
  // timeout should not trigger a config reload if no replies have been received
  ASSERT_FALSE(req.config_reload_requested_);

  const auto& per_node_requests = req.getAllPerNodeRequests();
  ASSERT_EQ(2, per_node_requests.size());

  // simulating a response on a stale request
  per_node_requests[0]->onReply(node(2), E::OK, 1, "binary_payload", 14);
  cb.assertNotCalled();

  // same response on the last request should work fine
  per_node_requests[1]->onReply(node(2), E::OK, 1, "binary_payload", 14);
  cb.assertCalled(E::OK, "binary_payload");
}

TEST(GetLogInfoRequestTest, Chunking) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID, "1", cb);
  cb.assertNotCalled();
  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(0), E::OK, 1, "binary_", 14);
  cb.assertNotCalled();
  req.getLastPerNodeRequest()->onReply(node(0), E::OK, 1, "payload", 0);
  cb.assertCalled(E::OK, "binary_payload");
}
