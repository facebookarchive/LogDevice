/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <functional>
#include <gtest/gtest.h>

#include <folly/Memory.h>
#include "logdevice/common/GetLogInfoRequest.h"
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
                        GET_LOG_INFO_Header::Type req_type,
                        logid_t log_id,
                        const std::string& log_group_name,
                        Callback& callback)
      : GetLogInfoRequest(req_type,
                          log_id,
                          log_group_name,
                          100_ms,
                          std::make_shared<GetLogInfoRequestSharedState>(),
                          std::ref(callback),
                          worker_id_t(-1)) {
    Configuration::NodesConfig nodes_config = createSimpleNodesConfig(nnodes);
    // metadata stored on all nodes with max replication factor 3
    Configuration::MetaDataLogsConfig meta_config = createMetaDataLogsConfig(
        nodes_config, nodes_config.getNodes().size(), 3);
    config_ = std::make_shared<Configuration>(
        ServerConfig::fromData(
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
  std::shared_ptr<Configuration> getConfig() const override {
    return config_;
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
      3, GET_LOG_INFO_Header::Type::BY_ID, logid_t(1), "", cb);
  cb.assertNotCalled();
  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(0), E::OK, "{}");
  cb.assertCalled(E::OK, "{}");
}

TEST(GetLogInfoRequestTest, Failures) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, GET_LOG_INFO_Header::Type::BY_ID, logid_t(1), "", cb);
  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(0), E::SHUTDOWN, "");
  cb.assertNotCalled();
  req.getLastPerNodeRequest()->onReply(node(1), E::OK, "{}");
  cb.assertCalled(E::OK, "{}");
}

TEST(GetLogInfoRequestTest, ClientTimeout) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, GET_LOG_INFO_Header::Type::BY_ID, logid_t(1), "", cb);
  cb.assertNotCalled();
  ASSERT_FALSE(req.config_reload_requested_);

  req.onClientTimeout();
  cb.assertNotCalled();
  // timeout should not trigger a config reload if no replies have been received
  ASSERT_FALSE(req.config_reload_requested_);

  ASSERT_TRUE(req.getLastPerNodeRequest());
  req.getLastPerNodeRequest()->onReply(node(2), E::OK, "{}");
  cb.assertCalled(E::OK, "{}");

  req.onClientTimeout();
  // timeout should trigger a reload if replies have been received
  ASSERT_TRUE(req.config_reload_requested_);
}

TEST(GetLogInfoRequestTest, StaleResponses) {
  Callback cb;
  MockGetLogInfoRequest req(
      3, GET_LOG_INFO_Header::Type::BY_ID, logid_t(1), "", cb);
  cb.assertNotCalled();
  ASSERT_FALSE(req.config_reload_requested_);

  req.onClientTimeout();
  cb.assertNotCalled();
  // timeout should not trigger a config reload if no replies have been received
  ASSERT_FALSE(req.config_reload_requested_);

  const auto& per_node_requests = req.getAllPerNodeRequests();
  ASSERT_EQ(2, per_node_requests.size());

  // simulating a response on a stale request
  per_node_requests[0]->onReply(node(2), E::OK, "{}");
  cb.assertNotCalled();

  // same response on the last request should work fine
  per_node_requests[1]->onReply(node(2), E::OK, "{}");
  cb.assertCalled(E::OK, "{}");
}
