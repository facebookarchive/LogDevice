/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/MemtableFlushedRequest.h"

#include <gtest/gtest.h>

#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"

using namespace facebook::logdevice::NodeSetTestUtil;

namespace facebook { namespace logdevice {

class MemtableFlushedRequestTest : public ::testing::Test {
 public:
  void init(NodeID my_node_id) {
    // initialize the cluster config
    Configuration::Nodes nodes;
    addNodes(&nodes, 1, 1, "rg0.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk0", 1);
    addNodes(&nodes, 2, 1, "rg1.dc0.cl0.ro0.rk1", 2);
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk2", 1);
    addNodes(&nodes, 1, 1, "rg2.dc0.cl0.ro0.rk0", 1);

    const size_t nodeset_size = nodes.size();
    Configuration::NodesConfig nodes_config(std::move(nodes));
    config = ServerConfig::fromDataTest(
        "memtableflushedrequest_test", std::move(nodes_config));
    this->my_node_id = my_node_id;
  }

  ~MemtableFlushedRequestTest() override {}

  NodeID my_node_id;
  std::shared_ptr<ServerConfig> config;
  uint32_t numMessageSent{0};
  bool applyFlushCalled{false};
};

class MockMemtableFlushedRequest : public MemtableFlushedRequest {
 public:
  using MockSender = SenderTestProxy<MockMemtableFlushedRequest>;

  MockMemtableFlushedRequest(MemtableFlushedRequestTest* test,
                             worker_id_t worker_id,
                             node_index_t node_index,
                             ServerInstanceId server_instance_id,
                             uint32_t shard_idx,
                             FlushToken token)
      : MemtableFlushedRequest(worker_id,
                               node_index,
                               server_instance_id,
                               shard_idx,
                               token),
        test_(test) {
    sender_ = std::make_unique<MockSender>(this);
  }

  NodeID getMyNodeID() const override {
    return test_->my_node_id;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    // TODO: migrate it to use NodesConfiguration with switchable source
    return test_->config->getNodesConfigurationFromServerConfigSource();
  }

  bool responsibleForNodesUpdates(node_index_t /*unused*/) override {
    return true;
  }

  void applyFlush() override {
    test_->applyFlushCalled = true;
  }

  bool canSendToImpl(const Address&, TrafficClass, BWAvailableCallback&) {
    return true;
  }

  int sendMessageImpl(std::unique_ptr<Message>&& /*msg*/,
                      const Address& addr,
                      BWAvailableCallback*,
                      SocketCallback* /*callback*/) {
    ld_check(!addr.isClientAddress());
    if (addr.isClientAddress()) {
      err = E::INTERNAL;
      return -1;
    }

    NodeID destination = addr.id_.node_;
    EXPECT_TRUE(destination.isNodeID());
    EXPECT_NE(destination.index(), test_->my_node_id.index());
    test_->numMessageSent++;
    return 0;
  }

  ~MockMemtableFlushedRequest() override {}

  MemtableFlushedRequestTest* test_;
};

TEST_F(MemtableFlushedRequestTest, BasicTest) {
  init(NodeID(1, 1));
  auto req = std::make_unique<MockMemtableFlushedRequest>(
      this, worker_id_t(0), node_index_t(1), 1234, 1, 1);
  EXPECT_EQ(Request::Execution::COMPLETE, req->execute());
  ASSERT_TRUE(applyFlushCalled);
  EXPECT_EQ(5, numMessageSent);
}

TEST_F(MemtableFlushedRequestTest, DoNotBroadcast) {
  init(NodeID(2, 1));
  auto req = std::make_unique<MockMemtableFlushedRequest>(
      this, worker_id_t(0), node_index_t(1), 1234, 1, 1);
  EXPECT_EQ(Request::Execution::COMPLETE, req->execute());
  ASSERT_TRUE(applyFlushCalled);
  EXPECT_EQ(0, numMessageSent);
}

}} // namespace facebook::logdevice
