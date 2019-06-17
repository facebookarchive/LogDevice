/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/ClusterMembershipAPIHandler.h"

#include <gtest/gtest.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace ::testing;
using namespace apache::thrift;
using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

class ClusterMemebershipAPIIntegrationTest : public IntegrationTestBase {
 protected:
  void SetUp() override {
    IntegrationTestBase::SetUp();

    cluster_ =
        IntegrationTestUtils::ClusterFactory()
            .useHashBasedSequencerAssignment()
            .enableSelfInitiatedRebuilding("1s")
            .setParam("--event-log-grace-period", "1ms")
            .setParam("--disable-event-log-trimming", "true")
            .setParam("--enable-nodes-configuration-manager",
                      "true",
                      IntegrationTestUtils::ParamScope::ALL)
            .setParam("--nodes-configuration-manager-store-polling-interval",
                      "1s",
                      IntegrationTestUtils::ParamScope::ALL)
            .setParam("--use-nodes-configuration-manager-nodes-configuration",
                      "true",
                      IntegrationTestUtils::ParamScope::ALL)
            .runMaintenanceManagerOn(node_index_t(0))
            .setMetaDataLogsConfig(createMetaDataLogsConfig({2, 3}, 2))
            .deferStart()
            .create(4);
  }

  thrift::RemoveNodesRequest
  buildRemoveNodesRequest(std::vector<int32_t> idxs) {
    thrift::RemoveNodesRequest req;
    for (auto idx : idxs) {
      thrift::NodesFilter filter;
      filter.set_node(mkNodeID(node_index_t(idx)));
      req.node_filters.push_back(std::move(filter));
    }
    return req;
  }

 public:
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster_;
};

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveAliveNodes) {
  cluster_->updateNodeAttributes(
      node_index_t(1), configuration::StorageState::DISABLED, 1, false);
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  try {
    thrift::RemoveNodesResponse resp;
    admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
    FAIL() << "RemoveNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(1, failed_node.node_id.node_index);
    EXPECT_EQ(
        thrift::ClusterMembershipFailureReason::NOT_DEAD, failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveNonExistentNode) {
  ASSERT_EQ(0, cluster_->start({0, 1, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::RemoveNodesResponse resp;
  admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({10}));
  EXPECT_EQ(0, resp.removed_nodes.size());
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveEnabledNodes) {
  ASSERT_EQ(0, cluster_->start({0, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  try {
    thrift::RemoveNodesResponse resp;
    admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
    FAIL() << "RemoveNodes call should fail, but it didn't";
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ASSERT_EQ(1, exception.failed_nodes.size());
    auto failed_node = exception.failed_nodes[0];
    EXPECT_EQ(1, failed_node.node_id.node_index);
    EXPECT_EQ(thrift::ClusterMembershipFailureReason::NOT_DISABLED,
              failed_node.reason);
  }
}

TEST_F(ClusterMemebershipAPIIntegrationTest, TestRemoveNodeSuccess) {
  cluster_->updateNodeAttributes(
      node_index_t(1), configuration::StorageState::DISABLED, 1, false);
  ASSERT_EQ(0, cluster_->start({0, 2, 3}));
  auto admin_client = cluster_->getNode(0).createAdminClient();

  thrift::RemoveNodesResponse resp;
  admin_client->sync_removeNodes(resp, buildRemoveNodesRequest({1}));
  EXPECT_EQ(1, resp.removed_nodes.size());
  EXPECT_EQ(1, resp.removed_nodes[0].node_index);

  wait_until("AdminServer's NC picks the removal", [&]() {
    thrift::NodesConfigResponse nodes_config;
    admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
    return nodes_config.version >= resp.new_nodes_configuration_version;
  });

  thrift::NodesConfigResponse nodes_config;
  admin_client->sync_getNodesConfig(nodes_config, thrift::NodesFilter{});
  EXPECT_EQ(3, nodes_config.nodes.size());
}
