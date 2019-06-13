/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <folly/Random.h>
#include <folly/hash/Checksum.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::NodesConfigurationTestUtil;
using NCAPI = facebook::logdevice::configuration::NodesConfigurationAPI;

namespace {
std::unique_ptr<ClientSettings>
createAdminClientSettings(std::string ncs_path) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  EXPECT_EQ(0, client_settings->set("admin-client-capabilities", "true"));
  EXPECT_EQ(
      0, client_settings->set("enable-nodes-configuration-manager", "true"));
  EXPECT_EQ(
      0, client_settings->set("nodes-configuration-file-store-dir", ncs_path));
  return client_settings;
}

NCAPI* getNCAPI(std::shared_ptr<Client>& client) {
  return static_cast<ClientImpl*>(client.get())->getNodesConfigurationAPI();
}

NodesConfiguration::Update buildSimpleUpdate() {
  NodesConfiguration::Update update{};
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(MembershipVersion::Type(1));
  update.sequencer_config_update->membership_update->addNode(
      0,
      {SequencerMembershipTransition::SET_WEIGHT,
       true /* doesn't matter */,
       0.6,
       MaintenanceID::Type(1000)});
  return update;
}
} // namespace

class NCMIntegrationTest : public IntegrationTestBase {};

TEST_F(NCMIntegrationTest, ToolingClientBasic) {
  // use 1s NCM polling interval to get the update faster
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--nodes-configuration-manager-store-polling-interval",
                    "1s",
                    IntegrationTestUtils::ParamScope::ALL)
          .setParam("--use-nodes-configuration-manager-nodes-configuration",
                    "true",
                    IntegrationTestUtils::ParamScope::ALL)
          .setParam("--fd-limit", "9999", IntegrationTestUtils::ParamScope::ALL)
          .setParam("--num-reserved-fds",
                    "999",
                    IntegrationTestUtils::ParamScope::ALL)
          .create(3);

  std::shared_ptr<Client> admin_client1 = cluster->createClient(
      testTimeout(), createAdminClientSettings(cluster->getNCSPath()));

  std::shared_ptr<Client> admin_client2 = cluster->createClient(
      testTimeout(), createAdminClientSettings(cluster->getNCSPath()));

  auto current_nc = getNCAPI(admin_client1)->getConfig();
  ASSERT_TRUE(current_nc); // Initialized by the integration testing framework
  EXPECT_NE(
      membership::MembershipVersion::EMPTY_VERSION, current_nc->getVersion());

  auto nc_expected = current_nc->applyUpdate(buildSimpleUpdate());
  ASSERT_TRUE(nc_expected);
  folly::Baton<> b;
  getNCAPI(admin_client1)
      ->update(buildSimpleUpdate(),
               [nc_expected, &b](
                   Status st, std::shared_ptr<const NodesConfiguration> nc) {
                 EXPECT_EQ(Status::OK, st);
                 EXPECT_TRUE(nc_expected->equalWithTimestampIgnored(*nc));
                 b.post();
               });
  b.wait();

  wait_until("admin_client1 gets the new NC", [&]() {
    return getNCAPI(admin_client1)->getConfig()->getVersion() ==
        nc_expected->getVersion();
  });

  auto nc_client1 = getNCAPI(admin_client1)->getConfig();
  ASSERT_TRUE(nc_expected->equalWithTimestampIgnored(*nc_client1));

  wait_until("admin_client2 gets the new NC", [&]() {
    auto config = getNCAPI(admin_client2)->getConfig();
    return config && config->getVersion() == nc_expected->getVersion();
  });

  auto nc_client2 = getNCAPI(admin_client2)->getConfig();
  ASSERT_TRUE(nc_expected->equalWithTimestampIgnored(*nc_client2));
}

// Test the behavior of servers and clients when the NCM is enabled but the
// source-of-truth switch is still ServerConfig (and thus the NC in Zookeeper
// may not be up-to-date). This is only relevant as we roll out NCM.
TEST_F(NCMIntegrationTest, InRollout) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--nodes-configuration-manager-store-polling-interval",
                    "1s",
                    IntegrationTestUtils::ParamScope::ALL)
          .setParam("--use-nodes-configuration-manager-nodes-configuration",
                    "false",
                    IntegrationTestUtils::ParamScope::ALL)
          .setParam("--fd-limit", "9999", IntegrationTestUtils::ParamScope::ALL)
          .setParam("--num-reserved-fds",
                    "999",
                    IntegrationTestUtils::ParamScope::ALL)
          .doNotSyncServerConfigToNodesConfiguration()
          .deferStart()
          .create(3);
  // Write the current config to NCS
  auto sc = cluster->getConfig()->getServerConfig();
  auto orig_version =
      sc->getNodesConfigurationFromServerConfigSource()->getVersion();
  cluster->updateNodesConfigurationFromServerConfig(sc.get());
  EXPECT_EQ(0, cluster->bumpGeneration(node_index_t{1}));
  EXPECT_EQ(0, cluster->bumpGeneration(node_index_t{2}));
  auto new_version = cluster->getConfig()
                         ->getServerConfig()
                         ->getNodesConfigurationFromServerConfigSource()
                         ->getVersion();
  EXPECT_GT(new_version, orig_version);
  EXPECT_EQ(0, cluster->start());
}
