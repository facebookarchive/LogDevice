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
using NCAPI = facebook::logdevice::configuration::NodesConfigurationAPI;
using NodesConfiguration =
    facebook::logdevice::configuration::nodes::NodesConfiguration;

namespace {

class NCMIntegrationTest : public IntegrationTestBase {};

std::unique_ptr<ClientSettings>
createAdminClientSettings(std::string ncs_path) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  EXPECT_EQ(0, client_settings->set("admin-client-capabilities", "true"));
  EXPECT_EQ(
      0, client_settings->set("enable-nodes-configuration-manager", "true"));
  EXPECT_EQ(
      0, client_settings->set("nodes-configuration-store-file-path", ncs_path));
  return client_settings;
}

NCAPI* getNCAPI(std::shared_ptr<Client>& client) {
  return static_cast<ClientImpl*>(client.get())->getNodesConfigurationAPI();
}

TEST_F(NCMIntegrationTest, ToolingClientBasic) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(3);

  std::shared_ptr<Client> admin_client1 = cluster->createClient(
      testTimeout(), createAdminClientSettings(cluster->getNCSPath()));

  std::shared_ptr<Client> admin_client2 = cluster->createClient(
      testTimeout(), createAdminClientSettings(cluster->getNCSPath()));

  auto nc_empty = std::make_shared<const NodesConfiguration>();
  auto nc_expected = nc_empty->applyUpdate(initialProvisionUpdate());
  folly::Baton<> b;
  getNCAPI(admin_client1)
      ->update(initialProvisionUpdate(),
               [nc_expected, &b](
                   Status st, std::shared_ptr<const NodesConfiguration> nc) {
                 EXPECT_EQ(Status::OK, st);
                 EXPECT_TRUE(nc_expected->equalWithTimestampIgnored(*nc));
                 b.post();
               });
  b.wait();

  wait_until("admin_client1 gets the new NC",
             [&]() { return getNCAPI(admin_client1)->getConfig() != nullptr; });

  auto nc_client1 = getNCAPI(admin_client1)->getConfig();
  ASSERT_TRUE(nc_expected->equalWithTimestampIgnored(*nc_client1));

  wait_until("admin_client2 gets the new NC",
             [&]() { return getNCAPI(admin_client2)->getConfig() != nullptr; });

  auto nc_client2 = getNCAPI(admin_client2)->getConfig();
  ASSERT_TRUE(nc_expected->equalWithTimestampIgnored(*nc_client2));
}
} // namespace
