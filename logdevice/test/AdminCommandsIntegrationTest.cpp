/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>

#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class AdminCommandsIntegrationTest : public IntegrationTestBase {};

// Verifies that the logdevice admin port supports SSL
TEST_F(AdminCommandsIntegrationTest, AdminPortSupportsSSL) {
  // Create cluster, each node in the cluster must have access to the server
  // identity certificate
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--ssl-cert-path",
                    TEST_SSL_FILE("logdevice_test_server_identity.cert"))
          .setParam("--ssl-key-path", TEST_SSL_FILE("logdevice_test.key"))
          .setParam(
              "--ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"))
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .useTcp()
          .create(3);

  cluster->waitForRecovery();

  // check command in clear text
  std::string response = cluster->getNode(0).sendCommand("info", false);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));
  EXPECT_EQ("END\r\n",
            response.substr(response.size() - std::min(response.size(), 5ul)));

  auto stats_map = cluster->getNode(0).stats();
  EXPECT_EQ(0, stats_map["command_port_connection_encrypted"]);
  EXPECT_EQ(0, stats_map["command_port_connection_failed_ssl_upgrade"]);
  EXPECT_EQ(0, stats_map["command_port_connection_failed_ssl_required"]);
  EXPECT_TRUE(stats_map["command_port_connection_plain"] > 0);

  // check command over SSL
  response = cluster->getNode(0).sendCommand("info", true);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));
  EXPECT_EQ("END\r\n",
            response.substr(response.size() - std::min(response.size(), 5ul)));

  auto stats_map2 = cluster->getNode(0).stats();
  EXPECT_EQ(1, stats_map2["command_port_connection_encrypted"]);
  EXPECT_EQ(0, stats_map2["command_port_connection_failed_ssl_upgrade"]);
  EXPECT_EQ(0, stats_map2["command_port_connection_failed_ssl_required"]);
  // Note the +1 is becasue stats() issues an admin command as well.
  EXPECT_EQ(stats_map["command_port_connection_plain"] + 1,
            stats_map2["command_port_connection_plain"]);
}

// Verifies that the logdevice admin port fails SSL if certs re not loaded
TEST_F(AdminCommandsIntegrationTest, AdminPortSSLErrorNoCerts) {
  // Create cluster without certificate
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .noSSLAddress()
          .setParam("--ssl-cert-path", "")
          .setParam("--ssl-key-path", "")
          .setParam("--ssl-ca-path", "")
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .useTcp()
          .create(3);

  cluster->waitForRecovery();

  // check command in clear text
  std::string response = cluster->getNode(0).sendCommand("info", false);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));
  EXPECT_EQ("END\r\n",
            response.substr(response.size() - std::min(response.size(), 5ul)));

  auto stats_map = cluster->getNode(0).stats();
  EXPECT_EQ(0, stats_map["command_port_connection_encrypted"]);
  EXPECT_EQ(0, stats_map["command_port_connection_failed_ssl_upgrade"]);
  EXPECT_EQ(0, stats_map["command_port_connection_failed_ssl_required"]);
  EXPECT_TRUE(stats_map["command_port_connection_plain"] > 0);

  // check command over SSL. should fail and return no response
  response = cluster->getNode(0).sendCommand("info", true);
  EXPECT_TRUE(response.empty());

  auto stats_map2 = cluster->getNode(0).stats();
  EXPECT_EQ(0, stats_map2["command_port_connection_encrypted"]);
  EXPECT_EQ(1, stats_map2["command_port_connection_failed_ssl_upgrade"]);
  EXPECT_EQ(0, stats_map2["command_port_connection_failed_ssl_required"]);
  // Note the +1 is becasue stats() issues an admin command as well.
  EXPECT_EQ(stats_map["command_port_connection_plain"] + 1,
            stats_map2["command_port_connection_plain"]);
}
