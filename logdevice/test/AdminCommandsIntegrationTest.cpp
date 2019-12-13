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
          .useTcp()
          .create(3);

  cluster->waitForRecovery();

  // check command in clear text
  std::string response = cluster->getNode(0).sendCommand("info", false);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));

  // check command over SSL
  response = cluster->getNode(0).sendCommand("info", true);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));

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
          .useTcp()
          .create(3);

  cluster->waitForRecovery();

  // check command in clear text
  std::string response = cluster->getNode(0).sendCommand("info", false);
  EXPECT_FALSE(response.substr(0, strlen("Failed")) == "Failed" ||
               response.substr(0, strlen("Error")) == "Error");
  EXPECT_EQ("PID", response.substr(0, 3));

  // check command over SSL. should fail and return no response
  response = cluster->getNode(0).sendCommand("info", true);
  EXPECT_TRUE(response.empty());
}
