/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class GetHeadAttributesIntegrationTest : public IntegrationTestBase {};

// Test getHeadAttributes() for a log that is empty and untrimmed
TEST_F(GetHeadAttributesIntegrationTest, GetHeadAttributesForEmptyLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  std::unique_ptr<LogHeadAttributes> attributes =
      client->getHeadAttributesSync(logid_t(1));
  ASSERT_NE(nullptr, attributes);
  ASSERT_EQ(LSN_INVALID, attributes->trim_point);
}

TEST_F(GetHeadAttributesIntegrationTest, GetHeadAttributesForInvalidLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  std::unique_ptr<LogHeadAttributes> attributes =
      client->getHeadAttributesSync(LOGID_INVALID);
  ASSERT_EQ(nullptr, attributes);
  ASSERT_EQ(E::INVALID_PARAM, err);
}
