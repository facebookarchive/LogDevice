/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

// Should be able to to make use of low priority port.
TEST(ConnectionIntegrationTest, ShouldBeAbleToUseLowPriorityPort) {
  logid_t log{1};
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setClientSetting("client-default-network-priority", "low")
                     .create(1);
  cluster->waitUntilAllSequencersQuiescent();

  auto& N0 = cluster->getNode(0);

  ASSERT_EQ(N0.stats()["num_connections_incoming_data_low_priority"], 0)
      << "There shouldn't be any low priority connections open so far";

  auto client = cluster->createClient(getDefaultTestTimeout());
  ASSERT_NE(nullptr, client) << "Could not create client";

  auto lsn = client->appendSync(log, "some payload");
  ASSERT_NE(LSN_INVALID, lsn) << "Should be able to append a payload";

  ASSERT_GT(N0.stats()["num_connections_incoming_data_low_priority"], 0)
      << "Some low priority connection should have been created on the server";
}
