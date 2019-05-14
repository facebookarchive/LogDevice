/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::thrift;

class MaintenanceAPITest : public IntegrationTestBase {};

TEST_F(MaintenanceAPITest, NotSupported) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .setNumDBShards(1)
                     .setParam("--enable-maintenance-manager", "false")
                     .setParam("--gossip-interval", "5ms")
                     .create(1);

  cluster->waitUntilAllAvailable();
  auto& node = cluster->getNode(0);
  auto admin_client = node.createAdminClient();

  MaintenanceDefinitionResponse response;
  MaintenancesFilter filter;
  ASSERT_THROW(admin_client->sync_getMaintenances(response, filter),
               thrift::NotSupported);
}
