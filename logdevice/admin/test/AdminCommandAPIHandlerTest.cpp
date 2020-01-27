/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

TEST(AdminCommandAPIHandlerTest, testExecuteAdminCommand) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .create(1);
  ASSERT_EQ(0, cluster->start());
  cluster->getNode(0).waitUntilAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();

  {
    // A single small request to test correctness

    thrift::AdminCommandRequest request;
    // The response should contain the character 'a' repeated 5 times
    request.set_request("fill 5 a");
    thrift::AdminCommandResponse resp;
    admin_client->sync_executeAdminCommand(resp, std::move(request));
    EXPECT_EQ("aaaaaEND\r\n", resp.get_response());
  }

  {
    // Testing handling big responses

    thrift::AdminCommandRequest request;
    request.set_request("fill 10000000 a");
    thrift::AdminCommandResponse resp;
    admin_client->sync_executeAdminCommand(resp, std::move(request));
    EXPECT_EQ(10000000 + 5 /* "aaaa...aEND\r\n" */, resp.get_response().size());
  }
}
