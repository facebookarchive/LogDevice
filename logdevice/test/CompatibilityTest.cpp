/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class CompatibilityTest : public IntegrationTestBase {};

const logid_t LOG_ID(1);

TEST_F(CompatibilityTest, InBandByteOffsetBasic) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setNumDBShards(3)
          .setNumLogs(1)
          .setParam("--max-protocol",
                    std::to_string(Compatibility::MIN_PROTOCOL_SUPPORTED))
          .create(5);

  EXPECT_EQ(0, cluster->waitForRecovery());

  auto client = cluster->createClient();

  // Write some records..
  folly::Optional<lsn_t> first;
  for (int i = 1; i <= 30; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }
  cluster->getNode(2).setParam(
      "--max-protocol", std::to_string(Compatibility::MAX_PROTOCOL_SUPPORTED));

  cluster->getNode(2).kill();
  cluster->getNode(2).start();

  ASSERT_EQ(0, cluster->waitForRecovery());

  auto reader = client->createReader(1);
  ASSERT_EQ(0, reader->startReading(LOG_ID, first.value()));
  read_records_no_gaps(*reader, 30);
}
