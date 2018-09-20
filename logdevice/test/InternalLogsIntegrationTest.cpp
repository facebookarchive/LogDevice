/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class InternalLogsIntegrationTest : public IntegrationTestBase {};

// test that Client cannot directly write to internal logs
TEST_F(InternalLogsIntegrationTest, ClientCannotWriteInternalLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  auto client = cluster->createClient();

  // Create arbitrary payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn = client->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                                 Payload((void*)data.c_str(), dataSize));

  ASSERT_EQ(LSN_INVALID, lsn);
  ASSERT_EQ(E::INVALID_PARAM, err);
}

// test that Client can write to internal logs with the right flags
TEST_F(InternalLogsIntegrationTest, ClientWriteInternalLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  auto client_orig = cluster->createClient();

  // cast the Client object back to ClientImpl and enable internal log writes
  auto client = std::dynamic_pointer_cast<ClientImpl>(client_orig);
  ASSERT_NE(nullptr, client);
  client->allowWriteInternalLog();

  // Create garbage payload. This is safe since we only append to deltas logs
  size_t dataSize = 512;
  std::string data(dataSize, 'x');

  lsn_t lsn = client->appendSync(configuration::InternalLogs::CONFIG_LOG_DELTAS,
                                 Payload((void*)data.c_str(), dataSize));

  ASSERT_NE(LSN_INVALID, lsn);
  ASSERT_NE(E::INVALID_PARAM, err);

  // verify we can read it back
  auto reader = client_orig->createReader(1);

  reader->setTimeout(std::chrono::seconds(5));
  auto rv =
      reader->startReading(configuration::InternalLogs::CONFIG_LOG_DELTAS, lsn);
  ASSERT_EQ(0, rv);

  std::vector<std::unique_ptr<DataRecord>> records;
  GapRecord gap;
  auto count = reader->read(1, &records, &gap);
  ASSERT_GT(count, 0);
  ASSERT_EQ(records[0]->payload.toString(), data);
}
