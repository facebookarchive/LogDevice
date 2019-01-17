/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class FindTimeIntegrationTest : public IntegrationTestBase {};

TEST_F(FindTimeIntegrationTest, FindTimeInvalidLogid) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();
  Status st;
  lsn_t lsn = client->findTimeSync(LOGID_INVALID,
                                   std::chrono::milliseconds::zero(),
                                   &st,
                                   FindKeyAccuracy::STRICT);
  ASSERT_EQ(E::INVALID_PARAM, st);
  ASSERT_EQ(LSN_INVALID, lsn);
}

static void
find_time_zero_trim(std::shared_ptr<Client> client,
                    FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) {
  const logid_t LOG_ID(1);
  const lsn_t TRIM_UPTO = 12345678;

  {
    Status find_time_status;
    lsn_t lsn = client->findTimeSync(
        LOG_ID, std::chrono::milliseconds::zero(), &find_time_status, accuracy);
    ASSERT_EQ(E::OK, find_time_status);
    ASSERT_EQ(1, lsn);
  }

  {
    int rv = client->trimSync(LOG_ID, TRIM_UPTO);
    ASSERT_EQ(0, rv);
  }

  {
    Status find_time_status;
    lsn_t lsn = client->findTimeSync(
        LOG_ID, std::chrono::milliseconds::zero(), &find_time_status, accuracy);
    ASSERT_EQ(E::OK, find_time_status);
    ASSERT_EQ(TRIM_UPTO + 1, lsn);
  }
}

// Test that findTime with a timestamp of 0 returns the log's head (trim point
// + 1)
TEST_F(FindTimeIntegrationTest, FindTimeZeroTrim) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();
  find_time_zero_trim(client);
}

TEST_F(FindTimeIntegrationTest, FindTimeZeroTrimApproximate) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();
  find_time_zero_trim(client, FindKeyAccuracy::APPROXIMATE);
}

static void findtime_max_entire_log_trimmed_test(
    std::shared_ptr<Client> client,
    FindKeyAccuracy accuracy = FindKeyAccuracy::STRICT) {
  const logid_t LOG_ID(1);
  const lsn_t last_released = client->appendSync(LOG_ID, "boo");
  ld_check(last_released != LSN_INVALID);
  // Having just written this one record, we can be sure it is the last
  // released LSN.  Wait until storage nodes learn this; findTime(max) should
  // return `last_released' + 1.
  int rv = wait_until([&]() {
    return client->findTimeSync(
               LOG_ID, std::chrono::milliseconds::max(), nullptr, accuracy) ==
        last_released + 1;
  });
  ASSERT_EQ(0, rv);

  // Now trim the last receord.
  ASSERT_EQ(0, client->trimSync(LOG_ID, last_released));
  // findTime(max) should still return `last_released' + 1.  The off-by-one
  // was making it return one more than that.
  ASSERT_EQ(last_released + 1,
            client->findTimeSync(
                LOG_ID, std::chrono::milliseconds::max(), nullptr, accuracy));
}

// findTime(max) respects the trim point but there was an off-by-one in the
// code when the entire log was trimmed.
TEST_F(FindTimeIntegrationTest, FindTimeMaxEntireLogTrimmed) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid breaking LSN math due to
                                         // seq reactivations
          .create(1);
  std::shared_ptr<Client> client = cluster->createClient();
  findtime_max_entire_log_trimmed_test(client);
}

TEST_F(FindTimeIntegrationTest, FindTimeMaxEntireLogTrimmedAllowSmallerLsn) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid breaking LSN math due to
                                         // seq reactivations
          .create(1);
  std::shared_ptr<Client> client = cluster->createClient();
  findtime_max_entire_log_trimmed_test(client, FindKeyAccuracy::APPROXIMATE);
}
