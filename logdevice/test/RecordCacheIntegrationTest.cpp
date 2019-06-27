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

#include <folly/Random.h>
#include <folly/hash/Checksum.h>
#include <gtest/gtest.h>

#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class RecordCacheIntegrationTest : public IntegrationTestBase {};

// test there should be no record cache miss for log recovery if cluster is
// taking new writes for logs for the first time
TEST_F(RecordCacheIntegrationTest, RecordCacheHitForNewAppends) {
  const int NNODES = 2;
  const int NLOGS = 1;

  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(2);
  log_attrs.set_maxWritesInFlight(1024);
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--enable-record-cache", "true")
          // If some reactivations are delayed they still complete quickly
          .setParam("--sequencer-reactivation-delay-secs", "1s..2s")
          .setNumLogs(NLOGS)
          .setLogAttributes(log_attrs)
          .useHashBasedSequencerAssignment()
          .create(NNODES);

  std::shared_ptr<Client> client =
      cluster->createClient(std::chrono::seconds(2));

  auto do_write = [&] {
    for (logid_t::raw_type log = 1; log <= NLOGS; ++log) {
      for (int i = 0; i < 10; ++i) {
        lsn_t lsn = client->appendSync(logid_t(log), Payload("dummy", 5));
        EXPECT_NE(LSN_INVALID, lsn);
      }
    }
  };

  do_write();

  int rv = cluster->waitForMetaDataLogWrites(std::chrono::steady_clock::now() +
                                             std::chrono::seconds(5));
  cluster->waitForRecovery();

  ld_info("disabling N1, expect sequencer re-activations...");
  for (auto n : {1}) {
    cluster->updateNodeAttributes(n, configuration::StorageState::READ_ONLY, 0);
  }
  cluster->waitForConfigUpdate();
  rv = cluster->waitForMetaDataLogWrites(std::chrono::steady_clock::now() +
                                         std::chrono::seconds(5));
  EXPECT_EQ(0, rv);
  cluster->waitForRecovery();

  auto get_stats_sum = [&](const std::string& name) {
    int64_t result = 0;
    for (node_index_t n = 0; n < NNODES; ++n) {
      result += cluster->getNode(n).stats()[name];
    }
    return result;
  };

  ASSERT_GT(get_stats_sum("recovery_success"), 0);
  ASSERT_GT(get_stats_sum("record_cache_seal_hit_datalog"), 0);
  ASSERT_GT(get_stats_sum("record_cache_digest_hit_datalog"), 0);

  // there should be no misses
  ASSERT_EQ(0, get_stats_sum("record_cache_seal_miss_datalog"));
  ASSERT_EQ(0, get_stats_sum("record_cache_digest_miss_datalog"));
}
