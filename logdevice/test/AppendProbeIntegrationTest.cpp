/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <mutex>
#include <string>
#include <thread>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice {

class AppendProbeIntegrationTest : public IntegrationTestBase {};

// Returns the number of successful writes
static int hammer_client_with_writes(Client& client,
                                     const int NAPPENDS,
                                     const int PAYLOAD_SIZE,
                                     const int NTHREADS) {
  std::atomic<int> sent{0}, success{0};
  // Have writer threads hammer the sequencer with writes in rapid succession.
  // Due to the small size of the sequencer window, many of them should fail.
  auto writer_fn = [&](int nwrites) {
    std::string payload(PAYLOAD_SIZE, '!');
    for (int i = 0; i < nwrites; ++i) {
      lsn_t rv = client.appendSync(logid_t(1), payload);
      ++sent;
      if (rv != LSN_INVALID) {
        ++success;
      }
    }
  };

  std::vector<std::thread> writers;
  for (int i = 0; i < NTHREADS; ++i) {
    int nwrites = NAPPENDS / NTHREADS + (i < NAPPENDS % NTHREADS);
    writers.emplace_back(writer_fn, nwrites);
  }
  for (auto& th : writers) {
    th.join();
  }

  ld_check(NAPPENDS == sent.load());
  return success.load();
}

// Common driver for tests in different configurations
static void test_impl(IntegrationTestUtils::Cluster& cluster,
                      const int NAPPENDS,
                      const int PAYLOAD_SIZE,
                      const int NTHREADS) {
  std::shared_ptr<Client> client = cluster.createClient();
  int success =
      hammer_client_with_writes(*client, NAPPENDS, PAYLOAD_SIZE, NTHREADS);

  ASSERT_LT(success, NAPPENDS)
      << "Some appends should have failed "
         "(if all succeed the test isn't set up right)";
  ASSERT_GT(success, 0) << "Some appends should have succeeded "
                           "(if all fail the test isn't set up right)";

  auto stats = cluster.getNode(0).stats();
  int probes_passed = stats["append_probes_passed"];
  int probes_denied = stats["append_probes_denied"];
  ld_info("%d/%d appends succeeded", success, NAPPENDS);
  ld_info("%d/%d probes accepted by sequencer",
          probes_passed,
          probes_passed + probes_denied);
  ld_info("%ld/%d payload bytes sent",
          stats["append_payload_bytes"],
          NAPPENDS * PAYLOAD_SIZE);
  // Without probing we would have sent a payload for all appends.  With
  // probing we get to not send the payload for appends that fail with
  // SEQNOBUFS.  Calculate how many bytes we saved.
  int bytes_saved = NAPPENDS * PAYLOAD_SIZE - stats["append_payload_bytes"];
  double savings_ratio =
      (double)bytes_saved / ((NAPPENDS - success) * PAYLOAD_SIZE);
  ld_info("%.1f%% payload bytes saved for failed appends", savings_ratio * 100);
  ASSERT_GT(bytes_saved, 0)
      << "Probing should have prevented the transfer of some payload bytes "
         "for appends that failed.";

  // Check that the client stat is correct
  Stats client_stats =
      checked_downcast<ClientImpl&>(*client).stats()->aggregate();
  ASSERT_EQ(
      bytes_saved,
      client_stats.client.append_probes_bytes_saved +
          client_stats.client.append_probes_bytes_unsent_probe_send_error);
}

// When we quickly send appends to a sequencer whose sliding window is full,
// append probes should kick in and reduce bandwith usage
TEST_F(AppendProbeIntegrationTest, SlidingWindowFull) {
  const int NAPPENDS = 5000;
  const int PAYLOAD_SIZE = 300;
  const int NTHREADS = 16;
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(1);
  log_attrs.set_maxWritesInFlight(2);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // not to throw off append counters
                                         // inside test_impl()
          .enableMessageErrorInjection()
          .setLogAttributes(log_attrs)
          .create(1);
  test_impl(*cluster, NAPPENDS, PAYLOAD_SIZE, NTHREADS);
}

// With a generous window but limited `max_total_appenders_size_hard' setting,
// probing should also kick in.
TEST_F(AppendProbeIntegrationTest, MaxTotalAppendersSize) {
  const int NAPPENDS = 5000;
  const int PAYLOAD_SIZE = 300;
  const int NTHREADS = 16;
  logsconfig::LogAttributes log_attrs =
      IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(1);
  log_attrs.set_maxWritesInFlight(NAPPENDS);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .enableMessageErrorInjection()
          .setLogAttributes(log_attrs)
          .setParam("--max-total-appenders-size-hard",
                    std::to_string(2 * (PAYLOAD_SIZE + sizeof(Appender) + 8)))
          // Limit currently gets divided across workers, set 1 worker to
          // simplify
          .setParam("--num-workers", "1")
          .doPreProvisionEpochMetaData() // not to throw off append counters
                                         // inside test_impl()
          .create(1);
  test_impl(*cluster, NAPPENDS, PAYLOAD_SIZE, NTHREADS);
}

// If sequencer batching is being used, should also respect its limit on how
// much data will be buffered.
TEST_F(AppendProbeIntegrationTest, SequencerBatchingBufferLimit) {
  const int NAPPENDS = 5000;
  const int PAYLOAD_SIZE = 300;
  const int NTHREADS = 16;
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .enableMessageErrorInjection()
          .setParam("--sequencer-batching")
          .setParam("--sequencer-batching-time-trigger", "5ms")
          // This is the cap on how many bytes SequencerBatching will buffer.
          // With NTHREADS large enough, plenty of them should see failures
          // while a few are waiting for the buffered ones to reach 5ms in age
          // and flush.
          .setParam("--max-total-buffered-append-size",
                    std::to_string(3 * PAYLOAD_SIZE))
          .doPreProvisionEpochMetaData() // not to throw off append counters
                                         // inside test_impl()
          .create(1);
  test_impl(*cluster, NAPPENDS, PAYLOAD_SIZE, NTHREADS);
}

}} // namespace facebook::logdevice
