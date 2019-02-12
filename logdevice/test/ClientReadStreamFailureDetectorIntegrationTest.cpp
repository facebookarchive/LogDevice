/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/ReaderImpl.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/AppendThread.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#include "logdevice/test/utils/ReaderThread.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

class ClientReadStreamFailureDetectorIntegrationTest
    : public IntegrationTestBase {};

TEST_F(ClientReadStreamFailureDetectorIntegrationTest, Simple) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_scdEnabled(true);

  const logid_t LOG_ID(1);

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     // Disable sticky copysets to increase scatter width of the
                     // data during the test, which will help guarantee that
                     // some records are replicated on the node we will be pick
                     // to be slow.
                     .setParam("--enable-sticky-copysets", "false")
                     .create(12);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  // Use high timeouts for SCD as we expect the ClientReadStreamFailureDetector
  // utility will be sufficient to maintain read availability.
  ASSERT_EQ(0, client_settings->set("scd-timeout", "10min"));
  ASSERT_EQ(0, client_settings->set("scd-all-send-all-timeout", "10min"));
  // Make sure SCD does not do any rewind to ALL_SEND_ALL to try hard to
  // be resilient to silent under-replication or copyset
  // inconsistencies, which should not happen in this test.
  ASSERT_EQ(0,
            client_settings->set(
                "read-stream-guaranteed-delivery-efficiency", "true"));
  // Disabling copyset reordering makes it easier to debug issues
  // related to replication in case this test fails.
  ASSERT_EQ(0, client_settings->set("scd-copyset-reordering-max", "none"));
  ASSERT_EQ(0, client_settings->set("client-read-buffer-size", "50"));

  // Enabling outlier detection (the feature that makes this test pass!).
  ASSERT_EQ(0, client_settings->set("reader-slow-shards-detection", "enabled"));
  ASSERT_EQ(0,
            client_settings->set(
                "reader-slow-shards-detection-moving-avg-duration", "5min"));
  ASSERT_EQ(0,
            client_settings->set(
                "reader-slow-shards-detection-required-margin", "5.0"));

  // Start a reader and a writer.
  auto append_thread =
      std::make_unique<AppendThread>(cluster->createClient(), LOG_ID);
  append_thread->start();
  auto reader = std::make_unique<ReaderThread>(
      cluster->createClient(testTimeout(), std::move(client_settings)), LOG_ID);
  reader->start();

  // Sync the reader after 2 seconds worth of data.
  ld_info("Writing/Reading 2s worth of data...");
  std::this_thread::sleep_for(std::chrono::seconds{2});
  ld_info("Syncing reader to tail...");
  reader->syncToTail();

  // Now, inject latency on N4, simulating it's super slow, taking 2min to send
  // each batch of records.
  bool success = cluster->getNode(4).injectShardFault(
      "all", "all", "all", "latency", false, folly::none, 60000);
  ASSERT_TRUE(success);
  ld_info("Injected latency on N4");

  // Continue writing for 2 more seconds.
  ld_info("Writing another 2s worth of data...");
  std::this_thread::sleep_for(std::chrono::seconds{2});

  // Stop the appends. Sync the reader to the tail. We should have read all
  // records without any dataloss although some of these records had a copyset
  // with N4 as leader recipient.
  ld_info("Stopping writes...");
  append_thread->stop();
  ld_info("Syncing reader to tail...");
  reader->syncToTail();
  reader->stop();
  ASSERT_FALSE(reader->foundDataLoss());
  ASSERT_EQ(
      reader->getNumRecordsRead(), append_thread->getNumRecordsAppended());
}
