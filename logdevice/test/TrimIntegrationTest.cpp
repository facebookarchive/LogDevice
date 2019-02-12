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
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class TrimIntegrationTest : public IntegrationTestBase {};

// Read from zero and verify the results for trim test
static void trim_test_read_verify(ssize_t num_records_read,
                                  lsn_t trim_upto,
                                  std::shared_ptr<Client> client,
                                  lsn_t bridge_upto = LSN_INVALID,
                                  logid_t logid = logid_t(2)) {
  const logid_t LOG_ID(logid);

  std::unique_ptr<Reader> reader = client->createReader(1);
  reader->setTimeout(std::chrono::seconds(1));
  ASSERT_EQ(0, reader->startReading(LOG_ID, 0));

  std::vector<std::unique_ptr<DataRecord>> read_data;
  GapRecord gap;
  ssize_t nread = 0;
  const lsn_t e1n1 = compose_lsn(EPOCH_MIN, ESN_MIN);

  // The first call should yield a bridge gap to the first epoch
  nread = reader->read(num_records_read, &read_data, &gap);
  ASSERT_EQ(-1, nread);
  EXPECT_EQ(0, gap.lo);
  EXPECT_EQ(e1n1 - 1, gap.hi);
  EXPECT_EQ(GapType::BRIDGE, gap.type);

  // The next call should yield a gap upto the trim point
  nread = reader->read(num_records_read, &read_data, &gap);
  ASSERT_EQ(-1, nread);
  EXPECT_EQ(e1n1, gap.lo);
  EXPECT_EQ(trim_upto, gap.hi);
  EXPECT_EQ(GapType::TRIM, gap.type);

  std::unique_ptr<LogHeadAttributes> attributes =
      client->getHeadAttributesSync(LOG_ID);
  ASSERT_NE(nullptr, attributes);
  ASSERT_EQ(trim_upto, attributes->trim_point);

  if (bridge_upto > trim_upto) {
    nread = reader->read(num_records_read, &read_data, &gap);
    ASSERT_EQ(-1, nread);
    EXPECT_EQ(trim_upto + 1, gap.lo);
    EXPECT_EQ(bridge_upto, gap.hi);
    EXPECT_EQ(GapType::BRIDGE, gap.type);
  }

  // The second should return data.
  reader->setTimeout(std::chrono::seconds(-1));
  std::vector<std::unique_ptr<DataRecord>> data2;
  nread = 0;
  while (nread < num_records_read) {
    ssize_t rv = reader->read(num_records_read, &data2, &gap);
    if (rv == 0) {
      break;
    }
    if (rv < 0) {
      ASSERT_EQ(E::GAP, err);
      ASSERT_EQ(GapType::BRIDGE, gap.type);
      continue;
    }

    ASSERT_EQ(rv, data2.size());
    nread += rv;
    if (read_data.empty()) {
      read_data = std::move(data2);
    } else {
      read_data.insert(
          read_data.end(),
          std::move_iterator<decltype(data2)::iterator>(data2.begin()),
          std::move_iterator<decltype(data2)::iterator>(data2.end()));
    }
  }
  EXPECT_EQ(num_records_read, nread);
  // attributes->trim_point_timestamp is an upper bound.
  ASSERT_LE(read_data[0]->attrs.timestamp, attributes->trim_point_timestamp);
  // Check that it's not way too overestimated.
  ASSERT_GT(read_data[0]->attrs.timestamp,
            attributes->trim_point_timestamp - std::chrono::minutes(1));
}

// Test for the synchronous trim operation
TEST_F(TrimIntegrationTest, Sync) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t LOG_ID(2);
  const size_t num_records = 10;
  const size_t num_records_to_trim = 4;

  ASSERT_TRUE(num_records_to_trim < num_records);
  ASSERT_TRUE(num_records_to_trim > 0);

  // Write the given number of records
  std::vector<lsn_t> lsn_written;

  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    lsn_written.push_back(lsn);
  }

  cluster->waitForRecovery();

  // Trim the given number of records
  lsn_t trim_upto = lsn_written[num_records_to_trim - 1];
  int rv = client->trimSync(LOG_ID, trim_upto);
  ASSERT_EQ(0, rv);

  trim_test_read_verify(num_records - num_records_to_trim, trim_upto, client);
}

// Test for the asynchronous trim operation
TEST_F(TrimIntegrationTest, Async) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t LOG_ID(2);
  const size_t num_records = 10;
  const size_t num_records_to_trim = 4;

  ASSERT_TRUE(num_records_to_trim < num_records);
  ASSERT_TRUE(num_records_to_trim > 0);

  // Write the given number of records
  std::vector<lsn_t> lsn_written;

  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    lsn_written.push_back(lsn);
  }

  cluster->waitForRecovery();

  // Trim the given number of records
  lsn_t trim_upto = lsn_written[num_records_to_trim - 1];
  Semaphore async_trim_sem;
  int rv = client->trim(LOG_ID, trim_upto, [&async_trim_sem](Status st) {
    if (st != E::OK) {
      ld_error("Async trim failed with status %s", error_description(st));
    }
    EXPECT_EQ(E::OK, st);
    async_trim_sem.post();
  });
  ASSERT_EQ(0, rv);
  async_trim_sem.wait();

  trim_test_read_verify(num_records - num_records_to_trim, trim_upto, client);
}

// Trimming a log past the tail should not succeed
TEST_F(TrimIntegrationTest, TrimPastTail) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  // We expect this to be a 1 seqencer + 1 storage node setup
  EXPECT_TRUE(cluster->getConfig()
                  ->get()
                  ->serverConfig()
                  ->getNode(0)
                  ->isSequencingEnabled());
  EXPECT_TRUE(cluster->getConfig()
                  ->get()
                  ->serverConfig()
                  ->getNode(1)
                  ->isReadableStorageNode());

  // Waiting for metadata provisioning to complete to avoid spurious sequencer
  // reactivations that can cause the test to fail
  cluster->waitForMetaDataLogWrites();

  ld_info("created cluster");

  const logid_t LOG_ID(1);

  int numAppends = 10;
  std::vector<lsn_t> lsn_written;
  lsn_t latestLSN = 0;
  for (int i = 0; i < numAppends; i++) {
    std::string data("data" + std::to_string(i));
    latestLSN = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, latestLSN);
    lsn_written.push_back(latestLSN);

    // Randomly select one of the previously appended records and trim below
    // it. It should be safe to trim everything <= the tail including
    // previously trimmed records.
    if ((folly::Random::rand32() % 3) == 0) {
      // Should be safe to trim anywhere upto the last LSN
      lsn_t trimLSN = lsn_written[folly::Random::rand32() % lsn_written.size()];
      ASSERT_EQ(0, client->trimSync(LOG_ID, trimLSN));
    }

    // Should never be possible to trim past the tail
    lsn_t highLSN = latestLSN + 1 + (folly::Random::rand32() % 1000000);
    ASSERT_EQ(-1, client->trimSync(LOG_ID, highLSN));
    ASSERT_EQ(E::TOOBIG, err);
  }
}

// Uses non-logsdb store (--rocksdb-partitioned=false).
// Just remove this test when removing support for the non-logsdb store.
TEST_F(TrimIntegrationTest, AutoLegacy) {
  const int NRECORDS = 30;
  const int NNODES = 2;
  const logid_t LOG_ID(1);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(1);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_backlogDuration(std::chrono::seconds(2));
  log_attrs.set_maxWritesInFlight(NRECORDS);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to prevent read_records_no_gaps from
                                         // failing below
          .setLogGroupName("my-log-range")
          .setLogAttributes(log_attrs)
          .setRocksDBType(IntegrationTestUtils::RocksDBType::SINGLE)
          .create(NNODES);

  auto client = cluster->createClient();

  folly::Optional<lsn_t> first;
  lsn_t lsn;
  // Write some records
  for (int i = 1; i <= NRECORDS; ++i) {
    std::string data("data" + std::to_string(i));
    lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }

  cluster->waitForRecovery();

  auto reader = client->createReader(1);
  int rv = reader->startReading(LOG_ID, first.value());
  ASSERT_EQ(0, rv);
  read_records_no_gaps(*reader, NRECORDS);

  rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);

  // wait for the records to expire
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // trigger compaction by sending COMPACT admin command
  rv = cluster->getNode(1).compact();
  EXPECT_EQ(0, rv);

  // wait a little for compaction to finish
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));

  wait_until("Records are trimmed by compaction", [&]() -> bool {
    rv = reader->startReading(LOG_ID, first.value());
    ld_assert_eq(0, rv);
    reader->setTimeout(std::chrono::seconds(1));

    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;
    int nread = reader->read(NRECORDS, &data, &gap);
    if (nread != -1) {
      // Wait some more
      return false;
    }
    // should expect a trim gap
    EXPECT_EQ(-1, nread);
    EXPECT_EQ(E::GAP, err);
    EXPECT_EQ(GapType::TRIM, gap.type);
    EXPECT_EQ(first.value(), gap.lo);
    EXPECT_EQ(lsn, gap.hi);
    return true;
  });
}

TEST_F(TrimIntegrationTest, Auto) {
  const int NRECORDS = 30;
  const int NNODES = 2;
  const logid_t LOG_ID(1);

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(1);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_backlogDuration(std::chrono::seconds(2));
  log_attrs.set_maxWritesInFlight(NRECORDS);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to prevent read_records_no_gaps from
                                         // failing below
          .setLogGroupName("my-log-range")
          .setLogAttributes(log_attrs)
          .setParam("--rocksdb-partition-lo-pri-check-period", "1s")
          .setNumDBShards(1)
          .create(NNODES);

  auto client = cluster->createClient();

  folly::Optional<lsn_t> first;
  lsn_t lsn;
  // Write some records
  for (int i = 1; i <= NRECORDS; ++i) {
    std::string data("data" + std::to_string(i));
    lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    if (!first.hasValue()) {
      first = lsn;
    }
  }

  cluster->waitForRecovery();

  auto reader = client->createReader(1);
  int rv = reader->startReading(LOG_ID, first.value());
  ASSERT_EQ(0, rv);
  read_records_no_gaps(*reader, NRECORDS);

  rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);

  // wait for the records to expire
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Start a new partition, so that the partition with our data can be dropped.
  partition_id_t created_partition = cluster->getNode(1).createPartition(0);
  EXPECT_NE(PARTITION_INVALID, created_partition);

  // Wait a little for drop to finish, just to reduce number of retries below.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  // Retry reading until we get a trim gap.
  while (true) {
    rv = reader->startReading(LOG_ID, first.value());
    ASSERT_EQ(0, rv);
    reader->setTimeout(std::chrono::seconds(1));

    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;
    int nread = reader->read(NRECORDS, &data, &gap);
    if (nread > 0) {
      // Not trimmed. Maybe we didn't wait long enough. Try again later.
      // If trimming is broken, test will time out.
      RATELIMIT_ERROR(
          std::chrono::seconds(5), 1, "Waiting for records to be trimmed.");
      rv = reader->stopReading(LOG_ID);
      ASSERT_EQ(0, rv);
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    // Expect a trim gap.
    EXPECT_EQ(-1, nread);
    ASSERT_EQ(E::GAP, err);
    EXPECT_EQ(GapType::TRIM, gap.type);
    EXPECT_EQ(first.value(), gap.lo);
    // We expect all records to be trimmed at once. This is the case with
    // current implementation of automatic trimming.
    EXPECT_EQ(lsn, gap.hi);
    break;
  }
}

// trivial test for now, will be expanded in the future
TEST_F(TrimIntegrationTest, Invalid) {
  const int NNODES = 3;
  const logid_t LOG_ID(1);
  auto cluster = IntegrationTestUtils::ClusterFactory().create(NNODES);

  auto client = cluster->createClient();

  int rv = client->trimSync(LOG_ID, LSN_INVALID);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);
  rv = client->trimSync(LOGID_INVALID, LSN_OLDEST);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);
  rv = client->trimSync(LOG_ID, LSN_MAX);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);
}

TEST_F(TrimIntegrationTest, LogDoesntExist) {
  const int NNODES = 3;
  const logid_t LOG_THAT_DOESNT_EXIST(42);
  auto cluster =
      IntegrationTestUtils::ClusterFactory().setNumLogs(1).create(NNODES);

  auto client = cluster->createClient();
  int rv = client->trimSync(LOG_THAT_DOESNT_EXIST, LSN_OLDEST);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOTFOUND, err);
}

// Write some data to partitioned store. Drop the oldest partition.
// Read the records. Check that only TRIM gaps are reported.
TEST_F(TrimIntegrationTest, TrimPointsAreUpdatedWhenPartitionIsDropped) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setNumDBShards(1)
                     .setParam("--rocksdb-new-partition-timestamp-margin", "0s")
                     .create(1);
  // This ensures that all appends go into the same epoch.
  // It's still not 100% guaranteed though, but seems good enough in practice
  // to avoid all flakiness.
  cluster->waitForMetaDataLogWrites();

  auto client = cluster->createClient();

  std::vector<lsn_t> lsn1, lsn2;
  int rv;

  lsn1.push_back(client->appendSync(logid_t(1), "1.1")); // goes to partition 1
  ASSERT_NE(LSN_INVALID, lsn1.back());
  lsn1.push_back(client->appendSync(logid_t(1), "1.2")); // goes to partition 1
  ASSERT_NE(LSN_INVALID, lsn1.back());

  partition_id_t partition_id = cluster->getNode(0).createPartition(0);
  ASSERT_NE(PARTITION_INVALID, partition_id);

  // Make sure that the next appended records have timestamps above
  // partition 2's creation timestamp.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(2));

  lsn1.push_back(client->appendSync(logid_t(1), "1.3")); // goes to partition 2
  ASSERT_NE(LSN_INVALID, lsn1.back());
  EXPECT_EQ(lsn1.back(), lsn1.front() + 2); // all appends are in same epoch
  lsn2.push_back(client->appendSync(logid_t(2), "2.2")); // goes to partition 2
  ASSERT_NE(LSN_INVALID, lsn2.back());

  std::string reply = cluster->getNode(0).sendCommand("logsdb drop 0 " +
                                                      toString(partition_id));
  ASSERT_EQ("Dropped", reply.substr(0, strlen("Dropped")));

  auto reader = client->createReader(2);
  rv = reader->startReading(logid_t(1), lsn1.front(), lsn1.back());
  ASSERT_EQ(rv, 0);
  rv = reader->startReading(logid_t(2), lsn2.front(), lsn2.back());
  ASSERT_EQ(rv, 0);

  bool found_gap = false;
  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  ssize_t nread;
  bool first_gap_seen = false;
  while ((nread = reader->read(1, &data, &gap))) {
    if (nread == -1) {
      found_gap = true;
      if (!first_gap_seen) {
        EXPECT_EQ(GapType::TRIM, gap.type);
        first_gap_seen = true;
      } else {
        EXPECT_EQ(GapType::BRIDGE, gap.type);
      }
    }
  }

  EXPECT_TRUE(found_gap);
  EXPECT_FALSE(data.empty());
}
