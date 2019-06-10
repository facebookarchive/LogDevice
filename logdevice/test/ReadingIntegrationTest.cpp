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

#include "logdevice/common/Processor.h"
#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

const lsn_t e1n1 = compose_lsn(EPOCH_MIN, ESN_MIN);

class ReadingIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<
          bool /*read-streams-use-metadata-log-only*/> {
 protected:
  IntegrationTestUtils::ClusterFactory clusterFactory() {
    return IntegrationTestUtils::ClusterFactory().setParam(
        "--read-streams-use-metadata-log-only", GetParam() ? "true" : "false");
  }
};

// We had bugs where a reader at the end of the read stream might respond to
// certain events (node disconnecting or config updates) by crashing, which is
// suboptimal
TEST_P(ReadingIntegrationTest, ReadStreamAtEndDoesNotCrash) {
  auto cluster = clusterFactory().create(3);
  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  // Read the range [1, 100].  This is below the LSN that sequencers start
  // issuing, so we should get a gap right away.
  auto reader = client->createAsyncReader();
  bool gap_received = false;
  Semaphore sem;
  reader->setRecordCallback([](std::unique_ptr<DataRecord>&) { return true; });
  reader->setGapCallback([&](const GapRecord& gap) {
    gap_received = true;
    return true;
  });
  reader->setDoneCallback([&](logid_t) { sem.post(); });
  int rv = reader->startReading(logid_t(1), lsn_t(1), e1n1);
  sem.wait();

  // The gap callback should've been called before the done callback
  ASSERT_TRUE(gap_received);

  // We should be able to kill a node without the reader crashing
  ld_check(cluster->getConfig()
               ->get()
               ->serverConfig()
               ->getNode(1)
               ->isReadableStorageNode());
  cluster->getNode(1).kill();

  // We should be able to replace a node without the reader crashing
  ld_check(cluster->getConfig()
               ->get()
               ->serverConfig()
               ->getNode(2)
               ->isReadableStorageNode());
  ASSERT_EQ(0, cluster->replace(2));

  // Give some time for Client to notice the node replacement
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_P(ReadingIntegrationTest, AsyncReaderTest) {
  auto cluster =
      clusterFactory()
          .doPreProvisionEpochMetaData() // for epoch counting to work below
          .create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t logid(2);
  const size_t num_records = 10;
  // a map <lsn, data> for appended records
  std::map<lsn_t, std::string> lsn_map;

  lsn_t first_lsn = LSN_INVALID;
  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn);
    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }

    EXPECT_EQ(lsn_map.end(), lsn_map.find(lsn));
    lsn_map[lsn] = data;
  }

  cluster->waitForRecovery();

  auto it = lsn_map.cbegin();
  bool has_payload = true;
  Semaphore sem;

  // should never expect a gap
  auto gap_cb = [](const GapRecord&) {
    ADD_FAILURE() << "don't expect gaps";
    return true;
  };
  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    EXPECT_EQ(logid, r->logid);
    EXPECT_NE(lsn_map.cend(), it);
    EXPECT_EQ(it->first, r->attrs.lsn);
    const Payload& p = r->payload;
    if (has_payload) {
      EXPECT_NE(nullptr, p.data());
      EXPECT_EQ(it->second.size(), p.size());
      EXPECT_EQ(it->second, p.toString());
    } else {
      EXPECT_EQ(nullptr, p.data());
      EXPECT_EQ(0, p.size());
    }
    if (++it == lsn_map.cend()) {
      sem.post();
    }
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid, first_lsn);
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();

  // restart reading with nopayload options
  it = lsn_map.cbegin();
  has_payload = false;
  reader->withoutPayload();
  reader->startReading(logid, first_lsn);
  sem.wait();

  rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();
}

TEST_P(ReadingIntegrationTest, ResumeAsyncReaderTest) {
  auto cluster =
      clusterFactory()
          .doPreProvisionEpochMetaData() // for epoch counting to work below
          .create(1);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t logid(1);
  std::string data("data");
  lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));

  Semaphore sem;

  // should never expect a gap
  auto gap_cb = [](const GapRecord&) {
    ADD_FAILURE() << "don't expect gaps";
    return true;
  };
  bool continue_reading = false;
  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    if (!continue_reading) {
      sem.post();
      return false;
    }
    EXPECT_EQ(logid, r->logid);
    EXPECT_EQ(lsn, r->attrs.lsn);
    const Payload& p = r->payload;

    EXPECT_EQ(data.size(), p.size());
    EXPECT_EQ(data, p.toString());
    sem.post();
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid, lsn);
  sem.wait();

  // Setting up client redelivery delay bigger than test timeout value
  // to make sure that resumeReading() call will trigger ClientReadStream to
  // continue delivery.
  int rv = client->settings().set("client-initial-redelivery-delay", "24hr");
  EXPECT_EQ(0, rv);
  rv = client->settings().set("client-max-redelivery-delay", "24hr");
  EXPECT_EQ(0, rv);

  reader->startReading(logid, lsn);
  sem.wait();
  continue_reading = true;
  reader->resumeReading(logid);
  sem.wait();
}

void IntegrationTest_RunReaderTest(IntegrationTestUtils::Cluster* cluster,
                                   std::shared_ptr<Client> client) {
  const logid_t logid(2);
  const size_t num_records = 10;
  // a map <lsn, data> for appended records
  using LsnData = std::pair<std::string, std::chrono::milliseconds>;
  std::map<lsn_t, LsnData> lsn_map;

  lsn_t first_lsn = LSN_INVALID;
  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    std::chrono::milliseconds stored_timestamp;
    lsn_t lsn = client->appendSync(logid,
                                   Payload(data.data(), data.size()),
                                   AppendAttributes(),
                                   &stored_timestamp);
    EXPECT_NE(LSN_INVALID, lsn);
    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }

    EXPECT_EQ(lsn_map.end(), lsn_map.find(lsn));
    lsn_map.emplace(lsn, LsnData(data, stored_timestamp));
  }

  cluster->waitForRecovery();

  const size_t max_logs = 1;
  std::vector<std::unique_ptr<DataRecord>> records;

  auto read_with_potential_bridge_gap =
      [](Reader* reader,
         logid_t p_logid,
         lsn_t p_first_lsn,
         size_t p_num_records,
         std::vector<std::unique_ptr<DataRecord>>* p_records) -> ssize_t {
    GapRecord gap;

    reader->startReading(p_logid, p_first_lsn);
    ssize_t nread = reader->read(p_num_records, p_records, &gap);

    if (nread < p_num_records) {
      // expecting bridge gaps and the rest of the records after - this could
      // happen due to automated log provisioning
      std::vector<std::unique_ptr<DataRecord>> records2;
      ssize_t rv;
      auto validate1 = [&]() {
        ASSERT_EQ(-1, rv);
        ASSERT_EQ(E::GAP, err);
        ASSERT_EQ(GapType::BRIDGE, gap.type);
      };
      do {
        rv = reader->read(p_num_records - nread, &records2, &gap);
        if (rv != -1) {
          break;
        }
        validate1();
      } while (1);
      auto validate2 = [&]() { ASSERT_GT(rv, 0); };
      validate2();
      nread += rv;
      p_records->insert(
          p_records->end(),
          std::move_iterator<decltype(records2)::iterator>(records2.begin()),
          std::move_iterator<decltype(records2)::iterator>(records2.end()));
    }
    return nread;
  };

  std::unique_ptr<Reader> reader(client->createReader(max_logs));

  ssize_t nread = read_with_potential_bridge_gap(
      reader.get(), logid, first_lsn, num_records, &records);

  EXPECT_EQ(num_records, nread);
  EXPECT_EQ(num_records, records.size());

  auto it = lsn_map.cbegin();
  for (int i = 0; i < num_records; ++i, ++it) {
    const DataRecord& r = *records[i];
    const std::string& appended_data = it->second.first;
    const std::chrono::milliseconds appended_timestamp = it->second.second;
    EXPECT_EQ(logid, r.logid);
    EXPECT_NE(lsn_map.cend(), it);
    EXPECT_EQ(it->first, r.attrs.lsn);
    const Payload& p = r.payload;
    EXPECT_NE(nullptr, p.data());
    EXPECT_EQ(appended_data.size(), p.size());
    EXPECT_EQ(appended_data, p.toString());
    EXPECT_EQ(appended_timestamp, r.attrs.timestamp);
  }

  int rv = reader->stopReading(logid);
  EXPECT_EQ(0, rv);
  records.clear();

  ld_info("Stopped reading with reader 1");

  // Create another reader with nopayload options
  std::unique_ptr<Reader> reader2(client->createReader(max_logs));
  reader2->withoutPayload();
  nread = read_with_potential_bridge_gap(
      reader2.get(), logid, first_lsn, num_records, &records);

  EXPECT_EQ(num_records, nread);
  EXPECT_EQ(num_records, records.size());

  it = lsn_map.cbegin();
  for (int i = 0; i < num_records; ++i, ++it) {
    const DataRecord& r = *records[i];
    EXPECT_EQ(logid, r.logid);
    EXPECT_NE(lsn_map.cend(), it);
    EXPECT_EQ(it->first, r.attrs.lsn);
    const Payload& p = r.payload;
    EXPECT_EQ(nullptr, p.data());
    EXPECT_EQ(0, p.size());
  }

  rv = reader2->stopReading(logid);
  EXPECT_EQ(0, rv);
}

TEST_P(ReadingIntegrationTest, ReaderTest) {
  auto cluster = clusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();
  IntegrationTest_RunReaderTest(cluster.get(), client);
}

// Thin end-to-end test for single copy delivery,
// where copyset shuffling is seeded using the client's session info
TEST_P(ReadingIntegrationTest, SeededSCDReaderTest) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_scdEnabled(true);

  auto cluster = clusterFactory()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     .create(5);

  std::unique_ptr<facebook::logdevice::ClientSettings> client_settings(
      facebook::logdevice::ClientSettings::create());
  // Toggling on seeded copyset shuffling
  client_settings->set(
      "scd-copyset-reordering-max", "hash-shuffle-client-seed");
  std::shared_ptr<facebook::logdevice::Client> client = cluster->createClient(
      facebook::logdevice::getDefaultTestTimeout(), std::move(client_settings));
  IntegrationTest_RunReaderTest(cluster.get(), client);
}

TEST_P(ReadingIntegrationTest, ReaderSSLTest) {
  auto cluster =
      clusterFactory()
          .setParam(
              "--ssl-cert-path", TEST_SSL_FILE("logdevice_test_valid.cert"))
          .setParam("--ssl-key-path", TEST_SSL_FILE("logdevice_test.key"))
          .setParam(
              "--ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"))
          .create(2);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0,
            client_settings->set(
                "ssl-cert-path", TEST_SSL_FILE("logdevice_test_valid.cert")));
  ASSERT_EQ(0,
            client_settings->set(
                "ssl-key-path", TEST_SSL_FILE("logdevice_test.key")));
  ASSERT_EQ(0,
            client_settings->set(
                "ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert")));
  ASSERT_EQ(0, client_settings->set("ssl-load-client-cert", 1));
  ASSERT_EQ(0, client_settings->set("ssl-boundary", "node"));
  auto client =
      cluster->createClient(testTimeout(), std::move(client_settings));

  IntegrationTest_RunReaderTest(cluster.get(), client);
}

TEST_P(ReadingIntegrationTest, ReaderSSLNoClientCertTest) {
  auto cluster =
      clusterFactory()
          .setParam(
              "--ssl-cert-path", TEST_SSL_FILE("logdevice_test_valid.cert"))
          .setParam("--ssl-key-path", TEST_SSL_FILE("logdevice_test.key"))
          .setParam(
              "--ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"))
          .create(2);

  // Client does not provide a certificate
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0,
            client_settings->set(
                "ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert")));
  ASSERT_EQ(0, client_settings->set("ssl-boundary", "node"));
  auto client =
      cluster->createClient(testTimeout(), std::move(client_settings));

  IntegrationTest_RunReaderTest(cluster.get(), client);
}

// Although 0 is technically an invalid LSN, let's make sure to handle
// startReading(0) gracefully instead of burdening API users.
TEST_P(ReadingIntegrationTest, ReadFromZero) {
  auto cluster =
      clusterFactory()
          .doPreProvisionEpochMetaData() // for epoch counting to work below
          .setParam("--bridge-record-in-empty-epoch", "false")
          .create(1);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t LOG_ID(1);
  const size_t NUM_RECORDS = 10;
  lsn_t first_lsn = LSN_INVALID, last_lsn;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    std::string data("data" + std::to_string(i));
    last_lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
    if (first_lsn == LSN_INVALID) {
      first_lsn = last_lsn;
    }
    ASSERT_NE(LSN_INVALID, last_lsn);
  }

  cluster->waitForRecovery();

  std::unique_ptr<Reader> reader = client->createReader(1);
  reader->setTimeout(std::chrono::seconds(1));
  ASSERT_EQ(0, reader->startReading(LOG_ID, 0));

  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  ssize_t nread;

  // The first call should yield a bridge gap to the first epoch
  nread = reader->read(NUM_RECORDS, &data, &gap);
  ASSERT_EQ(-1, nread);
  EXPECT_EQ(0, gap.lo);
  EXPECT_EQ(e1n1 - 1, gap.hi);
  EXPECT_EQ(GapType::BRIDGE, gap.type);

  // the next call should be another gap
  nread = reader->read(NUM_RECORDS, &data, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(e1n1, gap.lo);
  ASSERT_EQ(first_lsn - 1, gap.hi);
  ASSERT_EQ(GapType::BRIDGE, gap.type);

  // The second should return data.
  read_records_no_gaps(*reader, NUM_RECORDS);
}

// A blocking read for more records than needed to reach until_lsn should not
// indefinitely wait.
TEST_P(ReadingIntegrationTest, BlockingLargeBatchAtEnd) {
  const logid_t LOG_ID(1);
  auto cluster = clusterFactory().doPreProvisionEpochMetaData().create(1);
  std::shared_ptr<Client> client = cluster->createClient();
  const lsn_t first = client->appendSync(LOG_ID, "1");
  ASSERT_NE(first, LSN_INVALID);

  Semaphore sem;
  // To trigger the bug, the reader needs to enter the blocking read() call
  // before the record with until_lsn arrives.  Start reading in a background
  // thread before the second record is written.
  std::thread reader_thread([&] {
    std::unique_ptr<Reader> reader = client->createReader(1);
    // We'll read `first' and one record after it (not yet written).
    reader->startReading(LOG_ID, first, first + 1);
    std::vector<std::unique_ptr<DataRecord>> records_out;
    GapRecord gap_out;
    sem.post();
    // This is important for the repro; we are only reading 2 records but
    // specify the batch size as 100.
    ssize_t nread = reader->read(100, &records_out, &gap_out);
    EXPECT_EQ(2, nread);
  });

  sem.wait();
  // The reader thread is entering read() and will block while it waits for
  // the second record; give it a bit of time then append the record.  Timing
  // issues here are unlikely as the reader will go to sleep shortly, while we
  // still have to do a whole append.  Even then, timing issues wouldn't make
  // the test spuriously fail but pass as the bug won't repro.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));
  client->appendSync(LOG_ID, "2");
  // If the bug is fixed, `reader_thread' quickly gets both records and
  // finishes.  If the bug reproduces, this hangs until the test times out.
  reader_thread.join();
}

// A scenario set up to trigger purging.  Should not crash storage nodes.
TEST_P(ReadingIntegrationTest, PurgingSmokeTest) {
  const int NRECORDS_PER_EPOCH = 20;
  const logid_t LOG_ID(1);
  // starting epoch is 2, see doc block in ClusterFactory::create()
  const epoch_t start_epoch(2);

  Configuration::Nodes nodes;
  for (int i = 0; i < 4; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i == 0) {
      node.addSequencerRole();
    } else {
      node.addStorageRole(/*num_shards*/ 2);
    }
  }

  Configuration::NodesConfig nodes_config(nodes);

  // set replication factor for metadata log to be 2
  // otherwise recovery cannot complete for metadata log
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes.size(), 2);
  // Sequencers writing into metadata logs throw off the epoch counting logic
  // below
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = clusterFactory()
                     .doPreProvisionEpochMetaData()
                     .setNodes(nodes)
                     .setMetaDataLogsConfig(meta_config)
                     .create(4);

  std::shared_ptr<const Configuration> config = cluster->getConfig()->get();
  ld_check(config->serverConfig()->getNode(0)->isSequencingEnabled());
  ld_check(config->serverConfig()->getNode(1)->isReadableStorageNode());
  ld_check(config->serverConfig()->getNode(2)->isReadableStorageNode());
  ld_check(config->serverConfig()->getNode(3)->isReadableStorageNode());
  ld_check(config->getLogGroupByIDShared(LOG_ID)
               ->attrs()
               .replicationFactor()
               .value() == 2);

  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  // Write some records so that all storage nodes have something to look at
  // while purging
  for (int i = 0; i < NRECORDS_PER_EPOCH; ++i) {
    lsn_t lsn = client->appendSync(LOG_ID, Payload("b", 1));
    ASSERT_NE(lsn, LSN_INVALID);
    ASSERT_EQ(start_epoch, lsn_to_epoch(lsn));
  }

  // Make N1 unreachable so that it does not participate in recovery.
  cluster->getNode(1).suspend();

  // Replace the sequencer.  The new sequencer should be able to complete
  // recovery of epoch 1 talking just to N2 and N3.
  ASSERT_EQ(0, cluster->replace(0));

  // TODO check a stat to see when recovery is complete instead of sleeping
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  cluster->waitForRecovery();

  // Resume N1.  We should be able to write more records, which trigger
  // releasing, which triggers purging of start epoch, which should not crash.
  cluster->getNode(1).resume();
  epoch_t next_epoch = epoch_t(start_epoch.val_ + 1);

  lsn_t until_lsn = LSN_INVALID;
  for (int i = 0; i < NRECORDS_PER_EPOCH; ++i) {
    lsn_t lsn = client->appendSync(LOG_ID, Payload("b", 1));
    ASSERT_NE(lsn, LSN_INVALID);
    ASSERT_EQ(next_epoch, lsn_to_epoch(lsn));
    until_lsn = lsn;
  }

  // Make N3 unreachable.  Since r=2, we should be able to read all records
  // from just N1 and N2.  However, purging needs to complete on N1 before we
  // can read anything in next_epoch from it.
  cluster->getNode(3).suspend();

  std::unique_ptr<Reader> reader = client->createReader(1);
  reader->forceNoSingleCopyDelivery();
  lsn_t start_lsn = compose_lsn(next_epoch, esn_t(1));
  int rv = reader->startReading(LOG_ID, start_lsn, until_lsn);
  ASSERT_EQ(0, rv);
  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  ssize_t nread = reader->read(NRECORDS_PER_EPOCH, &data, &gap);
  ASSERT_EQ(NRECORDS_PER_EPOCH, nread);
}

// Tests Reader::isConnectionHealthy() and AsyncReader::isConnectionHealthy().
// They should return true while all storage nodes are up and false when
// enough of the cluster goes down that we expect service interruption.
TEST_P(ReadingIntegrationTest, ReadHealth) {
  // Make sure replication factor is 2
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(256);
  auto cluster = clusterFactory()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     .create(5);
  std::shared_ptr<Client> client = cluster->createClient();
  std::shared_ptr<const Configuration> config = cluster->getConfig()->get();
  std::unique_ptr<Reader> reader = client->createReader(1);
  std::unique_ptr<AsyncReader> async_reader = client->createAsyncReader();

  const logid_t LOG_ID(1);

  // Call should return -1 if we are not reading
  ASSERT_EQ(-1, reader->isConnectionHealthy(LOG_ID));
  ASSERT_EQ(-1, async_reader->isConnectionHealthy(LOG_ID));

  reader->startReading(LOG_ID, LSN_OLDEST);
  async_reader->setRecordCallback(
      [](std::unique_ptr<DataRecord>&) { return true; });
  async_reader->startReading(LOG_ID, LSN_OLDEST);

  ld_info("Waiting for readers to connect and reach healthy state");
  wait_until([&]() { return reader->isConnectionHealthy(LOG_ID) == 1; });
  wait_until([&]() { return async_reader->isConnectionHealthy(LOG_ID) == 1; });

  ld_info("Killing first storage node, should not affect connection health");
  ld_check(config->serverConfig()->getNode(1)->isReadableStorageNode());
  cluster->getNode(1).kill();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));
  wait_until([&]() { return reader->isConnectionHealthy(LOG_ID) == 1; });
  wait_until([&]() { return async_reader->isConnectionHealthy(LOG_ID) == 1; });

  ld_info("Killing sequencer node, should not affect connection health");
  ld_check(!config->serverConfig()->getNode(0)->isReadableStorageNode());
  cluster->getNode(0).kill();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));
  wait_until([&]() { return reader->isConnectionHealthy(LOG_ID) == 1; });
  wait_until([&]() { return async_reader->isConnectionHealthy(LOG_ID) == 1; });

  ld_info("Killing second storage node, should negatively affect cluster "
          "health because r=2");
  ld_check(config->serverConfig()->getNode(2)->isReadableStorageNode());
  cluster->getNode(2).kill();
  wait_until([&]() { return reader->isConnectionHealthy(LOG_ID) == 0; });
  wait_until([&]() { return async_reader->isConnectionHealthy(LOG_ID) == 0; });
}

// Tests AsyncReader::setHealthChangeCallback().
// Appropriate callbacks should be triggered at the right time.
TEST_P(ReadingIntegrationTest, HealthChangeCallback) {
  // Make sure replication factor is 2
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(256);
  auto cluster = clusterFactory()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     .create(5);
  std::shared_ptr<Client> client = cluster->createClient();
  std::shared_ptr<const Configuration> config = cluster->getConfig()->get();
  std::unique_ptr<AsyncReader> async_reader = client->createAsyncReader();

  const logid_t LOG_ID(1);

  // write some records
  auto write_records = [&] {
    for (int i = 0; i < 10; ++i) {
      std::string data;
      for (int j = 0; j < 10; ++j) {
        data += 'a' + folly::Random::rand32() % 26;
      }
      lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
      EXPECT_NE(LSN_INVALID, lsn);
    };
  };
  write_records();

  // The callback will post on this semaphore every time it sees a
  // health change on LOG_ID. Every other call is a logic error.
  Semaphore sem;
  bool currently_healthy = false;
  auto health_change_cb = [&](const logid_t id, const HealthChangeType status) {
    const bool healthy = (status == HealthChangeType::LOG_HEALTHY);
    ld_info("Health change callback called: id %lu, currently healthy %d "
            "new health %d",
            id.val_,
            currently_healthy,
            healthy);
    // validate parameters
    EXPECT_EQ(LOG_ID, id);
    if (currently_healthy) {
      EXPECT_EQ(HealthChangeType::LOG_UNHEALTHY, status);
    } else {
      EXPECT_EQ(HealthChangeType::LOG_HEALTHY, status);
    }

    // change internal state
    currently_healthy = healthy;
    sem.post();
  };

  Semaphore record_sem;
  async_reader->setRecordCallback([&](std::unique_ptr<DataRecord>&) {
    record_sem.post();
    return true;
  });
  async_reader->setHealthChangeCallback(health_change_cb);
  async_reader->startReading(LOG_ID, LSN_OLDEST);

  ld_info("Waiting for reader to connect and reach healthy state");
  sem.wait();
  EXPECT_TRUE(currently_healthy);

  ld_info("Waiting for reader to read at least one record");
  record_sem.wait();

  ld_info("Killing first storage node, should not affect connection health");
  ld_check(config->serverConfig()->getNode(1)->isReadableStorageNode());
  cluster->getNode(1).kill();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(currently_healthy);

  ld_info("Killing sequencer node, should not affect connection health");
  ld_check(!config->serverConfig()->getNode(0)->isReadableStorageNode());
  cluster->getNode(0).kill();
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_TRUE(currently_healthy);

  ld_info("Killing second storage node, should negatively affect cluster "
          "health because r=2");
  ld_check(config->serverConfig()->getNode(2)->isReadableStorageNode());
  cluster->getNode(2).kill();
  sem.wait();
  EXPECT_FALSE(currently_healthy);

  ld_info("Restarting second storage node, the cluster should go back to a "
          "healthy state");
  cluster->getNode(2).start();
  sem.wait();
  EXPECT_TRUE(currently_healthy);
}

// Tests AsyncReader::getBytesBuffered() and the postStatisticsRequest()
// callback it triggers.
// Test successful if callback does trigger.
TEST_P(ReadingIntegrationTest, StatisticsCallback) {
  auto cluster = clusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t LOG_ID(1);

  // write some records
  auto write_records = [&] {
    for (int i = 0; i < 10; ++i) {
      std::string data;
      for (int j = 0; j < 10; ++j) {
        data += 'a' + folly::Random::rand32() % 26;
      }
      lsn_t lsn = client->appendSync(LOG_ID, Payload(data.data(), data.size()));
      EXPECT_NE(LSN_INVALID, lsn);
    };
  };
  write_records();

  // Use stats semaphore to block till the callback triggers.
  Semaphore stats_sem;
  auto statistics_cb = [&](size_t bufferSize) {
    // Checking the actual bufferSize returned is not ideal;
    // It's likely that at the time of the callback,
    // records have been handled (and their contribution removed).
    stats_sem.post();
    return;
  };

  std::unique_ptr<AsyncReader> async_reader(client->createAsyncReader());

  // Use record semaphore to block till at least one record is read.
  Semaphore record_sem;
  async_reader->setRecordCallback([&](std::unique_ptr<DataRecord>&) {
    record_sem.post();
    return true;
  });

  async_reader->startReading(LOG_ID, LSN_OLDEST);
  ld_info("Waiting for reader to read at least one record");
  record_sem.wait();

  async_reader->getBytesBuffered(statistics_cb);
  // Ensure that the callback executes
  ld_info("Waiting for stats request to complete");
  stats_sem.wait();
}

// Get the same kind of hash as reader with PAYLOAD_HASH_ONLY flag.
static std::pair<uint32_t, uint32_t> calc_hash(const std::string& s) {
  return std::make_pair(
      (uint32_t)s.size(), folly::crc32c((const uint8_t*)s.data(), s.size()));
}

// Parse the hash returned by a reader with PAYLOAD_HASH_ONLY flag.
static std::pair<uint32_t, uint32_t> parse_hash(Payload payload) {
  std::pair<uint32_t, uint32_t> res;
  static_assert(sizeof(res) == 8, "Weird");
  if (payload.size() != sizeof(res) || payload.data() == nullptr) {
    ADD_FAILURE() << "Expected payload to be 8 bytes, " << payload.size()
                  << "found";
    res = std::make_pair(0, 0);
    return res;
  }
  memcpy(&res, payload.data(), sizeof(res));
  return res;
}

TEST_P(ReadingIntegrationTest, PayloadHashOnly) {
  const logid_t LOG_ID(1);

  auto cluster =
      clusterFactory()
          .doPreProvisionEpochMetaData() // for epoch counting to work below
          .setParam("--bridge-record-in-empty-epoch", "false")
          .create(2);

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);

  std::string payload1 = "abra";
  std::string payload2 = "cadabra";

  lsn_t lsn1 = client->appendSync(LOG_ID, payload1);
  ASSERT_NE(LSN_INVALID, lsn1);
  lsn_t lsn2 = client->appendSync(LOG_ID, payload2);
  ASSERT_NE(LSN_INVALID, lsn2);
  ASSERT_EQ(lsn1 + 1, lsn2);

  std::unique_ptr<Reader> reader = client->createReader(1);
  auto reader_impl = dynamic_cast<ReaderImpl*>(reader.get());
  ASSERT_NE(nullptr, reader_impl);
  reader_impl->payloadHashOnly();
  ASSERT_EQ(0, reader->startReading(LOG_ID, 1));

  std::vector<std::unique_ptr<DataRecord>> recs;
  GapRecord gap;
  ssize_t nread;

  // The first call should yield a bridge gap to the first epoch
  nread = reader->read(1, &recs, &gap);
  ASSERT_EQ(-1, nread);
  EXPECT_EQ(1, gap.lo);
  EXPECT_EQ(e1n1 - 1, gap.hi);
  EXPECT_EQ(GapType::BRIDGE, gap.type);

  nread = reader->read(1, &recs, &gap);
  ASSERT_EQ(-1, nread);
  ASSERT_EQ(E::GAP, err);
  ASSERT_EQ(e1n1, gap.lo);
  ASSERT_EQ(lsn1 - 1, gap.hi);
  ASSERT_EQ(GapType::BRIDGE, gap.type);

  nread = reader->read(2, &recs, &gap);
  ASSERT_EQ(2, nread);
  EXPECT_EQ(lsn1, recs[0]->attrs.lsn);
  EXPECT_EQ(calc_hash(payload1), parse_hash(recs[0]->payload));
  EXPECT_EQ(lsn2, recs[1]->attrs.lsn);
  EXPECT_EQ(calc_hash(payload2), parse_hash(recs[1]->payload));
}

TEST_P(ReadingIntegrationTest, LogTailAttributes) {
  using namespace std::chrono;

  const int NRECORDS_PER_EPOCH = 20;
  const int RECORD_SIZE = 10;
  const logid_t LOG_ID(1);
  const epoch_t start_epoch(2);

  Configuration::Nodes nodes;
  for (int i = 0; i < 4; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    if (i == 0) {
      node.addSequencerRole();
    } else {
      node.addStorageRole(/*num_shards*/ 2);
    }
  }

  Configuration::NodesConfig nodes_config(nodes);

  // set replication factor for metadata log to be 2
  // otherwise recovery cannot complete for metadata log
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(nodes_config, nodes.size(), 2);

  // Sequencers writing into metadata logs throw off the epoch counting logic
  // below
  meta_config.sequencers_write_metadata_logs = false;
  meta_config.sequencers_provision_epoch_store = false;

  auto cluster = clusterFactory()
                     .doPreProvisionEpochMetaData()
                     .setNodes(nodes)
                     .setParam("--byte-offsets")
                     .setMetaDataLogsConfig(meta_config)
                     .create(4);

  std::shared_ptr<const Configuration> config = cluster->getConfig()->get();
  ld_check(config->serverConfig()->getNode(0)->isSequencingEnabled());

  std::shared_ptr<Client> client = cluster->createClient();

  cluster->waitForRecovery();

  std::unique_ptr<LogTailAttributes> attributes =
      client->getTailAttributesSync(LOG_ID);
  ASSERT_NE(nullptr, attributes);
  ASSERT_FALSE(attributes->valid());

  Semaphore sem;
  auto empty_cb = [&](Status st,
                      std::unique_ptr<LogTailAttributes> tail_attributes) {
    ASSERT_EQ(E::OK, st);
    ASSERT_NE(nullptr, tail_attributes);
    ASSERT_FALSE(tail_attributes->valid());
    sem.post();
  };
  int rv = client->getTailAttributes(LOG_ID, empty_cb);
  ASSERT_EQ(0, rv);
  sem.wait();

  // timestamp of the last written record
  std::chrono::milliseconds last_timestamp{0};

  // write some records
  auto write_records = [&] {
    for (int i = 0; i < NRECORDS_PER_EPOCH; ++i) {
      std::string data;
      for (int j = 0; j < RECORD_SIZE; ++j) {
        data += 'a' + folly::Random::rand32() % 26;
      }
      lsn_t lsn = client->appendSync(LOG_ID,
                                     Payload(data.data(), data.size()),
                                     AppendAttributes(),
                                     &last_timestamp);
      EXPECT_NE(LSN_INVALID, lsn);
    };
  };
  write_records();

  attributes = client->getTailAttributesSync(LOG_ID);
  EXPECT_TRUE(attributes);
  EXPECT_EQ(compose_lsn(start_epoch, esn_t(NRECORDS_PER_EPOCH)),
            attributes->last_released_real_lsn);
  EXPECT_EQ(last_timestamp, attributes->last_timestamp);
  EXPECT_EQ(NRECORDS_PER_EPOCH * RECORD_SIZE,
            attributes->offsets.getCounter(BYTE_OFFSET));

  // Restart the sequencer to trigger recovery
  cluster->getNode(0).kill();
  cluster->getNode(0).start();
  cluster->getNode(0).waitUntilStarted();

  // write some more records in the new epoch, writes should succeed
  // since there are still two regions available
  write_records();

  cluster->waitForRecovery();

  attributes = client->getTailAttributesSync(LOG_ID);
  EXPECT_TRUE(attributes);
  EXPECT_EQ(
      compose_lsn(epoch_t(start_epoch.val_ + 1), esn_t(NRECORDS_PER_EPOCH)),
      attributes->last_released_real_lsn);
  EXPECT_EQ(last_timestamp, attributes->last_timestamp);
  EXPECT_EQ(2 * NRECORDS_PER_EPOCH * RECORD_SIZE,
            attributes->offsets.getCounter(BYTE_OFFSET));
}

// See T16695564. This test simulates a storage node not being able to complete
// purging because of a permanent error (IO error). The storage node should keep
// on sending records until it hits last released lsn (a stale version that
// won't move forward because purging is stuck), and then it should enter
// permanent error mode which means it will send a STARTED(E::FAILED) message to
// the reader so it can do SCD failover.
// TODO(T44746268): replace NDEBUG with folly::kIsDebug
// Can not remove now due to the defined functions
#ifndef NDEBUG // This test requires fault injection.
TEST_P(ReadingIntegrationTest, PurgingStuck) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(256);
  log_attrs.set_scdEnabled(true);

  const logid_t LOG_ID(1);

  auto cluster = clusterFactory()
                     .setLogGroupName("my-logs")
                     .setLogAttributes(log_attrs)
                     // Guarantees we write all over the place during the test.
                     .setParam("--sticky-copysets-block-size", "1")
                     .create(5);

  // Set high values for the SCD timeoaut to make it less likely that this test
  // will pass because we rewound in ALL_SEND_ALL mode.
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("scd-timeout", "10min"));
  ASSERT_EQ(0, client_settings->set("scd-all-send-all-timeout", "10min"));

  lsn_t tail = LSN_INVALID;
  std::atomic<lsn_t> last_read_lsn{LSN_INVALID};

  std::shared_ptr<Client> client = cluster->createClient();
  std::unique_ptr<AsyncReader> async_reader = client->createAsyncReader(10);
  async_reader->setRecordCallback([&](std::unique_ptr<DataRecord>& r) {
    last_read_lsn.store(r->attrs.lsn);
    return true;
  });
  async_reader->startReading(LOG_ID, LSN_OLDEST, LSN_MAX);

  for (int i = 0; i < 1000; ++i) {
    tail = client->appendSync(LOG_ID, Payload("b", 1));
    ASSERT_NE(tail, LSN_INVALID);
  }
  ld_info("Tail: %s", lsn_to_string(tail).c_str());
  wait_until("syncing reader", [&]() { return last_read_lsn.load() >= tail; });

  // Inject IO errors in N3 so it won't complete purging.
  std::string res = cluster->getNode(3).sendCommand(
      "inject shard_fault all metadata all io_error");
  ASSERT_EQ("END\r\n", res);

  // Restart the sequencer.
  cluster->getSequencerNode().kill();
  cluster->getSequencerNode().start();
  cluster->getSequencerNode().waitUntilStarted();

  // Expect the reader will pick up the writes in the new epoch.
  for (int i = 0; i < 1000; ++i) {
    tail = client->appendSync(LOG_ID, Payload("b", 1));
    ASSERT_NE(tail, LSN_INVALID);
  }
  ld_info("Tail: %s", lsn_to_string(tail).c_str());
  wait_until("syncing reader", [&]() { return last_read_lsn.load() >= tail; });
}
#endif

void filterTestHelper(Reader* reader,
                      lsn_t first_lsn,
                      logid_t LOG_ID,
                      const ReadStreamAttributes* attrs,
                      const std::string& payload,
                      bool should_have_payload) {
  std::vector<std::unique_ptr<DataRecord>> recs;
  GapRecord gap;
  ssize_t nread = 0;
  reader->startReading(LOG_ID, first_lsn, LSN_MAX, attrs);

  // wait until there is valid data
  // We need to skip first record. This record, which appended by appendSync(),
  // does not have custom key. So we will not do filtering on this record.
  // We use it only to locate first lsn.
  while (nread != 1) {
    nread = reader->read(1, &recs, &gap);
  }
  ASSERT_EQ(1, nread);

  // should_have_payload indicates whether we expect this record to be
  // filtered out in this test.
  for (int i = 0; i < 9; i++) {
    nread = reader->read(1, &recs, &gap);
    if (should_have_payload) {
      ASSERT_EQ(
          memcmp(
              recs[i]->payload.data(), payload.data(), recs[i]->payload.size()),
          0);
    } else {
      ASSERT_EQ(-1, nread);
      ASSERT_EQ(E::GAP, err);
      ASSERT_EQ(GapType::FILTERED_OUT, gap.type);
    }
  }

  int rv = reader->stopReading(LOG_ID);
  ASSERT_EQ(0, rv);
}

TEST_P(ReadingIntegrationTest, ReadWithServerRecordFilter) {
  const logid_t LOG_ID(2);

  auto cluster = clusterFactory().doPreProvisionEpochMetaData().create(3);

  std::shared_ptr<Client> client = cluster->createClient();
  ASSERT_TRUE((bool)client);
  std::string payload{"payload for test"};
  std::string key{"Custom Key"};

  lsn_t first_lsn = LSN_INVALID;

  // To locate first_lsn, which is the start location that read() function
  // needs.
  first_lsn =
      client->appendSync(LOG_ID, Payload(payload.data(), payload.size()));
  // Append another record(now we have 2 in total) with custom key
  // "Custom Key".
  AppendAttributes attrs;
  attrs.optional_keys[KeyType::FILTERABLE] = key;
  for (int i = 0; i < 9; i++) {
    client->appendSync(LOG_ID, Payload(payload.data(), payload.size()), attrs);
  }

  std::unique_ptr<Reader> reader = client->createReader(1);
  /*
   *  Test Case 1: Range Filter, Expect: pass.
   */
  ReadStreamAttributes attrs1{ServerRecordFilterType::RANGE, "C", "D"};
  filterTestHelper(reader.get(), first_lsn, LOG_ID, &attrs1, payload, true);

  /*
   *  Test Case 2: Range Filter, Expect: filtered out.
   */
  std::unique_ptr<Reader> reader2 = client->createReader(1);
  ReadStreamAttributes attrs2{ServerRecordFilterType::RANGE, "J", "K"};
  filterTestHelper(reader2.get(), first_lsn, LOG_ID, &attrs2, payload, false);
  /*
   *  Test Case 3: Equality Filter    Expected: Pass
   */
  std::unique_ptr<Reader> reader3 = client->createReader(1);
  ReadStreamAttributes attrs3{
      ServerRecordFilterType::EQUALITY, "Custom Key", ""};
  filterTestHelper(reader3.get(), first_lsn, LOG_ID, &attrs3, payload, true);

  /*
   *  Test Case 4: Equality Filter    Expected: Filtered out
   */
  std::unique_ptr<Reader> reader4 = client->createReader(1);
  ReadStreamAttributes attrs4{
      ServerRecordFilterType::EQUALITY, "Should not pass", ""};
  filterTestHelper(reader4.get(), first_lsn, LOG_ID, &attrs4, payload, false);
}

TEST_P(ReadingIntegrationTest, AsyncReaderRecordFiltering) {
  auto cluster =
      clusterFactory()
          .doPreProvisionEpochMetaData() // for epoch counting to work below
          .create(5);
  std::shared_ptr<Client> client = cluster->createClient();

  const logid_t logid(2);
  AppendAttributes pass_attrs, fail_attrs;
  // Expect this record pass filter
  pass_attrs.optional_keys[KeyType::FILTERABLE] = "20170623";

  // Expect this record not pass
  fail_attrs.optional_keys[KeyType::FILTERABLE] = "20160302";
  std::string payload{"payload"};

  // Expect: Pass
  lsn_t first_lsn = client->appendSync(
      logid, Payload(payload.data(), payload.size()), pass_attrs);
  // Expect: FILTERED_OUT
  lsn_t second_lsn = client->appendSync(
      logid, Payload(payload.data(), payload.size()), fail_attrs);

  cluster->waitForRecovery();
  Semaphore sem;

  auto gap_cb = [&](const GapRecord& gap) {
    EXPECT_EQ(GapType::FILTERED_OUT, gap.type);
    sem.post();
    return true;
  };

  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    const Payload& p = r->payload;
    EXPECT_EQ(std::memcmp(p.data(), payload.data(), payload.size()), 0);
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  // construct a range filter to do filtering
  ReadStreamAttributes attrs{
      ServerRecordFilterType::RANGE, "20170101", "20171231"};
  reader->startReading(logid, first_lsn, LSN_MAX, &attrs);
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();
}

TEST_P(ReadingIntegrationTest, ReaderTestWithShardIDInCopyset) {
  auto cluster = clusterFactory()
                     .setParam("--write-shard-id-in-copyset", "true")
                     .create(2);
  std::shared_ptr<Client> client = cluster->createClient();
  IntegrationTest_RunReaderTest(cluster.get(), client);
}

// testing that if we allow inserting bridge records in empty epochs,
// reader can keep reading without doing f-majority based gap detection
// as long as one storage node delivers a bridge record

// setup: storage nodes 5, replication 2. use a very large gap grace period
// so that the test can only success if no gap detection is performed
TEST_P(ReadingIntegrationTest, ReadAvailabilityWithBridgeRecordForEmptyEpoch) {
  Configuration::MetaDataLogsConfig meta_config =
      createMetaDataLogsConfig(/*nodeset=*/{1}, /*replication=*/1);

  auto cluster = clusterFactory()
                     .setParam("--bridge-record-in-empty-epoch", "true")
                     .setParam("--reactivation-limit", "100/1s")
                     .setMetaDataLogsConfig(meta_config)
                     .create(6);

  const logid_t logid(2);
  std::shared_ptr<Client> client = cluster->createClient();
  client->settings().set("gap-grace-period", "300s");

  lsn_t lsn = client->appendSync(logid, Payload("23", 2));
  ASSERT_NE(LSN_INVALID, lsn);
  ld_info("Log %lu first LSN: %s.", logid.val_, lsn_to_string(lsn).c_str());

  // reactivate sequencer gracefully five times to create empty epochs
  for (int i = 0; i < 5; ++i) {
    std::string reply;
    do {
      // retry if the previous activation is in progress
      reply = cluster->getNode(0).sendCommand("up --logid 2");
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    } while (reply.find("in progress") != std::string::npos);
    ASSERT_NE(std::string::npos, reply.find("Started sequencer activation"));
    cluster->waitForRecovery();
  }

  cluster->waitForRecovery();
  lsn = client->appendSync(logid, Payload("23", 2));
  ASSERT_NE(LSN_INVALID, lsn);
  ld_info("Log %lu final LSN: %s.", logid.val_, lsn_to_string(lsn).c_str());

  // stop one node to create incomplete f-majority
  ld_info("Stopping Node 5");
  cluster->getNode(5).suspend();

  // start a reader and read through LSN_OLDEST to final lsn
  auto reader = client->createReader(1);
  reader->setTimeout(this->testTimeout());
  int rv = reader->startReading(logid, compose_lsn(EPOCH_MIN, ESN_MIN), lsn);
  ASSERT_EQ(0, rv);
  lsn_t got_lsn;
  for (;;) {
    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;

    ssize_t nread = reader->read(1, &data, &gap);
    if (nread == 0) {
      break;
    }

    if (nread > 0) {
      ASSERT_EQ(1, nread);
      got_lsn = data.back()->attrs.lsn;
      ld_info("Got record for logid %lu: %s.",
              data.back()->logid.val_,
              lsn_to_string(got_lsn).c_str());
      if (got_lsn == lsn) {
        break;
      }
    } else {
      ld_info("Got gap type %s for logid %lu: [%s, %s].",
              gapTypeToString(gap.type).c_str(),
              gap.logid.val_,
              lsn_to_string(gap.lo).c_str(),
              lsn_to_string(gap.hi).c_str());
    }
  }

  ASSERT_EQ(lsn, got_lsn);
}

/**
 * The following integration test checks that we correctly deliver gaps when
 * gap-grace-period > reader-reconnect-delay and there is a reconnecting node X
 * (i.e. reader doesn't get stuck in that situation). It also verifies that
 * changing the authoritative status of X's shards to UNDERREPLICATION allow us
 * to deliver said dataloss gap (in case we had gotten stuck earlier).
 *
 * This is based on a failure experienced in production.
 */
TEST_P(ReadingIntegrationTest,
       ReadAvailabilityWhenGapGracePeriodGTReconnectTimer) {
  const ssize_t nnodes = 6;

  const logid_t logid{1};
  const shard_index_t shard = 0;
  const ssize_t nshards = 1;
  const std::chrono::seconds waitClientDataTimeout{21};
  const node_index_t node_down = 2;

  auto attrs = logsconfig::DefaultLogAttributes()
                   .with_scdEnabled(false)
                   .with_replicationFactor(3);

  auto cluster = clusterFactory()
                     .setNumLogs(1)
                     .setLogAttributes(attrs)
                     .setNumDBShards(nshards)
                     .doPreProvisionEpochMetaData()
                     .allowExistingMetaData()
                     .create(nnodes);
  cluster->waitForRecovery();
  std::shared_ptr<Client> client = cluster->createClient();

  // Step 1.  Have gap-grace-period > reader-reconnect-delay
  client->settings().set("gap-grace-period", "3s");
  client->settings().set("reader-reconnect-delay", "1ms..1s");

  // Step 2. Make a random storage node unresponsive.
  cluster->getNode(node_down).shutdown();
  cluster->waitForRecovery();

  // Step 2.a. Force shard status map to have a non-LSN_INVALID version so
  // reconnects may trigger a call to ClientReadStream::findGapsAndRecords().
  // (As of the date of this diff, that version being != LSN_INVALID is typical
  // because SHARD_STATUS_UPDATE_Messages are sent whenever a client first
  // connects to a server, see AllServerReadStreams::insertOrGet(); for the
  // purposes of this test we should avoid racing with that message).
  auto* client_impl = reinterpret_cast<ClientImpl*>(client.get());
  client_impl->getProcessor().applyToWorkers([&](Worker& w) {
    w.shardStatusManager().getShardAuthoritativeStatusMap() =
        ShardAuthoritativeStatusMap(LSN_OLDEST);
  });

  // Step 3. Create a gap.
  lsn_t lsn1 = client->appendSync(logid, Payload("first record", 12));
  lsn_t lsn2 = client->appendSync(logid, Payload("this is loss", 12));
  ASSERT_NE(LSN_INVALID, lsn1) << "We are unable to append to log";
  ASSERT_NE(LSN_INVALID, lsn2) << "We are unable to append to log";
  cluster->applyToNodes([&](auto& n) {
    for (shard_index_t shard_idx = 0; shard_idx < nshards; shard_idx++) {
      auto cmd = folly::sformat("record erase --shard {} {} {}",
                                shard_idx,
                                logid.val_,
                                lsn_to_string(lsn2));
      n.sendCommand(cmd, true);
    }
  });

  // Step 4. Add more data and try to read it by starting from e0n1
  lsn_t lsn3 = client->appendSync(logid, Payload("available data", 14));
  ASSERT_NE(LSN_INVALID, lsn3) << "We are unable to append to log";

  auto reader = client->createReader(1);
  reader->setTimeout(waitClientDataTimeout); // so we don't get stuck in read()
                                             // when nothing is being delivered
  int rv = reader->startReading(logid, LSN_OLDEST);
  ASSERT_EQ(0, rv) << "We are unable to start reading from log " << logid.val();

  ssize_t num_dataloss_gaps_read = 0;
  ssize_t num_records_read = 0;

  auto try_read_some_data = [&]() {
    return wait_until("Can read available data",
                      [&]() {
                        if (num_records_read >= 2) {
                          return true;
                        } else {
                          std::vector<std::unique_ptr<DataRecord>> data;
                          GapRecord gap;
                          ssize_t nread = reader->read(1, &data, &gap);
                          if (nread >= 0) {
                            num_records_read += nread;
                          } else {
                            if (gap.type == GapType::DATALOSS) {
                              num_dataloss_gaps_read++;
                            }
                          }
                          return num_records_read >= 2;
                        }
                      },
                      std::chrono::steady_clock::now() + waitClientDataTimeout);
  };

  EXPECT_EQ(try_read_some_data(), 0)
      << "Reader did not move past dataloss gap when shard was in "
         "(re-)connecting state";

  // Step 5. Override shard status
  client_impl->getProcessor().applyToWorkers([&](Worker& w) {
    w.shardStatusManager().getShardAuthoritativeStatusMap().setShardStatus(
        node_down, shard, AuthoritativeStatus::UNDERREPLICATION);
  });

  EXPECT_EQ(try_read_some_data(), 0)
      << "Reader did not move past dataloss gap when shard was in "
         "UNDERREPLICATION state";

  EXPECT_EQ(num_dataloss_gaps_read, 1)
      << "Unexpected number of dataloss gaps read";
  EXPECT_EQ(num_records_read, 2) << "Unexpected number of records read";
}

INSTANTIATE_TEST_CASE_P(ReadingIntegrationTest,
                        ReadingIntegrationTest,
                        ::testing::Values(false, true));
