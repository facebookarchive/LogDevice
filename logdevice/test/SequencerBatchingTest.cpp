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

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/APPENDED_Message.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/BufferedWriteDecoder.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class SequencerBatchingTest : public IntegrationTestBase {};

// Write&read some records with sequencer batching on
TEST_F(SequencerBatchingTest, Simple) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .setParam("--sequencer-batching")
                     .create(1);
  auto client = cluster->createClient(this->testTimeout());

  std::mutex mutex;
  std::set<lsn_t> lsns;
  std::multiset<int64_t> offsets;
  Semaphore sem;
  auto cb = [&](Status st, const DataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_EQ(E::OK, st);
    lsns.insert(record.attrs.lsn);
    offsets.insert(APPENDED_Message::last_seq_batching_offset);
    sem.post();
  };

  std::multiset<std::string> payloads_written;
  size_t payload_size_total = 0;
  const int NWRITES = 10;
  // Do a few nonblocking writes in quick succession.  They should get batched
  // on the sequencer.
  for (int i = 1; i <= NWRITES; ++i) {
    std::string payload = "payloadcompressible" + std::to_string(i);
    int rv = client->append(logid_t(1), payload, cb);
    ASSERT_EQ(0, rv);
    payloads_written.insert(payload);
    payload_size_total += payload.size();
  }
  for (int i = 1; i <= NWRITES; ++i) {
    sem.wait();
  }
  // Expect to get a single LSN assigned to all records due to batching
  ASSERT_EQ(1, lsns.size());
  // With just one batch, expect to see offsets [0, NWRITES - 1]
  {
    std::multiset<int64_t> expected;
    for (int i = 0; i < NWRITES; ++i) {
      expected.insert(i);
    }
    ASSERT_EQ(expected, offsets);
  }

  // Check that we can read all the payloads correctly
  auto reader = client->createReader(1);
  int rv = reader->startReading(logid_t(1), *lsns.begin(), *lsns.begin());
  ASSERT_EQ(0, rv);
  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  int nread = reader->read(NWRITES, &data, &gap);
  ASSERT_GE(nread, 0); // don't expect a gap

  std::multiset<std::string> payloads_read;
  for (const auto& record : data) {
    payloads_read.emplace(
        (const char*)record->payload.data(), record->payload.size());
  }
  ASSERT_EQ(payloads_written, payloads_read);

  auto stats = cluster->getNode(0).stats();
  ld_info("Payload bytes sent by client: %zu", payload_size_total);
  ld_info("Payload bytes unbatched by server: %zu",
          stats["append_bytes_seq_batching_in"]);
  ld_info("Payload bytes batched by server: %zu",
          stats["append_bytes_seq_batching_out"]);

  ASSERT_EQ(payload_size_total, stats["append_bytes_seq_batching_in"]);
  ASSERT_LT(stats["append_bytes_seq_batching_out"], payload_size_total);
}

// Write&read some records with sequencer batching on as well as buffered
// writers on the client.  The sequencer should be able to unbundle incoming
// buffered writes and recombine them into bigger batches.
TEST_F(SequencerBatchingTest, BufferedWriters) {
  const int NLOGS = 10;
  const int NBUFFEREDWRITERS = 50;
  const int NWRITES = 10000;
  const int PAYLOAD_SIZE = 100;
  std::uniform_int_distribution<int> log_dist(1, NLOGS);
  std::uniform_int_distribution<int> writer_dist(0, NBUFFEREDWRITERS - 1);
  auto rng = folly::ThreadLocalPRNG();

  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .setParam("--sequencer-batching")
                     .setNumLogs(NLOGS)
                     .create(2);
  auto client = cluster->createClient(this->testTimeout());

  std::mutex mutex;
  std::map<logid_t, std::pair<lsn_t, lsn_t>> lsns; // log -> (min, max)
  std::atomic<int> failed(0);
  Semaphore sem;
  struct Callback : BufferedWriter::AppendCallback {
    void onSuccess(logid_t log_id,
                   ContextSet contexts,
                   const DataRecordAttributes& attrs) override {
      {
        std::lock_guard<std::mutex> guard(*mutex_);
        auto insert_result =
            lsns_->emplace(log_id, std::make_pair(attrs.lsn, attrs.lsn));
        if (!insert_result.second) {
          std::pair<lsn_t, lsn_t>& range = insert_result.first->second;
          range.first = std::min(range.first, attrs.lsn);
          range.second = std::max(range.second, attrs.lsn);
        }
      }
      for (const auto& c : contexts) {
        sem_->post();
      }
    }
    void onFailure(logid_t log_id,
                   ContextSet contexts,
                   Status status) override {
      ld_error("unexpected write failure for log %lu: %s",
               log_id.val_,
               error_description(status));
      ++*failed_;
      for (const auto& c : contexts) {
        sem_->post();
      }
    }
    RetryDecision onRetry(logid_t, const ContextSet&, Status) override {
      ld_check(false);
      return RetryDecision::DENY;
    }

    std::mutex* mutex_;
    std::map<logid_t, std::pair<lsn_t, lsn_t>>* lsns_;
    std::atomic<int>* failed_;
    Semaphore* sem_;
  } cb;
  cb.mutex_ = &mutex;
  cb.lsns_ = &lsns;
  cb.failed_ = &failed;
  cb.sem_ = &sem;

  // Normally, a single BufferedWriter is used per client but here we want to
  // simulate multiple clients using BufferedWriter
  std::vector<std::unique_ptr<BufferedWriter>> buffered_writers;
  BufferedWriter::Options opts;
  // Each BufferedWriter should flush after ~10 writes
  opts.size_trigger = (PAYLOAD_SIZE + 5) * 10;
  opts.retry_count = 0;
  while (buffered_writers.size() < NBUFFEREDWRITERS) {
    buffered_writers.push_back(BufferedWriter::create(client, &cb, opts));
  }

  std::multiset<std::string> payloads_written;
  for (int i = 1; i <= NWRITES; ++i) {
    std::string payload = "pay" + std::to_string(i);
    payloads_written.insert(payload);
    BufferedWriter& writer = *buffered_writers[writer_dist(rng)];
    int rv = writer.append(logid_t(log_dist(rng)), std::move(payload), nullptr);
    ASSERT_EQ(0, rv);
  }
  for (auto& writer : buffered_writers) {
    writer->flushAll();
  }
  for (int i = 1; i <= NWRITES; ++i) {
    sem.wait();
  }

  // Check that we can read all the payloads correctly

  auto reader = client->createReader(NLOGS);

  int nbatches_from_sequencer = 0;
  for (const auto& entry : lsns) {
    std::pair<lsn_t, lsn_t> range = entry.second;
    nbatches_from_sequencer += range.second - range.first + 1;
    int rv = reader->startReading(entry.first, range.first, range.second);
    ASSERT_EQ(0, rv);
  }
  const int EXPECTED_BUFFERED_WRITER_BATCHES =
      ceil(NWRITES * PAYLOAD_SIZE / opts.size_trigger);
  ld_info("Observed %d (log, lsn) pairs with sequencer batching, which "
          "should be significantly less than without sequencer "
          "batching (approximately %d)",
          nbatches_from_sequencer,
          EXPECTED_BUFFERED_WRITER_BATCHES);
  EXPECT_LT(nbatches_from_sequencer, EXPECTED_BUFFERED_WRITER_BATCHES / 3);

  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  while (data.size() < NWRITES) {
    int nread = reader->read(NWRITES, &data, &gap);
    ld_info("%zu of %d records received", data.size(), NWRITES);
    ASSERT_GE(nread, 0); // don't expect a gap
  }

  std::multiset<std::string> payloads_read;
  for (const auto& record : data) {
    payloads_read.emplace(
        (const char*)record->payload.data(), record->payload.size());
  }
  ASSERT_EQ(payloads_written.size(), payloads_read.size());
  ASSERT_EQ(payloads_written, payloads_read);
}

// TODO(t10046246): test disabled, some appends still fail with PEER_CLOSED
TEST_F(SequencerBatchingTest, DISABLED_RollingRestart) {
  const int NNODES = 3;
  const int NLOGS = 100;

  // Make all nodes sequencers and storage nodes
  Configuration::Nodes nodes;
  for (node_index_t i = 0; i < NNODES; ++i) {
    Configuration::Node& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole(/*num_shards*/ 2);
  }
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .useHashBasedSequencerAssignment()
          .enableMessageErrorInjection()
          .setParam("--sequencer-batching")
          .setLogAttributes(
              IntegrationTestUtils::ClusterFactory::createDefaultLogAttributes(
                  1))
          .setNodes(nodes)
          .setNumLogs(NLOGS)
          .create(nodes.size());
  auto client = cluster->createClient();

  std::mutex mutex;
  std::map<Status, int> statuses;
  std::atomic<int> nwrites(0), writes_outstanding(0);
  auto cb = [&](Status st, const DataRecord& /*record*/) {
    std::lock_guard<std::mutex> guard(mutex);
    ++statuses[st];
    --writes_outstanding;
  };

  std::atomic<bool> finish(false);
  auto writer_thread = std::thread([&]() {
    for (int i = 1; !finish.load(); ++i) {
      std::string payload = "pay" + std::to_string(i);
      logid_t log(folly::Random::rand32(1, NLOGS));
      ++nwrites;
      ++writes_outstanding;
      int rv = client->append(log, payload, cb);
      ASSERT_EQ(0, rv);
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  auto roller_thread = std::thread([&]() {
    for (int i = 0; i < 2; ++i) {
      int node_index = i;
      ld_info("Stopping N%d", node_index);
      IntegrationTestUtils::Node& node = cluster->getNode(node_index);
      node.signal(SIGTERM);
      node.waitUntilExited();
      ld_info("Starting N%d", node_index);
      node.start();
    }
  });

  roller_thread.join();
  finish.store(true);
  writer_thread.join();
  wait_until([&] { return writes_outstanding.load() == 0; });

  std::map<Status, int> expected_statuses({{E::OK, nwrites}});
  ASSERT_EQ(expected_statuses, statuses);
}

TEST_F(SequencerBatchingTest, DifferentLogGroupSettings) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .setParam("--sequencer-batching")
                     .create(1);

  auto independent_client =
      cluster->createIndependentClient(this->testTimeout());

  // This log will have enabled sequencer batching since --sequencer-batching
  // option is provided. Compression setting for this log group is set via
  // attributes.
  auto lg1 = independent_client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(2)),
      client::LogAttributes()
          .with_replicationFactor(1)
          // Do not use time as flush criterion.
          .with_sequencerBatchingTimeTrigger(std::chrono::milliseconds{-1})
          // 5 records, each one is "payloadcompressible" + one digit.
          .with_sequencerBatchingSizeTrigger(5 * (19 + 1)));

  // Disabling sequencer batching for this log group.
  auto lg2 = independent_client->makeLogGroupSync(
      "/log2",
      logid_range_t(logid_t(3), logid_t(4)),
      client::LogAttributes().with_replicationFactor(1).with_sequencerBatching(
          false));

  ASSERT_NE(nullptr, lg1);
  ASSERT_NE(nullptr, lg2);

  auto client = cluster->createClient(this->testTimeout());

  std::mutex mutex;
  std::unordered_map<logid_t, std::set<lsn_t>> lsns;
  Semaphore sem;

  auto cb = [&](Status st, const DataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_EQ(E::OK, st);
    lsns[record.logid].insert(record.attrs.lsn);
    sem.post();
  };

  const int NWRITES = 10;

  for (int i = 1; i <= NWRITES; ++i) {
    std::string payload = "payloadcompressible" + std::to_string(i);
    ASSERT_EQ(0, client->append(logid_t(1), payload, cb));
    ASSERT_EQ(0, client->append(logid_t(3), payload, cb));
  }

  // Wait for all writes to finish.
  for (int i = 1; i <= NWRITES * 2; ++i) {
    sem.wait();
  }

  // Expect to get two LSN assigned for records of "/log1" log group.
  // First batch is "payloadcompressible{1,2,3,4,5}",
  // second is "payloadcompressible{6,7,8,9,10}".
  ASSERT_EQ(2, lsns[logid_t(1)].size());
  // Expect to get a different LSNs assigned to records of "/log2" log group.
  ASSERT_EQ(10, lsns[logid_t(3)].size());
}

TEST_F(SequencerBatchingTest, DifferentLogGroupSettingsReloading) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableMessageErrorInjection()
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .create(1);

  auto independent_client =
      cluster->createIndependentClient(this->testTimeout());

  auto lg1 = independent_client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(2)),
      client::LogAttributes()
          .with_replicationFactor(1)
          .with_sequencerBatching(true)
          .with_sequencerBatchingCompression(Compression::ZSTD));

  ASSERT_NE(nullptr, lg1);

  auto client = cluster->createClient(this->testTimeout());

  std::mutex mutex;
  std::set<lsn_t> lsns;
  Semaphore sem;

  auto cb = [&](Status st, const DataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_EQ(E::OK, st);
    lsns.insert(record.attrs.lsn);
    sem.post();
  };

  const int NWRITES = 10;

  auto syncDoWrites = [&] {
    for (int i = 1; i <= NWRITES; ++i) {
      std::string payload = "payloadcompressible" + std::to_string(i);
      ASSERT_EQ(0, client->append(logid_t(1), payload, cb));
    }

    // Wait for all writes to finish.
    for (int i = 1; i <= NWRITES; ++i) {
      sem.wait();
    }
  };

  auto checkCompression = [&](Compression expected) {
    auto reader = client->createReader(1);
    reader->doNotDecodeBufferedWrites();
    int rv = reader->startReading(logid_t(1), *lsns.begin(), *lsns.begin());
    ASSERT_EQ(0, rv);

    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;
    int nread = reader->read(NWRITES, &data, &gap);
    ASSERT_GE(nread, 0); // Don't expect a gap.

    for (const auto& record : data) {
      Compression c;
      ASSERT_EQ(0, BufferedWriteDecoderImpl::getCompression(*record, &c));
      ASSERT_EQ(expected, c);
    }
  };

  syncDoWrites();
  ASSERT_EQ(1, lsns.size());
  checkCompression(Compression::ZSTD);

  lsns.clear();

  EXPECT_TRUE(independent_client->setAttributesSync(
      "/log1",
      client::LogAttributes()
          .with_replicationFactor(1)
          .with_sequencerBatching(true)
          .with_sequencerBatchingCompression(Compression::LZ4_HC)));

  syncDoWrites();
  ASSERT_EQ(1, lsns.size());
  checkCompression(Compression::LZ4_HC);
}
