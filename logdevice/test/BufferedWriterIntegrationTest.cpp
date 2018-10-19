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

#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/BufferedWriterTestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

/**
 * @file Integration tests for BufferedWriter.  Tests in this file exercise
 * require the full stack, mostly to exercise the interaction with client-side
 * readers.  Most tests exercising BufferedWriter logic are in
 * common/test/BufferedWriterTest.cpp.
 */

using namespace facebook::logdevice;

class BufferedWriterIntegrationTest : public IntegrationTestBase {};

TEST_F(BufferedWriterIntegrationTest, MixedWithRegularWrites) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();
  TestCallback cb;
  auto writer = BufferedWriter::create(client, &cb);
  const logid_t LOG_ID(1);

  ASSERT_EQ(0, writer->append(LOG_ID, "1", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "2", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();
  cb.sem.wait();
  // Throw in a direct append through Client
  ASSERT_NE(LSN_INVALID, client->appendSync(LOG_ID, "3"));

  ASSERT_EQ(0, writer->append(LOG_ID, "4", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "5", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  cb.sem.wait();
  cb.sem.wait();

  std::vector<std::string> expected{"1", "2", "3", "4", "5"};
  std::vector<std::string> received;

  auto reader = client->createReader(1);
  // We expect 3 raw writes in the log
  lsn_t first_lsn = cb.lsn_range.first;
  lsn_t until_lsn = cb.lsn_range.second;
  int rv = reader->startReading(LOG_ID, first_lsn, until_lsn);
  ASSERT_EQ(0, rv);
  reader->setTimeout(std::chrono::milliseconds(100));

  // Read one by one to verify that isReading() stays true until we have
  // consumed all records.
  while (received.size() < expected.size()) {
    ASSERT_TRUE(reader->isReading(LOG_ID));
    std::vector<std::unique_ptr<DataRecord>> data;
    GapRecord gap;
    ssize_t nread = reader->read(1, &data, &gap);
    if (nread > 0) {
      for (const auto& record : data) {
        Payload p = record->payload;
        received.emplace_back((const char*)p.data(), p.size());
      }
    }
  }
  ASSERT_EQ(expected, received);
  ASSERT_FALSE(reader->isReading(LOG_ID));
}

// Simple test that uses a BufferedWriter to write a few records and
// AsyncReader to read them.
TEST_F(BufferedWriterIntegrationTest, AsyncReader) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();
  TestCallback cb;
  auto writer = BufferedWriter::create(client, &cb);
  const logid_t LOG_ID(1);

  std::set<std::string> orig_payloads, read_payloads;
  const int batch_count = 10;
  const int payloads_per_batch = 100;

  int counter = 1; // goes in payload
  for (int nbatch = 0; nbatch < batch_count; ++nbatch) {
    for (int i = 0; i < payloads_per_batch; ++i) {
      std::string str = std::to_string(counter++);
      orig_payloads.insert(str);
      int rv = writer->append(LOG_ID, std::move(str), NULL_CONTEXT);
      ASSERT_EQ(0, rv);
    }
    writer->flushAll();
  }

  for (int i = 0; i < batch_count * payloads_per_batch; ++i) {
    cb.sem.wait();
  }

  lsn_t last_lsn_written = cb.lsn_range.second;
  ASSERT_NE(last_lsn_written, LSN_INVALID);

  auto reader = client->createAsyncReader();

  std::mutex read_cb_mutex;
  folly::Optional<lsn_t> last_lsn_read;
  reader->setRecordCallback([&](std::unique_ptr<DataRecord>& record) {
    std::lock_guard<std::mutex> lock(read_cb_mutex);
    last_lsn_read.assign(record->attrs.lsn);
    Payload p = record->payload;
    read_payloads.emplace((const char*)p.data(), p.size());
    ld_info("ReadCallback for lsn %lu, content %s",
            record->attrs.lsn,
            p.toString().c_str());
    return true;
  });

  bool gap_received = false;
  reader->setGapCallback([&](const GapRecord&) {
    std::lock_guard<std::mutex> lock(read_cb_mutex);
    gap_received = true;
    ld_info("GapCallback");
    return true;
  });

  Semaphore sem_read;
  reader->setDoneCallback([&](logid_t logid) {
    ld_info("DoneCallback for logid %lu", logid.val_);
    sem_read.post();
  });

  int rv = reader->startReading(LOG_ID, LSN_OLDEST, last_lsn_written);
  ASSERT_EQ(0, rv);
  sem_read.wait();

  ASSERT_TRUE(gap_received);
  ASSERT_EQ(last_lsn_read, last_lsn_written);
  ASSERT_EQ(orig_payloads, read_payloads);
  ASSERT_EQ(orig_payloads, cb.payloadsSucceededAsSet());
}

// DataRecord::batch_offset should be set correctly on the read path
TEST_F(BufferedWriterIntegrationTest, BatchOffset) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();
  TestCallback cb;
  auto writer = BufferedWriter::create(client, &cb);
  const logid_t LOG_ID(1);

  std::set<std::string> orig_payloads, read_payloads;

  ASSERT_EQ(0, writer->append(LOG_ID, "1", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "2", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "3", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  ASSERT_EQ(0, writer->append(LOG_ID, "4", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());

  auto reader = client->createReader(1);
  int rv = reader->startReading(LOG_ID, LSN_OLDEST);
  ASSERT_EQ(0, rv);
  reader->setTimeout(std::chrono::milliseconds(100));
  std::vector<std::unique_ptr<DataRecord>> data;
  int to_read = 4;
  while (to_read > 0) {
    GapRecord gap;
    ssize_t nread = reader->read(to_read, &data, &gap);
    if (nread > 0) {
      to_read -= nread;
    }
  }

  EXPECT_EQ(0, data[0]->attrs.batch_offset);
  EXPECT_EQ(1, data[1]->attrs.batch_offset);
  EXPECT_EQ(2, data[2]->attrs.batch_offset);
  EXPECT_EQ(0, data[3]->attrs.batch_offset);
}

// Test a tricky interaction between BufferedWriter and AsyncReader.  If the
// application rejects a record that was part of a buffered write, AsyncReader
// needs to carefully handle it.
TEST_F(BufferedWriterIntegrationTest, AsyncReaderRejectBufferedWrite) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  // Ask ClientReadStream to redeliver faster than the default 1s
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("client-initial-redelivery-delay", "1ms"));

  std::shared_ptr<Client> client =
      cluster->createClient(this->testTimeout(), std::move(client_settings));
  TestCallback writer_cb;
  auto writer = BufferedWriter::create(client, &writer_cb);
  const logid_t LOG_ID(1);

  ASSERT_EQ(0, writer->append(LOG_ID, "1", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "2", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "3", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());
  ASSERT_EQ(0, writer->append(LOG_ID, "4", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());

  std::mutex mutex;
  std::multiset<std::string> rejected;
  std::vector<std::string> accepted;
  Semaphore sem;
  auto reader_cb = [&](std::unique_ptr<DataRecord>& record) {
    std::lock_guard<std::mutex> guard(mutex);
    std::string str{record->payload.toString()};
    // Reject the first two attempts at delivering "2", and the first at
    // delivering "4".
    if ((str == "2" && rejected.count(str) < 2) ||
        (str == "4" && rejected.count(str) < 1)) {
      rejected.insert(str);
      return false;
    }
    accepted.push_back(str);
    if (str == "4") {
      sem.post();
    }
    return true;
  };
  auto reader = client->createAsyncReader();
  reader->setRecordCallback(reader_cb);
  ASSERT_EQ(0, reader->startReading(LOG_ID, LSN_OLDEST));

  sem.wait();
  ASSERT_EQ(std::multiset<std::string>({"2", "2", "4"}), rejected);
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3", "4"}), accepted);
}

// Test that Reader::read(n) works as expected with batched writes, i.e. returns
// as soon as n individual records as available, even though the number of
// batches might be smaller.
TEST_F(BufferedWriterIntegrationTest, ReaderSingleBatch) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  std::shared_ptr<Client> client = cluster->createClient();
  const logid_t LOG_ID(1);

  TestCallback cb;
  BufferedWriter::Options opts;
  auto writer = BufferedWriter::create(client, &cb, opts);

  // Write 3 individual records in a single batch.
  ASSERT_EQ(0, writer->append(LOG_ID, "1", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "2", NULL_CONTEXT));
  ASSERT_EQ(0, writer->append(LOG_ID, "3", NULL_CONTEXT));
  ASSERT_EQ(0, writer->flushAll());

  auto reader = client->createReader(1);
  ASSERT_EQ(0, reader->startReading(LOG_ID, LSN_OLDEST));

  std::vector<std::unique_ptr<DataRecord>> data;
  GapRecord gap;
  for (;;) {
    ssize_t nread = reader->read(3, &data, &gap);
    if (nread >= 0) {
      // All written records should be ready to be consumed.
      ASSERT_EQ(3, nread);
      break;
    }
    ASSERT_EQ(GapType::BRIDGE, gap.type);
  }
}
