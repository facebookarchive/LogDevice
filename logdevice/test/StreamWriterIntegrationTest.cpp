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
#include "logdevice/common/StreamWriterAppendSink.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/test/BufferedWriterTestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

/**
 * @file Integration tests for BufferedWriter with StreamWriterAppendSink.
 */

using namespace facebook::logdevice;

class StreamWriterIntegrationTest : public IntegrationTestBase {};

TEST_F(StreamWriterIntegrationTest, SimpleAppendTest) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);
  auto client = cluster->createClient();
  TestCallback cb;
  ClientImpl* client_impl = checked_downcast<ClientImpl*>(client.get());
  std::shared_ptr<Processor> processor =
      std::static_pointer_cast<Processor>(client_impl->getProcessorPtr());
  std::unique_ptr<ClientBridge> bridge =
      std::make_unique<ClientBridgeImpl>(client_impl);
  auto stream_sink = std::make_unique<StreamWriterAppendSink>(
      client_impl->getProcessorPtr(), bridge.get(), client_impl->getTimeout());
  auto writer = BufferedWriter::create(client, stream_sink.get(), &cb);
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
