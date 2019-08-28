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

/**
 * Callback used to test if ACKs for messages happen in FIFO order and that once
 * accepted a message never fails.
 */
class FIFOTestCallback : public BufferedWriter::AppendCallback {
 public:
  // Ensures that messages are ACKed in FIFO order and posts on the semaphore
  // that the client waits on. Also, maintains lsn_range (the smallest and
  // largest LSN) that has been ACKed to the callback.
  void onSuccess(logid_t,
                 ContextSet contexts,
                 const DataRecordAttributes& attrs) override {
    std::lock_guard<std::mutex> guard(mutex_);
    if (lsn_range.first == LSN_INVALID) {
      lsn_range.first = attrs.lsn;
    }
    lsn_range.first = std::min(lsn_range.first, attrs.lsn);
    lsn_range.second = std::max(lsn_range.second, attrs.lsn);
    for (auto& ctx : contexts) {
      auto& seq_num = *((int*)ctx.first);
      ASSERT_EQ(next_seq_num, seq_num);
      delete ((int*)ctx.first);
      ++next_seq_num;
      sem.post();
    }
  }

  // Message once accepted in the stream must never fail.
  void onFailure(logid_t /*log_id*/,
                 ContextSet /*contexts*/,
                 Status /*status*/) override {
    ADD_FAILURE();
  }

  // Each successful append posts to this semaphore.
  Semaphore sem;
  // Next expected message seq number. Should always increase by one.
  int next_seq_num = 0;
  // LSN range that was written.
  std::pair<lsn_t, lsn_t> lsn_range{LSN_INVALID, LSN_INVALID};

 private:
  std::mutex mutex_;
};

class StreamWriterIntegrationTest : public IntegrationTestBase {
 public:
  using Node = IntegrationTestUtils::Node;
  using Cluster = IntegrationTestUtils::Cluster;
  using Context = BufferedWriter::AppendCallback::Context;

  // Initializes a cluster with the given configuration, creates a client and a
  // BufferedWriter that sends appends to StreamWriterAppendSink.
  void init(size_t num_nodes = 5,
            int replication_factor = 2,
            int node_set_size = 10,
            uint32_t num_logs = 32);

  // Reads back records and verifies if the payload and order are same as sent
  // over the stream according to the weak FIFO guarantee i.e. for all i, the
  // first occurrence of message m_i is before the first occurrence of message
  // m_{i+1}. Message m_i has a payload "i".
  void verifyRecords();

  // Obtains the sequencer in the cluster by posting a GET_SEQ_STATE request.
  Node& getSequencer() {
    int seq_idx = cluster->getHashAssignedSequencerNodeId(LOG_ID, client.get());
    ld_check_ne(-1, seq_idx);
    return cluster->getNode(seq_idx);
  }

  // Stalls append requests from being processed in any node by disallowing all
  // storage nodes as copysets. Node keeps trying again and again until we
  // explicitly unstall them.
  void stallRequests() {
    std::string all_nodes = "";
    bool first = true;
    for (auto& pr : cluster->getNodes()) {
      all_nodes += (first ? "" : ",");
      all_nodes += std::to_string(pr.first);
      first = false;
    }
    auto& nodes = cluster->getNodes();
    for (auto& it : nodes) {
      auto& node = it.second;
      if (node->isRunning()) {
        node->updateSetting("test-do-not-pick-in-copysets", all_nodes);
      }
    }
  }

  // Unstalls append requests. Now sequencer node can process append requests.
  void unstallRequests() {
    auto& nodes = cluster->getNodes();
    for (auto& it : nodes) {
      auto& node = it.second;
      if (node->isRunning()) {
        node->unsetSetting("test-do-not-pick-in-copysets");
      }
    }
  }

  // Sends messages m_{msgs_sent} ... m_{until-1} via the buffered stream
  // writer and flushes them periodically.
  void sendMessagesUntil(int until) {
    for (; msgs_sent < until; ++msgs_sent) {
      auto ctx = (Context) new int(msgs_sent);
      ASSERT_EQ(0, writer->append(LOG_ID, std::to_string(msgs_sent), ctx));
      if (msgs_sent && !(msgs_sent % num_messages_per_flush)) {
        ASSERT_EQ(0, writer->flushAll());
      }
    }
    writer->flushAll();
  }

  // Waits until all messages until m_{until-1} have been acked. Note that
  // messages until m_{msgs_rcvd-1} have already been ACKed.
  void waitForMessagesUntil(int until) {
    ld_check(until <= msgs_sent);
    for (; msgs_rcvd < until; ++msgs_rcvd) {
      callback.sem.wait();
    }
  }

  std::unique_ptr<Cluster> cluster;
  std::shared_ptr<Client> client;
  std::shared_ptr<Processor> processor;
  std::unique_ptr<ClientBridge> bridge;
  std::unique_ptr<StreamWriterAppendSink> stream_sink;
  std::unique_ptr<BufferedWriter> writer;
  FIFOTestCallback callback;
  const logid_t LOG_ID = logid_t(1);
  const int num_messages_per_flush = 5;
  int msgs_sent = 0;
  int msgs_rcvd = 0;
};

void StreamWriterIntegrationTest::init(size_t num_nodes,
                                       int replication_factor,
                                       int node_set_size,
                                       uint32_t num_logs) {
  Configuration::Nodes nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto& node = nodes[i];
    node.generation = 1;
    node.addSequencerRole();
    node.addStorageRole();
  }

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(replication_factor);
  log_attrs.set_nodeSetSize(node_set_size);

  cluster = IntegrationTestUtils::ClusterFactory()
                .setNodes(nodes)
                .setNumLogs(num_logs)
                .setLogAttributes(log_attrs)
                .useHashBasedSequencerAssignment(100, "10s")
                .enableMessageErrorInjection()
                .setLogGroupName("test_range")
                .setParam("--sequencer-reactivation-delay-secs", "1200s..2000s")
                .setParam("--nodeset-adjustment-period", "0s")
                .setParam("--nodeset-adjustment-min-window", "1s")
                .setParam("--nodeset-size-adjustment-min-factor", "0")
                .create(num_nodes);

  for (const auto& it : nodes) {
    node_index_t idx = it.first;
    cluster->waitUntilGossip(/* alive */ true, idx);
  }

  client = cluster->createClient(std::chrono::seconds(5));
  ClientImpl* client_impl = checked_downcast<ClientImpl*>(client.get());
  processor =
      std::static_pointer_cast<Processor>(client_impl->getProcessorPtr());
  bridge = std::make_unique<ClientBridgeImpl>(client_impl);
  stream_sink = std::make_unique<StreamWriterAppendSink>(
      client_impl->getProcessorPtr(),
      bridge.get(),
      client_impl->getTimeout(),
      chrono_expbackoff_t<std::chrono::milliseconds>(
          std::chrono::milliseconds(10), std::chrono::milliseconds(1000)));
  writer = BufferedWriter::create(client, stream_sink.get(), &callback);
}

void StreamWriterIntegrationTest::verifyRecords() {
  auto reader = client->createReader(1);
  lsn_t first_lsn = callback.lsn_range.first;
  lsn_t until_lsn = callback.lsn_range.second;
  int rv = reader->startReading(LOG_ID, first_lsn, until_lsn);
  ASSERT_EQ(0, rv);
  reader->setTimeout(std::chrono::milliseconds(100));

  // Read one by one to verify that isReading() stays true until we have
  // consumed all records.
  std::vector<std::string> received;
  while (reader->isReading(LOG_ID)) {
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

  int seq_num = -1;
  for (auto& data : received) {
    int data_seq_num = std::stoi(data);
    if (data_seq_num > seq_num) {
      ASSERT_EQ(data_seq_num, seq_num + 1);
      ++seq_num;
    }
  }
  ASSERT_EQ(seq_num, msgs_sent - 1);
  ASSERT_FALSE(reader->isReading(LOG_ID));
}

TEST_F(StreamWriterIntegrationTest, SimpleStreamTest) {
  init();

  // Send 100 messages over the stream and wait for the ACKs. Read it using a
  // reader and verify the payloads and order of messages.
  sendMessagesUntil(100);
  waitForMessagesUntil(100);
  verifyRecords();
}

TEST_F(StreamWriterIntegrationTest, SequencerFailure) {
  init();

  // Send some messages over the stream, wait for first ACK and then kill
  // sequencer. Now send some more records over the stream. Read and verify the
  // records back using a reader.
  auto& sequencer = getSequencer();
  sendMessagesUntil(25);
  waitForMessagesUntil(1);
  sequencer.kill();
  sendMessagesUntil(50);
  waitForMessagesUntil(50);
  verifyRecords();

  // Make sure that it is indeed a different sequencer.
  ASSERT_NE(lsn_to_epoch(callback.lsn_range.first),
            lsn_to_epoch(callback.lsn_range.second));
}

TEST_F(StreamWriterIntegrationTest, MultipleInflight) {
  init();

  // Send some messages to current sequencer and wait for ACKs.
  sendMessagesUntil(25);
  waitForMessagesUntil(25);

  // Now stall the sequencer so that multiple requests become inflight.
  stallRequests();
  sendMessagesUntil(50);

  // Here we check that no more ACKs since m25 have been received. This check is
  // indeed weak since we are checking that something did not happen at a
  // particular time, which does not preclude it from happening later. But, it
  // is strictly better than not checking.
  ASSERT_EQ(25, callback.next_seq_num);

  // Unstall the sequencer now. The inflight requests m26..m50 will be processed
  // now. Go ahead and wait for their ACKs now.
  unstallRequests();
  waitForMessagesUntil(50);

  // Send some more messages and wait for their ACKs.
  sendMessagesUntil(75);
  waitForMessagesUntil(75);

  // Read and verify the records we sent over the stream.
  verifyRecords();
}

TEST_F(StreamWriterIntegrationTest, InflightSequencerFailed) {
  init();

  // Send some messages to current sequencer and wait for ACKs.
  auto& sequencer = getSequencer();
  sendMessagesUntil(25);
  waitForMessagesUntil(25);

  // Stall the current sequencer and send some more requests. Once sent, kill
  // the sequencer. This simulates inflight requests that fail. The system must
  // identify next sequencer and append messages to log using that.
  stallRequests();
  sendMessagesUntil(50);
  sequencer.kill();
  unstallRequests();
  waitForMessagesUntil(50);

  // Make sure that it is indeed a different sequencer.
  ASSERT_NE(lsn_to_epoch(callback.lsn_range.first),
            lsn_to_epoch(callback.lsn_range.second));

  // Send some more messages over the stream.
  sendMessagesUntil(75);
  waitForMessagesUntil(75);

  verifyRecords();
}
