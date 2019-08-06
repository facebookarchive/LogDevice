/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <sstream>

#include <gtest/gtest.h>

#include "logdevice/common/StreamWriterAppendSink.h"

/** @file Contains unit tests for StreamWriterAppendSink class. */

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
// TestCommand is transferred as a pointer in payload, so it must not be
// destroyed until it has been executed completely by
// TestStreamWriterAppendSink. Adding new TestCommand is easy: add
// TestCommandType, use as many args you want, handle them appropriately in
// processTestRequests() and postAppend() of TestStreamWriterAppendSink.
enum TestCommandType {
  DROP,   // Simulates a request drop.
  ACCEPT, // Accepts the request in a sequencer with 'epoch' (essentially
          // updates seen_epoch with epoch using updateSeenEpoch). Requests
          // received by a sequencer are stored in the MessageLog. Calls back
          // with E::OK status when processTestRequests in invoked.
  REJECT_ONCE, // Rejects the first request with arg1 status at sequencer with
               // epoch. Accepts second message blindly. All requests (including
               // rejected ones) received by a sequencer are stored in the
               // MessageLog.
};

struct TestCommand {
  TestCommandType type;
  // Key is the identity of each message that is sent over the stream.
  std::string key;
  // Epoch corresponding to the sequencer that should execute the command.
  epoch_t epoch;
  // Additional arguments that are required to execute the command.
  std::vector<std::string> args;
  static TestCommand create(TestCommandType type,
                            std::string key,
                            epoch_t epoch = EPOCH_INVALID) {
    return TestCommand(type, key, epoch);
  }
  TestCommand& addArg(std::string arg) {
    args.push_back(std::move(arg));
    return *this;
  }
  static TestCommand& parsePayload(Payload payload) {
    return *((TestCommand*)payload.data());
  }
  static Payload createPayload(TestCommand& command) {
    return Payload(&command, 64 /* dummy value */);
  }

 private:
  TestCommand(TestCommandType type_, std::string key_, epoch_t epoch_)
      : type(type_), key(key_), epoch(epoch_), args() {}
};

/* Intercepts all messages and stores them in memory. */
class TestStreamWriterAppendSink : public StreamWriterAppendSink {
 public:
  using Message = std::string;
  using MessageLogPerSequencer = std::unordered_map<Message, int>;
  using MessageLog =
      std::unordered_map<epoch_t, MessageLogPerSequencer, epoch_t::Hash>;
  using RequestsQueue = std::queue<std::unique_ptr<StreamAppendRequest>>;

  epoch_t seen_epoch;
  MessageLog message_log;
  RequestsQueue incoming_queue;

  TestStreamWriterAppendSink()
      : StreamWriterAppendSink(std::shared_ptr<Processor>(nullptr),
                               nullptr,
                               std::chrono::milliseconds(10000)),
        seen_epoch(EPOCH_INVALID) {}

  void updateSeenEpoch(epoch_t epoch) {
    if (epoch != EPOCH_INVALID && epoch > seen_epoch) {
      seen_epoch = epoch;
    }
  }
  void processTestRequests(bool repeat_until_empty = true) {
    int round = 0;
    while (!incoming_queue.empty() && repeat_until_empty) {
      size_t num_reqs_to_process = incoming_queue.size();
      ld_info("Round: %d", ++round);
      for (size_t i = 0; i < num_reqs_to_process; i++) {
        auto& req = incoming_queue.front();
        auto& cmd = TestCommand::parsePayload(req->record_.payload);
        switch (cmd.type) {
          case ACCEPT: {
            ld_check(message_log[cmd.epoch][cmd.key] == 1);
            updateSeenEpoch(cmd.epoch);
            ld_info("Accepting message %s at epoch %u",
                    cmd.key.c_str(),
                    cmd.epoch.val());
            req->callback_(E::OK, req->record_);
            break;
          }
          case REJECT_ONCE: {
            int count = message_log[cmd.epoch][cmd.key];
            ld_check(count > 0 && count <= 2);
            E error_status = E::UNKNOWN;
            if (count == 1) {
              error_status = (E)std::stoi(cmd.args[0]);
              ld_info("Rejecting message %s at epoch %u",
                      cmd.key.c_str(),
                      cmd.epoch.val());
            } else {
              error_status = E::OK;
              ld_info("Accepting message %s at epoch %u",
                      cmd.key.c_str(),
                      cmd.epoch.val());
            }
            updateSeenEpoch(cmd.epoch);
            req->callback_(error_status, req->record_);
            break;
          }
          default:
            ADD_FAILURE();
            break;
        }
        incoming_queue.pop();
      }
    }
  }

  write_stream_seq_num_t getMaxAckedSequenceNum(logid_t logid) {
    auto stream = getStream(logid);
    return stream->max_acked_seq_num_;
  }

  // Returns the seen_epoch stored in the stream for a particular logid
  // (Currently the same as write stream id.)
  epoch_t getSeenEpochForTest(logid_t logid) {
    return getStream(logid)->seen_epoch_;
  }

 protected:
  void postAppend(std::unique_ptr<StreamAppendRequest> req_append) override {
    req_append->setFailedToPost(); // disables callback in destructor.
    auto& cmd = TestCommand::parsePayload(req_append->record_.payload);
    if (cmd.type == DROP) {
      return;
    }

    // adds the request to incoming_queue and maintains count of number of
    // requests received for every message.
    auto& seq_msg_log = message_log[cmd.epoch];
    auto it = seq_msg_log.find(cmd.key);
    if (it == seq_msg_log.end()) {
      seq_msg_log.insert(std::make_pair(cmd.key, 1));
    } else {
      it->second += 1;
    }
    incoming_queue.push(std::move(req_append));
  }

  size_t getMaxPayloadSize() noexcept override {
    return MAX_PAYLOAD_SIZE_PUBLIC;
  }

  std::chrono::milliseconds getAppendRetryTimeout() noexcept override {
    return std::chrono::milliseconds(10000);
  }

  epoch_t getSeenEpoch(worker_id_t, logid_t) override {
    return seen_epoch;
  }

 private:
};

}} // namespace facebook::logdevice

class StreamWriterAppendSinkTest : public ::testing::Test {
 public:
  void SetUp() override {
    test_sink_ = std::make_unique<TestStreamWriterAppendSink>();
  }

 protected:
  std::unique_ptr<TestStreamWriterAppendSink> test_sink_;

  worker_id_t target_worker_ = (worker_id_t)0;

  void singleAppend(TestCommand& cmd);
  std::pair<Status, NodeID>
  appendHelper(logid_t logid,
               TestCommand& command,
               StreamWriterAppendSink::AppendRequestCallback callback);
  std::string toString(E error_status) {
    return std::to_string((int)error_status);
  }
};

std::pair<Status, NodeID> StreamWriterAppendSinkTest::appendHelper(
    logid_t logid,
    TestCommand& command,
    StreamWriterAppendSink::AppendRequestCallback callback) {
  return test_sink_->appendBuffered(
      logid,                                        /* not used */
      BufferedWriter::AppendCallback::ContextSet(), /* not used */
      AppendAttributes(),                           /* not used */
      TestCommand::createPayload(command),
      callback,
      target_worker_, /* not used */
      0 /* not used */);
}

void StreamWriterAppendSinkTest::singleAppend(TestCommand& cmd) {
  int num_msg_received = 0;
  // The callback is called only on E::OK ie. after the test harness has
  // accepted the request. For instance, when the TestCommandType is ACCEPT, it
  // is directly accepted, whereas when it is REJECT_ONCE, this callback is not
  // called when it is first rejected. It is called only after the request has
  // been accepted after it is retried.
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  appendHelper(logid, cmd, callback);
  test_sink_->processTestRequests();
  ASSERT_EQ(1, num_msg_received);
  ASSERT_EQ(1UL, test_sink_->getMaxAckedSequenceNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, AcceptRequest) {
  auto cmd = TestCommand::create(ACCEPT, "a");
  singleAppend(cmd);
}

TEST_F(StreamWriterAppendSinkTest, RejectRequest) {
  auto cmd =
      TestCommand::create(REJECT_ONCE, "a").addArg(toString(E::CONNFAILED));
  singleAppend(cmd);
}

TEST_F(StreamWriterAppendSinkTest, MultipleRequests) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(ACCEPT, "a"));
  cmds.push_back(TestCommand::create(ACCEPT, "b"));
  cmds.push_back(
      TestCommand::create(REJECT_ONCE, "c").addArg(toString(E::CONNFAILED)));
  cmds.push_back(
      TestCommand::create(REJECT_ONCE, "d").addArg(toString(E::CONNFAILED)));
  cmds.push_back(TestCommand::create(ACCEPT, "e"));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  test_sink_->processTestRequests();
  ASSERT_EQ(5, num_msg_received);
  ASSERT_EQ(5UL, test_sink_->getMaxAckedSequenceNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, MultipleLogs) {
  int num_msg_received_log1 = 0;
  logid_t logid1(1UL);
  auto callback1 = [&num_msg_received_log1](
                       Status status, const DataRecord& record, NodeID) {
    ASSERT_EQ(Status::OK, status);
    ASSERT_EQ(1UL, record.logid.val());
    num_msg_received_log1++;
  };
  std::vector<TestCommand> cmds1;
  cmds1.push_back(TestCommand::create(ACCEPT, "a"));
  cmds1.push_back(TestCommand::create(ACCEPT, "b"));
  cmds1.push_back(
      TestCommand::create(REJECT_ONCE, "c").addArg(toString(E::CONNFAILED)));
  for (auto& cmd : cmds1) {
    appendHelper(logid1, cmd, callback1);
  }

  int num_msg_received_log2 = 0;
  logid_t logid2(2UL);
  auto callback2 = [&num_msg_received_log2](
                       Status status, const DataRecord& record, NodeID) {
    ASSERT_EQ(Status::OK, status);
    ASSERT_EQ(2UL, record.logid.val());
    num_msg_received_log2++;
  };
  std::vector<TestCommand> cmds2;
  cmds2.push_back(
      TestCommand::create(REJECT_ONCE, "d").addArg(toString(E::CONNFAILED)));
  cmds2.push_back(TestCommand::create(ACCEPT, "e"));
  for (auto& cmd : cmds2) {
    appendHelper(logid2, cmd, callback2);
  }

  test_sink_->processTestRequests();
  ASSERT_EQ(3, num_msg_received_log1);
  ASSERT_EQ(2, num_msg_received_log2);
  ASSERT_EQ(3UL, test_sink_->getMaxAckedSequenceNum(logid1).val());
  ASSERT_EQ(2UL, test_sink_->getMaxAckedSequenceNum(logid2).val());
}

TEST_F(StreamWriterAppendSinkTest, SeenEpoch) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  test_sink_->seen_epoch.val_ = 123;
  auto cmd1 = TestCommand::create(ACCEPT, "a");
  appendHelper(logid, cmd1, callback);
  auto cmd2 = TestCommand::create(ACCEPT, "b");
  appendHelper(logid, cmd2, callback);
  test_sink_->processTestRequests();
  ASSERT_EQ(123U, test_sink_->getSeenEpochForTest(logid).val());

  // Test with a lower seen epoch value
  auto cmd3 = TestCommand::create(REJECT_ONCE, "c", epoch_t(100U))
                  .addArg(toString(E::CONNFAILED));
  appendHelper(logid, cmd3, callback);
  ASSERT_EQ(123U, test_sink_->getSeenEpochForTest(logid).val());
  test_sink_->processTestRequests();
  ASSERT_EQ(123U, test_sink_->getSeenEpochForTest(logid).val());

  // Test with a higher seen epoch value
  auto cmd4 = TestCommand::create(REJECT_ONCE, "d", epoch_t(151U))
                  .addArg(toString(E::CONNFAILED));
  appendHelper(logid, cmd4, callback);
  ASSERT_EQ(123U, test_sink_->getSeenEpochForTest(logid).val());
  test_sink_->processTestRequests();
  ASSERT_EQ(151U, test_sink_->getSeenEpochForTest(logid).val());

  ASSERT_EQ(4, num_msg_received);
  ASSERT_EQ(4UL, test_sink_->getMaxAckedSequenceNum(logid).val());
}
