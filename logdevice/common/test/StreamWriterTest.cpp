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
#include "logdevice/common/test/MockBackoffTimer.h"

/** @file Contains unit tests for StreamWriterAppendSink class. */

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
// TestCommand is transferred as a pointer in payload, so it must not be
// destroyed until it has been executed completely by
// TestStreamWriterAppendSink. Adding new TestCommand is easy: add
// TestCommandType, use as many args you want, handle them appropriately in
// processTestRequests() and postAppend() of TestStreamWriterAppendSink.
enum TestCommandType {
  DROP,   // Simulates a request drop. Changes the test command type to
          // arg1 before calling back, if it exists.
  ACCEPT, // Accepts the request in a sequencer with 'epoch' (essentially
          // updates seen_epoch with epoch using updateSeenEpoch). Requests
          // received by a sequencer are stored in the MessageLog. Calls back
          // with E::OK status when processTestRequests in invoked.
  REJECT_ONCE, // Rejects the first request with arg1 status at sequencer with
               // epoch. Accepts second message blindly. All requests (including
               // rejected ones) received by a sequencer are stored in the
               // MessageLog.
  TEST_SEQUENCER, // Simulates a test sequencer that implements the stream
                  // ordering logic. Requests are accepted or rejected based on
                  // whether their sequencer numbers.
  IGNORE // Don't callback. Add back to the queue to process in a future round.
         // Be careful since this can lead to infinite loops if called with
         // processTestRequests(true). Change the command type after 1st round,
         // currently have an ld_check to ensure that.
};

struct TestCommand {
  TestCommandType type;
  // Key is the identity of each message that is sent over the stream.
  std::string key;
  // Epoch corresponding to the sequencer that should execute the command.
  epoch_t epoch;
  // Additional arguments that are required to execute the command.
  std::vector<std::string> args;

  // Payload of the record. Equal to the 8-byte pointer to this TestCommand.
  std::string payload;

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
    return **((TestCommand**)payload.data());
  }
  static PayloadHolder createPayload(TestCommand& command) {
    TestCommand* p = &command;
    return PayloadHolder::copyBuffer(&p, sizeof(void*));
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
  // Each sequencer maintains the latest accepted stream sequence number for
  // each write stream.
  using StreamStatePerSequencer = std::unordered_map<write_stream_id_t,
                                                     write_stream_seq_num_t,
                                                     write_stream_id_t::Hash>;
  using StreamState =
      std::unordered_map<epoch_t, StreamStatePerSequencer, epoch_t::Hash>;

  epoch_t seen_epoch = EPOCH_INVALID;
  MessageLog message_log;
  RequestsQueue incoming_queue;
  StreamState stream_state;
  std::unordered_set<StreamAppendRequest*> cancelled;

  TestStreamWriterAppendSink()
      : StreamWriterAppendSink(std::shared_ptr<Processor>(nullptr),
                               nullptr,
                               std::chrono::milliseconds(10000),
                               chrono_expbackoff_t<std::chrono::milliseconds>(
                                   std::chrono::milliseconds(0),
                                   std::chrono::milliseconds(0))) {}

  void updateSeenEpoch(epoch_t epoch) {
    if (epoch != EPOCH_INVALID && epoch > seen_epoch) {
      seen_epoch = epoch;
    }
  }
  void processTestRequests(bool repeat_until_empty = true) {
    int round = 0;
    do {
      ld_info("Round: %d", ++round);
      size_t num_reqs_to_process = incoming_queue.size();
      for (size_t i = 0; i < num_reqs_to_process; i++) {
        auto& req = incoming_queue.front();
        auto& cmd = TestCommand::parsePayload(req->record_.payload);
        // Check if request was cancelled.
        auto it = cancelled.find(req.get());
        if (it != cancelled.end()) {
          ld_info("Message %s cancelled", cmd.key.c_str());
          cancelled.erase(it);
          incoming_queue.pop();
          continue;
        }

        switch (cmd.type) {
          case IGNORE: {
            ld_check(round == 1);
            ld_info("Message %s ignored in first round", cmd.key.c_str());
            incoming_queue.push(std::move(req));
            break;
          }
          case DROP: {
            ld_info("Dropping message %s", cmd.key.c_str());
            if (cmd.args.size() > 0) {
              cmd.type = (TestCommandType)std::stoi(cmd.args[0]);
            }
            req->callback_(E::TIMEDOUT, req->record_);
            break;
          }
          case ACCEPT: {
            ld_info("Epoch %u: Accepting message %s",
                    cmd.epoch.val(),
                    cmd.key.c_str());
            updateSeenEpoch(cmd.epoch);
            req->record_.attrs.lsn = getLsnAtEpoch(cmd.epoch);
            req->callback_(E::OK, req->record_);
            break;
          }
          case REJECT_ONCE: {
            int count = message_log[cmd.epoch][cmd.key];
            ld_check(count > 0 && count <= 2);
            E error_status = E::UNKNOWN;
            if (count == 1) {
              ld_info("Epoch %u: Rejecting message %s",
                      cmd.epoch.val(),
                      cmd.key.c_str());
              error_status = (E)std::stoi(cmd.args[0]);
            } else {
              ld_info("Epoch %u: Accepting message %s",
                      cmd.epoch.val(),
                      cmd.key.c_str());
              error_status = E::OK;
              req->record_.attrs.lsn = getLsnAtEpoch(cmd.epoch);
            }
            updateSeenEpoch(cmd.epoch);
            req->callback_(error_status, req->record_);
            break;
          }
          case TEST_SEQUENCER: {
            auto& streams_map = stream_state[cmd.epoch];
            auto it = streams_map.find(req->stream_rqid_.id);
            E error_status = E::UNKNOWN;
            if (req->stream_resume_) {
              if (it == streams_map.end()) {
                streams_map.insert(std::make_pair(
                    req->stream_rqid_.id, req->stream_rqid_.seq_num));
              } else {
                it->second = req->stream_rqid_.seq_num;
              }
              req->record_.attrs.lsn = getLsnAtEpoch(cmd.epoch);
              error_status = E::OK;
              ld_info("Epoch %u: Resuming write stream %lu at seq# %lu",
                      cmd.epoch.val(),
                      req->stream_rqid_.id.val(),
                      req->stream_rqid_.seq_num.val());
            } else {
              if (it == streams_map.end()) {
                error_status = E::WRITE_STREAM_UNKNOWN;
                ld_info("Epoch %u: Unknown stream %lu",
                        cmd.epoch.val(),
                        req->stream_rqid_.id.val());
              } else if (req->stream_rqid_.seq_num <=
                         next_seq_num(it->second)) {
                increment_seq_num(it->second);
                req->record_.attrs.lsn = getLsnAtEpoch(cmd.epoch);
                error_status = E::OK;
                ld_info("Epoch %u: Accepting message %s with seq# %lu",
                        cmd.epoch.val(),
                        cmd.key.c_str(),
                        req->stream_rqid_.seq_num.val());
              } else {
                error_status = E::WRITE_STREAM_BROKEN;
                ld_info("Epoch %u: Write stream broken, rejecting message %s "
                        "with seq# %lu",
                        cmd.epoch.val(),
                        cmd.key.c_str(),
                        req->stream_rqid_.seq_num.val());
              }
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
    } while (!incoming_queue.empty() && repeat_until_empty);
  }

  write_stream_seq_num_t getMaxPrefixAckedSeqNum(logid_t logid) {
    auto stream = getStream(logid);
    return stream->max_prefix_acked_seq_num_;
  }
  write_stream_seq_num_t getMaxAckedSeqNum(logid_t logid) {
    auto stream = getStream(logid);
    return stream->max_acked_seq_num_;
  }

  // Returns the seen_epoch stored in the stream for a particular logid
  // (Currently the same as write stream id.)
  epoch_t getSeenEpochForTest(logid_t logid) {
    return getStream(logid)->seen_epoch_;
  }

 protected:
  void postAppend(Stream& stream,
                  StreamAppendRequestState& req_state) override {
    auto req_append = createAppendRequest(stream, req_state);
    ld_check_ne(stream.target_worker_, WORKER_ID_INVALID);
    req_append->setTargetWorker(stream.target_worker_);
    req_append->setBufferedWriterBlobFlag();
    // Store pointer to request in inflight_request before posting.
    ld_check(!req_state.inflight_request);
    req_state.inflight_request = req_append.get();
    doPostAppend(std::move(req_append));
  }

  std::unique_ptr<BackoffTimer> createBackoffTimer(Stream& stream) override {
    auto wrapped_callback = [this, &stream]() {
      postNextReadyRequestsIfExists(stream, 1);
    };
    std::unique_ptr<BackoffTimer> retry_timer =
        std::make_unique<MockBackoffTimer>(true);
    retry_timer->setCallback(wrapped_callback);
    return retry_timer;
  }

  void doPostAppend(std::unique_ptr<StreamAppendRequest> req_append) {
    req_append->setFailedToPost(); // disables callback in destructor.
    auto& cmd = TestCommand::parsePayload(req_append->record_.payload);

    // Add all requests to incoming_queue and add message to a sequencer only
    // if it is not dropped.
    if (cmd.type != DROP) {
      auto& seq_msg_log = message_log[cmd.epoch];
      auto it = seq_msg_log.find(cmd.key);
      if (it == seq_msg_log.end()) {
        seq_msg_log.insert(std::make_pair(cmd.key, 1));
      } else {
        it->second += 1;
      }
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

  void resetPreviousAttempts(StreamAppendRequestState& req_state) override {
    if (req_state.inflight_request) {
      cancelled.insert(req_state.inflight_request);
      req_state.inflight_request = nullptr;
      req_state.last_status = E::UNKNOWN;
    } else {
      req_state.last_status = E::UNKNOWN;
      // we are not clearing req_state.record_attrs, but it is fine!
    }
  }

 private:
  esn_t last_assigned_esn = ESN_MIN;
  lsn_t getLsnAtEpoch(epoch_t epoch) {
    // We allot LSNs such that ESNs are always increasing since some tests
    // require LSN monotonicity, hopefully the test sends the same cmd.epoch.
    auto lsn = compose_lsn(epoch, last_assigned_esn);
    ++last_assigned_esn.val_;
    return lsn;
  }
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
  std::string toString(TestCommandType type) {
    return std::to_string((int)type);
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
  ASSERT_EQ(1UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
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
  ASSERT_EQ(5UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
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
  ASSERT_EQ(3UL, test_sink_->getMaxPrefixAckedSeqNum(logid1).val());
  ASSERT_EQ(2UL, test_sink_->getMaxPrefixAckedSeqNum(logid2).val());
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
  ASSERT_EQ(4UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, MockSequencerCorrectStream) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "a"));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "b"));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "c"));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "d"));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "e"));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  test_sink_->processTestRequests();
  ASSERT_EQ(5, num_msg_received);
  ASSERT_EQ(5UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, MockSequencerMessageDrop) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "a", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "b", epoch));
  cmds.push_back(
      TestCommand::create(DROP, "c", epoch).addArg(toString(TEST_SEQUENCER)));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "d", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "e", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }
  test_sink_->processTestRequests();
  ASSERT_EQ(5, num_msg_received);
  ASSERT_EQ(5UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(1, test_sink_->stream_state[epoch].size());
  auto it = test_sink_->stream_state[epoch].begin();
  ASSERT_EQ(5UL, test_sink_->stream_state[epoch][it->first].val());
}

// In this test, we specifically test whether the ACKs from stream write  append
// sink occur in the input order. The first set of messages will cause A, B and
// E to be accepted (note this is not TEST_SEQUENCER). However, E should not be
// ACKed before C and D. Only after second round of processTestRequests will C
// and D be accepted. Checking if the callback order is A, B, C, D, E tests if
// the ACK for E waits until C and D are also ACKed.
TEST_F(StreamWriterAppendSinkTest, PrefixAcks) {
  int num_msg_received = 0;
  std::queue<std::string> expected_order;
  expected_order.push("a");
  expected_order.push("b");
  expected_order.push("c");
  expected_order.push("d");
  expected_order.push("e");
  auto callback = [&num_msg_received, &expected_order](
                      Status status, const DataRecord& record, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
    TestCommand cmd = TestCommand::parsePayload(record.payload);
    ASSERT_EQ(cmd.key, expected_order.front());
    expected_order.pop();
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
  ASSERT_EQ(5UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, SmartRetry) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "a", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "b", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "c", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "d", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "e", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "f", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "g", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  test_sink_->processTestRequests(false);
  ASSERT_EQ(7, num_msg_received);
  ASSERT_EQ(7UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, SmartRetryTimeout) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "a", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "b", epoch));
  cmds.push_back(
      TestCommand::create(DROP, "c", epoch).addArg(toString(TEST_SEQUENCER)));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "d", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "e", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "f", epoch));
  cmds.push_back(TestCommand::create(TEST_SEQUENCER, "g", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  // "a" and "b" would be ACKed, everything else will fail. "c" would be sent.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(2, num_msg_received);
  ASSERT_EQ(2UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  // "c" would be ACKed, "d" and "e" would be sent.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(3, num_msg_received);
  ASSERT_EQ(3UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  // "d", "e" would be ACKed. "f" and "g" would be sent.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(5, num_msg_received);
  ASSERT_EQ(5UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  // "f" and "g" will be ACKed.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(7, num_msg_received);
  ASSERT_EQ(7UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, SmartRetry_CheckMonotonicityFail) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  epoch_t larger_epoch(2U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(ACCEPT, "a", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "b", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "c", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "d", larger_epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "e", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "f", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "g", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  // "a", "b", "c", "d" will be ACKed since monotonic property. "e" will violate
  // monotonic property. So, "e" ACK is not accepted, We rewind stream until "d"
  // i.e. "e" ACK is ignored, "f" which is inflight is forgotten. Now, we send
  // "e" and "f" again.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(4, num_msg_received);
  ASSERT_EQ(4UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());

  // Changing epoch value so that when processing it returns a monotonic LSN.
  cmds[4].epoch = larger_epoch;
  cmds[5].epoch = larger_epoch;
  cmds[6].epoch = larger_epoch;

  // "e" and "f" would be ACKed, "g" is sent.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(6, num_msg_received);
  ASSERT_EQ(6UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());

  // "g" is ACKed.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(7, num_msg_received);
  ASSERT_EQ(7UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest,
       SmartRetry_EnsureMonotonicityFail_PrefixAck) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  epoch_t larger_epoch(2U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(ACCEPT, "a", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "b", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "c", epoch));
  cmds.push_back(TestCommand::create(IGNORE, "d", larger_epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "e", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "f", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "g", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  // "a", "b", "c", "e", "f", "g" are all ACKed. All in epoch 1.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(3, num_msg_received);
  ASSERT_EQ(3UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(7UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // "d" will be ACKed with epoch 2. stream will be rewound until "e".
  // "e" and "f" are posted again since "d" was accepted to prefix!
  cmds[3].type = ACCEPT;
  test_sink_->processTestRequests(false);
  ASSERT_EQ(4UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(4UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // Changing epoch value so that when processing it returns a monotonic LSN.
  cmds[4].epoch = larger_epoch;
  cmds[5].epoch = larger_epoch;
  cmds[6].epoch = larger_epoch;

  // "e" and "f" are ACKed. "g" is posted.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(6, num_msg_received);
  ASSERT_EQ(6UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(6UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // "g" is ACKed.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(7, num_msg_received);
  ASSERT_EQ(7UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest,
       SmartRetry_EnsureMonotonicityFail_NonPrefixAck) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  epoch_t epoch(1U);
  epoch_t larger_epoch(2U);
  std::vector<TestCommand> cmds;
  cmds.push_back(TestCommand::create(ACCEPT, "a", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "b", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "c", epoch));
  cmds.push_back(TestCommand::create(IGNORE, "d", larger_epoch));
  cmds.push_back(TestCommand::create(IGNORE, "e", larger_epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "f", epoch));
  cmds.push_back(TestCommand::create(ACCEPT, "g", epoch));
  for (auto& cmd : cmds) {
    appendHelper(logid, cmd, callback);
  }

  // "a", "b", "c", "f", "g" are all ACKed. All in epoch 1.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(3, num_msg_received);
  ASSERT_EQ(3UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(7UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // "e" will be ACKed with epoch 2. stream will be rewound until "e". nothing
  // is posted, but "d" is in flight.
  cmds[4].type = ACCEPT;
  test_sink_->processTestRequests(false);
  ASSERT_EQ(3UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(5UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // Changing epoch value so that when processing it returns a monotonic LSN.
  cmds[3].type = ACCEPT;

  // "d" is ACKed with LSN at epoch 2, but a larger ESN. So, wound back and post
  // "e" and "f".
  test_sink_->processTestRequests(false);
  ASSERT_EQ(4, num_msg_received);
  ASSERT_EQ(4UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(4UL, test_sink_->getMaxAckedSeqNum(logid).val());

  // "f" will also get an LSN in epoch 2 now.
  cmds[5].epoch = larger_epoch;

  // "e" and "f" are ACKed. "g" is posted.
  test_sink_->processTestRequests(false);
  ASSERT_EQ(6, num_msg_received);
  ASSERT_EQ(6UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(6UL, test_sink_->getMaxAckedSeqNum(logid).val());

  cmds[6].epoch = larger_epoch;
  test_sink_->processTestRequests(false);
  ASSERT_EQ(7, num_msg_received);
  ASSERT_EQ(7UL, test_sink_->getMaxPrefixAckedSeqNum(logid).val());
  ASSERT_EQ(7UL, test_sink_->getMaxAckedSeqNum(logid).val());
}
