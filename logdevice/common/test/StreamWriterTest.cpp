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
/* Intercepts all messages and stores them in memory. */
class TestStreamWriterAppendSink : public StreamWriterAppendSink {
 public:
  TestStreamWriterAppendSink()
      : StreamWriterAppendSink(std::shared_ptr<Processor>(nullptr),
                               nullptr,
                               std::chrono::milliseconds(10000)) {}

  static Payload createPayload(std::string key, std::string op_type) {
    std::string* payload_string = new std::string();
    (*payload_string) += key;
    (*payload_string) += ":";
    (*payload_string) += op_type;
    return Payload((void*)payload_string->c_str(), payload_string->length());
  }

  static void parsePayload(Payload payload,
                           std::string& key,
                           std::string& op_type) {
    folly::split(':', payload.toString(), key, op_type);
  }

  void processTestRequests() {
    for (auto& kv : accept_requests) {
      auto& val = kv.second;
      val->callback_(E::OK, val->record_);
    }
    accept_requests.clear();

    // first collect all callbacks so that we do not violate iterator stability.
    // A reject callback should call appendBuffered again and it writes into
    // accept_requests -- doing this to avoid issues may be in future.
    std::vector<std::unique_ptr<StreamAppendRequest>> reject_requests_cache;
    for (auto& kv : reject_requests) {
      auto& val = kv.second;
      reject_requests_cache.push_back(std::move(val));
    }

    for (auto& val : reject_requests_cache) {
      val->callback_(E::OK, val->record_);
    }
    reject_requests.clear();

    // repeat until all accept and reject requests are empty.
    if (!accept_requests.empty() || !reject_requests.empty()) {
      processTestRequests();
    }
  }

  write_stream_seq_num_t getMaxAckedSequenceNum(logid_t logid) {
    auto stream = getStream(logid);
    return stream->max_acked_seq_num_;
  }

 protected:
  void postAppend(std::unique_ptr<StreamAppendRequest> req_append) override {
    req_append->setFailedToPost();
    std::string key, op_type;
    parsePayload(req_append->record_.payload, key, op_type);
    if (op_type == "ACCEPT") {
      accept_requests[key] = std::move(req_append);
    } else if (op_type == "REJECT_ONCE") {
      if (reject_requests.find(key) == reject_requests.end()) {
        reject_requests[key] = std::move(req_append);
      } else {
        accept_requests[key] = std::move(req_append);
      }
    } else {
      ADD_FAILURE();
    }
  }

  size_t getMaxPayloadSize() noexcept override {
    return MAX_PAYLOAD_SIZE_PUBLIC;
  }

  std::chrono::milliseconds getAppendRetryTimeout() noexcept override {
    return std::chrono::milliseconds(10000);
  }

 private:
  std::unordered_map<std::string, std::unique_ptr<StreamAppendRequest>>
      accept_requests;
  std::unordered_map<std::string, std::unique_ptr<StreamAppendRequest>>
      reject_requests;
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

  void singleAppend(std::string key,
                    std::string op_type,
                    Status expected_status,
                    int expected_num_msgs,
                    uint64_t expected_max_seq_num);
  std::pair<Status, NodeID>
  appendHelper(logid_t logid,
               std::string key,
               std::string op_type,
               StreamWriterAppendSink::AppendRequestCallback callback);
};

std::pair<Status, NodeID> StreamWriterAppendSinkTest::appendHelper(
    logid_t logid,
    std::string key,
    std::string op_type,
    StreamWriterAppendSink::AppendRequestCallback callback) {
  return test_sink_->appendBuffered(
      logid,                                        /* not used */
      BufferedWriter::AppendCallback::ContextSet(), /* not used */
      AppendAttributes(),                           /* not used */
      TestStreamWriterAppendSink::createPayload(key, op_type),
      callback,
      target_worker_, /* not used */
      0 /* not used */);
}

void StreamWriterAppendSinkTest::singleAppend(std::string key,
                                              std::string op_type,
                                              Status expected_status,
                                              int expected_num_msgs,
                                              uint64_t expected_max_seq_num) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  auto result = appendHelper(logid, key, op_type, callback);

  ASSERT_EQ(expected_status, result.first);
  test_sink_->processTestRequests();
  ASSERT_EQ(expected_num_msgs, num_msg_received);
  ASSERT_EQ(
      expected_max_seq_num, test_sink_->getMaxAckedSequenceNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, AcceptRequest) {
  singleAppend("a", "ACCEPT", Status::OK, 1, 1UL);
}

TEST_F(StreamWriterAppendSinkTest, RejectRequest) {
  singleAppend("a", "REJECT_ONCE", Status::OK, 1, 1UL);
}

TEST_F(StreamWriterAppendSinkTest, MultipleRequests) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  auto result = appendHelper(logid, "a", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "b", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "c", "REJECT_ONCE", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "d", "REJECT_ONCE", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "e", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);

  test_sink_->processTestRequests();
  ASSERT_EQ(5, num_msg_received);
  ASSERT_EQ(5UL, test_sink_->getMaxAckedSequenceNum(logid).val());
}

TEST_F(StreamWriterAppendSinkTest, MultipleLogs) {
  int num_msg_received_log1 = 0, num_msg_received_log2 = 0;
  logid_t logid1(1UL), logid2(2UL);
  auto callback =
      [logid1, logid2, &num_msg_received_log1, &num_msg_received_log2](
          Status status, const DataRecord& record, NodeID) {
        ASSERT_EQ(Status::OK, status);
        if (record.logid.val() == logid1.val()) {
          num_msg_received_log1++;
        } else if (record.logid.val() == logid2.val()) {
          num_msg_received_log2++;
        } else {
          ADD_FAILURE();
        }
      };

  auto result = appendHelper(logid1, "a", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid1, "b", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid1, "c", "REJECT_ONCE", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid2, "d", "REJECT_ONCE", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid2, "e", "ACCEPT", callback);
  ASSERT_EQ(Status::OK, result.first);

  test_sink_->processTestRequests();
  ASSERT_EQ(3, num_msg_received_log1);
  ASSERT_EQ(2, num_msg_received_log2);
  ASSERT_EQ(3UL, test_sink_->getMaxAckedSequenceNum(logid1).val());
  ASSERT_EQ(2UL, test_sink_->getMaxAckedSequenceNum(logid2).val());
}
