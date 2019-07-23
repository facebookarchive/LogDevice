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

namespace {
/* Intercepts all messages and stores them in memory. */
class TestAppendSink : public BufferedWriterAppendSink {
 public:
  struct TestAppendBufferedRequest {
    logid_t logid;
    Payload payload;
    AppendRequestCallback callback;
  };
  using RequestsSet =
      std::unordered_map<std::string, TestAppendBufferedRequest>;

  bool checkAppend(logid_t, size_t, bool) override {
    return true;
  }

  Status canSendToWorker() override {
    return E::OK;
  }

  void onBytesSentToWorker(ssize_t) override {}

  void onBytesFreedByWorker(size_t) override {}

  std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet&,
                 AppendAttributes,
                 const Payload& payload,
                 AppendRequestCallback callback,
                 worker_id_t,
                 int) override {
    std::string test_option, data;
    folly::split(':', payload.toString(), test_option, data);

    if (test_option == "DO_NOT_ACCEPT") {
      return std::make_pair(E::CONNFAILED, NodeID());
    }

    if (test_option == "ACCEPT") {
      accept_requests[data] = {logid, payload, callback};
    } else if (test_option == "REJECT_ONCE") {
      if (reject_requests.count(data) == 0) {
        reject_requests[data] = {logid, payload, callback};
      } else {
        accept_requests[data] = {logid, payload, callback};
      }
    } else {
      ADD_FAILURE();
    }
    return std::make_pair(E::OK, NodeID());
  }

  void processTestRequests() {
    for (auto& keyValue : accept_requests) {
      TestAppendBufferedRequest& request = keyValue.second;
      DataRecord record(request.logid, request.payload, current_lsn++);
      request.callback(E::OK, record, NodeID());
    }
    accept_requests.clear();

    // first collect all callbacks so that we do not violate iterator stability.
    // A reject callback should call appendBuffered again and it writes into
    // accept_requests -- doing this to avoid issues may be in future.
    std::vector<TestAppendBufferedRequest> reject_requests_cache;
    for (auto& keyValue : reject_requests) {
      TestAppendBufferedRequest& request = keyValue.second;
      reject_requests_cache.push_back(request);
    }

    for (auto& request : reject_requests_cache) {
      DataRecord record(request.logid, request.payload, LSN_INVALID);
      request.callback(E::CONNFAILED, record, NodeID());
    }
    reject_requests.clear();

    // repeat until all accept and reject requests are empty.
    if (!accept_requests.empty() || !reject_requests.empty()) {
      processTestRequests();
    }
  }

 private:
  RequestsSet accept_requests;
  RequestsSet reject_requests;
  lsn_t current_lsn = 0;
};

} // namespace

class StreamWriterAppendSinkTest : public ::testing::Test {
 public:
  void SetUp() override {
    test_sink_ = std::make_unique<TestAppendSink>();
    stream_sink_ = std::make_unique<StreamWriterAppendSink>(test_sink_.get());
  }
  void TearDown() override {
    stream_sink_.reset();
    test_sink_.reset();
  }

 protected:
  std::unique_ptr<TestAppendSink> test_sink_;
  std::unique_ptr<StreamWriterAppendSink> stream_sink_;

  worker_id_t target_worker_ = (worker_id_t)0;

  void singleAppend(std::string payload_string,
                    Status expected_status,
                    int expected_num_msgs);
  std::pair<Status, NodeID>
  appendHelper(logid_t logid,
               std::string payload_string,
               StreamWriterAppendSink::AppendRequestCallback callback);
};

std::pair<Status, NodeID> StreamWriterAppendSinkTest::appendHelper(
    logid_t logid,
    std::string payload_string,
    StreamWriterAppendSink::AppendRequestCallback callback) {
  // create payload
  char* payload_c_string = new char[payload_string.length() + 1];
  strncpy(payload_c_string, payload_string.c_str(), payload_string.length());
  payload_c_string[payload_string.length()] = '\0';
  Payload* payload =
      new Payload((void*)payload_c_string, payload_string.length() + 1);

  // issue appendBuffered call
  return stream_sink_->appendBuffered(
      logid,                                        /* not used */
      BufferedWriter::AppendCallback::ContextSet(), /* not used */
      AppendAttributes(),                           /* not used */
      *payload,
      callback,
      target_worker_, /* not used */
      0 /* not used */);
}

void StreamWriterAppendSinkTest::singleAppend(std::string payload_string,
                                              Status expected_status,
                                              int expected_num_msgs) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  auto result = appendHelper(logid, payload_string, callback);

  ASSERT_EQ(expected_status, result.first);
  test_sink_->processTestRequests();
  ASSERT_EQ(expected_num_msgs, num_msg_received);
}

TEST_F(StreamWriterAppendSinkTest, DoNotAcceptRequest) {
  singleAppend("DO_NOT_ACCEPT:a", Status::CONNFAILED, 0);
}

TEST_F(StreamWriterAppendSinkTest, AcceptRequest) {
  singleAppend("ACCEPT:a", Status::OK, 1);
}

TEST_F(StreamWriterAppendSinkTest, RejectRequest) {
  singleAppend("REJECT_ONCE:a", Status::OK, 1);
}

TEST_F(StreamWriterAppendSinkTest, MultipleRequests) {
  int num_msg_received = 0;
  auto callback = [&num_msg_received](
                      Status status, const DataRecord&, NodeID) {
    ASSERT_EQ(Status::OK, status);
    num_msg_received++;
  };

  logid_t logid(1UL);
  auto result = appendHelper(logid, "ACCEPT:a", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "DO_NOT_ACCEPT:b", callback);
  ASSERT_EQ(Status::CONNFAILED, result.first);
  result = appendHelper(logid, "REJECT_ONCE:c", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "REJECT_ONCE:b", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid, "ACCEPT:d", callback);
  ASSERT_EQ(Status::OK, result.first);

  test_sink_->processTestRequests();
  ASSERT_EQ(4, num_msg_received);
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

  auto result = appendHelper(logid1, "ACCEPT:a", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid1, "DO_NOT_ACCEPT:b", callback);
  ASSERT_EQ(Status::CONNFAILED, result.first);
  result = appendHelper(logid1, "REJECT_ONCE:c", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid2, "REJECT_ONCE:b", callback);
  ASSERT_EQ(Status::OK, result.first);
  result = appendHelper(logid2, "ACCEPT:d", callback);
  ASSERT_EQ(Status::OK, result.first);

  test_sink_->processTestRequests();
  ASSERT_EQ(2, num_msg_received_log1);
  ASSERT_EQ(2, num_msg_received_log2);
}
