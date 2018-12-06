/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Client.h"

#include <chrono>

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"

using namespace facebook::logdevice;

class ClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // In order for writes to closed pipes to return EPIPE (instead of bringing
    // down the process), which we rely on to detect shutdown, ignore SIGPIPE.
    struct sigaction sa;
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPIPE, &sa, &oldact);
  }
  void TearDown() override {
    sigaction(SIGPIPE, &oldact, nullptr);
  }

 private:
  struct sigaction oldact {};
};

/**
 * We had a use-after-free during Client shutdown that was flagged by Valgrind
 * and gcc address sanitizer.
 */
TEST_F(ClientTest, ShutdownUseAfterFree) {
  // NOTE: assumes test is being run from top-level fbcode dir
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  std::shared_ptr<Client> client = ClientFactory().create(config_path);
  EXPECT_FALSE(client == nullptr);
}

TEST_F(ClientTest, Configuration) {
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  std::shared_ptr<Client> client = ClientFactory().create(config_path);

  auto range = client->getLogRangeByName("foo");
  EXPECT_EQ(logid_t(8), range.first);
  EXPECT_EQ(logid_t(10), range.second);

  range = client->getLogRangeByName("bar");
  EXPECT_EQ(LOGID_INVALID, range.first);
  EXPECT_EQ(LOGID_INVALID, range.second);
  EXPECT_EQ(E::NOTFOUND, err);

  folly::Baton<> baton;
  Status st;
  decltype(range) range2;
  client->getLogRangeByName(
      "foo", [&](Status err, std::pair<logid_t, logid_t> r) {
        st = err;
        range2 = r;
        baton.post();
      });
  baton.wait();
  EXPECT_EQ(E::OK, st);
  EXPECT_EQ(logid_t(8), range2.first);
  EXPECT_EQ(logid_t(10), range2.second);

  baton.reset();

  std::unique_ptr<ClusterAttributes> cfgattr = client->getClusterAttributes();
  EXPECT_NE(nullptr, cfgattr);
  EXPECT_EQ(cfgattr->getClusterName(), "sample_no_ssl");
  auto timestamp = cfgattr->getClusterCreationTime();
  EXPECT_EQ(timestamp.count(), 1467928224);
  auto customField = cfgattr->getCustomField("custom_field_for_testing");
  EXPECT_EQ(customField, "custom_value");
  auto nonExistentCustomField = cfgattr->getCustomField("non_existent");
  EXPECT_EQ(nonExistentCustomField, "");
}

TEST_F(ClientTest, OnDemandLogsConfigShutdown) {
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  std::shared_ptr<Client> client =
      ClientFactory()
          .setSetting("on-demand-logs-config", "true")
          .create(config_path);

  Semaphore sem;
  Status status = E::UNKNOWN;
  client->getLogRangeByName("foo", [&](Status st, std::pair<logid_t, logid_t>) {
    status = st;
    sem.post();
  });
  client.reset();
  sem.wait();
  ASSERT_EQ(E::FAILED, status);
}

TEST_F(ClientTest, nextFromLsnWhenStuck) {
  // Clients who know nothing should read from LSN_OLDEST.
  ASSERT_EQ(LSN_OLDEST, Reader::nextFromLsnWhenStuck(LSN_INVALID, LSN_INVALID));

  // Clients who do not know the tail LSN should read from the beginning of the
  // next epoch.
  ASSERT_EQ(compose_lsn(epoch_t(6), ESN_MIN),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(epoch_t(5), ESN_INVALID), LSN_INVALID));
  ASSERT_EQ(compose_lsn(epoch_t(6), ESN_MIN),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(epoch_t(5), ESN_MIN), LSN_INVALID));
  ASSERT_EQ(compose_lsn(epoch_t(6), ESN_MIN),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(epoch_t(5), esn_t(10)), LSN_INVALID));
  ASSERT_EQ(compose_lsn(epoch_t(6), ESN_MIN),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(epoch_t(5), ESN_MAX), LSN_INVALID));

  // Test overflow handling.
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MIN),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(EPOCH_MAX, ESN_MIN), LSN_INVALID));
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MAX),
            Reader::nextFromLsnWhenStuck(
                compose_lsn(EPOCH_MAX, ESN_MAX), LSN_INVALID));

  // Clients who know the tail LSN should read from the beginning of the tail
  // epoch. If they got stuck in the tail epoch, they should just keep reading
  // where they got stuck (never read twice).
  ASSERT_EQ(compose_lsn(epoch_t(7), ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(5), ESN_INVALID),
                                         compose_lsn(epoch_t(7), esn_t(20))));
  ASSERT_EQ(compose_lsn(epoch_t(7), ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(5), esn_t(10)),
                                         compose_lsn(epoch_t(7), esn_t(20))));
  ASSERT_EQ(compose_lsn(epoch_t(7), ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(5), ESN_MAX),
                                         compose_lsn(epoch_t(7), esn_t(20))));
  ASSERT_EQ(compose_lsn(epoch_t(7), ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(7), ESN_MIN),
                                         compose_lsn(epoch_t(7), esn_t(20))));
  ASSERT_EQ(compose_lsn(epoch_t(7), esn_t(10)),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(7), esn_t(10)),
                                         compose_lsn(epoch_t(7), esn_t(20))));
  ASSERT_EQ(compose_lsn(epoch_t(7), esn_t(10)),
            Reader::nextFromLsnWhenStuck(compose_lsn(epoch_t(7), esn_t(10)),
                                         compose_lsn(epoch_t(7), ESN_MAX)));

  // Test overflow handling.
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(EPOCH_MAX, ESN_MIN),
                                         compose_lsn(EPOCH_MAX, ESN_MIN)));
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(EPOCH_MAX, ESN_MIN),
                                         compose_lsn(EPOCH_MAX, esn_t(10))));
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MIN),
            Reader::nextFromLsnWhenStuck(compose_lsn(EPOCH_MAX, ESN_MIN),
                                         compose_lsn(EPOCH_MAX, ESN_MAX)));
  ASSERT_EQ(compose_lsn(EPOCH_MAX, esn_t(10)),
            Reader::nextFromLsnWhenStuck(compose_lsn(EPOCH_MAX, esn_t(10)),
                                         compose_lsn(EPOCH_MAX, ESN_MAX)));
  ASSERT_EQ(compose_lsn(EPOCH_MAX, ESN_MAX),
            Reader::nextFromLsnWhenStuck(compose_lsn(EPOCH_MAX, ESN_MAX),
                                         compose_lsn(EPOCH_MAX, ESN_MAX)));
}

TEST_F(ClientTest, clientEvents) {
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  std::shared_ptr<Client> client =
      ClientFactory()
          .setSetting("enable-logsconfig-manager", "false")
          .create(config_path);
  client->publishEvent(Severity::INFO,
                       "LD_CLIENT_TEST",
                       "TEST_EVENT",
                       "test_event_data",
                       "test_event_context");
}

// Test that:
// 1. dbg::abortOnFailedCheck is true by default.
// 2. Instantiating a client with default settings in opt build changes
//    dbg::abortOnFailedCheck to false.
// 3. Instantiating a client with "abort-on-failed-check" set to some value sets
//    dbg::abortOnFailedCheck to that value.
TEST_F(ClientTest, NoAbortOnFailedCheck) {
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  EXPECT_TRUE(dbg::abortOnFailedCheck.load());
  dbg::abortOnFailedCheck.store(!folly::kIsDebug);
  {
    auto client = ClientFactory().create(config_path);
    EXPECT_FALSE(client == nullptr);
    EXPECT_EQ(folly::kIsDebug, dbg::abortOnFailedCheck.load());
  }
  EXPECT_EQ(folly::kIsDebug, dbg::abortOnFailedCheck.load());
  {
    auto client = ClientFactory()
                      .setSetting("abort-on-failed-check",
                                  folly::kIsDebug ? "false" : "true")
                      .create(config_path);
    EXPECT_FALSE(client == nullptr);
    EXPECT_EQ(!folly::kIsDebug, dbg::abortOnFailedCheck.load());
  }
  EXPECT_EQ(!folly::kIsDebug, dbg::abortOnFailedCheck.load());
}

TEST_F(ClientTest, PayloadSizeLimitTest) {
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  std::shared_ptr<Client> client = ClientFactory().create(config_path);
  auto max_payload_size = client->getMaxPayloadSize();
  std::string payload_string(max_payload_size + 1, '1');
  Payload payload(payload_string.data(), payload_string.size());
  append_callback_t callback = [](Status, const DataRecord&) {};
  logid_t log_id(1);
  EXPECT_EQ(-1, client->append(log_id, payload, callback));
  EXPECT_EQ(E::TOOBIG, err);
  EXPECT_EQ(-1, client->append(log_id, std::move(payload_string), callback));
  EXPECT_EQ(E::TOOBIG, err);
  // Testing large payload which always fails
  payload_string.resize(128 * (1 << 20));
  payload = Payload(payload_string.data(), payload_string.size());
  EXPECT_EQ(-1, client->append(log_id, payload, callback));
  EXPECT_EQ(E::TOOBIG, err);
  EXPECT_EQ(-1, client->append(log_id, std::move(payload_string), callback));
  EXPECT_EQ(E::TOOBIG, err);
}
