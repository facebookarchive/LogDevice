/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SSLSessionCache.h"

#include <folly/FileUtil.h>
#include <gtest/gtest.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

folly::ssl::SSLSessionUniquePtr getSessionFromFile(const std::string& path) {
  std::string data;
  folly::readFile(path.c_str(), data);
  auto data_ptr = reinterpret_cast<const unsigned char*>(data.c_str());
  auto session = d2i_SSL_SESSION(nullptr, &data_ptr, data.size());
  ld_check(session);
  return folly::ssl::SSLSessionUniquePtr(session);
}

std::string id_from_session(const SSL_SESSION* session) {
  uint32_t session_id_len = 0;
  auto session_id_data = SSL_SESSION_get_id(session, &session_id_len);
  return std::string((char*)session_id_data, session_id_len);
}

TEST(SSLSessionCacheTest, setAndGetCachedSSLSession) {
  auto session =
      getSessionFromFile(TEST_SSL_FILE("fake_openssl_session_with_ticket.der"));

  SSLSessionCache cache;
  cache.setCachedSSLSession(
      folly::ssl::SSLSessionUniquePtr(SSL_SESSION_dup(session.get())));

  auto result = cache.getCachedSSLSession();
  ASSERT_NE(nullptr, result);
  EXPECT_EQ(id_from_session(session.get()), id_from_session(result.get()));
}

TEST(SSLSessionCacheTest, setCachedSSLSessionNoTicket) {
  auto session = getSessionFromFile(TEST_SSL_FILE("fake_openssl_session.der"));

  // For the correctness of the test, make sure that the session doesn't have a
  // ticket.
  ASSERT_FALSE(SSL_SESSION_has_ticket(session.get()));

  SSLSessionCache cache;
  cache.setCachedSSLSession(
      folly::ssl::SSLSessionUniquePtr(SSL_SESSION_dup(session.get())));

  auto result = cache.getCachedSSLSession();
  ASSERT_EQ(nullptr, result);
}

TEST(SSLSessionCacheTest, getCachedSSSLSessionLifetimeExceeded) {
  auto session =
      getSessionFromFile(TEST_SSL_FILE("fake_openssl_session_with_ticket.der"));

  auto lifetime =
      std::chrono::seconds(SSL_SESSION_get_ticket_lifetime_hint(session.get()));

  auto current_time = std::chrono::steady_clock::now();
  auto time_provider = [&current_time]() { return current_time; };

  // Caching it will use time.now, so we should be able to still fetch it.
  SSLSessionCache cache(std::move(time_provider));
  cache.setCachedSSLSession(
      folly::ssl::SSLSessionUniquePtr(SSL_SESSION_dup(session.get())));
  EXPECT_NE(nullptr, cache.getCachedSSLSession());

  // Advance time to 10 seconds before the lifetime, should still be able to
  // fetch it.
  current_time += lifetime - std::chrono::seconds(10);
  EXPECT_NE(nullptr, cache.getCachedSSLSession());

  // Advance time to after the lifetime, should return nullptr.
  current_time += std::chrono::seconds(20);
  EXPECT_EQ(nullptr, cache.getCachedSSLSession());
}

TEST(SSLSessionCacheTest, onSessionResumptionSuccess) {
  auto stats = std::make_unique<StatsHolder>(StatsParams());

  SSLSessionCache cache(stats.get());
  cache.onSessionResumptionSuccess();
  cache.onSessionResumptionSuccess();
  cache.onSessionResumptionSuccess();

  auto results = stats->aggregate();
  EXPECT_EQ(3, results.ssl_session_resumption_success);
}

// A test that should fail in TSAN if thread safety is not respected.
TEST(SSLSessionCacheTest, threadSafety) {
  auto stats = std::make_unique<StatsHolder>(StatsParams());
  SSLSessionCache cache(stats.get());

  std::thread read([&]() {
    for (int i = 0; i < 1000; i++) {
      cache.getCachedSSLSession();
    }
  });
  std::thread write([&]() {
    for (int i = 0; i < 1000; i++) {
      auto session = getSessionFromFile(
          TEST_SSL_FILE("fake_openssl_session_with_ticket.der"));
      cache.setCachedSSLSession(std::move(session));
    }
  });
  std::thread report([&]() {
    for (int i = 0; i < 1000; i++) {
      cache.onSessionResumptionSuccess();
    }
  });
  read.join();
  write.join();
  report.join();
}
