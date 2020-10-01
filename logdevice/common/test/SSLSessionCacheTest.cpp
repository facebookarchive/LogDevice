/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SSLSessionCache.h"

#include <folly/FileUtil.h>
#include <folly/ssl/SSLSession.h>
#include <gtest/gtest.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

TEST(SSLSessionCacheTest, setAndGetCachedSSLSession) {
  auto session = std::make_shared<folly::ssl::SSLSession>();

  SSLSessionCache cache;
  cache.setCachedSSLSession(session);

  auto result = cache.getCachedSSLSession();
  EXPECT_EQ(session, result);
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
      auto session = std::make_shared<folly::ssl::SSLSession>();
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
