/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Function.h>
#include <folly/Synchronized.h>
#include <folly/portability/OpenSSL.h>
#include <folly/ssl/OpenSSLPtrTypes.h>
#include <folly/ssl/SSLSession.h>

namespace facebook { namespace logdevice {

class StatsHolder;

/**
 * SSLSessionCache is a class for caching SSL sessions to be re-used for further
 * TLS session resumptions. Given that LogDevice's network stack only talks to
 * LD servers, we will be caching only a single session that's going to be used
 * by all the outgoing connections. The main assumption here is that SSL
 * sessions can be re-used accross multiple LD servers so make sure that servers
 * are configured with "use-tls-ticket-seeds = true".
 */
class SSLSessionCache {
 public:
  using TimeProvider =
      folly::Function<std::chrono::steady_clock::time_point() const>;

  explicit SSLSessionCache(StatsHolder* stats = nullptr) : stats_(stats) {}

  // Used by tests to mock system time
  explicit SSLSessionCache(TimeProvider time_provider,
                           StatsHolder* stats = nullptr)
      : stats_(stats), time_provider_(std::move(time_provider)) {}

  /**
   * Returns the cached SSL session, if no session is cached or we exceeded the
   * lifetime of the cached session this function will return a nullptr.
   */
  std::shared_ptr<folly::ssl::SSLSession> getCachedSSLSession() const;

  /**
   * Caches a new session if:
   *  1. The session is not nullptr.
   *  2. The session contains a TLS ticket.
   *  3. The session is different than the already cached session.
   */
  void setCachedSSLSession(std::shared_ptr<folly::ssl::SSLSession> session);

  /**
   * Called when session resumption succeeds mostly for bumping stats.
   */
  void onSessionResumptionSuccess() const;

 private:
  struct CachedSession {
    std::shared_ptr<folly::ssl::SSLSession> session;
  };

  StatsHolder* stats_{nullptr};
  const TimeProvider time_provider_ = []() {
    return std::chrono::steady_clock::now();
  };

  folly::Synchronized<CachedSession> cached_server_session_;
};

}} // namespace facebook::logdevice
