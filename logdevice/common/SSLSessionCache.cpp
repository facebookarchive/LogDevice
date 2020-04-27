/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/SSLSessionCache.h"

#include <folly/Range.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {
namespace {
folly::StringPiece id_from_session(const SSL_SESSION* session) {
  uint32_t session_id_len = 0;
  auto session_id_data = reinterpret_cast<const char*>(
      SSL_SESSION_get_id(session, &session_id_len));
  return folly::StringPiece(session_id_data, session_id_len);
}
}; // namespace

folly::ssl::SSLSessionUniquePtr SSLSessionCache::getCachedSSLSession() const {
  auto cached_session = cached_server_session_.rlock();

  auto session = cached_session->session.get();
  if (session == nullptr) {
    return nullptr;
  }
  ld_check(SSL_SESSION_has_ticket(session));

  if (SSL_SESSION_get_ticket_lifetime_hint(session) > 0) {
    auto now = time_provider_();
    auto secs_between = std::chrono::duration_cast<std::chrono::seconds>(
        now - cached_session->added_at);

    if (secs_between >=
        std::chrono::seconds(SSL_SESSION_get_ticket_lifetime_hint(session))) {
      return nullptr;
    }
  }
  STAT_INCR(stats_, ssl_session_resumption_attempt);
  return folly::ssl::SSLSessionUniquePtr(SSL_SESSION_dup(session));
}

void SSLSessionCache::setCachedSSLSession(
    folly::ssl::SSLSessionUniquePtr session) {
  if (session == nullptr) {
    return;
  }

  if (!SSL_SESSION_has_ticket(session.get())) {
    return;
  }

  auto session_id = id_from_session(session.get());

  auto cached_session_ulock = cached_server_session_.ulock();
  if (session_id == cached_session_ulock->session_id) {
    return;
  }

  auto cached_session = cached_session_ulock.moveFromUpgradeToWrite();

  cached_session->session = std::move(session);
  cached_session->session_id = session_id.str();
  cached_session->added_at = time_provider_();
  STAT_INCR(stats_, ssl_session_resumption_cached);
}

void SSLSessionCache::onSessionResumptionSuccess() const {
  STAT_INCR(stats_, ssl_session_resumption_success);
}

}} // namespace facebook::logdevice
