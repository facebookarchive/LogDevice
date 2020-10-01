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

std::shared_ptr<folly::ssl::SSLSession>
SSLSessionCache::getCachedSSLSession() const {
  auto cached_session = cached_server_session_.rlock();
  auto session = cached_session->session;

  if (session) {
    STAT_INCR(stats_, ssl_session_resumption_attempt);
  }
  return session;
}

void SSLSessionCache::setCachedSSLSession(
    std::shared_ptr<folly::ssl::SSLSession> session) {
  if (!session) {
    return;
  }

  auto cached_session_ulock = cached_server_session_.ulock();
  if (session == cached_session_ulock->session) {
    return;
  }

  auto cached_session = cached_session_ulock.moveFromUpgradeToWrite();
  cached_session->session = std::move(session);
  STAT_INCR(stats_, ssl_session_resumption_cached);
}

void SSLSessionCache::onSessionResumptionSuccess() const {
  STAT_INCR(stats_, ssl_session_resumption_success);
}

}} // namespace facebook::logdevice
