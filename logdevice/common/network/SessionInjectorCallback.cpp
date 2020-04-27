/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/SessionInjectorCallback.h"

#include <folly/io/async/AsyncSSLSocket.h>

#include "logdevice/common/SSLSessionCache.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/network/AsyncSocketAdapter.h"

namespace facebook { namespace logdevice {

SessionInjectorCallback::SessionInjectorCallback(
    std::unique_ptr<folly::AsyncSocket::ConnectCallback> cb,
    SSLSessionCache* session_cache,
    SocketAdapter* socket)
    : callback_{std::move(cb)},
      ssl_session_cache_(session_cache),
      socket_(socket),
      found_session_(false) {
  ld_check(socket_);
}

void SessionInjectorCallback::connectSuccess() noexcept {
  if (socket_->getSSLSessionReused() && found_session_) {
    ssl_session_cache_->onSessionResumptionSuccess();
  }
  ssl_session_cache_->setCachedSSLSession(socket_->getSSLSession());
  callback_->connectSuccess();
}

void SessionInjectorCallback::connectErr(
    const folly::AsyncSocketException& ex) noexcept {
  callback_->connectErr(ex);
}

void SessionInjectorCallback::preConnect(folly::NetworkSocket fd) {
  auto session = ssl_session_cache_->getCachedSSLSession();
  if (session) {
    socket_->setSSLSession(std::move(session));
    found_session_ = true;
  }
  callback_->preConnect(fd);
}

}} // namespace facebook::logdevice
