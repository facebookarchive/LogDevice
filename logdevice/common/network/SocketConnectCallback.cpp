/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/SocketConnectCallback.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/network/SocketAdapter.h"

namespace facebook { namespace logdevice {

void SocketConnectCallback::connectSuccess() noexcept {
  connect_status_.setValue(
      folly::AsyncSocketException(folly::AsyncSocketException::ALREADY_OPEN,
                                  "Connected to peer successfully."));
}

void SocketConnectCallback::connectErr(
    const folly::AsyncSocketException& ex) noexcept {
  // Make sure the retry does not lead to an error.
  ld_check(!connect_status_.isFulfilled());
  connect_status_.setValue(ex);
}
}} // namespace facebook::logdevice
