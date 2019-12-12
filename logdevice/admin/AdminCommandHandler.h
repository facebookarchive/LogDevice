/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/Function.h>
#include <folly/SocketAddress.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

namespace facebook { namespace logdevice {

/**
 * A function that accepts the admin command string as a string and the source
 * address and returns the response string.
 */
using AdminCommandHandler =
    folly::Function<folly::SemiFuture<std::unique_ptr<folly::IOBuf>>(
        const std::string& /* request */,
        const folly::SocketAddress& /* source address */) const>;

}} // namespace facebook::logdevice
