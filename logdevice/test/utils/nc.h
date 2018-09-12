/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/SocketAddress.h>
#include <string>

namespace facebook { namespace logdevice { namespace test {

// Connects to the specified address, feeds some input and returns the output.
// Try it to send admin commands to logdeviced!
std::string nc(const folly::SocketAddress& addr,
               const std::string& input,
               std::string* out_error,
               bool ssl = false);

}}} // namespace facebook::logdevice::test
