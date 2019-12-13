/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include <folly/SocketAddress.h>

#include "logdevice/ops/py_extensions/admin_command_client/AdminCommandClient.h"

namespace facebook { namespace logdevice { namespace test {

// Connects to the specified address, feeds some input and returns the output.
// Try it to send admin commands to logdeviced!
std::string
nc(const std::shared_ptr<AdminCommandClient>& client,
   const folly::SocketAddress& addr,
   const std::string& input,
   std::string* out_error,
   bool ssl = false,
   std::chrono::milliseconds command_timeout = std::chrono::milliseconds(10000),
   std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(5000));

}}} // namespace facebook::logdevice::test
