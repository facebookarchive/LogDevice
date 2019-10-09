/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/nc.h"

#include "logdevice/common/debug.h"
#include "logdevice/ops/admin_command_client/AdminCommandClient.h"

namespace facebook { namespace logdevice { namespace test {

std::string nc(const std::shared_ptr<AdminCommandClient>& adminclient,
               const folly::SocketAddress& addr,
               const std::string& input,
               std::string* out_error,
               bool ssl,
               std::chrono::milliseconds command_timeout,
               std::chrono::milliseconds connect_timeout) {
  std::vector<AdminCommandClient::Request> rr;
  rr.emplace_back(addr,
                  input,
                  ssl ? AdminCommandClient::ConnectionType::ENCRYPTED
                      : AdminCommandClient::ConnectionType::PLAIN);

  auto response = adminclient->send(rr, command_timeout, connect_timeout);
  ld_check_eq(1, response.size());

  if (out_error && !response[0].success) {
    *out_error = response[0].failure_reason;
  }

  return response[0].success ? response[0].response : "";
}

}}} // namespace facebook::logdevice::test
