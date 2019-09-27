/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/nc.h"

#include "logdevice/ops/admin_command_client/AdminCommandClient.h"

namespace facebook { namespace logdevice { namespace test {

std::string nc(const std::shared_ptr<AdminCommandClient>& adminclient,
               const folly::SocketAddress& addr,
               const std::string& input,
               std::string* out_error,
               bool ssl,
               std::chrono::milliseconds command_timeout,
               std::chrono::milliseconds connect_timeout) {
  AdminCommandClient::RequestResponses rr;
  rr.emplace_back(addr,
                  input,
                  ssl ? AdminCommandClient::ConnectionType::ENCRYPTED
                      : AdminCommandClient::ConnectionType::PLAIN);

  adminclient->send(rr, command_timeout, connect_timeout);

  if (out_error && !rr[0].success) {
    *out_error = rr[0].failure_reason;
  }

  return rr[0].success ? rr[0].response : "";
}

}}} // namespace facebook::logdevice::test
