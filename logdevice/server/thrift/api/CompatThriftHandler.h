/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/server/thrift/api/ThriftApiHandlerBase.h"

namespace facebook { namespace logdevice {

/**
 * Supports methods required for compatibility between new Thrift transport and
 * custom network stack.
 */
class CompatThriftHandler : public virtual ThriftApiHandlerBase {
 public:
  using SessionStream =
      apache::thrift::ResponseAndServerStream<thrift::SessionResponse,
                                              thrift::Message>;

  SessionStream
  createSession(std::unique_ptr<thrift::SessionRequest> request) override;

  void sendMessage(thrift::MessageReceipt& result,
                   std::unique_ptr<thrift::Message> message) override;
};

}} // namespace facebook::logdevice
