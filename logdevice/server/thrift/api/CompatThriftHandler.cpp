/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/thrift/api/CompatThriftHandler.h"

using apache::thrift::TApplicationException;

namespace facebook { namespace logdevice {

CompatThriftHandler::SessionStream CompatThriftHandler::createSession(
    std::unique_ptr<thrift::SessionRequest> /*request*/) {
  throw TApplicationException("Not implemented yet");
}

void CompatThriftHandler::sendMessage(
    thrift::MessageReceipt& /*result*/,
    std::unique_ptr<thrift::Message> /*message*/) {
  throw TApplicationException("Not implemented yet");
}

}} // namespace facebook::logdevice
