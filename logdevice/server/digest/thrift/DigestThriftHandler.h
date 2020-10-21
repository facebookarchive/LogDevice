/*
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/server/digest/if/gen-cpp2/Digest_types.h"
#include "logdevice/server/thrift/api/ThriftApiHandlerBase.h"
#include "thrift/lib/cpp2/async/ServerStream.h"

namespace facebook::logdevice::server::digest {

/**
 * Thrift handler for log digest service.
 */
class DigestThriftHandler : public virtual ThriftApiHandlerBase {
  folly::coro::Task<apache::thrift::ResponseAndServerStream<
      facebook::logdevice::thrift::digest::QueryResponse,
      facebook::logdevice::thrift::digest::QueryStreamResponse>>
      co_query(
          std::unique_ptr<facebook::logdevice::thrift::digest::QueryRequest>)
          override;

  folly::coro::Task<apache::thrift::ResponseAndServerStream<
      facebook::logdevice::thrift::digest::SubscribeResponse,
      facebook::logdevice::thrift::digest::SubscribeStreamResponse>>
      co_subscribe(
          std::unique_ptr<
              facebook::logdevice::thrift::digest::SubscribeRequest>) override;
};

} // namespace facebook::logdevice::server::digest
