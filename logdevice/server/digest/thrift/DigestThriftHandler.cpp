/*
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/digest/thrift/DigestThriftHandler.h"

using apache::thrift::ResponseAndServerStream;
using apache::thrift::TApplicationException;
using facebook::logdevice::thrift::digest::QueryRequest;
using facebook::logdevice::thrift::digest::QueryResponse;
using facebook::logdevice::thrift::digest::QueryStreamResponse;
using facebook::logdevice::thrift::digest::SubscribeRequest;
using facebook::logdevice::thrift::digest::SubscribeResponse;
using facebook::logdevice::thrift::digest::SubscribeStreamResponse;
using folly::coro::Task;
using std::unique_ptr;

namespace facebook::logdevice::server::digest {

Task<ResponseAndServerStream<QueryResponse, QueryStreamResponse>>
DigestThriftHandler::co_query(unique_ptr<QueryRequest> /* unused */) {
  throw TApplicationException("Not implemented yet");
}

Task<ResponseAndServerStream<SubscribeResponse, SubscribeStreamResponse>>
DigestThriftHandler::co_subscribe(unique_ptr<SubscribeRequest> /* unused */) {
  throw TApplicationException("Not implemented yet");
}

} // namespace facebook::logdevice::server::digest
