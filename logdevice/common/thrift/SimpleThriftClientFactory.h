/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "logdevice/common/thrift/RocketChannelWrapper.h"
#include "logdevice/common/thrift/ThriftClientFactory.h"

using apache::thrift::RocketClientChannel;
using facebook::logdevice::detail::RocketChannelWrapper;
using folly::AsyncSocket;

namespace facebook { namespace logdevice {

namespace {
constexpr int kDefaultPoolSize = 4;
constexpr std::chrono::milliseconds kDefaultConnectTimeout =
    std::chrono::milliseconds(100);
constexpr std::chrono::milliseconds kDefaultRequestTimeout =
    std::chrono::milliseconds(3000);
} // namespace

/**
 * Basic implementation of Thrift client factory. It uses IO thread pool
 * shared between all clients it creates and creates a new connection for each
 * new client.
 */
class SimpleThriftClientFactory : public ThriftClientFactory {
 public:
  SimpleThriftClientFactory(
      int thread_pool_size = kDefaultPoolSize,
      std::chrono::milliseconds connect_timeout = kDefaultConnectTimeout,
      std::chrono::milliseconds request_timeout = kDefaultRequestTimeout)
      : io_executor_(thread_pool_size),
        connect_timeout_(connect_timeout),
        request_timeout_(request_timeout) {}

 protected:
  ThriftClientFactory::ChannelPtr
  createChannel(const folly::SocketAddress& address,
                folly::Executor* callback_executor) override {
    // Get random evb for this client
    auto evb = io_executor_.getEventBase();
    ThriftClientFactory::ChannelPtr channel;
    evb->runInEventBaseThreadAndWait(
        [address, evb, &channel, this, callback_executor]() {
          AsyncSocket::UniquePtr socket(
              new AsyncSocket(evb, address, connect_timeout_.count()));
          auto rocket = RocketClientChannel::newChannel(std::move(socket));
          if (request_timeout_.count() > 0) {
            rocket->setTimeout(request_timeout_.count());
          }
          channel = RocketChannelWrapper::newChannel(
              std::move(rocket), evb, callback_executor);
        });
    return channel;
  }

 private:
  folly::IOThreadPoolExecutor io_executor_;
  std::chrono::milliseconds connect_timeout_;
  std::chrono::milliseconds request_timeout_;
};

}} // namespace facebook::logdevice
