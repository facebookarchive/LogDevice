/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/DelayedDestruction.h>

namespace apache { namespace thrift {
class RequestChannel;
}} // namespace apache::thrift

namespace facebook { namespace logdevice {

/**
 * API for creating instances of Thrift client.
 * Depending on implementation this object might be expensive to create, so try
 * avoid creating them ad-hoc. Ideally, single object should be created and
 * re-used across the whole application.
 */
class ThriftClientFactory {
 public:
  /**
   * Creates a new Thrift client using given address as a destination for RPC
   * requests.
   *
   * @param address Address of the Thrift server to connect to.
   *
   * @return Pointer to new client.
   */
  template <typename T>
  std::unique_ptr<T>
  createClient(const folly::SocketAddress& address,
               folly::Executor* callback_executor = nullptr) {
    ChannelPtr channel = createChannel(address, callback_executor);
    return std::make_unique<T>(std::move(channel));
  }

  virtual ~ThriftClientFactory() = default;

 protected:
  using ChannelPtr = std::unique_ptr<apache::thrift::RequestChannel,
                                     folly::DelayedDestruction::Destructor>;

  /**
   * Creates Thrift channel connected to given address.
   *
   * @param address Address of the Thrift server to connect to.
   */
  virtual ChannelPtr createChannel(const folly::SocketAddress& address,
                                   folly::Executor* callback_executor) = 0;
};

}} // namespace facebook::logdevice
