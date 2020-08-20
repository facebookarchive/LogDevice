/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace detail {

/**
 * Wraps RocketClientChannel to allow calling some of its methods (such as
 * setTimeout and d-tor) outside of EventBase loop.
 *
 * This object is not thread-safe, concurrent usages of the same object from
 * different threads will lead to undefined behaviour.
 */
class RocketChannelWrapper : public apache::thrift::RequestChannel {
 public:
  using Ptr = std::unique_ptr<RocketChannelWrapper,
                              folly::DelayedDestruction::Destructor>;

  /**
   * Creates a new wrapper with given underlying Rocker channel accessed through
   * given event base. The created wrapper takes ownership of channel but tt is
   * caller's responsibilty to ensure the passed event base out-lives the
   * wrapper.
   */
  static Ptr newChannel(apache::thrift::RocketClientChannel::Ptr channel,
                        folly::EventBase* evb) {
    return {new RocketChannelWrapper(std::move(channel), evb), {}};
  }

  void sendRequestResponse(
      const apache::thrift::RpcOptions& options,
      folly::StringPiece method_name,
      apache::thrift::SerializedRequest&& request,
      std::shared_ptr<apache::thrift::transport::THeader> header,
      apache::thrift::RequestClientCallback::Ptr cob) override {
    channel_->sendRequestResponse(options,
                                  method_name,
                                  std::move(request),
                                  std::move(header),
                                  std::move(cob));
  }

  void sendRequestNoResponse(
      const apache::thrift::RpcOptions& options,
      folly::StringPiece method_name,
      apache::thrift::SerializedRequest&& request,
      std::shared_ptr<apache::thrift::transport::THeader> header,
      apache::thrift::RequestClientCallback::Ptr cob) override {
    channel_->sendRequestNoResponse(options,
                                    method_name,
                                    std::move(request),
                                    std::move(header),
                                    std::move(cob));
  }

  void
  sendRequestStream(const apache::thrift::RpcOptions& options,
                    folly::StringPiece method_name,
                    apache::thrift::SerializedRequest&& request,
                    std::shared_ptr<apache::thrift::transport::THeader> header,
                    apache::thrift::StreamClientCallback* cob) override {
    channel_->sendRequestStream(
        options, method_name, std::move(request), std::move(header), cob);
  }

  void
  sendRequestSink(const apache::thrift::RpcOptions& options,
                  folly::StringPiece method_name,
                  apache::thrift::SerializedRequest&& request,
                  std::shared_ptr<apache::thrift::transport::THeader> header,
                  apache::thrift::SinkClientCallback* cob) override {
    channel_->sendRequestSink(
        options, method_name, std::move(request), std::move(header), cob);
  }

  void setCloseCallback(apache::thrift::CloseCallback* cob) override {
    channel_->setCloseCallback(cob);
  }

  folly::EventBase* getEventBase() const override {
    return evb_;
  }

  uint16_t getProtocolId() override {
    return channel_->getProtocolId();
  }

 private:
  ~RocketChannelWrapper() override {
    evb_->runInEventBaseThread([channel = std::move(channel_)]() {});
  }

  RocketChannelWrapper(apache::thrift::RocketClientChannel::Ptr channel,
                       folly::EventBase* evb)
      : channel_(std::move(channel)), evb_(evb) {
    ld_check(channel_);
    ld_check(evb_);
  }

  apache::thrift::RocketClientChannel::Ptr channel_;
  folly::EventBase* evb_;
};
}}} // namespace facebook::logdevice::detail
