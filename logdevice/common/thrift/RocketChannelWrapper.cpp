/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/RocketChannelWrapper.h"

using apache::thrift::ClientReceiveState;
using apache::thrift::RequestClientCallback;
using CallbackPtr = RequestClientCallback::Ptr;

namespace facebook { namespace logdevice { namespace detail {
namespace {
template <bool oneWay>
/**
 * Ensures client provided callback is called from provided executor unless they
 * are safe to call inline. User-defined callbacks are never safe by default but
 * most of Thrift callbacks are (e.g. callback adapting async call to
 * Future/SemiFuture/coroutines API).
 */
class ExecutorRequestCallback final : public RequestClientCallback {
 public:
  ExecutorRequestCallback(CallbackPtr cb,
                          folly::Executor::KeepAlive<> executor_keep_alive)
      : executor_keep_alive_(std::move(executor_keep_alive)),
        cb_(std::move(cb)) {
    ld_check(executor_keep_alive_);
  }

  void onRequestSent() noexcept override {
    if (oneWay) {
      executor_keep_alive_->add(
          [cb = std::move(cb_)]() mutable { cb.release()->onRequestSent(); });
      delete this;
    } else {
      requestSent_ = true;
    }
  }
  void onResponse(ClientReceiveState&& rs) noexcept override {
    executor_keep_alive_->add([requestSent = requestSent_,
                               cb = std::move(cb_),
                               rs = std::move(rs)]() mutable {
      if (requestSent) {
        cb->onRequestSent();
      }
      cb.release()->onResponse(std::move(rs));
    });
    delete this;
  }
  void onResponseError(folly::exception_wrapper ex) noexcept override {
    executor_keep_alive_->add([requestSent = requestSent_,
                               cb = std::move(cb_),
                               ex = std::move(ex)]() mutable {
      if (requestSent) {
        cb->onRequestSent();
      }
      cb.release()->onResponseError(std::move(ex));
    });
    delete this;
  }

 private:
  bool requestSent_{false};
  folly::Executor::KeepAlive<> executor_keep_alive_;
  CallbackPtr cb_;
};

} // namespace

template <bool oneWayCb>
CallbackPtr RocketChannelWrapper::wrapIfUnsafe(CallbackPtr cob) {
  if (!cob->isInlineSafe()) {
    return CallbackPtr(new ExecutorRequestCallback<oneWayCb>(
        std::move(cob), getKeepAliveToken(callback_executor_)));
  } else {
    return cob;
  }
}

void RocketChannelWrapper::sendRequestResponse(
    const apache::thrift::RpcOptions& options,
    folly::StringPiece method_name,
    apache::thrift::SerializedRequest&& request,
    std::shared_ptr<apache::thrift::transport::THeader> header,
    CallbackPtr cob) {
  cob = wrapIfUnsafe<false>(std::move(cob));
  channel_->sendRequestResponse(options,
                                method_name,
                                std::move(request),
                                std::move(header),
                                std::move(cob));
}

void RocketChannelWrapper::sendRequestNoResponse(
    const apache::thrift::RpcOptions& options,
    folly::StringPiece method_name,
    apache::thrift::SerializedRequest&& request,
    std::shared_ptr<apache::thrift::transport::THeader> header,
    CallbackPtr cob) {
  cob = wrapIfUnsafe<true>(std::move(cob));
  channel_->sendRequestNoResponse(options,
                                  method_name,
                                  std::move(request),
                                  std::move(header),
                                  std::move(cob));
}
}}} // namespace facebook::logdevice::detail
