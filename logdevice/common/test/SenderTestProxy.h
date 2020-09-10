/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketCallback.h"

namespace facebook { namespace logdevice {

/**
 * Routes outbound messages to the provided instance of a class
 * that implements sendMessageImpl(). Typically used to intercept
 * or mock out message transmission during tests.
 */
template <typename SenderLike>
class SenderTestProxy : public SenderProxy {
 public:
  explicit SenderTestProxy(SenderLike* mock_sender)
      : mock_sender_(mock_sender) {}

 protected:
  bool canSendToImpl(const Address& addr,
                     TrafficClass tc,
                     BWAvailableCallback& cb) override {
    return mock_sender_->canSendToImpl(addr, tc, cb);
  }

  int sendMessageImpl(std::unique_ptr<Message>&& msg,
                      const Address& addr,
                      BWAvailableCallback* on_bw_avail,
                      SocketCallback* onclose) override {
    return mock_sender_->sendMessageImpl(
        std::move(msg), addr, on_bw_avail, onclose);
  }

 private:
  SenderLike* mock_sender_;
};

}} // namespace facebook::logdevice
