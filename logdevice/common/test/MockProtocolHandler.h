/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/IProtocolHandler.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

class MockProtocolHandler : public IProtocolHandler {
 public:
  MOCK_METHOD2(asyncConnect,
               folly::Future<Status>(const folly::SocketAddress&,
                                     const Settings&));
  MOCK_METHOD2(sendBuffer,
               void(SocketObserver::observer_id_t,
                    std::unique_ptr<folly::IOBuf>&& buffer_chain));
  MOCK_METHOD1(close, void(Status));
  MOCK_METHOD1(closeNow, void(Status));
  MOCK_METHOD1(registerSocketObserver,
               SocketObserver::observer_id_t(SocketObserver*));
  MOCK_METHOD1(unregisterSocketObserver, void(SocketObserver::observer_id_t));
  MOCK_CONST_METHOD1(validateProtocolHeader, bool(const ProtocolHeader& hdr));
  MOCK_METHOD2(dispatchMessageBody,
               int(const ProtocolHeader& hdr,
                   std::unique_ptr<folly::IOBuf> body));
  MOCK_METHOD1(notifyErrorOnSocket, void(const folly::AsyncSocketException&));
  MOCK_METHOD0(notifyBytesWritten, void());
  MOCK_CONST_METHOD0(getSettings, const Settings&());
  MOCK_CONST_METHOD0(good, bool());
};

}} // namespace facebook::logdevice
