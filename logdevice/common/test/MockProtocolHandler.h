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
  MOCK_CONST_METHOD1(validateProtocolHeader, bool(const ProtocolHeader& hdr));
  MOCK_METHOD2(readMessageBody,
               int(const ProtocolHeader& hdr,
                   std::unique_ptr<folly::IOBuf> body));
  MOCK_METHOD1(notifyErrorOnSocket, void(const folly::AsyncSocketException&));
  MOCK_CONST_METHOD0(getSettings, const Settings&());
};

}} // namespace facebook::logdevice
