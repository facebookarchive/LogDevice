/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/thrift/ThriftRouter.h"

namespace facebook { namespace logdevice {

class MockThriftRouter : public ThriftRouter {
 public:
  MOCK_METHOD0(getApiAddress, Sockaddr());

  MOCK_METHOD0(createApiClient,
               std::unique_ptr<thrift::LogDeviceAPIAsyncClient>());

  std::unique_ptr<thrift::LogDeviceAPIAsyncClient>
  getApiClient(node_index_t /* unused */, Sockaddr* out_address) override {
    Sockaddr resolved_address = getApiAddress();
    if (out_address) {
      *out_address = resolved_address;
    }
    return createApiClient();
  }
};

// We cannot use Thrift automock because it does not support async methods
class MockApiClient : public thrift::LogDeviceAPIAsyncClient {
 public:
  MockApiClient() : LogDeviceAPIAsyncClient(nullptr) {}

  MOCK_METHOD2(
      createSession,
      void(std::unique_ptr<apache::thrift::RequestCallback> callback,
           const ::facebook::logdevice::thrift::SessionRequest& request));
};

}} // namespace facebook::logdevice
