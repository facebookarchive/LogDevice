/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/ZookeeperClientBase.h"

namespace facebook { namespace logdevice {

class MockZookeeperClient : public ZookeeperClientBase {
 public:
  MOCK_METHOD0(state, int());

  MOCK_METHOD2(getData, void(std::string path, data_callback_t cb));
  MOCK_METHOD2(exists, void(std::string path, stat_callback_t cb));
  MOCK_METHOD4(setData,
               void(std::string path,
                    std::string data,
                    stat_callback_t cb,
                    zk::version_t base_version));
  MOCK_METHOD5(create,
               void(std::string path,
                    std::string data,
                    create_callback_t cb,
                    std::vector<zk::ACL> acl,
                    int32_t flags));
  MOCK_METHOD2(multiOp, void(std::vector<zk::Op> ops, multi_op_callback_t cb));
  MOCK_METHOD1(sync, void(sync_callback_t cb));

  MOCK_METHOD5(createWithAncestors,
               void(std::string path,
                    std::string data,
                    create_callback_t cb,
                    std::vector<zk::ACL> acl,
                    int32_t flags));
};

}} // namespace facebook::logdevice
