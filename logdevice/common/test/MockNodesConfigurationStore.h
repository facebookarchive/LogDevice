/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class MockNodesConfigurationStore : public NodesConfigurationStore {
 public:
  void getConfig(value_callback_t cb) const {
    getConfig_(cb);
  }
  MOCK_CONST_METHOD1(getConfig_, void(value_callback_t& cb));

  MOCK_CONST_METHOD1(getConfigSync, Status(std::string* value_out));

  void getLatestConfig(value_callback_t cb) const {
    getLatestConfig_(cb);
  }
  MOCK_CONST_METHOD1(getLatestConfig_, void(value_callback_t& cb));

  void updateConfig(std::string value,
                    folly::Optional<version_t> base_version,
                    write_callback_t cb) {
    updateConfig_(std::move(value), std::move(base_version), cb);
  }

  MOCK_METHOD3(updateConfig_,
               void(std::string value,
                    folly::Optional<version_t> base_version,
                    write_callback_t& cb));

  MOCK_METHOD4(updateConfigSync,
               Status(std::string value,
                      folly::Optional<version_t> base_version,
                      version_t* version_out,
                      std::string* value_out));
};
}}}} // namespace facebook::logdevice::configuration::nodes
