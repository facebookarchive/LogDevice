/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/VersionedConfigStore.h"

namespace facebook { namespace logdevice {

class MockVersionedConfigStore : public VersionedConfigStore {
 public:
  explicit MockVersionedConfigStore(extract_version_fn extract_fn)
      : VersionedConfigStore(std::move(extract_fn)){};

  MOCK_CONST_METHOD3(getConfig,
                     void(std::string key,
                          value_callback_t cb,
                          folly::Optional<version_t> base_version));

  MOCK_CONST_METHOD2(getLatestConfig,
                     void(std::string key, value_callback_t cb));

  MOCK_METHOD3(readModifyWriteConfig,
               void(std::string key,
                    mutation_callback_t mcb,
                    write_callback_t cb));

  MOCK_METHOD0(shutdown, void());
};
}} // namespace facebook::logdevice
