/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <thread>

#include <gtest/gtest.h>

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/common/debug.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice {

/**
 * Retry a lambda for a number of attempts with a delay as long as it's throwing
 * NodeNotReady exception.
 */
void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation);
}} // namespace facebook::logdevice
