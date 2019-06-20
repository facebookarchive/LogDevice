/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/ConnectionFactory.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/MockSettings.h"

namespace facebook { namespace logdevice {

using namespace testing;

TEST(ConnectionFactoryTest, Create) {
  MockSettings settings;
  ConnectionFactory factory(settings);
}

}} // namespace facebook::logdevice
