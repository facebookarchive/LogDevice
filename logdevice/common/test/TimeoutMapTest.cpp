/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TimeoutMap.h"

#include <gtest/gtest.h>

#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/libevent/test/EvBaseMock.h"

using namespace facebook::logdevice;
using namespace testing;
