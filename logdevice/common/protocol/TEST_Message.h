/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

struct TEST_Message_Header {
  uint64_t id;
};

using TEST_Message = FixedSizeMessage<TEST_Message_Header,
                                      MessageType::TEST,
                                      TrafficClass::REBUILD>;

}} // namespace facebook::logdevice
