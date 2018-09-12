/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file Used to fetch a config from another host. Receiver should send a
 *       CONFIG_CHANGED message in response.
 */

struct CONFIG_FETCH_Header {
  enum class ConfigType : uint8_t { MAIN_CONFIG = 0, LOGS_CONFIG = 1 };
  ConfigType config_type;
};

static_assert(sizeof(CONFIG_FETCH_Header) == 1,
              "CONFIG_FETCH_Header is expected to be 1 byte");

using CONFIG_FETCH_Message = FixedSizeMessage<CONFIG_FETCH_Header,
                                              MessageType::CONFIG_FETCH,
                                              TrafficClass::RECOVERY>;
}} // namespace facebook::logdevice
