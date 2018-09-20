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

/**
 * @file Carries a config version. Updates the client config version on the
 *       socket upon reception.
 */

struct CONFIG_ADVISORY_Header {
  config_version_t version;
} __attribute__((__packed__));

using CONFIG_ADVISORY_Message = FixedSizeMessage<CONFIG_ADVISORY_Header,
                                                 MessageType::CONFIG_ADVISORY,
                                                 TrafficClass::RECOVERY>;
}} // namespace facebook::logdevice
