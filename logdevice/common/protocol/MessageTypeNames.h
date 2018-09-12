/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/include/EnumMap.h"

namespace facebook { namespace logdevice {

/**
 * @file a specialization of EnumMap for mapping MessageTypes and
 *       their int representations to human-readable names. Also can be used
 *       to check if a given int represents a valid MessageType.
 */

extern EnumMap<MessageType, std::string> messageTypeNames;

}} // namespace facebook::logdevice
