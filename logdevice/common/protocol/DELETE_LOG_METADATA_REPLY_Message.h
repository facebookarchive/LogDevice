/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "folly/Conv.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/DELETE_LOG_METADATA_Message.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file DELETE_LOG_METADATA_REPLY is a response to DELETE_LOG_METADATA message.
 */

struct DELETE_LOG_METADATA_REPLY_Header : public DELETE_LOG_METADATA_Header {
  Status status;
};

using DELETE_LOG_METADATA_REPLY_Message =
    FixedSizeMessage<DELETE_LOG_METADATA_REPLY_Header,
                     MessageType::DELETE_LOG_METADATA_REPLY,
                     TrafficClass::RECOVERY>;

}} // namespace facebook::logdevice
