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
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageType.h"

namespace facebook { namespace logdevice {

/**
 * @file DELETE_LOG_METADATA is a message for hotfixing issues with log
 * metadata by simply removing it from existence.
 */

struct DELETE_LOG_METADATA_Header {
  // log metadata to delete
  logid_t first_logid;
  logid_t last_logid;
  LogMetadataType log_metadata_type;
  shard_index_t shard_idx;

  // control info
  request_id_t client_rqid;

  std::string toString() const {
    return folly::to<std::string>("(",
                                  first_logid.val_,
                                  ", ",
                                  last_logid.val_,
                                  ", ",
                                  log_metadata_type,
                                  ", ",
                                  shard_idx,
                                  ")");
  }
} __attribute__((__packed__));

using DELETE_LOG_METADATA_Message =
    FixedSizeMessage<DELETE_LOG_METADATA_Header,
                     MessageType::DELETE_LOG_METADATA,
                     TrafficClass::RECOVERY>;

}} // namespace facebook::logdevice
