/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class EpochMetaDataMap;
class TailRecord;

// A struct to hold the results/attributes of the GetSeqStateRequest
// that may be required by the callback (on_complete).
struct GetSeqStateRequestResult {
  request_id_t rqid;
  logid_t log_id;
  Status status;
  NodeID last_seq;
  lsn_t last_released_lsn;
  lsn_t next_lsn;
  folly::Optional<LogTailAttributes> attributes;
  folly::Optional<OffsetMap> epoch_offsets;
  std::shared_ptr<const EpochMetaDataMap> metadata_map;
  std::shared_ptr<TailRecord> tail_record;
  folly::Optional<bool> is_log_empty;
};
}} // namespace facebook::logdevice
