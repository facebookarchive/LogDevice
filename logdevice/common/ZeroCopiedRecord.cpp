/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ZeroCopiedRecord.h"

#include <algorithm>
#include <cstring>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

ZeroCopiedRecord::ZeroCopiedRecord() : flags(0) {}

ZeroCopiedRecord::ZeroCopiedRecord(
    lsn_t lsn,
    STORE_flags_t flags,
    uint64_t timestamp,
    esn_t last_known_good,
    uint32_t wave_or_recovery_epoch,
    const copyset_t& copyset,
    OffsetMap offsets_within_epoch,
    std::map<KeyType, std::string>&& keys,
    Slice payload_raw,
    std::shared_ptr<PayloadHolder> payload_holder)
    : lsn(lsn),
      flags(flags),
      timestamp(timestamp),
      last_known_good(last_known_good),
      wave_or_recovery_epoch(wave_or_recovery_epoch),
      copyset(copyset),
      offsets_within_epoch(std::move(offsets_within_epoch)),
      keys(std::move(keys)),
      payload_raw(payload_raw),
      payload_holder_(std::move(payload_holder)) {}

/*static*/
size_t ZeroCopiedRecord::getBytesEstimate(Slice payload_raw) {
  return sizeof(ZeroCopiedRecord) + payload_raw.size +
      /* control block size estimation*/ 32;
}

size_t ZeroCopiedRecord::getBytesEstimate() const {
  return getBytesEstimate(payload_raw);
}

}} // namespace facebook::logdevice
