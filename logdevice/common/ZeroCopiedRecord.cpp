/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ZeroCopiedRecord.h"

#include <algorithm>
#include <cstring>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/ZeroCopiedRecordDisposal.h"
#include "logdevice/common/debug.h"
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
    // TODO(T33977412)
    uint64_t offset_within_epoch,
    std::map<KeyType, std::string>&& keys,
    Slice payload_raw,
    std::shared_ptr<PayloadHolder> payload_holder)
    : lsn(lsn),
      flags(flags),
      timestamp(timestamp),
      last_known_good(last_known_good),
      wave_or_recovery_epoch(wave_or_recovery_epoch),
      copyset(copyset),
      // TODO(T33977412)
      offset_within_epoch(offset_within_epoch),
      keys(std::move(keys)),
      payload_raw(payload_raw),
      payload_holder_(std::move(payload_holder)) {}

worker_id_t ZeroCopiedRecord::getDisposalThread() const {
  return payload_holder_ == nullptr ? worker_id_t(-1)
                                    : payload_holder_->getThreadAffinity();
}

WorkerType ZeroCopiedRecord::getDisposalWorkerType() const {
  return payload_holder_ == nullptr ? WorkerType::GENERAL
                                    : payload_holder_->getWorkerTypeAffinity();
}

/*static*/
size_t ZeroCopiedRecord::getBytesEstimate(Slice payload_raw) {
  return sizeof(ZeroCopiedRecord) + payload_raw.size +
      /* control block size estimation*/ 32;
}

size_t ZeroCopiedRecord::getBytesEstimate() const {
  return getBytesEstimate(payload_raw);
}

void ZeroCopiedRecord::Disposer::operator()(ZeroCopiedRecord* e) {
  std::unique_ptr<ZeroCopiedRecord> record_ptr(e);
  disposal_->disposeOfRecord(std::move(record_ptr));
}

}} // namespace facebook::logdevice
