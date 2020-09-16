/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdio>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/server/epoch_store/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/server/epoch_store/LastCleanEpochZRQ.h"

namespace facebook { namespace logdevice {

/**
 * @file  this ZookeeperEpochStoreRequest subclass drives ZookeeperEpochStore to
 *        execute a read-test-modify-write update of the <logid>/lce znode
 *        in order to advance the last clean epoch (LCE) value for the log.
 *        A write is done only if the current epoch number stored in /lce
 *        znode is smaller than epoch_.
 */

class SetLastCleanEpochZRQ : public LastCleanEpochZRQ {
 public:
  SetLastCleanEpochZRQ(logid_t logid,
                       epoch_t epoch,
                       TailRecord tail_record,
                       EpochStore::CompletionLCE cf)
      : LastCleanEpochZRQ(logid, std::move(cf)),
        epoch_(epoch),
        tail_record_(std::move(tail_record)) {
    ld_check(tail_record_.isValid());
    ld_check(tail_record_.header.log_id == logid);
  }

  // see ZookeeperEpochStoreRequest.h
  NextStep applyChanges(LogMetaData& log_metadata,
                        bool value_existed) override {
    if (!value_existed) {
      err = E::NOTFOUND;
      return NextStep::FAILED;
    }
    auto [parsed_epoch_ref, parsed_tail_ref] =
        referenceFromLogMetaData(log_metadata);

    if (epoch_ <= parsed_epoch_ref) {
      err = E::STALE;
      return NextStep::FAILED;
    }

    if (tail_record_.header.lsn == LSN_INVALID &&
        parsed_tail_ref.header.lsn != LSN_INVALID) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Setting the tail with LSN_INVALID while the previous "
                        "tail in LCE is %lu for log %lu epoch %u.",
                        parsed_tail_ref.header.lsn,
                        logid_.val_,
                        epoch_.val_);
      tail_record_.header.lsn = parsed_tail_ref.header.lsn;
    }

    if (tail_record_.header.timestamp == LSN_INVALID &&
        parsed_tail_ref.header.timestamp != LSN_INVALID) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Setting the tail with timestamp 0 while the previous "
                        "tail in LCE has timestamp of %s for log %lu epoch %u.",
                        format_time(std::chrono::milliseconds(
                                        parsed_tail_ref.header.timestamp))
                            .c_str(),
                        logid_.val_,
                        epoch_.val_);
      tail_record_.header.timestamp = parsed_tail_ref.header.timestamp;
    }

    if (!tail_record_.offsets_map_.isValid() &&
        parsed_tail_ref.offsets_map_.isValid()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Setting the tail with invalid OffsetMap while the "
                        "previous tail in LCE has OffsetMap of %s for log "
                        "%lu epoch %u.",
                        parsed_tail_ref.offsets_map_.toString().c_str(),
                        logid_.val_,
                        epoch_.val_);
      tail_record_.offsets_map_ = std::move(parsed_tail_ref.offsets_map_);
    }

    // Apply the modifications to the log_metadata struct.
    parsed_epoch_ref = epoch_;
    parsed_tail_ref = std::move(tail_record_);

    return NextStep::MODIFY;
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(LogMetaData& log_metadata,
                        char* buf,
                        size_t size) const override {
    ld_check(buf);

    auto [epoch_ref, tail_ref] = referenceFromLogMetaData(log_metadata);

    int expected_size =
        EpochStoreLastCleanEpochFormat::sizeInLinearBuffer(epoch_ref, tail_ref);
    if (expected_size > size && tail_record_.hasPayload()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Unable to fit tail record with payload in LCE znode "
                        "for log %lu epoch %u, required_size %d, znode size "
                        "%lu, removing its payload.",
                        logid_.val_,
                        epoch_ref.val_,
                        expected_size,
                        size);
      tail_ref.removePayload();
    }

    return EpochStoreLastCleanEpochFormat::toLinearBuffer(
        buf, size, epoch_ref, tail_ref);
  }

 private:
  epoch_t epoch_;
  TailRecord tail_record_;
};

}} // namespace facebook::logdevice
