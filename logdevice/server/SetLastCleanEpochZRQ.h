/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdio>

#include "logdevice/common/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/server/LastCleanEpochZRQ.h"

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
                       EpochStore::CompletionLCE cf,
                       ZookeeperEpochStore* store)
      : LastCleanEpochZRQ(logid, epoch, cf, store) {
    tail_record_ = std::move(tail_record);
    ld_check(tail_record_.isValid());
    ld_check(tail_record_.header.log_id == logid);
  }

  // see ZookeeperEpochStoreRequest.h
  NextStep onGotZnodeValue(const char* znode_value,
                           int znode_value_len) override {
    if (!znode_value) {
      ld_check(znode_value_len == 0);
      err = E::NOTFOUND;
      return NextStep::FAILED;
    }

    epoch_t parsed_epoch;
    TailRecord parsed_tail;
    int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
        znode_value, znode_value_len, logid_, &parsed_epoch, &parsed_tail);

    if (rv != 0) {
      err = E::BADMSG;
      return NextStep::FAILED;
    }

    if (epoch_ <= parsed_epoch) {
      epoch_ = parsed_epoch;
      tail_record_ = std::move(parsed_tail);
      err = E::STALE;
      return NextStep::FAILED;
    }

    if (tail_record_.header.lsn == LSN_INVALID &&
        parsed_tail.header.lsn != LSN_INVALID) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Setting the tail with LSN_INVALID while the previous "
                        "tail in LCE is %lu for log %lu epoch %u.",
                        parsed_tail.header.lsn,
                        logid_.val_,
                        epoch_.val_);
      tail_record_.header.lsn = parsed_tail.header.lsn;
    }

    if (tail_record_.header.timestamp == LSN_INVALID &&
        parsed_tail.header.timestamp != LSN_INVALID) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "Setting the tail with timestamp 0 while the previous "
          "tail in LCE has timestamp of %s for log %lu epoch %u.",
          format_time(std::chrono::milliseconds(parsed_tail.header.timestamp))
              .c_str(),
          logid_.val_,
          epoch_.val_);
      tail_record_.header.timestamp = parsed_tail.header.timestamp;
    }

    if (!tail_record_.offsets_map_.isValid() &&
        parsed_tail.offsets_map_.isValid()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Setting the tail with invalid OffsetMap while the "
                        "previous tail in LCE has OffsetMap of %s for log "
                        "%lu epoch %u.",
                        parsed_tail.offsets_map_.toString().c_str(),
                        logid_.val_,
                        epoch_.val_);
      tail_record_.offsets_map_ = std::move(parsed_tail.offsets_map_);
    }

    return NextStep::MODIFY;
  }

  // see ZookeeperEpochStoreRequest.h
  int composeZnodeValue(char* buf, size_t size) override {
    ld_check(buf);

    int expected_size = EpochStoreLastCleanEpochFormat::sizeInLinearBuffer(
        epoch_, tail_record_);
    if (expected_size > size && tail_record_.hasPayload()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Unable to fit tail record with payload in LCE znode "
                        "for log %lu epoch %u, required_size %d, znode size "
                        "%lu, removing its payload.",
                        logid_.val_,
                        epoch_.val_,
                        expected_size,
                        size);
      tail_record_.removePayload();
    }

    return EpochStoreLastCleanEpochFormat::toLinearBuffer(
        buf, size, epoch_, tail_record_);
  }
};

}} // namespace facebook::logdevice
