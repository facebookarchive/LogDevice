/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/hash/Hash.h>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * A RecordID uniquely identifies a data record stored on a LogDevice cluster.
 */
struct RecordID {
  esn_t esn;
  epoch_t epoch;
  logid_t logid;

  RecordID() {}
  RecordID(esn_t esn, epoch_t epoch, logid_t logid)
      : esn(esn), epoch(epoch), logid(logid) {}
  RecordID(lsn_t lsn, logid_t logid)
      : RecordID(lsn_to_esn(lsn), lsn_to_epoch(lsn), logid) {}

  bool operator==(const RecordID& r) const {
    return esn == r.esn && epoch == r.epoch && logid == r.logid;
  }
  bool operator!=(const RecordID& r) const {
    return !(*this == r);
  }
  bool operator<(const RecordID& r) const {
    return std::tie(logid, epoch, esn) < std::tie(r.logid, r.epoch, r.esn);
  }

  lsn_t lsn() const {
    return compose_lsn(epoch, esn);
  }

  class Hasher {
   public:
    size_t operator()(const RecordID& rid) const {
      return folly::hash::hash_combine(
          rid.epoch.val_, rid.esn.val_, rid.logid.val_);
    }
  };

  std::string toString() const {
    return std::to_string(logid.val_) + "e" + std::to_string(epoch.val_) + "n" +
        std::to_string(esn.val_);
  }

  bool valid() const {
    return logid != LOGID_INVALID && epoch != EPOCH_INVALID &&
        esn != ESN_INVALID;
  }
} __attribute__((__packed__));

static_assert(sizeof(RecordID) == 16, "struct RecordID must be 16 bytes");

}} // namespace facebook::logdevice
