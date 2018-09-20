/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * A type of DataRecord backed by TailRecord.
 */

class TailRecord;

class DataRecordFromTailRecord : public DataRecord {
 public:
  static std::unique_ptr<DataRecordFromTailRecord>
  create(std::shared_ptr<TailRecord> tail);

  bool checksumFailed() const {
    return checksum_failed_;
  }

 private:
  explicit DataRecordFromTailRecord(std::shared_ptr<TailRecord> tail);

  // Compute the actual payload of the tail record, perform checksum checks
  // if checksum data is included
  // @out_param checksum_failed   set to true if checksum failed
  // @return                      the actual payload of the record
  static Payload computePayload(const TailRecord& tail, bool* checksum_failed);

  std::shared_ptr<TailRecord> tail_record_;
  bool checksum_failed_{false};
};

}} // namespace facebook::logdevice
