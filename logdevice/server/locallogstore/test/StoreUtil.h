/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include <folly/Optional.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"

namespace facebook { namespace logdevice {

constexpr logid_t DEFAULT_LOG_ID(21091985);

/**
 * Simple class used to seed test data into the local log store via
 * store_fill().
 */
class TestRecord {
 public:
  TestRecord(logid_t log_id,
             lsn_t lsn,
             std::chrono::milliseconds timestamp,
             uint32_t wave)
      : log_id_(log_id), lsn_(lsn), timestamp_(timestamp), wave_(wave) {}

  TestRecord(lsn_t lsn, std::chrono::milliseconds timestamp, uint32_t wave = 1)
      : TestRecord(DEFAULT_LOG_ID, lsn, timestamp, wave) {}

  TestRecord(logid_t log_id, lsn_t lsn, esn_t last_known_good)
      : log_id_(log_id), lsn_(lsn), last_known_good_(last_known_good) {}

  TestRecord(logid_t log_id, lsn_t lsn, const std::string& key)
      : log_id_(log_id), lsn_(lsn) {
    optional_keys_.insert(std::make_pair(KeyType::FINDKEY, key));
    flags_ |= LocalLogStoreRecordFormat::FLAG_CUSTOM_KEY;
  }

  explicit TestRecord(lsn_t lsn) : log_id_(DEFAULT_LOG_ID), lsn_(lsn) {}

  TestRecord& wave(uint32_t wave) {
    wave_ = wave;
    return *this;
  }

  TestRecord& copyset(copyset_t copyset) {
    copyset_ = std::move(copyset);
    return *this;
  }

  TestRecord& hole() {
    flags_ |= LocalLogStoreRecordFormat::FLAG_HOLE;
    flags_ |= LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
    return *this;
  }

  TestRecord& timestamp(std::chrono::milliseconds ts) {
    timestamp_ = ts;
    return *this;
  }

  TestRecord& payload(const Payload& payload) {
    payload_ = payload;
    return *this;
  }

  TestRecord& flagAmend() {
    flags_ |= LocalLogStoreRecordFormat::FLAG_AMEND;
    return *this;
  }

  TestRecord& writtenByRebuilding() {
    flags_ |= LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_REBUILDING;
    return *this;
  }

  TestRecord& offsetsWithinEpoch(OffsetMap offsets_within_epoch) {
    flags_ |= LocalLogStoreRecordFormat::FLAG_OFFSET_WITHIN_EPOCH;
    flags_ |= LocalLogStoreRecordFormat::FLAG_OFFSET_MAP;
    offsets_within_epoch_ = std::move(offsets_within_epoch);
    return *this;
  }

  TestRecord& flagWrittenByRecovery() {
    flags_ |= LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
    return *this;
  }

  TestRecord& writtenByRecovery(epoch_t recovery_epoch) {
    flags_ |= LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
    wave_ = (uint32_t)recovery_epoch.val_;
    return *this;
  }

  TestRecord& writtenByRecovery(epoch_t::raw_type recovery_epoch) {
    return writtenByRecovery(epoch_t(recovery_epoch));
  }

  TestRecord& bridge() {
    flags_ |= LocalLogStoreRecordFormat::FLAG_HOLE |
        LocalLogStoreRecordFormat::FLAG_BRIDGE |
        LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_RECOVERY;
    return *this;
  }

  logid_t log_id_;
  lsn_t lsn_;
  esn_t last_known_good_{ESN_INVALID};
  copyset_t copyset_ = {ShardID(1, 0)};
  std::chrono::milliseconds timestamp_{0};
  uint32_t wave_{1};
  OffsetMap offsets_within_epoch_;
  LocalLogStoreRecordFormat::flags_t flags_ =
      LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY;
  folly::Optional<Payload> payload_;
  std::map<KeyType, std::string> optional_keys_;
};

void store_fill(LocalLogStore& store,
                const std::vector<TestRecord>& data,
                folly::Optional<lsn_t> block_starting_lsn = LSN_INVALID);

}} // namespace facebook::logdevice
