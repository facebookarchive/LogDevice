/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdio>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/common/EpochStoreLastCleanEpochFormat.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/LastCleanEpochZRQ.h"

namespace {

using namespace facebook::logdevice;

class LastCleanEpochStoreFormatTest : public ::testing::Test {
 public:
  const logid_t LOGID{2333};

  LastCleanEpochStoreFormatTest() {
    dbg::assertOnData = true;
  }

  TailRecord genTailRecord(bool include_payload) {
    TailRecordHeader::flags_t flags =
        (include_payload ? TailRecordHeader::HAS_PAYLOAD : 0);
    void* payload_flat = malloc(20);
    std::strncpy((char*)payload_flat, "Tail Record Test.", 20);
    return TailRecord(
        TailRecordHeader{
            LOGID,
            compose_lsn(epoch_t(933), esn_t(3347)),
            1502502135,
            {BYTE_OFFSET_INVALID /* deprecated, use OffsetMap instead */},
            flags,
            {}},
        OffsetMap({{BYTE_OFFSET, 2349045994592}}),
        include_payload ? std::make_shared<PayloadHolder>(payload_flat, 20)
                        : nullptr);
  }

  // original implemenetation in SetLastCleanEpochZRQ.h
  int composeZnodeValueLegacy(char* buf,
                              size_t size,
                              epoch_t lce,
                              uint64_t epoch_end_offset,
                              lsn_t last_released_real_lsn,
                              uint64_t last_timestamp) {
    ld_check(buf);
    return snprintf(buf,
                    size,
                    "%u@%lu@%lu@%lu",
                    lce.val_,
                    epoch_end_offset,
                    uint64_t(last_released_real_lsn),
                    last_timestamp);
  }
};

TEST_F(LastCleanEpochStoreFormatTest, ReadLegacyFormat) {
  char zbuf[EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX];
  const epoch_t lce = epoch_t(333);
  const uint64_t epoch_end_offset = 0xc00cbeefcace;
  const lsn_t last_released_real =
      compose_lsn(epoch_t(lce.val_ - 1), esn_t(2333333));
  const uint64_t last_timestamp = 7;

  int rv = composeZnodeValueLegacy(zbuf,
                                   sizeof(zbuf),
                                   lce,
                                   epoch_end_offset,
                                   last_released_real,
                                   last_timestamp);
  ASSERT_GT(rv, 0);

  epoch_t parsed_epoch;
  TailRecord tail;
  rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
      zbuf, sizeof(zbuf), LOGID, &parsed_epoch, &tail);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(lce, parsed_epoch);
  ASSERT_TRUE(tail.isValid());
  ASSERT_EQ(LOGID, tail.header.log_id);
  ASSERT_EQ(last_released_real, tail.header.lsn);
  ASSERT_EQ(epoch_end_offset, tail.offsets_map_.getCounter(BYTE_OFFSET));
  ASSERT_EQ(last_timestamp, tail.header.timestamp);
}

TEST_F(LastCleanEpochStoreFormatTest, EmptyBuffer) {
  char zbuf[EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX];
  zbuf[0] = '\0';

  //// current
  epoch_t parsed_epoch;
  TailRecord tail;
  int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
      zbuf, sizeof(zbuf), LOGID, &parsed_epoch, &tail);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(EPOCH_INVALID, parsed_epoch);
  ASSERT_TRUE(tail.isValid());
  ASSERT_EQ(LOGID, tail.header.log_id);
  ASSERT_EQ(LSN_INVALID, tail.header.lsn);
  ASSERT_EQ(0, tail.offsets_map_.getCounter(BYTE_OFFSET));
  ASSERT_EQ(0, tail.header.timestamp);
}

TEST_F(LastCleanEpochStoreFormatTest, ParsingCurrentFormat) {
  char zbuf[EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX];
  const epoch_t lce = epoch_t(3334462);
  for (int i = 0; i < 3; ++i) {
    TailRecord tail;
    if (i != 2) {
      tail = genTailRecord(i % 2 == 0);
    } else {
      // represent a empty log
      tail.header.log_id = LOGID;
      tail.offsets_map_.setCounter(BYTE_OFFSET, 0);
    }
    ld_check(tail.isValid());

    const size_t expected_size =
        EpochStoreLastCleanEpochFormat::sizeInLinearBuffer(lce, tail);

    int written = EpochStoreLastCleanEpochFormat::toLinearBuffer(
        zbuf, sizeof(zbuf), lce, tail);

    ASSERT_GT(written, 0);
    ASSERT_EQ(expected_size, written);
    ASSERT_LE(written, sizeof(zbuf));
    epoch_t parsed_epoch;
    TailRecord parsed_tail;
    int rv = EpochStoreLastCleanEpochFormat::fromLinearBuffer(
        zbuf, written, LOGID, &parsed_epoch, &parsed_tail);
    ASSERT_EQ(0, rv);
    ASSERT_EQ(lce, parsed_epoch);
    ASSERT_TRUE(parsed_tail.isValid());
    ASSERT_TRUE(parsed_tail.sameContent(tail));
  }
}

} // namespace
