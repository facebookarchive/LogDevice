/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochStoreLastCleanEpochFormat.h"

#include <cstdio>
#include <cstring>

#include "logdevice/common/EpochStoreEpochMetaDataFormat.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

namespace EpochStoreLastCleanEpochFormat {

int fromLinearBuffer(const char* buf,
                     size_t buf_len,
                     logid_t log_id,
                     epoch_t* epoch_out,
                     TailRecord* tail_out) {
  ld_check(buf);
  uint64_t parsed_epoch;
  uint64_t parsed_last_released_real_lsn = 0;
  uint64_t parsed_last_timestamp = 0;
  uint64_t parsed_epoch_end_offset = 0;
  bool got_tail_record = false;
  TailRecord parsed_tail_record;
  epoch_t epoch(EPOCH_INVALID);

  if (buf_len > EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  // null-terminate buf so that we can safely use sscanf(3)
  char val[EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX + 1];
  memcpy(val, buf, buf_len);
  val[buf_len] = '\0';

  if (val[0]) {
    int size_parsed = 0;

    // string can be stored as
    // "lce@offset@last_released_real_lsn@last_timestamp#tail_record", OR
    // "lce@offset@last_released_real_lsn@last_timestamp" (legacy)
    int nfields = sscanf(val,
                         "%lu@%lu@%lu@%lu%n",
                         &parsed_epoch,
                         &parsed_epoch_end_offset,
                         &parsed_last_released_real_lsn,
                         &parsed_last_timestamp,
                         &size_parsed);

    if (nfields < 3 || size_parsed <= 0 || size_parsed > buf_len) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Invalid epoch store LCE content for log %lu. Expected "
                      "format "
                      "lce@offset@last_released_real_lsn@last_timestamp. "
                      "Got '%s'. nfields %d, size_parsed %d.",
                      log_id.val_,
                      error_text_sanitize(val),
                      nfields,
                      size_parsed);
      err = E::BADMSG;
      return -1;
    }

    if (parsed_epoch < EPOCH_MIN.val_ || parsed_epoch > EPOCH_MAX.val_) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Invalid epoch in epoch store lce content for log %lu. "
                      "bad epoch: %lu. LCE entry content '%s'.",
                      log_id.val_,
                      parsed_epoch,
                      error_text_sanitize(val));
      err = E::BADMSG;
      return -1;
    }

    if (parsed_epoch_end_offset == BYTE_OFFSET_INVALID) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Invalid byte offset value found in epoch store for "
                        "epoch %lu log %lu. Byte offset is going to be reset "
                        "to 0. This is not expected unless byte offset "
                        "feature was just turned on.",
                        parsed_epoch,
                        log_id.val_);
      parsed_epoch_end_offset = 0;
    }

    epoch = epoch_t(parsed_epoch);

    // parse the (optional) tail record in binary format
    if (size_parsed < buf_len && val[size_parsed] == '#') {
      int rv = parsed_tail_record.deserialize(
          {val + size_parsed + 1, (size_t)buf_len - size_parsed - 1});
      if (rv < 0 || !parsed_tail_record.isValid()) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Invalid tail record in epoch store lce content for "
                        "log %lu. LCE entry content: %s.",
                        log_id.val_,
                        hexdump_buf(val, buf_len).c_str());
        err = E::BADMSG;
        return -1;
      }

      if (parsed_tail_record.header.flags &
          TailRecordHeader::OFFSET_WITHIN_EPOCH) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Tail record in epoch store content for log %lu "
                        "should contain accumulative byte offsets rather "
                        "than epoch offset!. LCE entry content: %s.",
                        log_id.val_,
                        hexdump_buf(val, buf_len).c_str());
        err = E::BADMSG;
        return -1;
      }
      got_tail_record = true;
    }
  }

  if (epoch_out) {
    *epoch_out = epoch;
  }

  if (tail_out) {
    if (got_tail_record) {
      *tail_out = parsed_tail_record;
    } else {
      // we got an entry with legacy format, construct a TailRecord from
      // the legacy tail attribute values.
      OffsetMap offsets;
      offsets.setCounter(BYTE_OFFSET, parsed_epoch_end_offset);
      tail_out->reset({log_id,
                       lsn_t(parsed_last_released_real_lsn),
                       parsed_last_timestamp,
                       {BYTE_OFFSET_INVALID /* deprecated, offsets_within_epoch used instead */},
                       /*flags*/ 0,
                       {}},
                      std::shared_ptr<PayloadHolder>(),
                      std::move(offsets));
    }
  }

  return 0;
}

int sizeInLinearBuffer(epoch_t lce, const TailRecord& tail) {
  if (lce < EPOCH_MIN || lce > EPOCH_MAX || !tail.isValid() ||
      (tail.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH)) {
    err = E::INVALID_PARAM;
    return -1;
  }
  int text_section_size = snprintf(nullptr,
                                   0,
                                   "%u@%lu@%lu@%lu#",
                                   lce.val_,
                                   tail.offsets_map_.getCounter(BYTE_OFFSET),
                                   tail.header.lsn,
                                   tail.header.timestamp);

  ld_check(text_section_size > 0);
  return text_section_size + tail.sizeInLinearBuffer();
}

int toLinearBuffer(char* buf,
                   size_t buf_len,
                   epoch_t lce,
                   const TailRecord& tail) {
  if (lce < EPOCH_MIN || lce > EPOCH_MAX || !tail.isValid() ||
      (tail.header.flags & TailRecordHeader::OFFSET_WITHIN_EPOCH)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Invalid input! lce given: %u.",
                    lce.val_);
    err = E::INVALID_PARAM;
    return -1;
  }

  int required_size = sizeInLinearBuffer(lce, tail);
  if (buf_len < required_size) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Not enough buffer space for serialization. required %d, "
                    "actual buffer size: %lu",
                    required_size,
                    buf_len);
    err = E::NOBUFS;
    return -1;
  }
  int text_section_size = snprintf(buf,
                                   buf_len,
                                   "%u@%lu@%lu@%lu#",
                                   lce.val_,
                                   tail.offsets_map_.getCounter(BYTE_OFFSET),
                                   tail.header.lsn,
                                   tail.header.timestamp);

  ld_check(text_section_size > 0);
  ld_check(text_section_size <= buf_len);
  int tail_len =
      tail.serialize(buf + text_section_size, buf_len - text_section_size);

  ld_check(tail_len > 0);
  ld_check(text_section_size + tail_len == required_size);
  return text_section_size + tail_len;
}

}}} // namespace facebook::logdevice::EpochStoreLastCleanEpochFormat
