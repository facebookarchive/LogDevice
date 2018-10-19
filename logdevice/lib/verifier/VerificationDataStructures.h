/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>

#include <folly/SharedMutex.h>

#include "logdevice/common/toString.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

typedef uint64_t vsn_t;

const uint64_t vflag_ = 17272412700594187103ull;

enum class VerExtractionStatus : int {
  OK = 0,

  // Set to agree with VerificationRecordStatus values below.
  MALFORMED_VER_DATA = 98,
  NO_VERIFICATION = 99
};

struct VerificationDataHeader {
  uint64_t magic_number;
  uint32_t version = 0;
  uint32_t payload_pos;
  uint32_t payload_size;
  uint32_t ack_list_length;
  uint64_t writer_id;
  vsn_t sequence_num;
  vsn_t oldest_in_flight;
  uint32_t payload_checksum;
  uint32_t dummyfield; // To get multiple of 8 bytes.
};

struct VerificationData {
  VerificationDataHeader vdh;
  std::vector<std::pair<vsn_t, lsn_t>> ack_list;
};

struct VerExtractionResult {
  VerExtractionStatus ves;
  VerificationData vd;
  Payload user_payload;
};

// Various possible statuses after verification.
// Note that each VerificationRecordStatus corresponds to a single record that
// is read and checked. Therefore, VerificationRecordStatus only encodes
// errors corresponding to that individual record. In the process of checking
// errors in other records may be discovered (e.g. data loss), in which case
// VerificationRecordStatus should be set to OTHER_RECORDS_ERROR, and the
// appropriate information about those other incorrect records should be set
// in VerificationResult.
enum class VerificationRecordStatus : int {
  DATALOSS = 1,
  REORDERING = 2,
  DUPLICATE = 3,
  CHECKSUM_MISMATCH = 4,
  LSN_MISMATCH = 5, // Some info from Reader::read() doesn't match ack list.
  LOST_ACK = 6,

  MALFORMED_VER_DATA = 98,
};

// Struct containing information on a record describing its append, returned by
// Reader::read(). This can then be checked against its verification data.
// Currently only contains an LSN field, but can later be
// modified to contain other fields (e.g. timestamp)
// Note: the LSN field will denote the LSN from Reader::read() in
// unacked_windows_ and trimmed_cache_. However, in acked_unread_, the LSN will
// denote the LSN from the ack.
struct VerificationAppendInfo {
  lsn_t lsn;

  bool operator==(const VerificationAppendInfo& vai1) const {
    return lsn == vai1.lsn;
  }
};

// Struct that encodes an error instance discovered through the verification
// framework.
// Summary of Behavior.
// vrs = DATALOSS: error_record = the record that was lost;
// error_discovery_record = the record whose ack list contained the lost record.
// vrs = REORDERING: error_record = the reordered record, error_discovery_record
// = empty (since there could be many records whose ack list contained the
// reordered record; all we know is that the reordered record was not seen at
// the time that we read at least one ack list containing it)
// vrs = DUPLICATE: error_record = the duplicated record, error_discovery_record
// = the already existing record that got duplicated.
// vrs = CHECKSUM_MISMATCH: error_record = the checksum-mismatched record;
// error_discovery_record = empty.
// vrs = MALFORMED_VER_DATA: error_record = the malformed record;
// error_discovery_record = empty.
// vrs = VAI_MISMATCH: error_record = the record info from an ack list,
// error_discovery_record = the record info from Reader::read()
// vrs = LOST_ACK: error_record = the sequence number + LSN of the lost ack,
// error_discovery_record = empty.
//
struct VerificationFoundError {
  VerificationRecordStatus vrs = {};
  std::pair<vsn_t, VerificationAppendInfo> error_record = {};
  std::pair<vsn_t, VerificationAppendInfo> error_discovery_record = {};
};

// Struct returned by ReadVerifyData::checkForErrors.
// Note that vrs only encodes any errors of the DataRecord pointed to by dr
// (e.g. reording or duplicate). If in the process of checking dr, we find
// errors in other records (e.g. data loss), vrs is set to
// VerificationRecordStatus::OTHER_RECORDS_ERROR and we add the information to
// the suitable field.

typedef std::function<void(const VerificationFoundError& vfe)> error_callback_t;

class VerificationDataStructures {
 public:
  // Takes a VerificationData object and writes it into a string
  // buffer which can then be directly concatenated with the user's payload
  // string.
  // If buffer_ptr is not a nullptr, writes vd into it and returns the number of
  // bytes that have been written. Else, does a "dry run" to calculate the size
  // of vd in bytes and returns it.
  static size_t serializeVerificationData(const VerificationData& vd,
                                          std::string* buffer_ptr);

  // Takes a Payload pointing to a chunk of memory containing a
  // VerificationDataWithAckList and a user's payload string, and extracts each.
  // Returns an VerExtractionResult object containing the verification data's
  // status (OK, malformed, or none), the extracted VerificationData object
  // (which will be empty if the record's verification status is malformed or
  // nonexistent), and a Payload pointing to the user's payload (which will
  // simply point to the same location as the original Payload p if verification
  // is not being used, or if verification data is malformed).
  static VerExtractionResult extractVerificationData(const Payload& p);
};
}} // namespace facebook::logdevice
