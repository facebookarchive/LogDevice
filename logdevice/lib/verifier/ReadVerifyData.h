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
#include <unordered_set>
#include <utility>

#include <folly/SharedMutex.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/toString.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/GenVerifyData.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"

namespace facebook { namespace logdevice {

class ReadVerifyData {
 public:
  // Takes in a DataRecord. If this DataRecord isn't using verification, will
  // return the given dr (which is a DataRecordOwnsPayload if called on a return
  // value from Reader::read()). Else, will extract verification data, check it
  // for errors using the checkForErrors function, and return a unique_ptr to a
  // DataRecordOwnsPayload, whose payload is the customer's original payload
  // without any verification data prepended. Any errors will be reported as
  // VerificationFoundError instances in errors_out.
  // Note that the same error may be reported multiple times in errors_out
  // (e.g a data loss being discovered from multiple other records' ack lists
  // will be reported by each of those records).
  // Note that if called on the return values of read(), then
  // given_dr will actually be pointing to an instance of DataRecordOwnsPayload.
  std::unique_ptr<DataRecord>
  verifyRestoreRecordPayload(std::unique_ptr<DataRecord> dr,
                             std::vector<VerificationFoundError>& errors_out);

  ReadVerifyData();

 private:
  // Checks a payload and verification data for errors. If any errors are
  // discovered, they are inserted as VerificationFoundError instances in
  // errors_out. The same error may be inserted multiple times into errors_out
  // as it is discovered through multiple different records' ack lists.
  void checkForErrors(logid_t log_id,
                      const Payload& p,
                      const VerificationData& vd,
                      VerificationAppendInfo vai,
                      std::vector<VerificationFoundError>& errors_out);

  // The reader will maintain a separate state for each Client-log pair. Hence
  // the identifier used in this unordered map is a pair of uint64_t (for the
  // Client's writer ID) and log ID. Each Client-log pair is mapped to an
  // ordered map from sequence numbers to a boolean. The reason we are using an
  // ordered map and not, say, a bitset is that we need to store 3 states: not
  // present (i.e. has not been read), present in the list with bool value false
  // (i.e. was read but not acked), and present in the list with
  // bool value true (i.e. was read and was acked).
  // We also store the VerificationAppendInfo FROM THE READER to check against
  // the VerificationAppendInfo from the callback later (they should match).
  // Note that the acked indicator is kept separate from the
  // VerificationAppendInfo because 1) when we check VerificationAppendInfo to
  // see whether it agrees with an ack, we don't want to have to worry about
  // matching the boolean acked indicator, and 2) in certain settings (e.g.
  // trimmed_cache_) we don't need whether or not the record was acked. We use
  // VerificationAppendInfo here rather than just lsn_t so that we can later
  // add other info to help us identify the error record if necessary.
  std::unordered_map<std::pair<uint64_t, logid_t>,
                     std::map<vsn_t, std::pair<bool, VerificationAppendInfo>>>
      unacked_windows_;
  // Cache of all sequence numbers that were acked successfully and therefore
  // trimmed from the unacked_windows_, up to a certain size. Used to check
  // duplicates. Also carries the VerificationAppendInfo FROM THE READER
  // which can be checked against the VerificationAppendInfo FROM THE CALLBACK
  // later.
  std::unordered_map<std::pair<uint32_t, logid_t>,
                     std::map<vsn_t, VerificationAppendInfo>>
      trimmed_cache_;
  // Cache of all sequence numbers that were in an ack list with ack status of
  // "success" (i.e. they were assigned LSN), but were NOT in the window of
  // records (i.e. we never actually read them). This is because, if we ever
  // actually do read such a record later, it is reordering because we will have
  // seen that record in an ack list before actually reading that record.
  // Explanation: records are moved onto the to-ack list only after their append
  // callbacks, so if record y's ack list--a snapshot of the to-ack list at the
  // time append(y) was called--contains record x, then append(y) must have been
  // called AFTER append_callback of x completed, and LogDevice therefore
  // guarantees that record y MUST come after record x in the log unless there
  // is reordering. Hence, if we see a record y with an ack list containing a
  // record x that we haven't read (i.e. meaning that y comes before x in the
  // log) there is reordering.
  // Note that we also store a VerificationFoundError corresponding to each
  // reordered sequence number. Each VerificationFoundError will contain the
  // following field values:
  // vrs: VerificationRecordStatus::REORDERING
  // error_record: {vsn of reordered record, its VerificationAppendInfo}
  // error_discovery_record: {vsn of record x, VerificationAppendInfo of record
  // x}, where record x = any record such that the reordered record was in x's
  // ack list before it was read.
  //
  // A note on definition of reordered records: a record x is defined as
  // reordered if and only if there exists a record y before record x, such that
  // y's ack list contains record x.
  std::unordered_map<std::pair<uint32_t, logid_t>,
                     std::map<vsn_t, VerificationFoundError>>
      acked_unread_;
};
}} // namespace facebook::logdevice
