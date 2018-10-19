/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Further detailed explanations of each error scenario can be found at
 * https://fb.quip.com/bD3KAPFDNewb
 */
#include "logdevice/lib/verifier/ReadVerifyData.h"

#include <chrono>
#include <exception>
#include <string.h>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/verifier/VerificationDataStructures.h"

namespace facebook { namespace logdevice {

ReadVerifyData::ReadVerifyData() {}

// Checks the data verification parameters for errors, and if it finds any,
// reports them. Will eventually use Scuba, but using returned error codes
// for now. Requires that the block of memory pointed to by p is not freed
// during execution of this function.
// Note: vai is from Reader::read(), and is the VerificationAppendInfo of the
// record we are *currently* checking.
void ReadVerifyData::checkForErrors(
    logid_t log_id,
    const Payload& payload,
    const VerificationData& vd,
    VerificationAppendInfo vai,
    std::vector<VerificationFoundError>& errors_out) {
  std::pair<uint64_t, logid_t> p(vd.vdh.writer_id, log_id);

  VerificationFoundError vfe;
  if (vd.vdh.payload_checksum != folly::hash::fnv32(payload.toString())) {
    vfe.vrs = VerificationRecordStatus::CHECKSUM_MISMATCH;
    vfe.error_record = std::make_pair(vd.vdh.sequence_num, vai);
    errors_out.push_back(vfe);
    return;
  }

  auto& curr_unacked_window = unacked_windows_[p];
  auto& curr_trimmed_cache = trimmed_cache_[p];
  auto& curr_acked_unread = acked_unread_[p];

  // If we see the sequence number of the record we are currently reading in the
  // window of unacked records, it means that the record was duplicated, because
  // we add a record to unacked_windows_ exactly when we read it, so we are
  // reading the same record twice.
  auto curr_it = curr_unacked_window.find(vd.vdh.sequence_num);
  if (curr_it != curr_unacked_window.end()) {
    vfe.vrs = VerificationRecordStatus::DUPLICATE;
    vfe.error_record = std::make_pair(vd.vdh.sequence_num, vai);
    vfe.error_discovery_record =
        std::make_pair(curr_it->first, curr_it->second.second);
    errors_out.push_back(vfe);
    return;
  }

  // If the sequence number is below the left end of the window, then it's
  // either a duplicate or an append failure. If the append
  // is acked as failure, it means that it was not assigned an lsn and therefore
  // cannot be read, so it would never appear in the unacked_windows_ for that
  // log. (Aside: an append acked as failure still counts as an append
  // completion, so that record's sequence number would still get put in the
  // to-ack-list in the writer.)
  //
  // Therefore, we keep a (later, size-capped) cache containing ALL sequence
  // numbers we have trimmed from the left end of the window (up to a certain
  // point at which we trim the cache). If the current record (being read)'s
  // sequence number is in the cache, then surely it's a duplicate because we
  // must have read it before in order to have added it to the window. If the
  // current record's sequence number is within the cache range but is not in
  // the cache, then it surely is not a duplicate because the only way it is not
  // in the cache when it should have been (being in range) is if the append
  // failed and hence the record's sequence number was never read and therefore
  // is not in the cache. If the current record's sequence number is beyond
  // (i.e. lower than) the cache range then we don't know, but assume that it is
  // not a duplicate.
  auto trimmed_it = curr_trimmed_cache.find(vd.vdh.sequence_num);
  if (trimmed_it != curr_trimmed_cache.end()) {
    vfe.vrs = VerificationRecordStatus::DUPLICATE;
    vfe.error_record = std::make_pair(vd.vdh.sequence_num, vai);
    vfe.error_discovery_record = *trimmed_it;
    errors_out.push_back(vfe);
    return;
  }

  // If we have reached here, then we are seeing this record for the first time.
  // If we read a record for the first time whose sequence num was already acked
  // (i.e. callback already executed), then it is reordering because
  // we will have seen that record in an ack list before actually reading that
  // record. Explanation: records are moved onto the to-ack list only after
  // their append callbacks, so if record y's ack list--a snapshot of the to-ack
  // list at the time append(y) was called--contains record x, then append(y)
  // must have been called AFTER append_callback of x completed, and LogDevice
  // therefore guarantees that record y MUST come after record x in the log
  // unless there is reordering. Hence, if we see a record y with an ack list
  // containing a record x that we haven't read (i.e. meaning that y comes
  // before x in the log), then if we see x at all later, x must have been
  // reordered. If we never see x, then it is data loss (see below).
  auto reordering_it = curr_acked_unread.find(vd.vdh.sequence_num);
  if (reordering_it != curr_acked_unread.end()) {
    // Found reordering. Before finishing, check whether there is a LSN_MISMATCH
    // as well.
    if (reordering_it->second.error_record.second.lsn != vai.lsn) {
      VerificationFoundError vfe_lsn_mismatch;
      vfe_lsn_mismatch.vrs = VerificationRecordStatus::LSN_MISMATCH;
      vfe_lsn_mismatch.error_record = reordering_it->second.error_record;
      vfe_lsn_mismatch.error_discovery_record =
          std::make_pair(vd.vdh.sequence_num, vai);
      errors_out.push_back(vfe_lsn_mismatch);
    }

    // curr_acked_unread already has the error stored for us to return.
    errors_out.push_back(reordering_it->second);
    return;
  }

  // If still no errors at this point, we add the Record to our window. Note
  // that adding to our window below the left end is allowed, as it may be
  // the Client retrying a failed append as explained above.
  curr_unacked_window[vd.vdh.sequence_num] = std::make_pair(false, vai);

  // Check the ack list of the record that we just added to the window. If we
  // see a record in an ack list before actually reading that record, regardless
  // of whether it was acked as success or failure, then that record will be
  // marked as lost (and if we see that record later we mark it as reordering
  // as explained above).
  for (const std::pair<vsn_t, lsn_t>& curr_ack : vd.ack_list) {
    auto uw_it = curr_unacked_window.find(curr_ack.first);
    if (uw_it == curr_unacked_window.end() &&
        !curr_trimmed_cache.count(curr_ack.first)) {
      // The append of this sequence number was acked (whether as success or
      // failure), but we have not seen the corresponding record yet. Therefore,
      // if we ever see the corresponding record, it is a reordering error,
      // because a record x that is acked (whether as success or failure) by the
      // time append(y) is called should come before record y. Store this
      // as a reordering error in acked_unread_ so that we can report this
      // reordering error if we ever see record x later.
      VerificationAppendInfo lost_vai;
      lost_vai.lsn = curr_ack.second;
      vfe.vrs = VerificationRecordStatus::REORDERING;
      vfe.error_record = std::make_pair(curr_ack.first, lost_vai);
      vfe.error_discovery_record = std::make_pair(vd.vdh.sequence_num, vai);
      curr_acked_unread[curr_ack.first] = vfe;

      // IF THE RECORD WAS ACKED AS SUCCESS, then this is data loss. Note that
      // in this case, the record being checked is fine; instead, the record
      // with sequence number = curr_ack.first is the one that was lost.
      // TODO: adjust for cache trimming. If it's less than the cache lower
      // bound, still assume it's not an error. Also, if it's less than the
      // log trim point or the starting lsn, it won't be marked as error
      // either.
      if (curr_ack.second != LSN_INVALID) {
        vfe.vrs = VerificationRecordStatus::DATALOSS;
        errors_out.push_back(vfe);
      }
    }
    if (uw_it != curr_unacked_window.end() && !uw_it->second.first) {
      // We found the sequence number corresponding to the ack in the window,
      // and it still hasn't been acked yet. Hence, mark it as acked. (Whether
      // it's acked as success or failure is determined by its lsn, which we
      // check for accuracy below.)
      uw_it->second.first = true;

      // Check to see that LSN from Reader::read() matches the LSN from
      // verification data. May also include other checks (e.g. timestamp)
      // here in the future. Note that if the LSN from the ack list is invalid
      // (i.e. acked as failure) this is not an error.
      if (uw_it->second.second.lsn != curr_ack.second &&
          curr_ack.second != LSN_INVALID) {
        vfe.vrs = VerificationRecordStatus::LSN_MISMATCH;

        // Mark as a "reader mismatch" which presently is just an LSN mismatch
        // but in the future will include mismatched timestamps etc.
        VerificationAppendInfo ack_vai;
        ack_vai.lsn = curr_ack.second;
        vfe.error_record = std::make_pair(curr_ack.first, ack_vai);
        vfe.error_discovery_record =
            std::make_pair(uw_it->first, uw_it->second.second);
        errors_out.push_back(vfe);
      }
    }
  }

  // Trim the window and check for lost acks.
  while (true) {
    if (curr_unacked_window.begin()->second.first) {
      curr_trimmed_cache[curr_unacked_window.begin()->first] =
          curr_unacked_window.begin()->second.second;
      curr_unacked_window.erase(curr_unacked_window.begin()); // Trim window
      continue;
    }
    if (curr_unacked_window.begin()->first < vd.vdh.oldest_in_flight) {
      VerificationFoundError curr_vfe;
      curr_vfe.vrs = VerificationRecordStatus::LOST_ACK;
      curr_vfe.error_record =
          std::make_pair(curr_unacked_window.begin()->first,
                         curr_unacked_window.begin()->second.second);
      errors_out.push_back(curr_vfe);
      curr_unacked_window.erase(curr_unacked_window.begin());
      continue;
    }
    break;
  }
}

// Note that the unique_ptr to DataRecord that is passed to this function will
// actually point to an instance of DataRecordOwnsPayload.
// Returns a VerificationResult.
std::unique_ptr<DataRecord> ReadVerifyData::verifyRestoreRecordPayload(
    std::unique_ptr<DataRecord> dr,
    std::vector<VerificationFoundError>& errors_out) {
  logid_t log_id = dr->logid;
  DataRecordOwnsPayload* drop_ptr =
      dynamic_cast<DataRecordOwnsPayload*>(dr.get());
  ld_check(drop_ptr != nullptr);

  VerExtractionResult ver =
      VerificationDataStructures::extractVerificationData(dr->payload);

  if (ver.ves == VerExtractionStatus::NO_VERIFICATION) {
    return dr;
  }

  VerificationAppendInfo vai;
  vai.lsn = dr->attrs.lsn;
  // Later, insert other info gotten from Reader::read() into vai here.

  if (ver.ves == VerExtractionStatus::MALFORMED_VER_DATA) {
    VerificationFoundError vfe;
    vfe.vrs = VerificationRecordStatus::MALFORMED_VER_DATA;
    vfe.error_record = std::make_pair(ver.vd.vdh.sequence_num, vai);
    errors_out.push_back(vfe);
    // Cannot remove the verification data from the payload if it's malformed.
    return dr;
  }

  checkForErrors(log_id, ver.user_payload, ver.vd, vai, errors_out);

  // Extract the original payload.
  // Note that we must perform a payload duplication here.
  // This is because the Reader's read() function returns unique_ptr's to
  // instances of DataRecordOwnsPayload upcasted to DataRecords. Hence, given_dr
  // points to a DataRecordOwnsPayload. We need to return a DataRecord that
  // points to the actual (non-verification-data-prepended) payload, but we
  // cannot modify the DataRecordOwnsPayload's payload field. Therefore, we must
  // instantiate a new DataRecordOwnsPayload with the correct payload.
  // TODO: use a shared_ptr<void> owning dr, to avoid needing to duplicate
  // the payload.
  Payload p2 = ver.user_payload.dup();
  return std::make_unique<DataRecordOwnsPayload>(drop_ptr->logid,
                                                 std::move(p2),
                                                 drop_ptr->attrs.lsn,
                                                 drop_ptr->attrs.timestamp,
                                                 drop_ptr->flags_);
}

}} // namespace facebook::logdevice
