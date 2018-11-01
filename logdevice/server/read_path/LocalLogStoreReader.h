/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>

#include <boost/noncopyable.hpp>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/SCDCopysetReordering.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

// Reason why the read was initiated.
enum class CatchupEventTrigger : uint8_t {
  // New records were released.
  RELEASE = 0,
  OTHER = 1,
};

/**
 * @file Logic to read batches of records from the local log store, subject to
 *       constraints on LSN, total size in bytes etc.
 */

class IteratorCache;

/**
 * Raw record read from the local log store.  The blob is the serialized
 * header+value combo, to be parsed by LocalLogStoreRecordFormat::parse().
 */
class RawRecord : boost::noncopyable {
 public:
  RawRecord(lsn_t lsn,
            Slice blob,
            bool owned,
            bool from_under_replicated_region = false)
      : lsn(lsn),
        blob(blob),
        owned(owned),
        from_under_replicated_region(from_under_replicated_region) {}

  ~RawRecord() {
    if (owned) {
      std::free(const_cast<void*>(blob.data));
    }
  }

  RawRecord(RawRecord&& other) noexcept
      : lsn(other.lsn),
        blob(other.blob),
        owned(other.owned),
        from_under_replicated_region(other.from_under_replicated_region) {
    other.lsn = LSN_INVALID;
    other.blob = Slice();
    other.owned = false;
    other.from_under_replicated_region = false;
  }

  lsn_t lsn;
  Slice blob;
  bool owned;
  bool from_under_replicated_region;
};

namespace LocalLogStoreReader {

struct ReadPointer {
  lsn_t lsn;

  bool operator<(const ReadPointer& y) const {
    return lsn < y.lsn;
  }
};

/**
 * Bundles information about what and how much to read.
 */
struct ReadContext {
  ReadContext(logid_t logid,
              ReadPointer read_ptr,
              lsn_t until_lsn,
              lsn_t window_high,
              std::chrono::milliseconds ts_window_high,
              lsn_t last_released_lsn,
              size_t max_bytes_to_deliver,
              bool first_record_any_size,
              bool is_rebuilding,
              std::shared_ptr<LocalLogStore::ReadFilter> lls_filter,
              CatchupEventTrigger catchup_reason)
      : logid_(logid),
        read_ptr_(read_ptr),
        until_lsn_(until_lsn),
        window_high_(window_high),
        ts_window_high_(ts_window_high),
        last_released_lsn_(last_released_lsn),
        max_bytes_to_deliver_(max_bytes_to_deliver),
        first_record_any_size_(first_record_any_size),
        rebuilding_(is_rebuilding),
        lls_filter_(std::move(lls_filter)),
        catchup_reason_(catchup_reason) {}

  // Log ID being read from.
  logid_t logid_;
  // Requesting first record >= this lsn.
  ReadPointer read_ptr_;
  // Max LSN requested by client.
  lsn_t until_lsn_;
  // Max LSN client will accept at the moment.
  lsn_t window_high_;
  // Max timestamp that can be read at the moment.
  // This is only enforced by it_stats_, and only approximately (based on
  // logsdb partition timestamps).
  // checkBatchComplete() doesn't check it.
  // If we reach the end of the window, the timestamp of the next record past
  // the window is written here.
  std::chrono::milliseconds ts_window_high_;
  // Max released LSN. Records with greater LSNs will not be delivered.
  lsn_t last_released_lsn_;
  // max amount of bytes to be delivered.
  size_t max_bytes_to_deliver_;
  // Should the first record be delivered even if it is bigger than
  // `max_bytes_to_deliver_`?
  bool first_record_any_size_;
  // True if the read is being done as part of rebuilding
  bool rebuilding_{false};
  // Filter to be used for filtering records that should not be sent.
  std::shared_ptr<LocalLogStore::ReadFilter> lls_filter_;
  // A reason of the current catchup
  CatchupEventTrigger catchup_reason_;
  // Iterator statistics. Reset by LocalLogStoreReader::read().
  // It duplicates some of the stopping conditions, e.g.
  // the lsn in it_stats_.stop_reading_after is usually set to
  // min(until_lsn_, window_high_, last_released_lsn_).
  // However, some users of ReadContext don't use it_stats_, so we shouldn't
  // rely on it being in sync with the rest of ReadContext.
  LocalLogStore::ReadStats it_stats_;

  /**
   * Should we not send the current message and any other messages, because
   * we've reached max_bytes_to_deliver_?
   *
   * @param nrecords If first_record_any_size_, used to determine if this is the
   * first record (nrecords == 0).  Otherwise unused.
   *
   * @param bytes_delivered Number of bytes we've delivered before this message.
   *
   * @param msg_size Number of bytes in this message.
   *
   */
  bool byteLimitReached(int nrecords,
                        size_t bytes_delivered,
                        size_t msg_size) const {
    if (first_record_any_size_ && nrecords == 0) {
      return false;
    }
    return bytes_delivered + msg_size > max_bytes_to_deliver_;
  }

  /**
   * Determine if we are done with this batch and why.
   * read_ptr_ should be updated before the call.
   * If the returned Status is not OK, the current record (read_ptr_) shouldn't
   * be sent.
   *
   * We may update read_ptr_ if we're CAUGHT_UP, UNTIL_LSN_REACHED,
   * or WIDNOW_END_REACHED, but not otherwise.
   *
   * @return
   *   E::FAILED             if the iterator is in an error state.
   *   E::WOULDBLOCK         if record at the current position of the iterator
   *                         cannot be read without blocking
   *   E::CAUGHT_UP          if we reached the end of the log, but more readable
   *                         records could appear with subsequent writes.
   *   E::UNTIL_LSN_REACHED  if we reached until_lsn.
   *   E::WINDOW_END_REACHED if we reached the end of the window.
   *   E::OK                 if no conditions above are satisfied, and we can
   *                         continue processing records in this batch.
   */
  Status checkBatchComplete(IteratorState state);

  std::string toString() const;
};

/**
 * Interface for callbacks to process data as it is being read from the
 * local log store.
 */
class Callback {
 public:
  /**
   * Process a single record.  This will be called by read() zero or more
   * times as it reads records from the local log store.  The records will
   * have increasing LSNs.
   *
   * @return On success, return 0.  On failure, return -1 in which case read()
   *         will stop reading with a status of either E::ABORTED or
   *         E::CBREGISTERED.
   */
  virtual int processRecord(const RawRecord& record) = 0;

  virtual ~Callback() {}
};

/**
 * Reads a batch of records from a local log store.  Results are communicated
 * through the provided callback instance, which must remain valid for the
 * duration of the method.  callback->processRecord() will be called zero or
 * more times, followed by one final callback->done().
 *
 * @param iterator                 an iterator used to read data for a log; the
 *                                 iterator is modified by this function
 * @param callback                 callback instance to pass results to
 * @param read_ctx                 @see ReadContext.
 *                                 read_ctx->read_ptr_ will be advanced by
 *                                 this function up to the next lsn to be read
 *                                 following this batch of records, which is not
 *                                 necessarily the lsn of the last record sent +
 *                                 1 but can be greater than that if we
 *                                 determine for sure that there are no more
 *                                 records up to a certain lsn.
 *
 * @return
 * Status will be one of:
 * - E::CAUGHT_UP (after 0+ records): delivered all (possibly none) records
 *   with LSN >= from_lsn, but new ones may get added
 * - E::WINDOW_END_REACHED (after 0+ records): delivered all (possibly none)
 *   records with LSN <= window_high
 * - E::UNTIL_LSN_REACHED (after 0+ records): delivered all (possibly none)
 *   records with LSN in [from_lsn, until_lsn] and new ones will not get
 *   added
 * - E::BYTE_LIMIT_REACHED (after 0+ records): size of next record to
 *   deliver exceeds the limit on the amount of bytes to be delivered.
 * - E::PARTIAL: (after 0+ records): exceeded the limit on the amount of bytes
 *   read but not necessarily delivered, or exceeded execution time limit.
 * - E::WOULDBLOCK (after 0+ records): no more data could be returned
 *   without performing blocking I/O, which was forbidden
 * - E::FAILED (after 0+ records): unrecoverable error such as data corruption
 * - E::ABORTED (after 0+ records): callback processRecord() signalled failure
 * - E::CBREGISTERED (after 0+ records): callback processRecord() signalled
 *   lack of bandwidth to process the record.
 *
 * For statuses WINDOW_END_REACHED and CAUGHT_UP, `read_ptr' is set to
 * an LSN such that there are no more records (other than those already
 * delivered) in the local log store before  that LSN.
 *
 * For example, if records {1, 2, 5} are currently available, from_lsn = 1, and
 * last_released_lsn = 3, this function will return CAUGHT_UP, pass records
 * {1, 2} to `callback', and set `read_ctx_->read_ptr' to 4 (since 3 was last
 * released, a record with LSN 4 might still end up being stored on this node).
 */
Status read(LocalLogStore::ReadIterator& iterator,
            Callback& callback,
            ReadContext* read_ctx,
            StatsHolder* stats,
            const Settings& settings);

/**
 * Utility function that finds the last known good ESN for a given epoch by
 * extracting it from a data record.
 *
 * @param iterator  an iterator used to read data for a log; the iterator is
 *                  modified by this function
 * @param epoch     epoch to get the value of LNG for
 * @param lng_out   output param; on success, set to the LNG for the specified
 *                  epoch
 * @param last_record_out  output param; on success, set to the ESN of the last
 *                         record of this epoch found in local log store
 * @param last_record_timestamp_out output param; on success, set to timestamp
 *                  of the last record of the epoch found in local log store
 * @param next_non_empty_epoch    output param; if not nullptr, on success or
 *                                empty epoch (ret -1 err == E::NOTFOUND), set
 *                                to the next higher epoch that is not empty,
 *                                or EPOCH_INVALID if there is no records with
 *                                epoch higher than @param epoch stored for
 *                                the log
 *
 * @return          On success, returns 0 and updates `lng_out'. Otherwise,
 *                  returns -1 and sets err to:
 *                  NOTFOUND          local log store has no records in `epoch'
 *                  MALFORMED_RECORD  parse error
 *                  FAILED            `iterator' moved to an invalid state
 *                  WOULDBLOCK        iterator was created with
 *                                    !allow_blocking_io and is outside cached
 *                                    range
 */
int getLastKnownGood(LocalLogStore::ReadIterator& iterator,
                     epoch_t epoch,
                     esn_t* lng_out,
                     esn_t* last_record_out,
                     std::chrono::milliseconds* last_record_timestamp_out,
                     epoch_t* next_non_empty_epoch = nullptr);

/**
 * Utilituy function that gets the tail record for an epoch, given the LNG
 * of the epoch.
 *
 * @param iterator   an iterator used to read data for a log; the iterator is
 *                   modified by this function
 * @param epoch      epoch to get the tail record for
 * @param lng        last known good (per-epoch released) ESN of the epoch,
 *                   used to determine the tail record
 * @param tail_out   output param; on success, set to the TailRecord found
 *
 * @include_payload  if set, include payload in the TailRecord returned
 *
 * @return           On success, return 0 and updates `tail_out'. Otherwise,
 *                   returns -1 and sets err to:
 *                  NOTFOUND          local log store has no records in `epoch'
 *                  MALFORMED_RECORD  parse error
 *                  FAILED            `iterator' moved to an invalid state
 *                  WOULDBLOCK        iterator was created with
 *                                    !allow_blocking_io and is outside cached
 *                                    range
 */
int getTailRecord(LocalLogStore::ReadIterator& iterator,
                  logid_t log_id,
                  epoch_t epoch,
                  esn_t lng,
                  TailRecord* tail_out,
                  bool include_payload = false);

} // namespace LocalLogStoreReader

}} // namespace facebook::logdevice
