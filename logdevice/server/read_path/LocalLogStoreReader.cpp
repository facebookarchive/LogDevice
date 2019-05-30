/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/LocalLogStoreReader.h"

#include <algorithm>
#include <memory>

#include <folly/ScopeGuard.h>
#include <folly/small_vector.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/server/read_path/IteratorCache.h"

namespace facebook { namespace logdevice { namespace LocalLogStoreReader {

static Status maybeSendRecord(LocalLogStore::ReadIterator& read_iterator,
                              Callback& callback,
                              ReadContext* read_ctx,
                              int& nrecords,
                              size_t& bytes_delivered) {
  Slice record_blob = read_iterator.getRecord();
  size_t payload_size = record_blob.size;
  size_t msg_size = RECORD_Message::expectedSize(payload_size);
  const lsn_t lsn = read_iterator.getLSN();

  // Have we shipped too much data to the client already?
  if (read_ctx->byteLimitReached(nrecords, bytes_delivered, msg_size)) {
    return E::BYTE_LIMIT_REACHED;
  }

  RawRecord record(lsn,
                   record_blob,
                   false, // unowned, points into LocalLogStore memory
                   read_iterator.accessedUnderReplicatedRegion());

  if (callback.processRecord(record) != 0) {
    if (err != E::CBREGISTERED) {
      err = E::ABORTED;
    }
    return err;
  }
  bytes_delivered += msg_size;
  ++nrecords;
  return E::OK;
}

Status readImpl(LocalLogStore::ReadIterator& read_iterator,
                Callback& callback,
                ReadContext* read_ctx,
                StatsHolder* stats,
                const Settings& settings) {
  ld_check(read_ctx != nullptr);
  ld_check(read_ctx->lls_filter_ != nullptr);

  // Stat initialization and increments at exit
  read_ctx->it_stats_.max_bytes_to_read =
      settings.max_record_bytes_read_at_once < 0
      ? std::numeric_limits<size_t>::max()
      : static_cast<size_t>(settings.max_record_bytes_read_at_once);
  read_ctx->it_stats_.stop_reading_after.first = read_ctx->logid_;
  read_ctx->it_stats_.stop_reading_after.second =
      std::min(std::min(read_ctx->until_lsn_, read_ctx->last_released_lsn_),
               read_ctx->window_high_);
  read_ctx->it_stats_.stop_reading_after_timestamp = read_ctx->ts_window_high_;
  read_ctx->it_stats_.max_execution_time =
      std::min(settings.max_record_read_execution_time,
               std::chrono::milliseconds::max());
  // set the read_start_time
  read_ctx->it_stats_.read_start_time = std::chrono::steady_clock::now();

  STAT_INCR(stats, read_streams_num_ops);
  if (read_ctx->rebuilding_) {
    STAT_INCR(stats, read_streams_num_ops_rebuilding);
  }

  size_t prev_block_bytes_read = read_iterator.getIOBytesUnnormalized();

  SCOPE_EXIT {
    size_t block_bytes_read =
        read_iterator.getIOBytesUnnormalized() - prev_block_bytes_read;

    STAT_ADD(
        stats, read_streams_num_records_read, read_ctx->it_stats_.read_records);
    STAT_ADD(stats,
             read_streams_num_bytes_read,
             read_ctx->it_stats_.read_record_bytes +
                 read_ctx->it_stats_.read_csi_bytes);
    STAT_ADD(stats,
             read_streams_num_record_bytes_read,
             read_ctx->it_stats_.read_record_bytes);
    STAT_ADD(stats,
             read_streams_num_records_filtered,
             read_ctx->it_stats_.filtered_records);
    STAT_ADD(stats,
             read_streams_num_bytes_filtered,
             read_ctx->it_stats_.filtered_record_bytes);
    STAT_ADD(stats,
             read_streams_num_csi_entries_read,
             read_ctx->it_stats_.read_csi_entries);
    STAT_ADD(stats,
             read_streams_num_csi_bytes_read,
             read_ctx->it_stats_.read_csi_bytes);
    STAT_ADD(stats,
             read_streams_num_csi_entries_filtered,
             read_ctx->it_stats_.filtered_csi_entries);
    STAT_ADD(stats,
             read_streams_num_csi_entries_sent,
             read_ctx->it_stats_.sent_csi_entries);
    STAT_ADD(stats, read_streams_block_bytes_read, block_bytes_read);
    if (read_ctx->rebuilding_) {
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_records_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         read_ctx->it_stats_.read_records);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_bytes_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         read_ctx->it_stats_.read_record_bytes +
                             read_ctx->it_stats_.read_csi_bytes);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_record_bytes_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         read_ctx->it_stats_.read_record_bytes);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_csi_entries_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         read_ctx->it_stats_.read_csi_entries);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_csi_bytes_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         read_ctx->it_stats_.read_csi_bytes);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_block_bytes_read_rebuilding,
                         read_iterator.getStore()->getShardIdx(),
                         block_bytes_read);
      STAT_ADD(stats,
               read_streams_num_records_filtered_rebuilding,
               read_ctx->it_stats_.filtered_records);
      STAT_ADD(stats,
               read_streams_num_bytes_filtered_rebuilding,
               read_ctx->it_stats_.filtered_record_bytes);
    }
  };

  ld_spew("Starting batch: log %lu, read_ptr %s",
          read_ctx->logid_.val_,
          lsn_to_string(read_ctx->read_ptr_.lsn).c_str());

  size_t bytes_delivered = 0;
  int nrecords = 0;

  for (read_iterator.seek(read_ctx->read_ptr_.lsn,
                          &*read_ctx->lls_filter_,
                          &read_ctx->it_stats_);
       ;
       read_iterator.next(&*read_ctx->lls_filter_, &read_ctx->it_stats_)) {
    IteratorState state = read_iterator.state();

    // Advance read pointer.
    if (state == IteratorState::AT_RECORD ||
        state == IteratorState::LIMIT_REACHED) {
      read_ctx->read_ptr_ = {
          std::max(read_ctx->read_ptr_.lsn, read_iterator.getLSN())};
    }
    ld_spew("Advanced read_ptr to %s, log %lu, last_read %s, state %s%s",
            lsn_to_string(read_ctx->read_ptr_.lsn).c_str(),
            read_ctx->logid_.val_,
            lsn_to_string(read_ctx->it_stats_.last_read.second).c_str(),
            logdevice::toString(state).c_str(),
            read_iterator.accessedUnderReplicatedRegion()
                ? " - UNDER_REPLICATED"
                : "");

    // Check if we're done.
    Status st = read_ctx->checkBatchComplete(state);
    if (st != E::OK) {
      if (st == E::WOULDBLOCK &&
          read_ctx->byteLimitReached(nrecords, bytes_delivered, 0)) {
        return E::BYTE_LIMIT_REACHED;
      }
      return st;
    }

    // Ship the record if needed.
    if (state == IteratorState::AT_RECORD) {
      lsn_t lsn = read_iterator.getLSN();

      if (lsn < read_ctx->read_ptr_.lsn) {
        // Iterator seeked or stepped to a smaller lsn than we requested.
        // This is possible in rare cases with current implementation of the
        // iterators.
        // TODO (#10357210): Different DataKey versions is the only way it can
        //                   happen? Fix.
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          2,
                          "Iterator returned an unexpectedly small LSN. Log "
                          "%lu, read_ptr %s, "
                          "lsn %s.",
                          read_ctx->logid_.val_,
                          lsn_to_string(read_ctx->read_ptr_.lsn).c_str(),
                          lsn_to_string(lsn).c_str());
      } else {
        ld_check(lsn == read_ctx->read_ptr_.lsn);

        // There is a valid record - consider it for sending
        st = maybeSendRecord(
            read_iterator, callback, read_ctx, nrecords, bytes_delivered);
        if (st != E::OK) {
          return st;
        }

        // We shipped that record, we can safely read the next batch from the
        // next lsn.
        read_ctx->read_ptr_ = {std::min(lsn, LSN_MAX - 1) + 1};
      }
    }

    // Check if we're over some limit.
    // The first condition is just an optimization.
    if (state == IteratorState::LIMIT_REACHED ||
        read_ctx->it_stats_.readLimitReached()) {
      ld_check(read_ctx->it_stats_.readLimitReached());

      if (state == IteratorState::AT_RECORD) {
        // We updated read_ptr_ after shipping a record.
        // Re-check batch end conditions.
        st = read_ctx->checkBatchComplete(state);
        if (st != E::OK) {
          ld_check_ne(st, E::WOULDBLOCK);
          return st;
        }
      }

      // Check and move timestamp window.
      if (read_ctx->it_stats_.maxTimestampReached()) {
        ld_check(read_ctx->it_stats_.max_read_timestamp_lower_bound.hasValue());
        read_ctx->ts_window_high_ =
            read_ctx->it_stats_.max_read_timestamp_lower_bound.value();
        return E::WINDOW_END_REACHED;
      }

      // Hit the limit on bytes read or execution time. All the "hard" limits
      // should be covered by checkBatchComplete() (first of the two calls) and
      // the maxTimestampReached() check above.
      ld_check(read_ctx->it_stats_.softLimitReached());
      return E::PARTIAL;
    }
  }

  // We can't get here, because the above loop doesn't have an exit condition or
  // a 'break' clause, so the only way to exit it is through 'return'
  // statements.
}

Status read(LocalLogStore::ReadIterator& read_iterator,
            Callback& callback,
            ReadContext* read_ctx,
            StatsHolder* stats,
            const Settings& settings) {
  // Reset the iterator stats.
  read_ctx->it_stats_ = decltype(read_ctx->it_stats_)();

  Status st = readImpl(read_iterator, callback, read_ctx, stats, settings);

  // checks the return value of the read before returning it

  if (!folly::kIsDebug) {
    return st;
  }
  auto verify = [&](folly::small_vector<Status, 3> expected) {
    if (std::find(expected.begin(), expected.end(), st) == expected.end()) {
      std::string expected_str;
      for (auto& ev : expected) {
        if (!expected_str.empty()) {
          expected_str += " | ";
        }
        expected_str += error_name(ev);
      }

      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "Unexpected return value for read(): %s, expecting %s. "
                      "Logid %lu, read_ptr lsn %lu, until_lsn %lu, last "
                      "released lsn %lu, window high lsn %lu",
                      error_name(st),
                      expected_str.c_str(),
                      read_ctx->logid_.val(),
                      read_ctx->read_ptr_.lsn,
                      read_ctx->until_lsn_,
                      read_ctx->last_released_lsn_,
                      read_ctx->window_high_);
      ld_check(false);
    }
  };

  if (read_ctx->read_ptr_.lsn > std::min(LSN_MAX - 1, read_ctx->until_lsn_)) {
    verify({E::UNTIL_LSN_REACHED});
  } else if (read_ctx->read_ptr_.lsn > read_ctx->last_released_lsn_) {
    verify({E::CAUGHT_UP});
  } else if (read_ctx->read_ptr_.lsn > read_ctx->window_high_) {
    verify({E::WINDOW_END_REACHED, E::WOULDBLOCK, E::FAILED});
  }
  return st;
}

Status ReadContext::checkBatchComplete(IteratorState state) {
  auto print_result = [&](Status st) {
    ld_spew("Read context: %s, state: %s, result: %s",
            this->toString().c_str(),
            logdevice::toString(state).c_str(),
            error_name(st));
    return st;
  };

  if (state == IteratorState::ERROR) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "Returning E::FAILED for log:%lu",
                    logid_.val());
    return print_result(E::FAILED);
  }

  if (state == IteratorState::AT_END ||
      // We must ignore any records after last_released_lsn, handling this
      // exactly the same way as AT_END.
      read_ptr_.lsn > last_released_lsn_) {
    read_ptr_ = {std::min(last_released_lsn_, LSN_MAX - 1) + 1};

    // Status CAUGHT_UP means "delivered everything so far but new writes may
    // add records that need to be shipped to the client".  When
    // last_released_lsn >= until_lsn, the second part is false; we know we
    // already have everything the client is interested in.  So it only makes
    // sense to send back CAUGHT_UP when last_released_lsn < until_lsn.
    return print_result(last_released_lsn_ < until_lsn_ ? E::CAUGHT_UP
                                                        : E::UNTIL_LSN_REACHED);
  }

  if (read_ptr_.lsn > until_lsn_) {
    return print_result(E::UNTIL_LSN_REACHED);
  }

  if (read_ptr_.lsn > window_high_) {
    return print_result(E::WINDOW_END_REACHED);
  }

  if (state == IteratorState::WOULDBLOCK) {
    return print_result(E::WOULDBLOCK);
  }

  ld_check_in(
      state, ({IteratorState::AT_RECORD, IteratorState::LIMIT_REACHED}));

  return print_result(E::OK);
}

int getLastKnownGood(LocalLogStore::ReadIterator& iterator,
                     epoch_t epoch,
                     esn_t* lng_out,
                     esn_t* last_record_out,
                     std::chrono::milliseconds* last_record_timestamp_out,
                     epoch_t* next_non_empty_epoch) {
  static_assert(EPOCH_MAX.val_ < std::numeric_limits<epoch_t::raw_type>::max(),
                "EPOCH_MAX must be smaller than epoch_t::max()");

  ld_check(lng_out != nullptr);
  ld_check(epoch <= EPOCH_MAX);
  iterator.seekForPrev(compose_lsn(epoch, ESN_MAX));

  switch (iterator.state()) {
    case IteratorState::AT_RECORD:
      break;
    case IteratorState::AT_END:
      err = E::NOTFOUND;
      return -1;
    case IteratorState::WOULDBLOCK:
      err = E::WOULDBLOCK;
      return -1;
    case IteratorState::LIMIT_REACHED:
    case IteratorState::MAX:
      ld_check(false);
      FOLLY_FALLTHROUGH;
    case IteratorState::ERROR:
      err = E::FAILED;
      return -1;
  }
  lsn_t lsn = iterator.getLSN();
  if (lsn_to_epoch(lsn) != epoch) {
    ld_check(lsn_to_epoch(lsn) < epoch);
    err = E::NOTFOUND;
    return -1;
  }
  int rv = LocalLogStoreRecordFormat::parse(iterator.getRecord(),
                                            last_record_timestamp_out,
                                            lng_out,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            0,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            -1 /* unused */);
  if (rv != 0) {
    return -1;
  }
  if (last_record_out != nullptr) {
    *last_record_out = lsn_to_esn(iterator.getLSN());
  }
  if (next_non_empty_epoch != nullptr) {
    iterator.seek(compose_lsn(epoch_t(epoch.val_ + 1), esn_t(0)));

    switch (iterator.state()) {
      case IteratorState::AT_RECORD:
        *next_non_empty_epoch = lsn_to_epoch(iterator.getLSN());
        ld_check(*next_non_empty_epoch > epoch);
        break;
      case IteratorState::AT_END:
        *next_non_empty_epoch = EPOCH_INVALID;
        break;
      case IteratorState::WOULDBLOCK:
        err = E::WOULDBLOCK;
        return -1;
      case IteratorState::LIMIT_REACHED:
      case IteratorState::MAX:
        ld_check(false);
        FOLLY_FALLTHROUGH;
      case IteratorState::ERROR:
        err = E::FAILED;
        return -1;
    }
  }
  return 0;
}

int getTailRecord(LocalLogStore::ReadIterator& iterator,
                  logid_t log_id,
                  epoch_t epoch,
                  esn_t lng,
                  TailRecord* tail_out,
                  bool include_payload) {
  ld_check(log_id != LOGID_INVALID);
  ld_check(tail_out != nullptr);
  ld_check(epoch <= EPOCH_MAX);
  // find the first record whose lsn <= LNG
  iterator.seekForPrev(compose_lsn(epoch, lng));

  switch (iterator.state()) {
    case IteratorState::AT_RECORD:
      break;
    case IteratorState::AT_END:
      err = E::NOTFOUND;
      return -1;
    case IteratorState::WOULDBLOCK:
      err = E::WOULDBLOCK;
      return -1;
    case IteratorState::LIMIT_REACHED:
    case IteratorState::MAX:
      ld_check(false);
      FOLLY_FALLTHROUGH;
    case IteratorState::ERROR:
      err = E::FAILED;
      return -1;
  }
  lsn_t lsn = iterator.getLSN();
  if (lsn_to_epoch(lsn) != epoch) {
    ld_check(lsn_to_epoch(lsn) < epoch);
    err = E::NOTFOUND;
    return -1;
  }

  std::chrono::milliseconds timestamp;
  LocalLogStoreRecordFormat::flags_t record_flags;
  Payload payload;

  // if the tail record does not have byte offset included, use
  // BYTE_OFFSET_INVALID
  OffsetMap offsets_within_epoch;

  int rv =
      LocalLogStoreRecordFormat::parse(iterator.getRecord(),
                                       &timestamp,
                                       nullptr,
                                       &record_flags,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       0,
                                       &offsets_within_epoch,
                                       nullptr,
                                       include_payload ? &payload : nullptr,
                                       -1 /* unused */);
  if (rv != 0) {
    ld_check(err == E::MALFORMED_RECORD);
    return -1;
  }

  if (record_flags & STORE_Header::HOLE) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "Fould a hole record (log id %lu, lsn %s, flags %s) "
        "as the tail record in log store! This shouldn't happen.",
        log_id.val_,
        lsn_to_string(lsn).c_str(),
        LocalLogStoreRecordFormat::flagsToString(record_flags).c_str());
  }

  TailRecordHeader::flags_t flags = TailRecordHeader::OFFSET_WITHIN_EPOCH;
  if (record_flags & LocalLogStoreRecordFormat::FLAG_OFFSET_MAP) {
    flags |= TailRecordHeader::OFFSET_MAP;
  }
  std::shared_ptr<PayloadHolder> ph;
  if (include_payload) {
    // make a private copy so that payload can be owned by the PayloadHolder
    payload = payload.dup();
    ph = std::make_shared<PayloadHolder>(payload.data(), payload.size());
    flags |= TailRecordHeader::HAS_PAYLOAD;
    flags |= (record_flags &
              (TailRecordHeader::CHECKSUM | TailRecordHeader::CHECKSUM_64BIT |
               TailRecordHeader::CHECKSUM_PARITY));
  } else {
    flags &= ~(TailRecordHeader::CHECKSUM | TailRecordHeader::CHECKSUM_64BIT);
    flags |= TailRecordHeader::CHECKSUM_PARITY;
  }

  tail_out->reset(
      {log_id,
       lsn,
       static_cast<uint64_t>(timestamp.count()),
       {BYTE_OFFSET_INVALID /* unused, use offsets_within_epoch instead*/},
       flags,
       {}},
      std::move(ph),
      std::move(offsets_within_epoch));
  return 0;
}

std::string ReadContext::toString() const {
  std::string res;
  res += "logid=" + folly::to<std::string>(logid_.val_) + ", ";
  res += "read_ptr.lsn=" + lsn_to_string(read_ptr_.lsn) + ", ";
  res += "until_lsn=" + lsn_to_string(until_lsn_) + ", ";
  res += "window_high=" + lsn_to_string(window_high_) + ", ";
  res += "ts_window_high=" + folly::to<std::string>(ts_window_high_.count()) +
      "ms, ";
  res += "partitions seen=" +
      folly::to<std::string>(it_stats_.seen_logsdb_partitions);
  return res;
}

}}} // namespace facebook::logdevice::LocalLogStoreReader
