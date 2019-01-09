/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/IteratorSearch.h"

#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

IteratorSearch::IteratorSearch(const RocksDBLogStoreBase* store,
                               rocksdb::ColumnFamilyHandle* cf,
                               char index_type,
                               uint64_t target_timestamp,
                               std::string target_key,
                               logid_t log_id,
                               lsn_t lo,
                               lsn_t hi,
                               bool allow_blocking_io,
                               std::chrono::steady_clock::time_point deadline)
    : store_(store),
      cf_(cf),
      index_type_(index_type),
      target_timestamp_(target_timestamp),
      target_key_(std::move(target_key)),
      log_id_(log_id),
      hi_(hi),
      lo_(lo),
      allow_blocking_io_(allow_blocking_io),
      deadline_(deadline) {}

int IteratorSearch::execute(lsn_t* result_lo, lsn_t* result_hi) {
  if (store_->getSettings()->read_find_time_index ||
      index_type_ == FIND_KEY_INDEX) {
    return executeWithIndex(result_lo, result_hi);
  } else {
    return executeWithBinarySearch(result_lo, result_hi);
  }
}

int IteratorSearch::executeWithBinarySearch(lsn_t* result_lo,
                                            lsn_t* result_hi) {
  if (!allow_blocking_io_) {
    err = E::WOULDBLOCK;
    return -1;
  }

  LocalLogStore::ReadOptions options("FindTime::binarySearch");
  options.allow_blocking_io = true;
  options.tailing = false;

  auto it = std::make_unique<RocksDBLocalLogStore::CSIWrapper>(
      store_, log_id_, options, cf_);

  // The result range.  This is only updated with LSNs of actual records that
  // we find in the local log store.
  lsn_t result_so_far_lo = lo_; // left side of result range is exclusive
  lsn_t result_so_far_hi = LSN_MAX;

  // The search range.  This is the range of LSNs (inclusive) in which we may
  // find records pertinent to the result that we have not seen before.
  lsn_t lo = lo_ + 1;
  lsn_t hi = hi_;

  while (lo <= hi) {
    if (std::chrono::steady_clock::now() >= deadline_) {
      err = E::TIMEDOUT;
      return -1;
    }
    lsn_t mid = lo + (hi - lo) / 2;

    it->seek(mid);

    Evaluation ev = evaluateDatabaseResult(*it);
    if (ev == Evaluation::ERROR) {
      err = E::FAILED;
      return -1;
    }

    if (ev == Evaluation::MOVE_LO) {
      ld_check_eq(IteratorState::AT_RECORD, it->state());
      ld_check_ge(it->getLSN(), mid);

      // We found a record with a timestamp that is too low, push up the lower
      // bound of our result range.
      result_so_far_lo = it->getLSN();

      // Also move up the low bound of the search range.
      lo = it->getLSN() + 1;
    } else {
      if (it->state() == IteratorState::AT_RECORD && it->getLSN() <= hi) {
        // We found a record in the search range with a timestamp that is >=
        // target; push down the upper bound of our result range.  The
        // additional `hi` check in the condition is needed because the first
        // record the iterator found may have been beyond last_released_lsn_
        // (so should be ignored).
        result_so_far_hi = it->getLSN();
      }

      // End of log or timestamp too high; move down the high bound of the
      // search range.
      hi = mid - 1;
    }
  }

  *result_lo = result_so_far_lo;
  *result_hi = result_so_far_hi;
  return 0;
}

IteratorSearch::Evaluation IteratorSearch::evaluateDatabaseResult(
    const LocalLogStore::ReadIterator& it) const {
  switch (it.state()) {
    case IteratorState::AT_RECORD:
      break;
    case IteratorState::AT_END:
      // End of log is too high
      return Evaluation::MOVE_HI;
    case IteratorState::ERROR:
      return Evaluation::ERROR;
    case IteratorState::WOULDBLOCK:
    case IteratorState::LIMIT_REACHED:
    case IteratorState::MAX:
      ld_check(false);
      return Evaluation::ERROR;
  }

  if (it.getLSN() > hi_) {
    return Evaluation::MOVE_HI;
  }

  std::chrono::milliseconds timestamp;
  int rv = LocalLogStoreRecordFormat::parse(it.getRecord(),
                                            &timestamp,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            0,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            -1 /* unused */);
  if (rv < 0) {
    return Evaluation::ERROR;
  }

  return timestamp.count() < target_timestamp_ ? Evaluation::MOVE_LO
                                               : Evaluation::MOVE_HI;
}

int IteratorSearch::executeWithIndex(lsn_t* result_lo, lsn_t* result_hi) {
  *result_lo = LSN_INVALID;
  *result_hi = LSN_MAX;

  if (index_type_ == FIND_TIME_INDEX) {
    uint64_t timestamp_big_endian;
    timestamp_big_endian = htobe64(target_timestamp_);
    target_key_.assign(
        reinterpret_cast<const char*>(&timestamp_big_endian), sizeof(uint64_t));
  }
  auto key = RocksDBKeyFormat::IndexKey::create(
      log_id_, index_type_, std::move(target_key_), 0);

  rocksdb::ReadOptions ropt = RocksDBLogStoreBase::getReadOptionsSinglePrefix();
  ropt.read_tier =
      (allow_blocking_io_ ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier);
  RocksDBIterator it = store_->newIterator(ropt, cf_);

  auto it_error = [&] {
    rocksdb::Status status = it.status();
    if (status.ok()) {
      return false;
    }
    err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
    if (err == E::WOULDBLOCK) {
      ld_check(!allow_blocking_io_);
    }
    return true;
  };
  // Useful lambda for checking if we are pointing at an entry in the index for
  // our log id.
  auto entry_is_valid = [&] {
    return RocksDBKeyFormat::IndexKey::valid(
               it.key().data(), it.key().size()) &&
        RocksDBKeyFormat::IndexKey::getLogID(it.key().data()) == log_id_ &&
        RocksDBKeyFormat::IndexKey::getIndexType(it.key().data()) ==
        index_type_;
  };

  it.Seek(rocksdb::Slice(key.data(), key.size()));
  if (it_error()) {
    return -1;
  }

  if (it.Valid() && entry_is_valid()) {
    // We found the upper bound.
    *result_hi =
        RocksDBKeyFormat::IndexKey::getLSN(it.key().data(), it.key().size());
    it.Prev();
  } else {
    // There are no index entries with target key or higher.
    // Seek to max key smaller than target. Note that we can't use Prev() or
    // SeekToLast() here because the iterator may be outside the prefix for
    // which the Seek() was called; see comment
    // above getReadOptionsSinglePrefix().
    it.SeekForPrev(rocksdb::Slice(key.data(), key.size()));
  }

  if (it_error()) {
    return -1;
  }

  if (it.Valid() && entry_is_valid()) {
    // We found the lower bound.
    *result_lo =
        RocksDBKeyFormat::IndexKey::getLSN(it.key().data(), it.key().size());
  }

  if (*result_lo >= *result_hi) {
    RATELIMIT_ERROR(
        std::chrono::seconds(2),
        2,
        "Inversion detected in findTime/custom index: LSN %s has key greater "
        "than LSN %s. The result of findKey might be off.",
        lsn_to_string(*result_hi).c_str(),
        lsn_to_string(*result_lo).c_str());
    // The right thing to do here depends on the cause of the inversion.
    // So far we've only seen one kind of inversions, and the following line
    // addresses only that kind. The inversion is: sometimes a hole plug would
    // get a timestamp much smaller than preceding records; sometimes it's
    // a few days smaller, sometimes it's a zero timestamp (t16009894). So, in
    // this case we would hit this path only if *result_hi is the first record
    // of partition, and *result_lo is a hole with a bogus timestamp. So the
    // right thing to do is to trust *result_hi and discard *result_lo, as if
    // we didn't have that hole.
    *result_lo = lo_;
    *result_hi = std::max(*result_hi, *result_lo + 1);
  }

  return 0;
}

}} // namespace facebook::logdevice
