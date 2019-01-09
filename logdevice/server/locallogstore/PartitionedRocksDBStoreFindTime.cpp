/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreFindTime.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/IteratorSearch.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreIterators.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

using RocksDBKeyFormat::PartitionDirectoryKey;

PartitionedRocksDBStore::FindTime::FindTime(
    const PartitionedRocksDBStore& store,
    logid_t logid,
    std::chrono::milliseconds timestamp,
    lsn_t min_lo,
    lsn_t max_hi,
    bool approximate,
    bool allow_blocking_io,
    std::chrono::steady_clock::time_point deadline)
    : store_(store),
      logid_(logid),
      timestamp_(timestamp),
      min_lo_(min_lo),
      max_hi_(max_hi),
      approximate_(approximate),
      allow_blocking_io_(allow_blocking_io),
      use_index_(store_.getSettings()->read_find_time_index),
      deadline_(deadline) {}

int PartitionedRocksDBStore::FindTime::execute(lsn_t* lo, lsn_t* hi) {
  // Store the pointers so that we don't have to pass them around.
  lo_ = lo;
  hi_ = hi;

  // The result range.  This is only updated with LSNs of actual records that
  // we find in the local log store.
  *lo_ = LSN_INVALID;
  *hi_ = LSN_MAX;

  lsn_t p_first_lsn = LSN_INVALID;
  PartitionPtr p;
  rocksdb::ColumnFamilyHandle* cf = nullptr;

  auto enforce_range = [this, &p, p_first_lsn] {
    if (*lo_ >= *hi_) {
      RATELIMIT_ERROR(
          std::chrono::seconds(2),
          2,
          "FindTime resulted in lo >= hi. findTime result might "
          "be off. This is supposed to be impossible, please investigate. "
          "logid: %lu, target timestamp: %s, lo: %s, "
          "hi: %s, partition: %s, partition min LSN: %s, partition "
          "timestamp: %s, min_lo: %s, max_hi: %s, approximate: %d, "
          "allow_blocking_io: %d, use_index: %d",
          logid_.val_,
          timestamp_.toString().c_str(),
          lsn_to_string(*lo_).c_str(),
          lsn_to_string(*hi_).c_str(),
          p ? std::to_string(p->id_).c_str() : "unpartitioned",
          p ? lsn_to_string(p_first_lsn).c_str() : "null",
          p ? format_time(p->starting_timestamp).c_str() : "null",
          lsn_to_string(min_lo_).c_str(),
          lsn_to_string(max_hi_).c_str(),
          approximate_,
          allow_blocking_io_,
          use_index_);
      // Arbitrary.
      *lo_ = *hi_ - 1;
    }
    // Clamp *lo_ to be >= min_lo_ because min_lo_ is usually the trim point,
    // and we'd like to try to not return LSNs below trim point from findTime.
    // No need to clamp *hi_ with max_hi_.
    if (*lo_ < min_lo_) {
      *lo_ = min_lo_;
      *hi_ = std::max(*hi_, *lo_ + 1);
    }
    ld_check(*lo_ < *hi_);
  };

  if (!store_.isLogPartitioned(logid_)) {
    // Look in the unpartitioned column family.
    cf =
        const_cast<PartitionedRocksDBStore&>(store_).getUnpartitionedCFHandle();
  } else {
    // Find the column family that is most likely to store records with
    // timestamp `timestamp_` by doing a binary search on the partition
    // directory.

    int rv = findPartition(&p, &p_first_lsn);
    // Note that `p` is nullptr on success if searching within
    // a partition cannot improve upon the lo_ and high_ as updated
    // by findPartition().

    if (rv != 0) {
      if (err == E::WOULDBLOCK) {
        ld_check(!allow_blocking_io_);
        return -1;
      } else if (err == E::TIMEDOUT) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "PartitionedRocksDBStore::FindTime execution timed out "
                       "during findPartition() for logid %lu, timestamp %s.",
                       uint64_t(logid_),
                       timestamp_.toString().c_str());
        return -1;
      } else {
        ld_check(err == E::FAILED);
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "PartitionedRocksDBStore::FindTime hit error on read "
                       "during findPartition() for logid %lu, timestamp %s.",
                       uint64_t(logid_),
                       timestamp_.toString().c_str());
        err = E::FAILED;
        return -1;
      }
    }

    if (approximate_) {
      if (p) {
        *lo_ = std::max(*lo_, std::max(1ul, p_first_lsn) - 1);
      }
      enforce_range();
      return 0;
    }
    if (p) {
      cf = p->cf_->get();
    }
  }

  // Do a search on the found column family.
  if (cf) {
    int rv = partitionSearch(cf);
    if (rv != 0) {
      if (err == E::WOULDBLOCK) {
        ld_check(!allow_blocking_io_);
        return -1;
      } else if (err == E::TIMEDOUT) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "PartitionedRocksDBStore::FindTime execution timed out "
                       "during binary search for logid %lu, timestamp %s on "
                       "partition %s.",
                       uint64_t(logid_),
                       timestamp_.toString().c_str(),
                       p ? std::to_string(p->id_).c_str() : "unpartitioned");
        return -1;
      } else {
        ld_check(err == E::FAILED);
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "PartitionedRocksDBStore::FindTime execution failed "
                       "during binary search for logid %lu, timestamp %s on "
                       "partition %s.",
                       uint64_t(logid_),
                       timestamp_.toString().c_str(),
                       p ? std::to_string(p->id_).c_str() : "unpartitioned");
        err = E::FAILED;
        return -1;
      }
    }
  }

  enforce_range();
  return 0;
}

int PartitionedRocksDBStore::FindTime::findPartition(
    PartitionPtr* out_partition,
    lsn_t* out_first_lsn) const {
  *out_partition = nullptr;
  *out_first_lsn = LSN_INVALID;

  // Let's call a partition "relevant" if it has a directory entry for our log
  // `logid_` and it's contained in our partition list `partitions`.
  //
  // Consider this example:
  // +-----+-----+-----+-----+-----+-----+-----+
  // |     |  A  |     |  B  |     |     |  C  |
  // |     | p.4 |     | p.6 |     |     | p.9 |
  // +-----+-----+-----+-----+-----+-----+-----+
  //         lo_ ^    out ^              ^ hi_
  // Partitions 4, 6 and 9 (aka A, B and C) are relevant (contain records of
  // our log, as evidenced by directory entries), partitions 3, 5, 7, 8 aren't.
  // Out timestamp_ is between starting_timestamp of partitions 6 and 7
  // (B and B+1). This method will set lo_ to max_lsn of partition A,
  // hi_ to min_lsn of partition C, and out_partition to partition B.
  // The caller is expected to search inside partition B and, if possible,
  // shrink the range (lo_, hi_] based on what it finds.
  //
  // This algorithm may provide incorrect results when timestamps decrease
  // with increasing LSN.
  // E.g. if in our example partition 100 contains a record with timestamp
  // -infinity, that record won't be considered, and lo_ will be underestimated.
  // However, this implementation is expected to:
  //  * be fully correct when timestamps are nondecreasing, and
  //  * not fail catastrophically when there's a small decrease; e.g. if
  //    sequencer failover and clock skew cause a small jump down in the
  //    sequence of timestamps, findTime() result should be off by no more than
  //    the size of the jump.

  // This method works like this:
  //  1. Binary search in directory to find:
  //      a. the first relevant partition with starting_timestamp >= timestamp_;
  //         that's partition C; update hi_ to its min_lsn,
  //      b. the last relevant partition with starting_timestamp < timestamp_;
  //         that's either partition A or partition B; let's call it X.
  //  2. Find A and B as follows:
  //      a. If starting_timestamp of partition X+1 is < timestamp_,
  //         then X is A, and B doesn't exist (set *out_partition = nullptr).
  //         (This is only possible if partition X+1 is irrelevant.)
  //      b. Otherwise, X is B (set *out_partition = X). Do a prev() in
  //         directory to get A.
  //     Set lo_ to max_lsn from A.

  auto partitions = store_.getPartitionList();
  RocksDBIterator it = store_.createMetadataIterator(allow_blocking_io_);

  auto convert_error = [] {
    ld_check(err == E::WOULDBLOCK || err == E::LOCAL_LOG_STORE_READ);
    if (err == E::LOCAL_LOG_STORE_READ) {
      err = E::FAILED;
    }
  };

  // First, find the first partition's lsn.
  int rv = store_.seekToLastInDirectory(&it, logid_, LSN_OLDEST);
  if (rv != 0) {
    if (err == E::NOTFOUND) {
      // Log is empty.
      return 0;
    }
    convert_error();
    return -1;
  }

  lsn_t left = PartitionDirectoryKey::getLSN(it.key().data());

  // Then, find the last partition's lsn.
  rv = store_.seekToLastInDirectory(&it, logid_, LSN_MAX);
  if (rv != 0) {
    if (err == E::NOTFOUND) {
      // Log is empty. This is unexpected because the previous seek said that
      // the log is not empty. Note that we're using a snapshot iterator,
      // which iterates over an immutable view of directory, so directory
      // entries can't disappear from under our feet.
      ld_check(false);
      return 0;
    }
    convert_error();
    return -1;
  }

  lsn_t right = PartitionDirectoryKey::getLSN(it.key().data());

  // Use this lambda to detect when we reach the end of the PartitionDirectory
  // for our log.
  auto at_end = [this, &it]() {
    ld_check(it.status().ok());
    if (!it.Valid()) {
      return true;
    }
    rocksdb::Slice key = it.key();
    if (!PartitionDirectoryKey::valid(key.data(), key.size()) ||
        PartitionDirectoryKey::getLogID(key.data()) != logid_) {
      return true;
    }
    return false;
  };

  // 1. Binary search.
  //
  // Invariant:
  //  * all relevant partitions with min_lsn >= right have
  //    starting_timestamp >= timestamp_,
  //    and *hi_ contains min_lsn of the first of them (if any),
  //  * all relevant partitions with min_lsn <= left have
  //    starting_timestamp < timestamp_,
  //  * one of:
  //     a. there are no relevant partitions with min_lsn <= left;
  //        left_partition is nullptr, or
  //     b. left is equal to min_lsn of a relevant partition; left_partition
  //        points to it.

  PartitionPtr left_partition;

  // avoid overflow, although it's very unexpected
  left = std::max(1ul, left) - 1;
  right = std::min(std::numeric_limits<lsn_t>::max() - 1, right) + 1;

  while (left + 1 < right) {
    if (isTimedOut()) {
      err = E::TIMEDOUT;
      return -1;
    }
    const lsn_t mid = left + (right - left) / 2;
    ld_check(mid > left);
    ld_check(mid < right);

    PartitionDirectoryKey key(logid_, mid, 0);
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    rocksdb::Status status = it.status();
    if (!status.ok()) {
      err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
      return -1;
    }
    // We should not be at end because we know there is an entry with
    // lsn >= right - 1, and mid < right.
    if (at_end()) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "RocksDB iterator failed to provide repeatable reads. This is most "
          "likely a bug, but might theoretically be data corruption. Seeked to "
          "log %lu lsn %s (key %s), right %s",
          logid_.val_,
          lsn_to_string(mid).c_str(),
          hexdump_buf(&key, sizeof(key)).c_str(),
          lsn_to_string(right).c_str());
      err = E::FAILED;
      return -1;
    }

    auto id = PartitionDirectoryKey::getPartition(it.key().data());
    const lsn_t real_mid = PartitionDirectoryKey::getLSN(it.key().data());
    ld_check(real_mid >= mid);

    if (real_mid >= right || id >= partitions->nextID()) {
      // We seeked to an entry after right. This means there are no partitions
      // in [mid, right) (or there's a partition that was created after this
      // method was called; we can safely pretend it's not there), so we can
      // reduce right to mid and continue.
      ld_check(mid < right); // must make progress
      right = mid;
      continue;
    }

    if (id < partitions->firstID()) {
      // This partition is dropped.
      ld_check(real_mid > left); // must make progress
      left = real_mid;
      continue;
    }

    // real_mid points to a relevant partition.

    PartitionPtr partition = partitions->get(id);
    ld_check(partition);
    const auto timestamp = partition->starting_timestamp;

    if (timestamp >= timestamp_) {
      ld_check(mid < right); // must make progress
      right = mid;
      *hi_ = std::min(*hi_, real_mid);
    } else {
      ld_check(real_mid > left); // must make progress
      left = real_mid;
      left_partition = partition;
    }
  }

  // 2. Figure out what to do with `left_partition` (aka X).
  if (left_partition != nullptr) {
    // Make sure iterator points at `left`.
    if (at_end() || PartitionDirectoryKey::getLSN(it.key().data()) != left) {
      PartitionDirectoryKey key(logid_, left, 0);
      it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
      rocksdb::Status status = it.status();
      if (!status.ok()) {
        err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
        return -1;
      }
      // The binary search has seen a key with min_lsn = `left`, so we should
      // see it too. (Iterator is iterating over a snapshot.)
      ld_check(!at_end());
      ld_check(PartitionDirectoryKey::getLSN(it.key().data()) == left);
    }
    ld_check(PartitionDirectoryKey::getPartition(it.key().data()) ==
             left_partition->id_);

    // Figure out if it's 2a or 2b.
    ld_check(left_partition->starting_timestamp < timestamp_);
    PartitionPtr next_partition = partitions->get(left_partition->id_ + 1);
    if (next_partition && next_partition->starting_timestamp < timestamp_) {
      // 2a - left_partition is A, and B doesn't exist. Do nothing.
    } else {
      // 2b - left_partition is B.
      *out_partition = left_partition;
      *out_first_lsn = left;
      it.Prev();
      rocksdb::Status status = it.status();
      if (!status.ok()) {
        err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
        return -1;
      }
    }

    // Iterator points to A now. Get max_lsn from it.
    if (!at_end()) {
      if (PartitionDirectoryValue::valid(
              it.value().data(), it.value().size())) {
        *lo_ = std::max(*lo_,
                        PartitionDirectoryValue::getMaxLSN(
                            it.value().data(), it.value().size()));
      } else {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Malformed value in logsdb directory; key: %s, value: %s",
            hexdump_buf(it.key().data(), it.key().size()).c_str(),
            hexdump_buf(it.value().data(), it.value().size()).c_str());
        // Use min_lsn instead of max_lsn.
        *lo_ = std::max(*lo_, PartitionDirectoryKey::getLSN(it.key().data()));
      }
    }
  }

  return 0;
}

int PartitionedRocksDBStore::FindTime::partitionSearch(
    rocksdb::ColumnFamilyHandle* cf) const {
  IteratorSearch search(&store_,
                        cf,
                        FIND_TIME_INDEX,
                        timestamp_.toMilliseconds().count(),
                        std::string(""),
                        logid_,
                        min_lo_,
                        max_hi_,
                        allow_blocking_io_,
                        deadline_);

  lsn_t new_lo = *lo_;
  lsn_t new_hi = *hi_;

  const int rv = search.execute(&new_lo, &new_hi);

  *lo_ = std::max(*lo_, new_lo);
  *hi_ = std::min(*hi_, new_hi);

  return rv;
}

}} // namespace facebook::logdevice
