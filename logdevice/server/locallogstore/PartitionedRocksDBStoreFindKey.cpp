/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreFindKey.h"

#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

using RocksDBKeyFormat::CustomIndexDirectoryKey;

PartitionedRocksDBStore::FindKey::FindKey(const PartitionedRocksDBStore& store,
                                          logid_t logid,
                                          std::string key,
                                          bool approximate,
                                          bool allow_blocking_io)
    : store_(store),
      logid_(logid),
      key_(std::move(key)),
      approximate_(approximate),
      allow_blocking_io_(allow_blocking_io) {}

int PartitionedRocksDBStore::FindKey::execute(lsn_t* lo, lsn_t* hi) {
  lo_ = lo;
  hi_ = hi;

  *lo_ = LSN_INVALID;
  *hi_ = LSN_MAX;

  PartitionPtr p_lo;
  int rv = findPartitionLo(&p_lo);
  if (rv != 0) {
    if (err == E::FAILED) {
      return -1;
    } else {
      ld_check(err == E::WOULDBLOCK);
      ld_check(!allow_blocking_io_);
      return -1;
    }
  }

  if (approximate_) {
    return 0;
  }

  if (p_lo) {
    rv = findPreciseBound(p_lo);
    if (rv != 0) {
      if (err == E::FAILED) {
        return -1;
      } else {
        ld_check(err == E::WOULDBLOCK);
        ld_check(!allow_blocking_io_);
        return -1;
      }
    }
  }

  return 0;
}

int PartitionedRocksDBStore::FindKey::findPartitionLo(
    PartitionPtr* out_partition_lo) {
  ld_check(out_partition_lo != nullptr);

  RocksDBIterator it = store_.createMetadataIterator(allow_blocking_io_);
  auto it_error = [&] {
    rocksdb::Status status = it.status();
    if (status.ok()) {
      return false;
    }
    err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
    return true;
  };
  // Detect whether we are not in the CustomIndexDirectory for the log anymore.
  auto outside_dir = [this, &it]() {
    ld_check(it.status().ok());
    if (!it.Valid()) {
      return true;
    }
    rocksdb::Slice key = it.key();
    if (!CustomIndexDirectoryKey::valid(key.data(), key.size()) ||
        CustomIndexDirectoryKey::getLogID(key.data()) != logid_) {
      return true;
    }
    return false;
  };

  auto partitions = store_.getPartitionList();
  auto min = partitions->firstID();
  auto max = partitions->nextID() - 1;

  while (min <= max) {
    const partition_id_t mid = (min + max) / 2;

    CustomIndexDirectoryKey key(logid_, FIND_KEY_INDEX, mid);
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    if (it_error()) {
      return -1;
    }

    if (outside_dir()) {
      max = mid - 1;
      continue;
    }

    auto real_mid = CustomIndexDirectoryKey::getPartition(it.key().data());
    if (real_mid > max) {
      // There are no partitions between mid and max, so only search between
      // min and mid - 1.
      max = mid - 1;
      continue;
    }
    ld_check(real_mid >= mid);
    ld_check(partitions->get(real_mid));

    if (!CustomIndexDirectoryValue::valid(
            it.value().data(), it.value().size())) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Current mid key %s is not valid",
          hexdump_buf(it.value().data(), it.value().size()).c_str());
      err = E::FAILED;
      return -1;
    }

    rocksdb::Slice mid_key =
        CustomIndexDirectoryValue::getKey(it.value().data());
    std::string mid_key_string(mid_key.data(), mid_key.size());
    if (mid_key_string.compare(key_) >= 0) {
      *hi_ = CustomIndexDirectoryValue::getLSN(
          it.value().data(), it.value().size());
      max = mid - 1;
    } else {
      *out_partition_lo = partitions->get(real_mid);
      *lo_ = CustomIndexDirectoryValue::getLSN(
          it.value().data(), it.value().size());
      min = real_mid + 1;
    }
  }

  return 0;
}

int PartitionedRocksDBStore::FindKey::findPreciseBound(PartitionPtr partition) {
  folly::small_vector<char, 26> key =
      RocksDBKeyFormat::IndexKey::create(logid_, FIND_KEY_INDEX, key_, 0);

  auto options = RocksDBLogStoreBase::getReadOptionsSinglePrefix();
  options.read_tier =
      (allow_blocking_io_ ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier);

  RocksDBIterator it = store_.newIterator(options, partition->cf_->get());
  auto it_error = [&] {
    rocksdb::Status status = it.status();
    if (status.ok()) {
      return false;
    }
    err = status.IsIncomplete() ? E::WOULDBLOCK : E::FAILED;
    return true;
  };
  // Detect whether we are not in the findKey index for the log anymore.
  auto outside_index = [&] {
    if (!it.Valid() ||
        !RocksDBKeyFormat::IndexKey::valid(it.key().data(), it.key().size())) {
      return true;
    }
    if (RocksDBKeyFormat::IndexKey::getLogID(it.key().data()) != logid_ ||
        RocksDBKeyFormat::IndexKey::getIndexType(it.key().data()) !=
            FIND_KEY_INDEX) {
      return true;
    }
    return false;
  };

  it.Seek(rocksdb::Slice(key.data(), key.size()));
  if (it_error()) {
    return -1;
  }

  if (!outside_index()) {
    // Improve the upper bound previously found in the directory index.
    *hi_ = std::min(
        *hi_,
        RocksDBKeyFormat::IndexKey::getLSN(it.key().data(), it.key().size()));
    it.Prev();
  } else {
    it.SeekForPrev(rocksdb::Slice(key.data(), key.size()));
  }
  if (it_error()) {
    return -1;
  }

  if (!outside_index()) {
    *lo_ = std::max(
        *lo_,
        RocksDBKeyFormat::IndexKey::getLSN(it.key().data(), it.key().size()));
  }

  return 0;
}

}} // namespace facebook::logdevice
