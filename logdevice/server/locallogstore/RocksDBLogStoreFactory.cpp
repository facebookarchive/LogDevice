/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "RocksDBLogStoreFactory.h"

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

std::unique_ptr<LocalLogStore>
RocksDBLogStoreFactory::create(uint32_t shard_idx, std::string path) const {
  LocalLogStore* store;

  try {
    if (rocksdb_config_.getRocksDBSettings()->partitioned) {
      store = new PartitionedRocksDBStore(
          shard_idx, path, rocksdb_config_, config_.get(), stats_);
    } else {
      store =
          new RocksDBLocalLogStore(shard_idx, path, rocksdb_config_, stats_);
    }
  } catch (ConstructorFailed&) {
    return nullptr;
  }

  return std::unique_ptr<LocalLogStore>(store);
}

}} // namespace facebook::logdevice
