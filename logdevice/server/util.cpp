/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/util.h"

#include <folly/Memory.h>

#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/DumpReleaseStateStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

void dump_release_states(const LogStorageStateMap& map,
                         ShardedStorageThreadPool& pool) {
  for (shard_index_t i = 0; i < pool.numShards(); ++i) {
    auto all_entries = map.getAllLastReleasedLSNs(i);
    if (!all_entries.empty()) {
      // storage task doesn't have reply_fd set since we don't expect a reply
      auto task =
          std::make_unique<DumpReleaseStateStorageTask>(std::move(all_entries));
      pool.getByIndex(i).blockingPutTask(std::move(task));
    }
  }
}

}} // namespace facebook::logdevice
