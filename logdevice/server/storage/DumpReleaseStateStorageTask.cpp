/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/DumpReleaseStateStorageTask.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void DumpReleaseStateStorageTask::execute() {
  std::vector<const WriteOp*> ops{&op_};

  int rv = storageThreadPool_->getLocalLogStore().writeMulti(ops);
  if (rv != 0) {
    ld_info("Error writing release state map to log store");
  }
}

}} // namespace facebook::logdevice
