/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ReleaseRequest.h"

#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

Request::Execution ReleaseRequest::execute() {
  ld_spew("ReleaseRequest(%s) running on worker %s for shard %u",
          rid_.toString().c_str(),
          Worker::onThisThread()->getName().c_str(),
          shard_);
  ServerWorker::onThisThread()->serverReadStreams().onRelease(
      rid_, shard_, force_);
  return Execution::COMPLETE;
}

void ReleaseRequest::retry(ServerProcessor* processor,
                           logid_t log_id,
                           shard_index_t shard,
                           worker_id_t idx,
                           bool force) {
  // We expect a LogStorageState instance to exist if we're releasing.
  processor->getLogStorageStateMap()
      .get(log_id, shard)
      .retryRelease(idx, force);
}

}} // namespace facebook::logdevice
