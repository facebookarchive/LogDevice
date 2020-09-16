/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

ZookeeperEpochStoreRequest::ZookeeperEpochStoreRequest(logid_t logid)
    : logid_(logid),
      worker_idx_(Worker::onThisThread(false /* enforce_worker */)
                      ? Worker::onThisThread()->idx_
                      : worker_id_t(-1)),
      worker_type_(Worker::onThisThread(false /* enforce_worker */)
                       ? Worker::onThisThread()->worker_type_
                       : WorkerType::GENERAL) {
  ld_check(logid_ != LOGID_INVALID);
}

}} // namespace facebook::logdevice
