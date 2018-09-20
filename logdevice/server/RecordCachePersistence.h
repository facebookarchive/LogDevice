/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class StorageThreadPool;

/**
 * @file  A function for persisting record caches for all logs stored
 *        on a particular shard. Called at the very end of shutting down storage
 *        threads, from the last thread to be shut down.
 */

namespace RecordCachePersistence {

void persistRecordCaches(shard_index_t, StorageThreadPool*);
}

}} // namespace facebook::logdevice
