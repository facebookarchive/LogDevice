/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <string>

#include <folly/dynamic.h>

namespace facebook { namespace logdevice {

class LogStorageStateMap;
class ShardedStorageThreadPool;

/**
 * Persist last released LSNs for all logs to the local log store.  This is
 * called by shutdown_server() when the storage node is shutting down.
 */
void dump_release_states(const LogStorageStateMap& map,
                         ShardedStorageThreadPool& pool);

}} // namespace facebook::logdevice
