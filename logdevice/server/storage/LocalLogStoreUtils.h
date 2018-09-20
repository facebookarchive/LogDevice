/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

class ServerProcessor;
class StatsHolder;

namespace LocalLogStoreUtils {

using TrimPointUpdateMap = std::unordered_map<logid_t, lsn_t, logid_t::Hash>;

using PerEpochLogMetadataTrimPointUpdateMap =
    std::unordered_map<logid_t, epoch_t, logid_t::Hash>;

// Updates trim points in LogStorageStateMap and LocalLogStore metadata.
// If there's an error updating some of the trim points, still tries to
// update the others.
// If processor is nullptr, doesn't update LogStorageStateMap.
// @return 0 in case of success. In case of error returns -1 and sets err to:
//  - LOCAL_LOG_STORE_WRITE if we couldn't write to log store,
//  - NOBUFS if maximum number of logs was reached
int updateTrimPoints(const TrimPointUpdateMap& trim_points,
                     ServerProcessor* processor,
                     LocalLogStore& store,
                     bool sync,
                     StatsHolder* stats);

// Update the trim points for per-epoch log metadata. Currently
// only update their in-memory state in LogStorageState.
void updatePerEpochLogMetadataTrimPoints(
    shard_index_t shard_idx,
    const PerEpochLogMetadataTrimPointUpdateMap& metadata_trim_points,
    ServerProcessor* processor);

} // namespace LocalLogStoreUtils
}} // namespace facebook::logdevice
