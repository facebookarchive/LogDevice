/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"

/**
 * @file   Parse utilities.
 */

namespace facebook { namespace logdevice {

/**
 * @param descriptor A descriptor describing one shard or a set of shards.
 *                   For instance, N2:S1 describes ShardID(2, 1), "N2" describes
 *                   all shards on N2, frc.frc3.09.14B.ei describes all shards
 *                   on rack "ei";
 * @param cfg        Server configuration.
 * @param out        Populated set of ShardID objects.
 *
 * @return           0 on success, or -1 if the descriptor did not match any
 *                   shard.
 */
int parseShardIDSet(const std::string& descriptor,
                    const ServerConfig& cfg,
                    ShardSet& out);

/**
 * @param descriptors A list of descriptors, @see parseShardIDSet.
 * @param cfg          Server configuration.
 * @param out        Populated set of ShardID objects.
 *
 * @return           0 on success, or -1 if one of the descriptors did not match
 *                   any shard.
 */
int parseShardIDSet(const std::vector<std::string>& descriptors,
                    const ServerConfig& cfg,
                    ShardSet& out);

}} // namespace facebook::logdevice
