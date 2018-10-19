/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

class UpdateableConfig;
class ShardedStorageThreadPool;
class StatsHolder;
class ServerProcessor;
class GossipSettings;
class ServerPluginPack;

std::shared_ptr<ServerPluginPack> make_test_server_plugin_pack();
std::shared_ptr<ServerProcessor> make_test_server_processor(
    const Settings& settings,
    const ServerSettings& server_settings,
    const GossipSettings& gossip_settings,
    std::shared_ptr<UpdateableConfig> config = nullptr,
    ShardedStorageThreadPool* sharded_storage_thread_pool = nullptr,
    StatsHolder* stats = nullptr);

std::shared_ptr<ServerProcessor> make_test_server_processor(
    const Settings& settings,
    const ServerSettings& server_settings,
    std::shared_ptr<UpdateableConfig> config = nullptr,
    ShardedStorageThreadPool* sharded_storage_thread_pool = nullptr,
    StatsHolder* stats = nullptr);

void shutdown_test_server(std::shared_ptr<ServerProcessor>& processor);
}} // namespace facebook::logdevice
