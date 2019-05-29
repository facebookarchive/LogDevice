/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

class UpdateableConfig;
class ShardedStorageThreadPool;
class StatsHolder;
class ServerProcessor;

class TestServerProcessorBuilder {
 public:
  explicit TestServerProcessorBuilder(const Settings& settings);

  TestServerProcessorBuilder&
  setServerSettings(const ServerSettings& server_settings);

  TestServerProcessorBuilder&
  setGossipSettings(const GossipSettings& gossip_settings);

  TestServerProcessorBuilder&
  setAdminServerSettings(const AdminServerSettings& admin_settings);

  TestServerProcessorBuilder&
  setUpdateableConfig(std::shared_ptr<UpdateableConfig> config);

  TestServerProcessorBuilder& setShardedStorageThreadPool(
      ShardedStorageThreadPool* sharded_storage_thread_pool);

  TestServerProcessorBuilder& setStatsHolder(StatsHolder* stats);

  TestServerProcessorBuilder& setMyNodeID(NodeID my_node_id);

  // This is rvalue qualified to make it obvious to the caller that the object
  // will get consumed and is not reusable anymore.
  std::shared_ptr<ServerProcessor> build() &&;

 private:
  UpdateableSettings<Settings> settings_;

  folly::Optional<UpdateableSettings<ServerSettings>> server_settings_{
      folly::none};
  folly::Optional<UpdateableSettings<GossipSettings>> gossip_settings_{
      folly::none};
  folly::Optional<UpdateableSettings<AdminServerSettings>> admin_settings_{
      folly::none};
  std::shared_ptr<UpdateableConfig> config_{nullptr};
  folly::Optional<NodeID> my_node_id_{folly::none};

  ShardedStorageThreadPool* sharded_storage_thread_pool_{nullptr};
  StatsHolder* stats_{nullptr};
};

void shutdown_test_server(std::shared_ptr<ServerProcessor>& processor);
}} // namespace facebook::logdevice
