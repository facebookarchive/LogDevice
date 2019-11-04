/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/CheckpointStoreFactory.h"

#include "logdevice/common/FileBasedVersionedConfigStore.h"
#include "logdevice/common/RSMBasedVersionedConfigStore.h"
#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/common/ZookeeperVersionedConfigStore.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/ZookeeperClientFactory.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

namespace facebook { namespace logdevice {

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createFileBasedCheckpointStore(std::string root_path) {
  auto versioned_config_store = std::make_unique<FileBasedVersionedConfigStore>(
      root_path, CheckpointStoreImpl::extractVersion);
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createZookeeperBasedCheckpointStore(
    std::shared_ptr<Client>& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);

  std::shared_ptr<ZookeeperClientFactory> zookeeper_client_factory =
      client_impl->getProcessor()
          .getPluginRegistry()
          ->getSinglePlugin<ZookeeperClientFactory>(
              PluginType::ZOOKEEPER_CLIENT_FACTORY);
  auto zookeeper_config = client_impl->getConfig()->getZookeeperConfig();
  auto zookeeper_client =
      zookeeper_client_factory->getClient(*zookeeper_config);
  if (!zookeeper_client) {
    ld_error("Failed to create a zookeeper client in CheckpointStoreFactory");
    return nullptr;
  }
  auto versioned_config_store = std::make_unique<ZookeeperVersionedConfigStore>(
      CheckpointStoreImpl::extractVersion, std::move(zookeeper_client));
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createRSMBasedCheckpointStore(
    std::shared_ptr<Client>& client,
    logid_t log_id,
    std::chrono::milliseconds stop_timeout) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ld_check(client_impl);
  auto versioned_config_store = std::make_unique<RSMBasedVersionedConfigStore>(
      log_id,
      CheckpointStoreImpl::extractVersion,
      &client_impl->getProcessor(),
      stop_timeout);
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

}} // namespace facebook::logdevice
