/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/NodeRegistrationHandler.h"

#include <folly/String.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;

namespace {
constexpr size_t kMaxNumRetries = 5;
};

folly::Expected<node_index_t, E>
NodeRegistrationHandler::registerSelf(NodeIndicesAllocator allocator) {
  Status status = Status::OK;
  for (size_t trials = 0; trials < kMaxNumRetries; trials++) {
    auto idxs =
        allocator.allocate(*nodes_configuration_->getServiceDiscovery(), 1);
    ld_assert(idxs.size() > 0);
    auto my_idx = idxs.front();
    ld_info("Trying to register in the NodesConfiguration as N%d. Trial #%zu",
            my_idx,
            trials);

    auto update = buildSelfUpdate(my_idx, /* is_update= */ false);
    if (!update.hasValue()) {
      return folly::makeUnexpected(Status::INVALID_ATTRIBUTES);
    }
    status = applyUpdate(std::move(update).value());

    // Keep retrying as long as we get a VERSION_MISMATCH
    if (status == Status::OK) {
      return my_idx;
    } else if (status != Status::VERSION_MISMATCH) {
      return folly::makeUnexpected(status);
    } else {
      // It's a VERSION_MISMATCH. Keep retrying.
    }
  }
  ld_error("Exhusted all retries. Giving up.");
  ld_check(status == Status::VERSION_MISMATCH);
  return folly::makeUnexpected(status);
}

Status NodeRegistrationHandler::updateSelf(node_index_t my_idx) {
  Status status = Status::OK;
  for (size_t trials = 0; trials < kMaxNumRetries; trials++) {
    ld_info("Checking if my attributes as N%d are up to date in the "
            "NodesConfiguration. Trial #%zu",
            my_idx,
            trials);
    auto update = buildSelfUpdate(my_idx, /* is_update= */ true);
    if (!update.hasValue()) {
      return Status::INVALID_ATTRIBUTES;
    }
    status = applyUpdate(std::move(update).value());

    if (status != Status::VERSION_MISMATCH) {
      return status;
    } else {
      // It's a VERSION_MISMATCH. Keep retrying.
    }
  }
  ld_error("Exhusted all retries. Giving up.");
  ld_check(status == Status::VERSION_MISMATCH);
  return status;
}

configuration::nodes::NodeUpdateBuilder
NodeRegistrationHandler::updateBuilderFromSettings(node_index_t my_idx) const {
  NodeUpdateBuilder update_builder;

  update_builder.setNodeIndex(my_idx).setName(server_settings_.name);

  if (!server_settings_.unix_socket.empty()) {
    update_builder.setDataAddress(Sockaddr(server_settings_.unix_socket));
  } else {
    update_builder.setDataAddress(
        Sockaddr(server_settings_.address, server_settings_.port));
  }

  // Gossip address is optional, so only set it if the unix socket is passed or
  // if the port is greater than the default 0.
  if (!server_settings_.gossip_unix_socket.empty()) {
    update_builder.setGossipAddress(
        Sockaddr(server_settings_.gossip_unix_socket));
  } else if (server_settings_.gossip_port > 0) {
    update_builder.setGossipAddress(
        Sockaddr(server_settings_.address, server_settings_.gossip_port));
  }

  // SSL address is optional, so only set it if the unix socket is passed or
  // if the port is greater than the default 0.
  if (!server_settings_.ssl_unix_socket.empty()) {
    update_builder.setSSLAddress(Sockaddr(server_settings_.ssl_unix_socket));
  } else if (server_settings_.ssl_port > 0) {
    update_builder.setSSLAddress(
        Sockaddr(server_settings_.address, server_settings_.ssl_port));
  }

  // Admin address is optional, you only need it if the admin-enabled flag is
  // set.
  if (server_settings_.admin_enabled) {
    if (!admin_server_settings_.admin_unix_socket.empty()) {
      update_builder.setAdminAddress(
          Sockaddr(admin_server_settings_.admin_unix_socket));
    } else if (admin_server_settings_.admin_port > 0) {
      update_builder.setAdminAddress(Sockaddr(
          server_settings_.address, admin_server_settings_.admin_port));
    }
  }

  if (!server_settings_.location.isEmpty()) {
    update_builder.setLocation(server_settings_.location);
  }

  if (hasRole(
          server_settings_.roles, configuration::nodes::NodeRole::SEQUENCER)) {
    update_builder.isSequencerNode().setSequencerWeight(
        server_settings_.sequencer_weight);
  }

  if (hasRole(
          server_settings_.roles, configuration::nodes::NodeRole::STORAGE)) {
    update_builder.isStorageNode()
        .setStorageCapacity(server_settings_.storage_capacity)
        .setNumShards(server_settings_.num_shards);
  }
  return update_builder;
}

folly::Optional<configuration::nodes::NodesConfiguration::Update>
NodeRegistrationHandler::buildSelfUpdate(node_index_t my_idx,
                                         bool is_update) const {
  // The update structure will be filled by the builder.
  NodesConfiguration::Update update;
  NodeUpdateBuilder::Result result;
  if (!is_update) {
    result =
        std::move(updateBuilderFromSettings(my_idx))
            .buildAddNodeUpdate(
                update,
                nodes_configuration_->getSequencerMembership()->getVersion(),
                nodes_configuration_->getStorageMembership()->getVersion());
  } else {
    result = std::move(updateBuilderFromSettings(my_idx))
                 .buildUpdateNodeUpdate(update, *nodes_configuration_);
  }
  if (result.status != Status::OK) {
    ld_error("Failed building selfUpdate: %s", result.message.c_str());
    return folly::none;
  }
  return update;
}

Status NodeRegistrationHandler::applyUpdate(
    configuration::nodes::NodesConfiguration::Update update) const {
  if (update.empty()) {
    return Status::UPTODATE;
  }

  auto new_config = nodes_configuration_->applyUpdate(std::move(update));
  if (new_config == nullptr) {
    return err;
  }

  auto nc_serialized = NodesConfigurationCodec::serialize(*new_config);
  if (nc_serialized.empty()) {
    return err;
  }

  NodesConfigurationStore::version_t new_version;
  std::string config_out;
  return store_->updateConfigSync(std::move(nc_serialized),
                                  nodes_configuration_->getVersion(),
                                  &new_version,
                                  &config_out);
}

}} // namespace facebook::logdevice
