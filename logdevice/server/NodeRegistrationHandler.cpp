/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/NodeRegistrationHandler.h"

#include <folly/Expected.h>
#include <folly/Random.h>
#include <folly/String.h>

#include "logdevice/common/RetryHandler.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"

namespace facebook { namespace logdevice {

using namespace facebook::logdevice::configuration::nodes;

namespace {
constexpr size_t kMaxNumRetries = 10;

// Maximum sleep duration before attempting register/update again
constexpr std::chrono::seconds kMaxSleepDuration(60);
}; // namespace

folly::Expected<node_index_t, E>
NodeRegistrationHandler::registerSelf(NodeIndicesAllocator allocator) {
  node_index_t my_idx;
  auto result = RetryHandler<Status>::syncRun(
      [this, &my_idx, &allocator](size_t trial_num) -> Status {
        auto idxs = allocator.allocate(
            *getNodesConfiguration().getServiceDiscovery(), 1);
        ld_assert(idxs.size() > 0);
        my_idx = idxs.front();
        ld_info(
            "Trying to register in the NodesConfiguration as N%d. Trial #%zu",
            my_idx,
            trial_num);

        auto update = buildSelfUpdate(my_idx, /* is_update= */ false);
        if (!update.hasValue()) {
          return Status::INVALID_ATTRIBUTES;
        }
        return applyUpdate(std::move(update).value());
      },
      [](const Status& st) { return st == Status::VERSION_MISMATCH; },
      /* num_tries */ kMaxNumRetries,
      /* backoff_min */ std::chrono::seconds(1),
      /* backoff_max */ kMaxSleepDuration,
      /* jitter_param */ 0.25);
  return result.hasValue() ? folly::makeExpected<E>(my_idx)
                           : folly::makeUnexpected(result.error());
}

Status NodeRegistrationHandler::updateSelf(node_index_t my_idx) {
  auto result = RetryHandler<Status>::syncRun(
      [this, my_idx](size_t trial_num) -> Status {
        ld_info("Trying to update node as N%d in the NodesConfiguration. "
                "Trial #%zu",
                my_idx,
                trial_num);
        auto update = buildSelfUpdate(my_idx, /* is_update= */ true);
        if (!update.hasValue()) {
          return Status::INVALID_ATTRIBUTES;
        }
        return applyUpdate(std::move(update).value());
      },
      [](const Status& st) { return st == Status::VERSION_MISMATCH; },
      /* num_tries */ kMaxNumRetries,
      /* backoff_min */ std::chrono::seconds(1),
      /* backoff_max */ kMaxSleepDuration,
      /* jitter_param */ 0.25);

  return result.hasValue() ? result.value() : result.error();
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
                getNodesConfiguration().getSequencerMembership()->getVersion(),
                getNodesConfiguration().getStorageMembership()->getVersion());
  } else {
    result = std::move(updateBuilderFromSettings(my_idx))
                 .buildUpdateNodeUpdate(update, getNodesConfiguration());
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

  auto new_config = getNodesConfiguration().applyUpdate(std::move(update));
  if (new_config == nullptr) {
    return err;
  }

  auto nc_serialized = NodesConfigurationCodec::serialize(*new_config);
  if (nc_serialized.empty()) {
    return err;
  }

  NodesConfigurationStore::version_t new_version;
  std::string config_out;
  auto status = store_->updateConfigSync(std::move(nc_serialized),
                                         getNodesConfiguration().getVersion(),
                                         &new_version,
                                         &config_out);
  if (status == Status::VERSION_MISMATCH) {
    // There's a new NC, let's refresh our updatable.
    auto new_nc = NodesConfigurationCodec::deserialize(config_out);
    if (new_nc == nullptr) {
      ld_error("Got a NodesConfiguration version mismatch during update, but "
               "failed to deserialize the new version: %s",
               error_name(err));
      return err;
    }

    ld_info("Got a NodesConfiguration version mismatch during update. "
            "Updating NC version from %ld to %ld",
            getNodesConfiguration().getVersion().val(),
            new_nc->getVersion().val());
    nodes_configuration_->update(std::move(new_nc));
  }
  return status;
}

const NodesConfiguration&
NodeRegistrationHandler::getNodesConfiguration() const {
  return *nodes_configuration_->get();
}

}} // namespace facebook::logdevice
