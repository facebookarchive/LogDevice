/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/ops/utils/MaintenanceManagerUtils.h"

#include <folly/container/F14Map.h>

#include "logdevice/admin/maintenance/APIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/ops/EventLogUtils.h"

namespace facebook { namespace logdevice { namespace maintenance {

Status migrateToMaintenanceManager(Client& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());

  ld_info("Checking if NCM is enabled on this tier");
  if (client_settings->getSettings()->enable_nodes_configuration_manager) {
    ld_info("NCM is enabled");
  } else {
    ld_critical("NCM is disabled on this tier. Please enable NCM before "
                "running migration");
    return E::FAILED;
  }

  ld_info("Fetching EventLogRebuildingSet...");
  EventLogRebuildingSet set;
  EventLogUtils::getRebuildingSet(*client_impl, set);

  ld_info("Fetching ClusterMaintenanceState...");
  thrift::ClusterMaintenanceState state;
  int rv = getClusterMaintenanceState(*client_impl, state);
  if (rv != 0) {
    return E::FAILED;
  }

  ld_info("Generating MaintenanceDefinitions...");
  MaintenanceDefMap shard_defs = generateMaintenanceDefinition(set, state);

  auto ncm = client_impl->getNodesConfigurationManager();
  // Upgrade NCM to be proposer before making changes to StorageState
  ncm->upgradeToProposer();

  auto nodes_config = ncm->getConfig();
  NodesConfiguration::Update update{};
  update.storage_config_update =
      std::make_unique<configuration::nodes::StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          nodes_config->getStorageMembership()->getVersion());

  Status result = E::OK;

  // Default admin server settings
  UpdateableSettings<AdminServerSettings> admin_server_settings;
  auto state_machine =
      std::make_unique<maintenance::ClusterMaintenanceStateMachine>(
          admin_server_settings);

  std::unique_ptr<Request> req =
      std::make_unique<maintenance::StartClusterMaintenanceStateMachineRequest>(
          state_machine.get(),
          maintenance::ClusterMaintenanceStateMachine::workerType(
              &client_impl->getProcessor()));

  rv = client_impl->getProcessor().blockingRequest(req);
  if (rv != 0) {
    ld_error("Cannot post request to start cluster maintenance state "
             "machine: %s (%s)",
             error_name(err),
             error_description(err));
    ld_check(false);
    return E::FAILED;
  }

  MaintenanceLogWriter log_writer(&client_impl->getProcessor());

  for (const auto& kv : shard_defs) {
    ShardID shard = kv.first;
    auto& defs = kv.second;

    bool maintenance_log_writes_succeeded = true;

    Semaphore write_sem;
    auto cb = [&](Status st, lsn_t version, const std::string& failure_reason) {
      if (st == E::OK) {
        ld_info("Successfully wrote MaintenanceDelta for %s, lns %s",
                toString(shard).c_str(),
                lsn_to_string(version).c_str());
      } else {
        ld_info(
            "write MaintenanceDelta failed for %s with status %s - reason %s",
            toString(shard).c_str(),
            lsn_to_string(version).c_str(),
            failure_reason.c_str());
        maintenance_log_writes_succeeded = false;
      }
      write_sem.post();
    };
    // Write this definition to Maintenance log
    for (const auto& def : defs) {
      MaintenanceDelta delta;
      delta.set_apply_maintenances({def});
      log_writer.writeDelta(
          delta,
          cb,
          ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPEND_ONLY);
      write_sem.wait();
    }

    if (!maintenance_log_writes_succeeded) {
      result = E::PARTIAL;
      continue;
    }
  }
  req = std::make_unique<StopClusterMaintenanceStateMachineRequest>(
      maintenance::ClusterMaintenanceStateMachine::workerType(
          &client_impl->getProcessor()));
  rv = client_impl->getProcessor().blockingRequestImportant(req);
  ld_check(rv == 0);
  state_machine.reset();

  return result;
}

MaintenanceDefMap
generateMaintenanceDefinition(const EventLogRebuildingSet& set,
                              const thrift::ClusterMaintenanceState& state) {
  MaintenanceDefMap output;
  for (const auto& shard_info_kv : set.getRebuildingShards()) {
    for (const auto& node_info_kv : shard_info_kv.second.nodes_) {
      ShardID shard_id(node_info_kv.first, shard_info_kv.first);
      const auto& node_info = node_info_kv.second;
      if (node_info.acked) {
        // The shard already ack'd it rebuilding.
        // This is an artifact of current EventLogRebuildingSet
        // where we keep the ack'd shards in the set until all nodes
        // for this shard are ack'd or removed
        // No need to create a maintenance
        continue;
      }
      std::string reason = "Migration - " + lsn_to_string(node_info.version);
      if (node_info.mode == RebuildingMode::RESTORE &&
          node_info.dc_dirty_ranges.empty()) {
        // Shard is being rebuilt in Restore mode. Create a
        // maintenance to reflect this
        if (output.count(shard_id) == 0) {
          // Create an entry for the shard in `output`
          // This is required because the contract for this method is to return
          // an entry for all shards that are in rebuilding set except mini
          // rebuilding even if maintenance definitions exist for these shards
          // already
          output.emplace(
              shard_id, std::vector<thrift::MaintenanceDefinition>());
        }
        if (!isDefinitionExists(shard_id, RebuildingMode::RESTORE, state)) {
          output[shard_id].push_back(
              MaintenanceLogWriter::buildMaintenanceDefinitionForRebuilding(
                  shard_id, reason));
        }
      }
      if (node_info.drain) {
        // Shard is also set to drain. Create a maintenance
        // to reflect this
        if (output.count(shard_id) == 0) {
          // Create an entry for the shard in `output`
          output.emplace(
              shard_id, std::vector<thrift::MaintenanceDefinition>());
        }
        if (!isDefinitionExists(shard_id, RebuildingMode::RELOCATE, state)) {
          output[shard_id].push_back(
              buildMaintenanceDefinitionForExternalMaintenance(
                  shard_id, reason));
        }
      }
    }
  }

  return output;
}

bool isDefinitionExists(const ShardID& shard,
                        RebuildingMode mode,
                        const ClusterMaintenanceState& state) {
  thrift::ShardID shard_id;
  thrift::NodeID node_id;
  node_id.set_node_index(shard.node());
  shard_id.set_node(node_id);
  shard_id.set_shard_index(shard.shard());
  auto defs = state.get_maintenances();
  for (auto& def : defs) {
    auto is_shard_in_existing_def = [&](thrift::ShardID s) {
      return std::find(def.get_shards().begin(), def.get_shards().end(), s) !=
          def.get_shards().end();
    };

    if (is_shard_in_existing_def(shard_id)) {
      if (def.get_shard_target_state() !=
          thrift::ShardOperationalState::DRAINED) {
        continue;
      }

      return def.get_force_restore_rebuilding()
          ? mode == RebuildingMode::RESTORE
          : mode == RebuildingMode::RELOCATE;
    }
  }
  return false;
}

thrift::MaintenanceDefinition
buildMaintenanceDefinitionForExternalMaintenance(ShardID shard,
                                                 std::string reason) {
  thrift::MaintenanceDefinition def;
  auto nodeid = thrift::NodeID();
  nodeid.set_node_index(shard.node());
  auto shardid = thrift::ShardID();
  shardid.set_node(nodeid);
  shardid.set_shard_index(shard.shard());
  def.set_shards({shardid});
  def.set_shard_target_state(thrift::ShardOperationalState::DRAINED);
  def.set_user(MIGRATION_USER);
  def.set_reason(std::move(reason));
  // This is deliberate. Since we are migrating the existing shards in
  // event log, we do not want MM to run safety checks again for these
  // shards.
  def.set_skip_safety_checks(true);
  def.set_force_restore_rebuilding(false);
  def.set_group_id(maintenance::APIUtils::generateGroupID(8));
  def.set_group(true);
  def.set_ttl_seconds(0);
  def.set_allow_passive_drains(false);
  def.set_created_on(SystemTimestamp::now().toMilliseconds().count());
  return def;
}

int getClusterMaintenanceState(Client& client,
                               thrift::ClusterMaintenanceState& cms) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  // Default admin server settings
  UpdateableSettings<AdminServerSettings> admin_server_settings;
  auto state_machine =
      std::make_unique<maintenance::ClusterMaintenanceStateMachine>(
          admin_server_settings);

  // Stop the state machine once tail is reached
  state_machine->stopAtTail();
  std::unique_ptr<Request> req =
      std::make_unique<maintenance::StartClusterMaintenanceStateMachineRequest>(
          state_machine.get(),
          maintenance::ClusterMaintenanceStateMachine::workerType(
              &client_impl->getProcessor()));

  int rv = client_impl->getProcessor().postRequest(req);
  if (rv != 0) {
    ld_error("Cannot post request to start cluster maintenance state "
             "machine: %s (%s)",
             error_name(err),
             error_description(err));
    ld_check(false);
    return -1;
  }

  state_machine->wait(std::chrono::milliseconds::max());
  cms = state_machine->getCurrentState();

  req = std::make_unique<StopClusterMaintenanceStateMachineRequest>(
      maintenance::ClusterMaintenanceStateMachine::workerType(
          &client_impl->getProcessor()));
  rv = client_impl->getProcessor().blockingRequestImportant(req);
  ld_check(rv == 0);
  state_machine.reset();

  return 0;
}

}}} // namespace facebook::logdevice::maintenance
