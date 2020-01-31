/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/utils/LogsConfigUtils.h"

#include <lz4.h>
#include <zstd.h>

#include <folly/dynamic.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"

using facebook::logdevice::configuration::LocalLogsConfig;
using facebook::logdevice::logsconfig::Delta;
using facebook::logdevice::logsconfig::DeltaHeader;
using facebook::logdevice::logsconfig::DirectoryNode;
using facebook::logdevice::logsconfig::FBuffersLogsConfigCodec;
using facebook::logdevice::logsconfig::LogsConfigTree;
using facebook::logdevice::logsconfig::SetTreeDelta;

namespace facebook {
  namespace logdevice {
    namespace ops {
      namespace LogsConfig {

int migrateLogsConfig(Client& client) {
  // load the config from a file and extract a LogsConfigTree out of this

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());

  std::shared_ptr<UpdateableServerConfig> updateable_server_config =
      client_impl->getConfig()->updateableServerConfig();
  // The config must be loaded locally on this client in order to perform and
  // import.
  ld_check(client_impl->getConfig()->updateableLogsConfig()->get()->isLocal());

  std::shared_ptr<LocalLogsConfig> logs_config =
      client_impl->getConfig()->getLocalLogsConfig();

  const LogsConfigTree& tree = logs_config->getLogsConfigTree();

  Semaphore sem;
  // We have to disable snapshotting here because we are running this on the
  // Client side, snapshotting is not supported on the client so it crashe,
  // so we are disabling it.
  auto logsconfig_state_machine = std::make_unique<LogsConfigStateMachine>(
      client_settings->getSettings(),
      updateable_server_config,
      nullptr, // snapshot_store
      true,
      false /* disable snapshotting */);

  auto update_cb = [&sem](const LogsConfigTree& /* unused */,
                          const Delta* /* unused */,
                          lsn_t version) {
    ld_info("LogsConfigStateMachine State Replay to version '%s' Completed",
            lsn_to_string(version).c_str());
    sem.post();
  };

  auto subscription_handle = logsconfig_state_machine->subscribe(update_cb);

  // we need to write a snapshot with the current LogsConfig, let's get the
  // tree out
  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  subscription_handle.reset(nullptr);

  // prepare the Delta (SetTreeDelta) and serialize to string
  DeltaHeader header;
  std::unique_ptr<LogsConfigTree> tree_to_import = tree.copy();

  SetTreeDelta delta{header, std::move(tree_to_import)};

  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */);

  std::string delta_blob = payload.toString();

  std::cout << "Serialized Full copy of the configuration is "
            << delta_blob.size() / 1024.0 / 1024.0 << "Mb." << std::endl;

  bool success;

  auto write_cb = [&sem, &success](Status st,
                                   lsn_t version,
                                   const std::string& failure_reason) {
    if (st == E::OK) {
      ld_info("New Config was written in the replicated state machine under "
              "version %s",
              lsn_to_string(version).c_str());
      success = true;
    } else {
      ld_critical("Cannot set the configuration in LogsConfigStateMachine, "
                  "reason: %s %s",
                  error_description(st),
                  failure_reason.c_str());
      success = false;
      err = st;
    }
    sem.post();
  };
  rq = std::make_unique<WriteDeltaRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get(), delta_blob, write_cb);
  int rv = client_impl->getProcessor().postWithRetrying(rq);
  if (rv != 0) {
    success = false;
    ld_critical("Processor rejected the SetTree deltas, reason: %s",
                error_description(err));
    sem.post();
  }
  sem.wait();
  if (!success) {
    ld_critical("Migration has failed, nothing has been written. reason: %s",
                error_description(err));
    return -1;
  }
  // let's take a snapshot at this point to make booting the RSM quick.
  auto snapshot_cb = [&sem](Status /* unused */) {
    ld_info("New Snapshot was taken in the replicated state machine");
    sem.post();
  };
  rq = std::make_unique<CreateSnapshotRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get(), snapshot_cb);
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  rq = std::make_unique<
      StopReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  ld_debug("Waiting for the replicated state machine to stop");
  logsconfig_state_machine->wait(client_impl->getTimeout());
  ld_info("Replicated state machine stopped.");
  return 0;
}

/**
 * A helper to count the total number of log-groups in a directory
 */
size_t countLogGroups(const DirectoryNode* dir) {
  size_t total = 0;
  if (dir == nullptr) {
    return total;
  }
  total += dir->logs().size();
  for (const auto& it : dir->children()) {
    total += countLogGroups(it.second.get());
  }
  return total;
}

std::string dump(Client& client) {
  // load the config from a file and extract a LogsConfigTree out of this

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  std::shared_ptr<UpdateableServerConfig> updateable_server_config =
      client_impl->getConfig()->updateableServerConfig();
  // The config must be loaded locally on this client in order to perform and
  // import.
  ld_check(client_impl->getConfig()->updateableLogsConfig()->get()->isLocal());

  std::shared_ptr<LocalLogsConfig> logs_config =
      client_impl->getConfig()->getLocalLogsConfig();

  const LogsConfigTree& tree = logs_config->getLogsConfigTree();

  // prepare the Delta (SetTreeDelta) and serialize to string
  DeltaHeader header;
  std::unique_ptr<LogsConfigTree> tree_to_import = tree.copy();

  auto root = tree_to_import->root();
  size_t total_log_groups = countLogGroups(root);
  std::cout << "The Number of LogGroups is: " << total_log_groups << std::endl;

  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize(*tree_to_import, false /* flatten */);

  std::string tree_blob = payload.toString();
  std::cout << "Serialized Full copy of the configuration is "
            << tree_blob.size() / 1024.0 / 1024.0 << "Mb." << std::endl;

  return tree_blob;
}

Status trim(Client& client, std::chrono::milliseconds timestamp) {
  Semaphore sem;
  Status res;

  auto cb = [&](Status st) {
    res = st;
    sem.post();
  };

  logid_t delta_log_id = configuration::InternalLogs::CONFIG_LOG_DELTAS;
  logid_t snapshot_log_id = configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS;

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  std::unique_ptr<Request> rq =
      std::make_unique<TrimRSMRequest>(delta_log_id,
                                       snapshot_log_id,
                                       timestamp,
                                       cb,
                                       worker_id_t{0},
                                       WorkerType::GENERAL,
                                       RSMType::LOGS_CONFIG_STATE_MACHINE,
                                       false, /* do not trim_everything */
                                       client_impl->getTimeout(),
                                       client_impl->getTimeout());

  client_impl->getProcessor().postWithRetrying(rq);

  sem.wait();
  return res;
}

Status trimEverything(Client& client) {
  Semaphore sem;
  Status res;

  auto cb = [&](Status st) {
    res = st;
    sem.post();
  };

  auto cur_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch());
  logid_t delta_log_id = configuration::InternalLogs::CONFIG_LOG_DELTAS;
  logid_t snapshot_log_id = configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS;

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  std::unique_ptr<Request> rq =
      std::make_unique<TrimRSMRequest>(delta_log_id,
                                       snapshot_log_id,
                                       cur_timestamp,
                                       cb,
                                       worker_id_t{0},
                                       WorkerType::GENERAL,
                                       RSMType::LOGS_CONFIG_STATE_MACHINE,
                                       true, /* trim everything */
                                       client_impl->getTimeout(),
                                       client_impl->getTimeout());

  client_impl->getProcessor().postWithRetrying(rq);

  sem.wait();
  return res;
}

int restoreFromDump(Client& client, std::string serialized_dump) {
  // load the config from a file and extract a LogsConfigTree out of this

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());

  std::shared_ptr<UpdateableServerConfig> updateable_server_config =
      client_impl->getConfig()->updateableServerConfig();

  std::shared_ptr<ServerConfig> server_config =
      client_impl->getConfig()->getServerConfig();

  Payload payload(serialized_dump.data(), serialized_dump.size());

  std::unique_ptr<LogsConfigTree> tree =
      FBuffersLogsConfigCodec::deserialize<LogsConfigTree>(
          payload, server_config->getNamespaceDelimiter());

  ld_info(
      "LogsConfigTree has been loaded, its version is %lu", tree->version());

  size_t total_log_groups = countLogGroups(tree->root());
  std::cout << "The Number of LogGroups is: " << total_log_groups << std::endl;

  Semaphore sem;
  // We have to disable snapshotting here because we are running this on the
  // Client side, snapshotting is not supported on the client so this will
  // crash, so we are disabling it.
  auto logsconfig_state_machine = std::make_unique<LogsConfigStateMachine>(
      client_settings->getSettings(),
      updateable_server_config,
      nullptr, // snapshot store
      true,
      false /* disable snapshotting */);
  // This allows the state machine to make progress to the tail even if there
  // are bad snapshots. We don't care about what's in the state machine anyway
  // since we are adding a new delta and a snapshot at the tail.
  logsconfig_state_machine->allowSkippingBadSnapshots(true);

  auto update_cb = [&sem](const LogsConfigTree& /* unused */,
                          const Delta* /* unused */,
                          lsn_t version) {
    ld_info("LogsConfigStateMachine State Replay to version '%s' Completed",
            lsn_to_string(version).c_str());
    sem.post();
  };

  auto subscription_handle = logsconfig_state_machine->subscribe(update_cb);

  // we need to write a snapshot with the current LogsConfig, let's get the
  // tree out
  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  subscription_handle.reset(nullptr);

  // prepare the Delta (SetTreeDelta) and serialize to string
  DeltaHeader header;

  SetTreeDelta delta{header, std::move(tree)};

  PayloadHolder delta_payload =
      FBuffersLogsConfigCodec::serialize(delta, false /* flatten */);

  std::string delta_blob = delta_payload.toString();
  bool success;

  auto write_cb = [&sem, &success](Status st,
                                   lsn_t version,
                                   const std::string& failure_reason) {
    if (st == E::OK) {
      ld_info("New Config was written in the replicated state machine under "
              "version %s",
              lsn_to_string(version).c_str());
      success = true;
    } else {
      ld_critical("Cannot set the configuration in LogsConfigStateMachine, "
                  "reason: %s %s",
                  error_description(st),
                  failure_reason.c_str());
      success = false;
      err = st;
    }
    sem.post();
  };
  rq = std::make_unique<WriteDeltaRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get(), delta_blob, write_cb);
  int rv = client_impl->getProcessor().postWithRetrying(rq);
  if (rv != 0) {
    success = false;
    ld_critical("Processor rejected the SetTree deltas, reason: %s",
                error_description(err));
    sem.post();
  }
  sem.wait();
  if (!success) {
    ld_critical("Restore has failed, nothing has been written. reason: %s",
                error_description(err));
    return -1;
  }
  // let's take a snapshot at this point to make booting the RSM quick.
  auto snapshot_cb = [&sem](Status status) {
    ld_info("Snapshot creation status: %s", error_description(status));
    sem.post();
  };
  rq = std::make_unique<CreateSnapshotRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get(), snapshot_cb);
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  rq = std::make_unique<
      StopReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  ld_debug("Waiting for the replicated state machine to stop");
  logsconfig_state_machine->wait(client_impl->getTimeout());
  ld_info("Replicated state machine stopped.");
  return 0;
}

std::string convertToJson(Client& client, std::string serialized_dump) {
  // load the config from a file and extract a LogsConfigTree out of this

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  std::shared_ptr<ServerConfig> server_config =
      client_impl->getConfig()->getServerConfig();

  Payload payload(serialized_dump.data(), serialized_dump.size());

  std::unique_ptr<LogsConfigTree> tree =
      FBuffersLogsConfigCodec::deserialize<LogsConfigTree>(
          payload, server_config->getNamespaceDelimiter());

  ld_info(
      "LogsConfigTree has been loaded, its version is %lu", tree->version());

  auto data = LocalLogsConfig();
  data.setLogsConfigTree(std::move(tree));
  return folly::json::serialize(
      data.toJson(), folly::json::serialization_opts());
}

int takeSnapshot(Client& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());

  std::shared_ptr<UpdateableServerConfig> updateable_server_config =
      client_impl->getConfig()->updateableServerConfig();

  Semaphore sem;
  // We have to disable snapshotting here because we are running this on the
  // Client side, snapshotting is not supported on the client so this will
  // crash, so we are disabling it.
  auto logsconfig_state_machine = std::make_unique<LogsConfigStateMachine>(
      client_settings->getSettings(),
      updateable_server_config,
      nullptr, // snapshot store
      true,
      false /* disable automatic snapshotting */);

  auto update_cb = [&sem](const LogsConfigTree& tree,
                          const Delta* /* unused */,
                          lsn_t version) {
    ld_info("LogsConfigStateMachine State Replay to version '%s' Completed",
            lsn_to_string(version).c_str());
    size_t total_log_groups = countLogGroups(tree.root());
    if (total_log_groups == 0) {
      // The tree is empty, we need to warn the user that he is taking an empty
      // snapshot.
      ld_warning("LogsConfigTree is EMPTY (no log groups), "
                 "we will take the snapshot anyway");
    }
    sem.post();
  };

  auto subscription_handle = logsconfig_state_machine->subscribe(update_cb);

  // we need to write a snapshot with the current LogsConfig, let's get the
  // tree out
  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  subscription_handle.reset(nullptr);

  // let's take a snapshot at this point.
  Status st;
  auto snapshot_cb = [&sem, &st](Status status) {
    ld_info("Snapshot creation status: %s", error_description(status));
    st = status;
    sem.post();
  };
  rq = std::make_unique<CreateSnapshotRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get(), snapshot_cb);
  client_impl->getProcessor().postWithRetrying(rq);
  sem.wait();
  int ret = 0;
  if (st != E::OK) {
    // Snapshot creation failed!
    ld_error("Could not create a new LogsConfig RSM snapshot: %s",
             error_description(st));
    err = st;
    ret = 1;
  }
  rq = std::make_unique<
      StopReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);
  ld_debug("Waiting for the replicated state machine to stop");
  logsconfig_state_machine->wait(client_impl->getTimeout());
  ld_info("Replicated state machine stopped.");
  return ret;
}

bool isConfigLogReadable(Client& client) {
  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(&client);
  ld_check(client_impl);

  ClientSettingsImpl* client_settings =
      dynamic_cast<ClientSettingsImpl*>(&client_impl->settings());

  std::shared_ptr<UpdateableServerConfig> updateable_server_config =
      client_impl->getConfig()->updateableServerConfig();

  lsn_t tail_lsn = LSN_INVALID;
  auto logsconfig_state_machine = std::make_unique<LogsConfigStateMachine>(
      client_settings->getSettings(),
      updateable_server_config,
      nullptr, // snapshot store
      true,
      false /* disable snapshotting */);

  auto update_cb = [&tail_lsn](const LogsConfigTree& /* unused */,
                               const Delta* /* unused */,
                               lsn_t version) {
    ld_info("LogsConfigStateMachine State Replay to version '%s' Completed",
            lsn_to_string(version).c_str());
    tail_lsn = version;
  };

  auto subscription_handle = logsconfig_state_machine->subscribe(update_cb);
  logsconfig_state_machine->stopAtTail();
  std::unique_ptr<Request> rq = std::make_unique<
      StartReplicatedStateMachineRequest<LogsConfigStateMachine>>(
      logsconfig_state_machine.get());
  client_impl->getProcessor().postWithRetrying(rq);

  while (true) {
    if (logsconfig_state_machine->wait(client_impl->getTimeout() * 2)) {
      break;
    } else {
      rq = std::make_unique<
          StopReplicatedStateMachineRequest<LogsConfigStateMachine>>(
          logsconfig_state_machine.get());
      client_impl->getProcessor().postWithRetrying(rq);
      ld_debug("Waiting for the replicated state machine to stop");
    }
  }
  // There is a small race condition here. It is possible that we timed out
  // waiting for the state machine to stop at tail but before the request to
  // stop the state machine is processed, the callback is called. This is fine
  // as the result is still considered valid.
  return tail_lsn != LSN_INVALID;
}

}}}} // namespace facebook::logdevice::ops::LogsConfig
