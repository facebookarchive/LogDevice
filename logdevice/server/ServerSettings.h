/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <chrono>
#include <string>

#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/ZookeeperConfigSource.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"

/**
 * @file Mains server settings.
 */

namespace facebook { namespace logdevice {

struct ServerSettings : public SettingsBundle {
  struct TaskQueueParams {
    int nthreads = 0;
  };
  using StoragePoolParams =
      std::array<TaskQueueParams, (size_t)StorageTaskThreadType::MAX>;

  const char* getName() const override {
    return "ServerSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  int port;
  std::string unix_socket;
  int command_port;
  bool require_ssl_on_command_port;
  std::string command_unix_socket;
  int ssl_command_port;
  bool admin_enabled;
  int command_conn_limit;
  dbg::Level loglevel;
  dbg::Level external_loglevel;
  dbg::LogLevelMap loglevel_overrides;
  bool assert_on_data;
  // number of background workers
  int num_background_workers;
  std::string log_file;
  std::string config_path;
  std::string epoch_store_path;
  StoragePoolParams storage_pool_params;
  std::chrono::milliseconds shutdown_timeout;
  // Interval between invoking syncs for delayable storage tasks.
  // Ignored when undelayable task is being enqueued.
  std::chrono::milliseconds storage_thread_delaying_sync_interval;
  std::string server_id;
  int fd_limit;
  bool eagerly_allocate_fdtable;
  int num_reserved_fds;
  bool lock_memory;
  std::string user;
  SequencerOptions sequencer;
  bool unmap_caches;
  bool disable_event_log_trimming;
  bool ignore_cluster_marker;
  // When set represents the file where trim actions will be logged.
  // All changes to Trim points are stored in this log.
  std::string audit_log;

  bool shutdown_on_my_node_id_mismatch;

  // (server-only setting) Maximum number of incoming connections that have been
  // accepted by listener (have an open FD) but have not been processed by
  // workers (made logdevice protocol handshake)
  size_t connection_backlog;

  bool test_mode;

  // Self Registration Specific attributes
  bool enable_node_self_registration;
  std::string name;
  // TODO(mbassem): This is the IP, do we need a better name?
  std::string address;
  int ssl_port;
  std::string ssl_unix_socket;
  int gossip_port;
  std::string gossip_unix_socket;
  configuration::nodes::RoleSet roles;
  NodeLocation location;
  double sequencer_weight;
  double storage_capacity;
  int num_shards;

 private:
  // Only UpdateableSettings can create this bundle to ensure defaults are
  // populated.
  ServerSettings() {}
  friend class UpdateableSettingsRaw<ServerSettings>;
};

}} // namespace facebook::logdevice
