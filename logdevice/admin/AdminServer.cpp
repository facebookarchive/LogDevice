/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminServer.h"

#include "logdevice/admin/AdminAPIHandler.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

namespace facebook { namespace logdevice {
AdminServer::AdminServer(
    Sockaddr listen_addr,
    Processor* processor,
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<AdminServerSettings> admin_server_settings,
    StatsHolder* stats_holder)
    : processor_(processor), admin_server_settings_(admin_server_settings) {
  admin_api_handler_ =
      std::make_shared<AdminAPIHandler>(processor,
                                        std::move(settings_updater),
                                        std::move(server_settings),
                                        admin_server_settings_,
                                        stats_holder);

  // Thrift Server
  server_ = std::make_shared<apache::thrift::ThriftServer>();
  server_->setInterface(admin_api_handler_);

  ld_check(listen_addr.valid());
  if (listen_addr.isUnixAddress()) {
    ld_info("Using unix socket for admin server: %s",
            listen_addr.toString().c_str());
    // We must unlink the previous socket if exists otherwise we will get
    // "Address already in use" error.
    unlink(listen_addr.toString().c_str());
    server_->setAddress(listen_addr.getSocketAddress());
  } else {
    ld_info("Admin server will listen on port %i", listen_addr.port());
    server_->setPort(listen_addr.port());
  }
}

void AdminServer::setShardedRocksDBStore(
    ShardedRocksDBLocalLogStore* sharded_store) {
  admin_api_handler_->setShardedRocksDBStore(sharded_store);
}

void AdminServer::setMaintenanceManager(maintenance::MaintenanceManager* mm) {
  admin_api_handler_->setMaintenanceManager(mm);
}

void AdminServer::setAdminCommandHandler(AdminCommandHandler handler) {
  // TODO: Wire it up in the next diff
}

std::shared_ptr<SafetyChecker> AdminServer::getSafetyChecker() {
  return admin_api_handler_->getSafetyChecker();
}

}} // namespace facebook::logdevice
