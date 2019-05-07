/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/SimpleAdminServer.h"

#include "logdevice/common/configuration/UpdateableConfig.h"

using apache::thrift::util::ScopedServerThread;

namespace facebook { namespace logdevice {
SimpleAdminServer::SimpleAdminServer(
    Processor* processor,
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<AdminServerSettings> admin_server_settings,
    StatsHolder* stats_holder)
    : AdminServer(processor,
                  std::move(settings_updater),
                  std::move(server_settings),
                  std::move(admin_server_settings),
                  stats_holder) {
  ld_check(processor);
}

bool SimpleAdminServer::start() {
  ld_check(!started_);

  ld_info("Starting a listener thread for the Admin API");
  server_thread_ = std::make_unique<ScopedServerThread>();
  server_thread_->start(server_);
  started_.store(true);
  return started_;
}

void SimpleAdminServer::stop() {
  if (started_) {
    ld_info("Waiting for Admin API server to stop");
    server_thread_->stop();
    ld_info("Admin API server stopped.");
    started_.store(false);
  }
}
}} // namespace facebook::logdevice
