/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <wangle/ssl/TLSTicketKeySeeds.h>

#include "logdevice/common/TLSCredMonitor.h"

namespace facebook { namespace logdevice {

/**
 * @file ServerTLSCredMonitor extends the common TLSCredMonitor for servers to
 * add server specific functionality.
 *
 * The main server specific functionality here is watching the ticket seed path
 * for changes and notifying all the SSLFetchers about the new seed.
 */
class ServerTLSCredMonitor : public TLSCredMonitor {
 public:
  ServerTLSCredMonitor(Processor* processor,
                       std::chrono::seconds cert_refresh_interval,
                       const std::set<std::string>& cert_paths,
                       const std::string& tls_ticket_path)
      : TLSCredMonitor(processor,
                       cert_refresh_interval,
                       std::move(cert_paths)) {
    addTicketCallback([this](wangle::TLSTicketKeySeeds seeds) {
      onTicketSeedFileUpdated(std::move(seeds));
    });
    setTicketPathToWatch(tls_ticket_path);
  }

  virtual ~ServerTLSCredMonitor() {
    // We need to stop the poller before destructing this class to avoid any
    // callbacks during shutdown.
    stop();
  }

 private:
  void onTicketSeedFileUpdated(wangle::TLSTicketKeySeeds seeds);
};

}} // namespace facebook::logdevice
