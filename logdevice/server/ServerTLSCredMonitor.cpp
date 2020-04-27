/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ServerTLSCredMonitor.h"

#include "logdevice/common/Worker.h"
#include "logdevice/server/ServerSSLFetcher.h"

namespace facebook { namespace logdevice {

void ServerTLSCredMonitor::onTicketSeedFileUpdated(
    wangle::TLSTicketKeySeeds seeds) {
  postToAllWorkers([seeds]() {
    Worker* w = Worker::onThisThread();
    auto& ssl_fetcher = w->sslFetcher();
    auto server_fetcher = dynamic_cast<ServerSSLFetcher*>(&ssl_fetcher);
    ld_check(server_fetcher);
    server_fetcher->reloadTLSTicketSeed(seeds);
  });
}

}} // namespace facebook::logdevice
