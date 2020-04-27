/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerSSLFetcher.h"

#include <wangle/ssl/TLSCredProcessor.h>
#include <wangle/ssl/TLSTicketKeySeeds.h>

#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/* static */ std::unique_ptr<ServerSSLFetcher>
ServerSSLFetcher::create(const std::string& cert_path,
                         const std::string& key_path,
                         const std::string& ca_path,
                         bool use_tls_ticket_seeds,
                         const std::string& tls_ticket_seeds_path,
                         StatsHolder* stats) {
  std::unique_ptr<ServerSSLFetcher> fetcher{new ServerSSLFetcher(
      cert_path, key_path, ca_path, use_tls_ticket_seeds, stats)};
  fetcher->reloadSSLContext();

  if (use_tls_ticket_seeds) {
    auto seeds =
        wangle::TLSCredProcessor::processTLSTickets(tls_ticket_seeds_path);
    if (!seeds.has_value()) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          1,
          "Failed to load TLS tickets seeds while --use-tls-ticket-seeds "
          "is set. TLS session resumption will effectivly be disabled.");
      return fetcher;
    }
    fetcher->reloadTLSTicketSeed(std::move(seeds).value());
  }
  return fetcher;
}

ServerSSLFetcher::ServerSSLFetcher(const std::string& cert_path,
                                   const std::string& key_path,
                                   const std::string& ca_path,
                                   bool use_tls_ticket_seeds,
                                   StatsHolder* stats)
    : SSLFetcher(cert_path, key_path, ca_path, true, stats),
      use_tls_ticket_seeds_(use_tls_ticket_seeds) {}

void ServerSSLFetcher::reloadSSLContext() {
  // TLSTicketKeyManager should have a 1:1 relationship with the SSL context, so
  // when the context is resetted, the ticket manager should be resetted as
  // well.

  // 1. Save the seeds from the current ticket manager.
  wangle::TLSTicketKeySeeds seeds;
  if (ticket_manager_) {
    ticket_manager_->getTLSTicketKeySeeds(
        seeds.oldSeeds, seeds.currentSeeds, seeds.newSeeds);
    ticket_manager_.reset();
  }

  // 2. Reset the SSL context.
  SSLFetcher::reloadSSLContext();
  if (!context_ || !use_tls_ticket_seeds_) {
    return;
  }

  // 3. Recreate the ticket manager and attach it to the new context.
  ticket_manager_ =
      std::make_unique<wangle::TLSTicketKeyManager>(context_.get(), nullptr);
  if (seeds.isEmpty()) {
    return;
  }

  // 4. Reload the saved seeds into the ticket manager.
  reloadTLSTicketSeed(std::move(seeds));
}

void ServerSSLFetcher::reloadTLSTicketSeed(wangle::TLSTicketKeySeeds seeds) {
  if (!ticket_manager_) {
    return;
  }
  ticket_manager_->setTLSTicketKeySeeds(
      seeds.oldSeeds, seeds.currentSeeds, seeds.newSeeds);

  if (stats_) {
    STAT_INCR(stats_, tls_ticket_seeds_reloaded);
  }
}

}} // namespace facebook::logdevice
