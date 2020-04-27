/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <wangle/ssl/TLSTicketKeyManager.h>
#include <wangle/ssl/TLSTicketKeySeeds.h>

#include "logdevice/common/SSLFetcher.h"

namespace facebook { namespace logdevice {

/**
 * @file ServerSSLFetcher extends the common SSLFetcher to add support for
 * server specific features (mainly TLS ticket seeds).
 *
 * You can read more about the need for the ticket seeds in the documentation of
 * wangle::TLSTicketKeyManager.
 *
 * This class is not thread safe and should only be accessed from the Worker
 * owning it.
 */
class ServerSSLFetcher : public SSLFetcher {
 public:
  static std::unique_ptr<ServerSSLFetcher>
  create(const std::string& cert_path,
         const std::string& key_path,
         const std::string& ca_path,
         bool use_tls_ticket_seeds,
         const std::string& tls_ticket_seeds_path,
         StatsHolder* stats = nullptr);

  virtual ~ServerSSLFetcher() = default;

  /**
   * Invoked by an external entity to force recreate the SSL context and the
   * associated ticket manager. Used mainly when a change of certs is detected
   * on disk.
   *
   * It's not thread safe.
   */
  void reloadSSLContext() override;

  /**
   * Invoked by an external entity to update the TLS tickets seed when the file
   * changes on disk.
   */
  void reloadTLSTicketSeed(wangle::TLSTicketKeySeeds seeds);

 private:
  ServerSSLFetcher(const std::string& cert_path,
                   const std::string& key_path,
                   const std::string& ca_path,
                   bool enable_shared_tickets,
                   StatsHolder* stats = nullptr);

 private:
  const bool use_tls_ticket_seeds_;

  std::unique_ptr<wangle::TLSTicketKeyManager> ticket_manager_;
};

}} // namespace facebook::logdevice
