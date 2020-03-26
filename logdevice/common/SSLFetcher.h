/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <string>

#include <folly/FileUtil.h>
#include <folly/io/async/SSLContext.h>
#include <folly/portability/OpenSSL.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

class StatsHolder;

/**
 * @file Loads the SSL context from the specified files, reloads it if it gets
 *       older than the defined expiration interval, provides a shared_ptr to
 *       folly::SSLContext. Does not implement any thread safety mechanics.
 */

class SSLFetcher {
 public:
  SSLFetcher(const std::string& cert_path,
             const std::string& key_path,
             const std::string& ca_path,
             std::chrono::seconds refresh_interval,
             StatsHolder* stats = nullptr)
      : cert_path_(cert_path),
        key_path_(key_path),
        ca_path_(ca_path),
        refresh_interval_(refresh_interval),
        stats_(stats) {}

  /**
   * @param loadCert          Defines whether or not the certificate will be
   *                          loaded into the SSLContext.
   * @param ssl_accepting     Defines whether the SSLContext is for accepting or
   *                          connecting side of the socket.
   *
   * @return                  a pointer to the created SSLContext or a null
   *                          pointer if the certificate could not be loaded.
   */
  std::shared_ptr<folly::SSLContext> getSSLContext(bool loadCert);

 private:
  const std::string cert_path_;
  const std::string key_path_;
  const std::string ca_path_;
  const std::chrono::seconds refresh_interval_;

  struct ContextState {
    std::chrono::time_point<std::chrono::steady_clock> last_loaded_;
    bool last_load_cert_{false};
    bool context_created_{false};
  };

  std::shared_ptr<folly::SSLContext> context_;
  StatsHolder* stats_{nullptr};
  ContextState state_;
  std::mutex mutex_;

  // a context update is required when refresh_interval_ has passed or when any
  // of the input information is changed
  // lock needs to be acquired in at least read mode
  bool requireContextUpdate(bool loadCert) const;

  // lock needs to be acquired in write mode
  void updateState(bool loadCert, X509* cert);
};

}} // namespace facebook::logdevice
