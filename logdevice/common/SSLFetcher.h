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
 * @file Loads the SSL context from the specified files, and provides a
 * shared_ptr to folly::SSLContext. Does not implement any thread safety
 * mechanics.
 */

class SSLFetcher {
 public:
  static std::unique_ptr<SSLFetcher> create(const std::string& cert_path,
                                            const std::string& key_path,
                                            const std::string& ca_path,
                                            bool load_certs,
                                            StatsHolder* stats = nullptr);

  virtual ~SSLFetcher() = default;

  /**
   * @return                  a pointer to the created SSLContext or a null
   *                          pointer if the certificate could not be loaded.
   */
  std::shared_ptr<folly::SSLContext> getSSLContext() const;

  /**
   * Invoked by an external entity to force recreate the SSL context. Used
   * mainly when a change of certs is detected on disk.
   *
   * It's not thread safe, so should only be called from the worker owning the
   * SSLFetcher.
   */
  virtual void reloadSSLContext();

 protected:
  SSLFetcher(const std::string& cert_path,
             const std::string& key_path,
             const std::string& ca_path,
             bool load_certs,
             StatsHolder* stats = nullptr);

 protected:
  const std::string cert_path_;
  const std::string key_path_;
  const std::string ca_path_;
  const bool load_certs_;

  std::shared_ptr<folly::SSLContext> context_;
  StatsHolder* stats_{nullptr};
};

}} // namespace facebook::logdevice
