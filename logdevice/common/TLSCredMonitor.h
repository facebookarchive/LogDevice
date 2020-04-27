/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <wangle/ssl/TLSCredProcessor.h>

#include "logdevice/common/Processor.h"

namespace facebook { namespace logdevice {

/**
 * @file TlsCredMonitor is responsible for monitoring the TLS credential files
 * (certs, keys, etc.) and when a change is detected (through file last modified
 * time), it enqueues notificates to all the SSLFetchers to reload their
 * contexts.
 */

class TLSCredMonitor : public wangle::TLSCredProcessor {
 public:
  /**
   * @param processor: The processor owning the workers that will get notified
   * when a change is detected.
   * @param cert_refresh_interval: How frequently should we poll for changes.
   * @param cert_paths: Cert files to watch. Empty strings are ignored.
   */
  TLSCredMonitor(Processor* processor,
                 std::chrono::seconds cert_refresh_interval,
                 const std::set<std::string>& cert_paths)
      : wangle::TLSCredProcessor(cert_refresh_interval), processor_(processor) {
    addCertCallback([this]() { onCertificatesUpdated(); });
    setCertPathsToWatch(cert_paths);
  }

  virtual ~TLSCredMonitor() = default;

 protected:
  virtual void onCertificatesUpdated();

  void postToAllWorkers(std::function<void()> func);

 private:
  Processor* processor_;
};

}} // namespace facebook::logdevice
