/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/TLSCredMonitor.h"

#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"

namespace facebook { namespace logdevice {

void TLSCredMonitor::postToAllWorkers(std::function<void()> func) {
  processor_->applyToWorkers([&](Worker& worker) {
    auto f = func;
    auto req = FuncRequest::make(worker.idx_,
                                 worker.worker_type_,
                                 RequestType::TLS_CRED_MONITOR,
                                 std::move(f));
    processor_->postImportant(req);
  });
}

void TLSCredMonitor::onCertificatesUpdated() {
  postToAllWorkers([]() {
    Worker* w = Worker::onThisThread();
    w->sslFetcher().reloadSSLContext();
  });
}

}} // namespace facebook::logdevice
