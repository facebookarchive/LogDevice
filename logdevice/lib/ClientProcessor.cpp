/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ClientProcessor.h"

#include "logdevice/common/EventLoopHandle.h"
#include "logdevice/lib/ClientWorker.h"

namespace facebook { namespace logdevice {

Worker* ClientProcessor::createWorker(worker_id_t i, WorkerType type) {
  return new ClientWorker(this, i, config_, stats_, type);
}
}} // namespace facebook::logdevice
