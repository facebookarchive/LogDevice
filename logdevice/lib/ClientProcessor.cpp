/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/ClientProcessor.h"

#include "logdevice/lib/ClientWorker.h"

namespace facebook { namespace logdevice {

Worker* ClientProcessor::createWorker(WorkContext::KeepAlive executor,
                                      worker_id_t idx,
                                      WorkerType worker_type) {
  auto worker =
      new ClientWorker(executor, this, idx, config_, stats_, worker_type);
  // Finish the remaining initialization on the executor.
  worker->add([worker] { worker->setupWorker(); });
  return worker;
}
}} // namespace facebook::logdevice
