/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ServerConfigUpdatedRequest.h"

#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

Request::Execution ServerConfigUpdatedRequest::execute() {
  Worker* worker = Worker::onThisThread();
  worker->onServerConfigUpdated();
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
