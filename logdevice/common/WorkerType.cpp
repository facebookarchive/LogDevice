/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/WorkerType.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

const char* workerTypeStr(WorkerType type) {
  switch (type) {
#define WORKER_TYPE(type, ch) \
  case WorkerType::type:      \
    return #type;
#include "logdevice/common/worker_types.inc"
    default:
      return "INVALID";
  }
}

char workerTypeChar(WorkerType type) {
  switch (type) {
#define WORKER_TYPE(type, ch) \
  case WorkerType::type:      \
    return ch;
#include "logdevice/common/worker_types.inc"
    default:
      return '?';
  }
}

WorkerType workerTypeByIndex(int i) {
  ld_check(i < static_cast<uint8_t>(WorkerType::MAX));
  return static_cast<WorkerType>(i);
}

WorkerType workerTypeByChar(char ch) {
  switch (ch) {
#define WORKER_TYPE(type, ch) \
  case ch:                    \
    return WorkerType::type;
#include "logdevice/common/worker_types.inc"
    default:
      return WorkerType::MAX;
  }
}

}} // namespace facebook::logdevice
