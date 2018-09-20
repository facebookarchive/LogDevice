/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice {

/**
 * WorkerType defines the different pool of workers, Requests can pin a
 * specific worker type, or a specific worker in a pool of workers
 */
enum class WorkerType : uint8_t {
#define WORKER_TYPE(type, ch) type,
#include "logdevice/common/worker_types.inc"
  MAX,
};

// Converts a WorkerType to a string

constexpr int numOfWorkerTypes() {
  return static_cast<int>(WorkerType::MAX);
}

const char* workerTypeStr(WorkerType type);
char workerTypeChar(WorkerType type);

// Return MAX if invalid.
WorkerType workerTypeByIndex(int i);
WorkerType workerTypeByChar(char ch);

constexpr int workerIndexByType(WorkerType type) {
  return static_cast<uint8_t>(type);
}

}} // namespace facebook::logdevice
