/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "StorageTask-enums.h"

namespace facebook { namespace logdevice {

EnumMap<StorageTaskType, std::string, StorageTaskType::UNKNOWN>
    storageTaskTypeNames;

template <>
const std::string& StorageTaskTypeNames::invalidValue() {
  static const std::string invalidName("UNKNOWN");
  return invalidName;
}

template <>
void StorageTaskTypeNames::setValues() {
#define STORAGE_TASK_TYPE(name, class_name, _) \
  set(StorageTaskType::name, class_name);
#include "logdevice/common/storage_task_types.inc"
}

std::string toString(const StorageTaskType& t) {
  return storageTaskTypeNames[t];
}
const char* storageTaskThreadTypeName(StorageTaskThreadType type) {
  using ThreadType = StorageTaskThreadType;
  switch (type) {
    case ThreadType::FAST_TIME_SENSITIVE:
      return "fast_time_sensitive";
    case ThreadType::FAST_STALLABLE:
      return "fast_stallable";
    case ThreadType::METADATA:
      return "meta";
    case ThreadType::SLOW:
      return "slow";
    case ThreadType::MAX:
      return "invalid";
  };
  return "invalid";
}

StorageTaskPriorityNames storageTaskPriorityNames;

template <>
const std::string& StorageTaskPriorityNames::invalidValue() {
  static const std::string invalidName("unknown");
  return invalidName;
}

template <>
void StorageTaskPriorityNames::setValues() {
#define STORAGE_TASK_PRIORITY(name, class_name) \
  set(StorageTaskPriority::name, class_name);
#include "logdevice/common/storage_task_priorities.inc"
}

std::string toString(const StorageTaskPriority& p) {
  return storageTaskPriorityNames[p];
}

}} // namespace facebook::logdevice
