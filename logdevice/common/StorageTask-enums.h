/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/include/EnumMap.h"

/**
 * @file Enum with identifiers for all StorageTask subclasses.  In common/ so
 * that stats code can pull in.
 */

namespace facebook { namespace logdevice {

// storage_task_types.inc defines the list of storage task types for which we
// want to have stats.
enum class StorageTaskType : char {
  UNKNOWN = 0,
#define STORAGE_TASK_TYPE(name, class_name, _) name,
#include "logdevice/common/storage_task_types.inc"
  MAX
};

using StorageTaskTypeNames =
    EnumMap<StorageTaskType, std::string, StorageTaskType::UNKNOWN>;

extern StorageTaskTypeNames storageTaskTypeNames;

std::string toString(const StorageTaskType& x);

// Determines which storage threads should execute instances of a specific
// StorageTask.
enum class StorageTaskThreadType : uint8_t {
  // These threads execute tasks that may take longer to complete (because
  // they need to do disk I/O, for example), such as reads and findtime.
  SLOW = 0,
  // Threads which execute low-priority writes. If the total write
  // rate is higher than the disk can sustain, these threads get stalled
  // periodically to reduce the write rate.
  // Main use is writing records for rebuilding.
  FAST_STALLABLE = 1,
  // Threads which execute writes (which are expected to be fast, memory only
  // operations) as fast as possible.
  // Note that rocksdb can still stall these writes if flushing falls behind
  // despite logdevice-side stalling of the FAST_STALLABLE writes.
  // Main use is writing records from Appenders.
  FAST_TIME_SENSITIVE = 2,
  // These threads execute mostly metadata operations (w/ few exceptions of
  // data operations related to log recovery), which are usually of
  // higher priority than tasks in SLOW threads.
  DEFAULT = 3,
  MAX
};

const char* storageTaskThreadTypeName(StorageTaskThreadType type);

// Determines which priority should the storage tasks have
enum class StorageTaskPriority : uint8_t {
#define STORAGE_TASK_PRIORITY(name, class_name) name,
#include "logdevice/common/storage_task_priorities.inc"
  NUM_PRIORITIES,
  UNKNOWN
};

using StorageTaskPriorityNames =
    EnumMap<StorageTaskPriority,
            std::string,
            StorageTaskPriority::UNKNOWN,
            static_cast<size_t>(StorageTaskPriority::NUM_PRIORITIES)>;

extern StorageTaskPriorityNames storageTaskPriorityNames;

std::string toString(const StorageTaskPriority& p);

// IO principals for storage tasks
enum class StorageTaskPrincipal : uint8_t {
#define STORAGE_TASK_PRINCIPAL(name, class_name, share) name,
#include "logdevice/common/storage_task_principals.inc"
  NUM_PRINCIPALS,
  UNKNOWN
};

using StorageTaskPrincipalNames =
    EnumMap<StorageTaskPrincipal,
            std::string,
            StorageTaskPrincipal::UNKNOWN,
            static_cast<size_t>(StorageTaskPrincipal::NUM_PRINCIPALS)>;

extern StorageTaskPrincipalNames storageTaskPrincipalNames;

std::string toString(const StorageTaskPrincipal& p);
}} // namespace facebook::logdevice
