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

#include <folly/Optional.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/types_internal.h"

/**
 * @file A structure with debug info for all StorageTask subclasses. Optionals
 * refer to subclasses-specific fields.
 */

namespace facebook { namespace logdevice {

struct StorageTaskDebugInfo {
 public:
  explicit StorageTaskDebugInfo(uint64_t shard_id,
                                std::string priority,
                                std::string thread_type,
                                std::string task_type,
                                std::chrono::milliseconds enqueue_time,
                                std::string durability)
      : shard_id(shard_id),
        priority(priority),
        thread_type(thread_type),
        task_type(task_type),
        enqueue_time(enqueue_time),
        durability(durability) {}

 public:
  // Common information
  uint64_t shard_id;
  std::string priority;
  std::string thread_type;
  std::string task_type;
  std::chrono::milliseconds enqueue_time;
  std::string durability;
  // Thread-specific information
  folly::Optional<bool> is_write_queue;
  // Task-specific information
  folly::Optional<logid_t> log_id;
  folly::Optional<lsn_t> lsn;
  folly::Optional<std::chrono::milliseconds> execution_start_time;
  folly::Optional<std::chrono::milliseconds> execution_end_time;
  folly::Optional<NodeID> node_id;
  folly::Optional<ClientID> client_id;
  folly::Optional<Sockaddr> client_address;
  folly::Optional<std::string> extra_info;
};

}} // namespace facebook::logdevice
