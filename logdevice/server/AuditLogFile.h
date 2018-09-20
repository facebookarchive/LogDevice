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

#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/include/types.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

struct TrimAuditLogEntry {
  logid_t log_id;
  std::string log_group;
  std::string retention;
  lsn_t new_lsn;
  std::chrono::system_clock::time_point timestamp;
  std::chrono::system_clock::time_point partition_timestamp;
  std::string cluster_name;
  std::string host_address;
  std::string build_info;
  std::string client;
  std::string client_address;
  PrincipalIdentity identity;
};

void log_trim_movement(ServerProcessor& processor,
                       LocalLogStore& store,
                       logid_t log_id,
                       lsn_t trim_lsn,
                       const std::string& client = "",
                       const std::string& client_address = "",
                       const PrincipalIdentity& identity = PrincipalIdentity());

}} // namespace facebook::logdevice
