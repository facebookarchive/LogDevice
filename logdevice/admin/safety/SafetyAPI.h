/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

using SafetyMargin = std::map<NodeLocationScope, int>;

struct Impact {
  enum ImpactResult {
    NONE = 0,
    // ERROR during check. For example event log is not readable
    ERROR = (1u << 0),
    // operation could lead to rebuilding stall, as full rebuilding
    // of all logs a is not possible due to historical nodesets
    REBUILDING_STALL = (1u << 1),
    // operation could lead to loss of write availability.
    WRITE_AVAILABILITY_LOSS = (1u << 2),
    // operation could lead to loss of read availability,
    // as there is no f-majority for certain logs
    READ_AVAILABILITY_LOSS = (1u << 3),
    // operation could lead to data loss.
    DATA_LOSS = (1u << 4),
    // operation could lead to metadata being inaccessible
    METADATA_LOSS = (1u << 5),
  };
  // bit set of ImpactResult
  int result;
  std::string details;

  // Set of data logs affected.
  std::vector<logid_t> logs_affected;

  // Whether metadata logs are also affected (ie the operations have impact on
  // the metadata nodeset).
  bool metadata_logs_affected;

  explicit Impact(int result,
                  std::string details = "",
                  std::vector<logid_t> logs_affected = {},
                  bool ml_affected = false);
  std::string toString() const;
  static std::string toStringImpactResult(int);
};

enum Operation {
  DISABLE_WRITES = (1u << 0),
  DISABLE_READS = (1u << 1),
  WIPE_DATA = (1u << 2)
};

}} // namespace facebook::logdevice
