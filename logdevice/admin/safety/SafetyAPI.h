/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

using SafetyMargin = std::map<NodeLocationScope, int>;

struct Impact {
  enum ImpactResult {
    NONE = 0,
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
    // Impact Could not be established due to an error.
    INVALID = (1u << 30),
  };

  // What was the status of the operation. E::OK means that it's safe to trust
  // the result of the check impact request. Any other value means that some
  // error has happened. In such case the ImpactResult will be set to INVALID
  Status status;
  // bit set of ImpactResult
  int32_t result;
  std::string details;

  // Set of data logs affected.
  std::vector<logid_t> logs_affected;

  // Whether metadata logs are also affected (ie the operations have impact on
  // the metadata nodeset or the internal logs).
  bool internal_logs_affected;

  Impact(Status status,
         int result,
         std::string details = "",
         std::vector<logid_t> logs_affected = {},
         bool internal_logs_affected = false);

  // A helper constructor that must only be used if status != E::OK
  explicit Impact(Status status);
  // Empty constructor sets everything as if the operation is safe.
  Impact();

  std::string toString() const;
  static std::string toStringImpactResult(int);
};

enum Operation {
  DISABLE_WRITES = (1u << 0),
  DISABLE_READS = (1u << 1),
  WIPE_DATA = (1u << 2)
};

/**
 * @param descriptor A descriptor describing one safety margin or a set of .
                     safety marings. Safety marging is simular to replication
                     property - it is list of <domain>:<number> pairs.
 *                   For instance, \"rack:1\",\"node:2\".
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 */
int parseSafetyMargin(const std::vector<std::string>& descriptors,
                      SafetyMargin& out);

/**
 * @param descriptors A list of descriptors, @see parseSafetyMargin.
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 *
 */
int parseSafetyMargin(const std::string& descriptor, SafetyMargin& out);

}} // namespace facebook::logdevice
