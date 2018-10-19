/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/MockDataSourceWriter.h"

#include <chrono>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

MockDataSourceWriter::MockDataSourceWriter() {}

// Stores payloads to be examined by a test class.
// For now, LSN's are assigned in a monotonically increasing sequence,
// incrementing by 1 each time. We don't need to do tests regarding
// LSN's because all interactions with LSN's are covered by the
// guaranteed behavior of append() and read() in the Client and Reader API.

int MockDataSourceWriter::append(logid_t logid,
                                 std::string payload,
                                 append_callback_t cb,
                                 AppendAttributes attrs) {
  lsn_t curr_lsn = ++append_lsn_[logid];

  Payload p(payload.data(), payload.size());
  Payload p2 = p.dup();

  // Note: using dummy values for timestamp and record flags for now since we
  // currently don't do timestamp checking.
  records_[logid].push_back(std::make_pair(
      cb,
      std::make_unique<DataRecordOwnsPayload>(
          logid, std::move(p2), curr_lsn, std::chrono::milliseconds(0), 0)));

  return 0;
}

}} // namespace facebook::logdevice
