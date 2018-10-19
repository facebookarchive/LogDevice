/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <folly/Random.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/toString.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/DataSourceWriter.h"

namespace facebook { namespace logdevice {

// Mock Data Source class, used for testing. Mimics the behavior of logdevice,
// but instead of actually appending payloads, stores them as a vector of pairs
// of callbacks and DataRecordOwnsPayload's.
class MockDataSourceWriter : public DataSourceWriter {
 public:
  MockDataSourceWriter();

  int append(logid_t logid,
             std::string payload,
             append_callback_t cb,
             AppendAttributes attrs = AppendAttributes()) override;

  // Vector of unique pointers to DataRecords, as well as callbacks to indicate
  // to the VerificationWriter that the append has been successful. Tests
  // will call these callbacks at arbitrary times to simulate out-of-order
  // callbacks. Public so that tests can access and modify this.
  std::map<logid_t,
           std::vector<std::pair<append_callback_t,
                                 std::unique_ptr<DataRecordOwnsPayload>>>>
      records_;

  // Stores the next LSN to be appended to a given log.
  std::map<logid_t, lsn_t> append_lsn_;
};

}} // namespace facebook::logdevice
