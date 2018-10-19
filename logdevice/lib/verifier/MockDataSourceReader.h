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

#include <folly/SharedMutex.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/DataSourceReader.h"
#include "logdevice/lib/verifier/GenVerifyData.h"

namespace facebook { namespace logdevice {

// Wrapper class that contains all interactions with LogDevice (i.e. Client
// and Reader API's) that will be needed for our verification framework.
class MockDataSourceReader : public DataSourceReader {
 public:
  MockDataSourceReader();

  ssize_t read(size_t nrecords,
               std::vector<std::unique_ptr<DataRecord>>* data_out,
               GapRecord* gap_out) override;

  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until = LSN_MAX,
                   const ReadStreamAttributes* attrs = nullptr) override;

  bool isReading(logid_t log_id) override;

  bool isReadingAny() override;

  logid_t getReadingLogid() override;

  int stopReading(logid_t log_id) override;

  std::map<logid_t,
           std::vector<std::pair<append_callback_t,
                                 std::unique_ptr<DataRecordOwnsPayload>>>>
      records_;

 private:
  logid_t rlog_id_ = LOGID_INVALID;
  std::map<logid_t, uint64_t> rpos_;
  std::map<logid_t, uint64_t> stop_pos_;
};
}} // namespace facebook::logdevice
