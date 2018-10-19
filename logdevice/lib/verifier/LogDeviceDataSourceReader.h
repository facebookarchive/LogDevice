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

#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/DataSourceReader.h"

namespace facebook { namespace logdevice {

// Wrapper class that contains all interactions with LogDevice (i.e. Client
// and Reader API's) that will be needed for our verification framework.
class LogDeviceDataSourceReader : public DataSourceReader {
 public:
  explicit LogDeviceDataSourceReader(std::shared_ptr<Reader> r);

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

 private:
  std::shared_ptr<logdevice::Reader> reader_;
  logid_t rlog_id_;
};
}} // namespace facebook::logdevice
