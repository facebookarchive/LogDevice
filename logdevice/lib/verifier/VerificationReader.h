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
#include "logdevice/lib/verifier/LogDeviceDataSourceReader.h"
#include "logdevice/lib/verifier/ReadVerifyData.h"

namespace facebook { namespace logdevice {

class VerificationReader {
 public:
  VerificationReader(std::unique_ptr<DataSourceReader> mydatasource,
                     std::unique_ptr<ReadVerifyData> myrvd);

  // Note: I'm providing default value for LogErrors in case the customer
  // wants to call read() with the same parameters as the Reader's read(),
  // i.e. without verification.
  ssize_t read(size_t nrecords,
               std::vector<std::unique_ptr<DataRecord>>* data_out,
               GapRecord* gap_out,
               error_callback_t ecb = [](const VerificationFoundError& /*t*/) {
               });
  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until = LSN_MAX,
                   const ReadStreamAttributes* attrs = nullptr);
  int stopReading(logid_t log_id);

  bool isReading(logid_t log_id);

  bool isReadingAny();

  std::unique_ptr<DataSourceReader> ds_;
  std::unique_ptr<ReadVerifyData> rvd_;
};
}} // namespace facebook::logdevice
