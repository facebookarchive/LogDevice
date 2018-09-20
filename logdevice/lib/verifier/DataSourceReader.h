/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

namespace facebook { namespace logdevice {

// Interface for reading.
class DataSourceReader {
 public:
  // The destructor must be virtual in order to work correctly.
  virtual ~DataSourceReader(){};

  virtual int startReading(logid_t log_id,
                           lsn_t from,
                           lsn_t until = LSN_MAX,
                           const ReadStreamAttributes* attrs = nullptr) = 0;

  virtual int stopReading(logid_t log_id) = 0;

  virtual bool isReading(logid_t log_id) = 0;

  virtual bool isReadingAny() = 0;

  virtual ssize_t read(size_t nrecords,
                       std::vector<std::unique_ptr<DataRecord>>* data_out,
                       GapRecord* gap_out) = 0;
  virtual logid_t getReadingLogid() = 0;
};
}} // namespace facebook::logdevice
