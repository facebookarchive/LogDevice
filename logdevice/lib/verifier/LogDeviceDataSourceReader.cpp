/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/LogDeviceDataSourceReader.h"

#include <chrono>
#include <exception>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

LogDeviceDataSourceReader::LogDeviceDataSourceReader(std::shared_ptr<Reader> r)
    : reader_(r) {}

ssize_t LogDeviceDataSourceReader::read(
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out,
    GapRecord* gap_out) {
  return reader_->read(nrecords, data_out, gap_out);
}

int LogDeviceDataSourceReader::startReading(logid_t log_id,
                                            lsn_t from,
                                            lsn_t until,
                                            const ReadStreamAttributes* attrs) {
  rlog_id_ = log_id;
  return reader_->startReading(log_id, from, until, attrs);
}

logid_t LogDeviceDataSourceReader::getReadingLogid() {
  return rlog_id_;
}

bool LogDeviceDataSourceReader::isReading(logid_t log_id) {
  return reader_->isReading(log_id);
}

bool LogDeviceDataSourceReader::isReadingAny() {
  return reader_->isReadingAny();
}

int LogDeviceDataSourceReader::stopReading(logid_t log_id) {
  return reader_->stopReading(log_id);
  rlog_id_ = LOGID_INVALID;
}
}} // namespace facebook::logdevice
