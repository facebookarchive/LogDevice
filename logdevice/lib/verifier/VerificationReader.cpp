/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/VerificationReader.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/include/Record.h"
#include "logdevice/lib/verifier/DataSourceReader.h"

namespace facebook { namespace logdevice {

VerificationReader::VerificationReader(
    std::unique_ptr<DataSourceReader> mydatasource,
    std::unique_ptr<ReadVerifyData> myrvd)
    : ds_(std::move(mydatasource)), rvd_(std::move(myrvd)) {}

ssize_t
VerificationReader::read(size_t nrecords,
                         std::vector<std::unique_ptr<DataRecord>>* data_out,
                         GapRecord* gap_out,
                         error_callback_t ecb) {
  std::vector<std::unique_ptr<DataRecord>> temp_results;
  ssize_t rval = ds_->read(nrecords, &temp_results, gap_out);
  std::vector<VerificationFoundError> errors_out;
  for (uint64_t i = 0; i < temp_results.size(); i++) {
    std::unique_ptr<DataRecord> t = rvd_->verifyRestoreRecordPayload(
        std::move(temp_results.at(i)), errors_out);

    data_out->push_back(std::move(t));
  }

  for (uint64_t i = 0; i < errors_out.size(); i++) {
    ecb(errors_out[i]);
  }

  // We know that data_out's size must equal the size of temp_results, hence
  // we can just return the return value of Reader::read()
  return rval;
}

int VerificationReader::startReading(logid_t log_id,
                                     lsn_t from,
                                     lsn_t until,
                                     const ReadStreamAttributes* attrs) {
  return ds_->startReading(log_id, from, until, attrs);
}
int VerificationReader::stopReading(logid_t log_id) {
  return ds_->stopReading(log_id);
}

bool VerificationReader::isReading(logid_t log_id) {
  return ds_->isReading(log_id);
}

bool VerificationReader::isReadingAny() {
  return ds_->isReadingAny();
}

}} // namespace facebook::logdevice
