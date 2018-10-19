/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/MockDataSourceReader.h"

#include <chrono>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

MockDataSourceReader::MockDataSourceReader() {}

int MockDataSourceReader::startReading(logid_t log_id,
                                       lsn_t from,
                                       lsn_t until,
                                       const ReadStreamAttributes* /*attrs*/) {
  // Check that we're not being told to read an invalid log.
  ld_check(log_id != LOGID_INVALID);
  // Check that we're currently not reading anything.
  ld_check(rlog_id_ == LOGID_INVALID);
  rlog_id_ = log_id;
  rpos_[rlog_id_] = 0;
  while (records_[rlog_id_][rpos_[rlog_id_]].second->attrs.lsn != from) {
    rpos_[rlog_id_]++;
    // Check that starting LSN was in the log.
    ld_check(rpos_[rlog_id_] != records_[rlog_id_].size());
  }
  while (stop_pos_[rlog_id_] < records_[rlog_id_].size() - 1 &&
         records_[rlog_id_][stop_pos_[rlog_id_]].second->attrs.lsn != until) {
    stop_pos_[rlog_id_]++;
  }
  return 0;
}

logid_t MockDataSourceReader::getReadingLogid() {
  return rlog_id_;
}

int MockDataSourceReader::stopReading(logid_t log_id) {
  ld_check(rlog_id_ == LOGID_INVALID || rlog_id_ == log_id);
  rlog_id_ = LOGID_INVALID;
  return 0;
}

bool MockDataSourceReader::isReading(logid_t log_id) {
  return rlog_id_ == log_id;
}

bool MockDataSourceReader::isReadingAny() {
  return rlog_id_ != LOGID_INVALID;
}

ssize_t
MockDataSourceReader::read(size_t nrecords,
                           std::vector<std::unique_ptr<DataRecord>>* data_out,
                           GapRecord* /*gap_out*/) {
  ssize_t nread = 0;
  while (nread < nrecords) {
    if (rpos_[rlog_id_] == records_[rlog_id_].size() ||
        rpos_[rlog_id_] > stop_pos_[rlog_id_]) {
      // Stop reading, as we have either reached the 'until' lsn or the
      // end of the (simulated) log.
      return nread;
    }

    // Note that we intentionally don't include gap handling here. In the actual
    // Reader, when a gap is encountered, 2 things can happen. If the user asks
    // to skip gaps, the gap is skipped. If not, an error is eventually thrown.
    // Here, we skip gaps instead of throwing errors, in order to test the
    // verification framework's ability to catch records that have been deleted
    // (data loss).

    data_out->push_back(std::move(records_[rlog_id_][rpos_[rlog_id_]].second));
    rpos_[rlog_id_]++;
    nread++;
  }
  return nread;
}
}} // namespace facebook::logdevice
