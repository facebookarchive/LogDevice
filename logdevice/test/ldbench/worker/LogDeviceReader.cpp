/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogDeviceReader.h"

#include <string>

#include "logdevice/common/checks.h"
#include "logdevice/test/ldbench/worker/LogDeviceClient.h"

namespace facebook { namespace logdevice { namespace ldbench {

LogDeviceReader::LogDeviceReader(LogDeviceClient* ldclient)
    : owner_client_(ldclient) {
  auto raw_client =
      std::static_pointer_cast<Client>(owner_client_->getRawClient());
  // Creates an AsyncReader object that can be used to read from one or more
  // logs via callbacks.
  async_reader_ = raw_client->createAsyncReader();
  ld_check(async_reader_ != nullptr);
  // async-reader callbacks
  // We add one more indrection layer to do two things:
  // 1. report reading results to client holder
  // 2. report reading results to readerworker
  auto record_cb = [this](std::unique_ptr<DataRecord>& record) {
    bool rv = false;
    // report results to readworker
    if (worker_record_callback_) {
      rv = worker_record_callback_(record->logid.val_,
                                   record->attrs.lsn,
                                   record->attrs.timestamp,
                                   record->payload.toString());
    }
    // report to client holder
    if (owner_client_ && owner_client_->reader_stats_updates_cb_) {
      owner_client_->reader_stats_updates_cb_(true,
                                              record->logid.val(),
                                              record->attrs.lsn,
                                              1,
                                              record->payload.toString());
    }
    return rv;
  };
  auto gap_cb = [this](const GapRecord& gap_record) {
    bool rv = false;
    // report to worker
    if (worker_gap_callback_) {
      LogStoreGapType type;
      switch (gap_record.type) {
        case GapType::ACCESS:
          type = LogStoreGapType::ACCESS;
          break;
        case GapType::DATALOSS:
          type = LogStoreGapType::DATALOSS;
          break;
        default:
          type = LogStoreGapType::OTHER;
          break;
      }
      rv = worker_gap_callback_(
          type, gap_record.logid.val_, gap_record.lo, gap_record.hi);
    }
    // report to client holder
    if (owner_client_ && owner_client_->reader_stats_updates_cb_) {
      owner_client_->reader_stats_updates_cb_(false,
                                              gap_record.logid.val(),
                                              0,
                                              gap_record.hi - gap_record.lo + 1,
                                              "");
    }
    return rv;
  };
  auto stop_cb = [this](logid_t logid) {
    // only report to reader worker
    // No need to report to client holder
    if (worker_done_callback_) {
      worker_done_callback_(logid.val_);
    }
  };

  async_reader_->setRecordCallback(std::move(record_cb));
  async_reader_->setGapCallback(std::move(gap_cb));
  async_reader_->setDoneCallback(std::move(stop_cb));
}

bool LogDeviceReader::startReading(LogIDType logid,
                                   LogPositionType start_pos,
                                   LogPositionType end_pos) {
  ld_assert(start_pos <= end_pos);
  int rv = async_reader_->startReading(logid_t(logid), start_pos, end_pos);
  return rv == 0;
}

bool LogDeviceReader::stopReading(LogIDType logid) {
  int rv = async_reader_->stopReading(logid_t(logid));
  return rv == 0;
}

bool LogDeviceReader::resumeReading(LogIDType logid) {
  int rv = async_reader_->resumeReading(logid_t(logid));
  return rv == 0;
}
}}} // namespace facebook::logdevice::ldbench
