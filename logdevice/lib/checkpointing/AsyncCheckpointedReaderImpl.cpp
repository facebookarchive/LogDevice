/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/AsyncCheckpointedReaderImpl.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

AsyncCheckpointedReaderImpl::AsyncCheckpointedReaderImpl(
    const std::string& reader_name,
    std::unique_ptr<AsyncReader> reader,
    std::unique_ptr<CheckpointStore> store,
    CheckpointingOptions opts)
    : AsyncCheckpointedReader(reader_name, std::move(store), opts),
      reader_(std::move(reader)) {
  ld_check(reader_);
}

void AsyncCheckpointedReaderImpl::startReadingFromCheckpoint(
    logid_t log_id,
    StatusCallback scb,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  auto cb = [this, log_id, until, attrs, scb = std::move(scb)](
                Status status, lsn_t from) mutable {
    // We don't want to read the checkpoint twice, so we start from the next
    // record.
    ++from;
    if (status == Status::NOTFOUND) {
      from = LSN_OLDEST;
      status = Status::OK;
    }

    if (status == Status::OK) {
      int return_value = startReading(log_id, from, until, attrs);
      if (return_value == -1) {
        status = err;
      }
    }
    scb(status);
  };
  store_->getLSN(reader_name_, log_id, std::move(cb));
}

void AsyncCheckpointedReaderImpl::setRecordCallback(
    std::function<bool(std::unique_ptr<DataRecord>&)> cb) {
  reader_->setRecordCallback(cb);
}

void AsyncCheckpointedReaderImpl::setGapCallback(
    std::function<bool(const GapRecord&)> cb) {
  reader_->setGapCallback(cb);
}

void AsyncCheckpointedReaderImpl::setDoneCallback(
    std::function<void(logid_t)> cb) {
  reader_->setDoneCallback(cb);
}

void AsyncCheckpointedReaderImpl::setHealthChangeCallback(
    std::function<void(logid_t, HealthChangeType)> cb) {
  reader_->setHealthChangeCallback(cb);
}

int AsyncCheckpointedReaderImpl::startReading(
    logid_t log_id,
    lsn_t from,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  return reader_->startReading(log_id, from, until, attrs);
}

int AsyncCheckpointedReaderImpl::stopReading(logid_t log_id,
                                             std::function<void()> callback) {
  return reader_->stopReading(log_id, callback);
}

int AsyncCheckpointedReaderImpl::resumeReading(logid_t log_id) {
  return reader_->resumeReading(log_id);
}

void AsyncCheckpointedReaderImpl::withoutPayload() {
  reader_->withoutPayload();
}

void AsyncCheckpointedReaderImpl::forceNoSingleCopyDelivery() {
  reader_->forceNoSingleCopyDelivery();
}

void AsyncCheckpointedReaderImpl::includeByteOffset() {
  reader_->includeByteOffset();
}

void AsyncCheckpointedReaderImpl::doNotSkipPartiallyTrimmedSections() {
  reader_->doNotSkipPartiallyTrimmedSections();
}

int AsyncCheckpointedReaderImpl::isConnectionHealthy(logid_t log_id) const {
  return reader_->isConnectionHealthy(log_id);
}

void AsyncCheckpointedReaderImpl::doNotDecodeBufferedWrites() {
  reader_->doNotDecodeBufferedWrites();
}

void AsyncCheckpointedReaderImpl::getBytesBuffered(
    std::function<void(size_t)> callback) {
  reader_->getBytesBuffered(callback);
}
}} // namespace facebook::logdevice
