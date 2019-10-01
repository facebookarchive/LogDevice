/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/SyncCheckpointedReaderImpl.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

SyncCheckpointedReaderImpl::SyncCheckpointedReaderImpl(
    const std::string& reader_name,
    std::unique_ptr<Reader> reader,
    std::unique_ptr<CheckpointStore> store,
    CheckpointingOptions opts)
    : SyncCheckpointedReader(reader_name, std::move(store), opts),
      reader_(std::move(reader)) {
  ld_check(reader_);
}

int SyncCheckpointedReaderImpl::startReading(
    logid_t log_id,
    lsn_t from,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  return reader_->startReading(log_id, from, until, attrs);
}

int SyncCheckpointedReaderImpl::stopReading(logid_t log_id) {
  return reader_->stopReading(log_id);
}

bool SyncCheckpointedReaderImpl::isReading(logid_t log_id) const {
  return reader_->isReading(log_id);
}

bool SyncCheckpointedReaderImpl::isReadingAny() const {
  return reader_->isReadingAny();
}

int SyncCheckpointedReaderImpl::setTimeout(std::chrono::milliseconds timeout) {
  return reader_->setTimeout(timeout);
}

ssize_t SyncCheckpointedReaderImpl::read(
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out,
    GapRecord* gap_out) {
  return reader_->read(nrecords, data_out, gap_out);
}

void SyncCheckpointedReaderImpl::waitOnlyWhenNoData() {
  reader_->waitOnlyWhenNoData();
}

void SyncCheckpointedReaderImpl::withoutPayload() {
  reader_->withoutPayload();
}

void SyncCheckpointedReaderImpl::forceNoSingleCopyDelivery() {
  reader_->forceNoSingleCopyDelivery();
}

void SyncCheckpointedReaderImpl::includeByteOffset() {
  reader_->includeByteOffset();
}

void SyncCheckpointedReaderImpl::doNotSkipPartiallyTrimmedSections() {
  reader_->doNotSkipPartiallyTrimmedSections();
}

int SyncCheckpointedReaderImpl::isConnectionHealthy(logid_t log_id) const {
  return reader_->isConnectionHealthy(log_id);
}

void SyncCheckpointedReaderImpl::doNotDecodeBufferedWrites() {
  reader_->doNotDecodeBufferedWrites();
}

}} // namespace facebook::logdevice
