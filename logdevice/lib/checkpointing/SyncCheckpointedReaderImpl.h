/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/SyncCheckpointedReader.h"

namespace facebook { namespace logdevice {

/*
 * @file SyncCheckpointedReaderImpl implements SyncCheckpointedReader interface
 *   by proxying all Reader functions.
 */
class SyncCheckpointedReaderImpl : public SyncCheckpointedReader {
 public:
  SyncCheckpointedReaderImpl(const std::string& reader_name,
                             std::unique_ptr<Reader> reader,
                             std::unique_ptr<CheckpointStore> store,
                             CheckpointingOptions opts);

  int startReadingFromCheckpoint(
      logid_t log_id,
      lsn_t until = LSN_MAX,
      const ReadStreamAttributes* attrs = nullptr) override;

  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until = LSN_MAX,
                   const ReadStreamAttributes* attrs = nullptr) override;

  int stopReading(logid_t log_id) override;

  bool isReading(logid_t log_id) const override;

  bool isReadingAny() const override;

  int setTimeout(std::chrono::milliseconds timeout) override;

  ssize_t read(size_t nrecords,
               std::vector<std::unique_ptr<DataRecord>>* data_out,
               GapRecord* gap_out) override;

  void waitOnlyWhenNoData() override;

  void withoutPayload() override;

  void forceNoSingleCopyDelivery() override;

  void includeByteOffset() override;

  void doNotSkipPartiallyTrimmedSections() override;

  int isConnectionHealthy(logid_t) const override;

  void doNotDecodeBufferedWrites() override;

 private:
  std::unique_ptr<Reader> reader_;
};

}} // namespace facebook::logdevice
