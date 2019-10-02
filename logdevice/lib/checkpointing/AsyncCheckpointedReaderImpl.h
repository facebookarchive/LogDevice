/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/AsyncCheckpointedReader.h"

namespace facebook { namespace logdevice {

/*
 * @file AsyncCheckpointedReaderImpl implements AsyncCheckpointedReader
 * interface by proxying all AsyncReader functions.
 */
class AsyncCheckpointedReaderImpl : public AsyncCheckpointedReader {
 public:
  AsyncCheckpointedReaderImpl(const std::string& reader_name,
                              std::unique_ptr<AsyncReader> reader,
                              std::unique_ptr<CheckpointStore> store,
                              CheckpointingOptions opts);

  void
  setRecordCallback(std::function<bool(std::unique_ptr<DataRecord>&)>) override;

  void setGapCallback(std::function<bool(const GapRecord&)>) override;

  void setDoneCallback(std::function<void(logid_t)>) override;

  void setHealthChangeCallback(
      std::function<void(logid_t, HealthChangeType)>) override;

  int startReading(logid_t log_id,
                   lsn_t from,
                   lsn_t until = LSN_MAX,
                   const ReadStreamAttributes* attrs = nullptr) override;

  int stopReading(logid_t log_id,
                  std::function<void()> callback = nullptr) override;

  int resumeReading(logid_t log_id) override;

  void withoutPayload() override;

  void forceNoSingleCopyDelivery() override;

  void includeByteOffset() override;

  void doNotSkipPartiallyTrimmedSections() override;

  int isConnectionHealthy(logid_t) const override;

  void doNotDecodeBufferedWrites() override;

  void getBytesBuffered(std::function<void(size_t)> callback) override;

 private:
  std::unique_ptr<AsyncReader> reader_;
};

}} // namespace facebook::logdevice
