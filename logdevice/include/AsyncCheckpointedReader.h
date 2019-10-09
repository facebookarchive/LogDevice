/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/CheckpointedReaderBase.h"

namespace facebook { namespace logdevice {

/*
 * @file AsyncCheckpointedReader proxies all AsyncReader functions but also
 *   provides checkpointing by inheriting CheckpointedReaderBase class.
 */
class AsyncCheckpointedReader : public CheckpointedReaderBase,
                                public AsyncReader {
 public:
  AsyncCheckpointedReader(const std::string& reader_name,
                          std::unique_ptr<CheckpointStore> store,
                          CheckpointingOptions opts)
      : CheckpointedReaderBase(reader_name, std::move(store), std::move(opts)) {
  }

  /*
   * Runs asynchronously startReading on underlying reader with status handling
   * callback. If underlying startReading returns -1, the status will be set to
   * err variable, according to startReading description.
   */
  virtual void
  startReadingFromCheckpoint(logid_t log_id,
                             StatusCallback cb,
                             lsn_t until = LSN_MAX,
                             const ReadStreamAttributes* attrs = nullptr) = 0;
};

}} // namespace facebook::logdevice
