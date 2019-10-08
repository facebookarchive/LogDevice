/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/CheckpointedReaderBase.h"
#include "logdevice/include/Reader.h"

namespace facebook { namespace logdevice {

/*
 * @file SyncCheckpointedReader proxies all Reader functions but also
 *   provides checkpointing by inheriting CheckpointedReaderBase class.
 */
class SyncCheckpointedReader : public CheckpointedReaderBase, public Reader {
 public:
  SyncCheckpointedReader(const std::string& reader_name,
                         std::unique_ptr<CheckpointStore> store,
                         CheckpointingOptions opts)
      : CheckpointedReaderBase(reader_name, std::move(store), opts) {}

  /*
   * This function is blocking.
   */
  virtual int
  startReadingFromCheckpoint(logid_t log_id,
                             lsn_t until = LSN_MAX,
                             const ReadStreamAttributes* attrs = nullptr) = 0;
};

}} // namespace facebook::logdevice
