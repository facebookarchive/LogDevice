/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include <folly/Function.h>
#include <folly/Optional.h>

#include "logdevice/include/CheckpointStore.h"

namespace facebook { namespace logdevice {

struct ReadStreamAttributes;

/*
 * @file CheckpointedReaderBase provides API for checkpointing logs which
 *   will be shared between SyncCheckpointedReader and AsyncCheckpointedReader.
 */
class CheckpointedReaderBase {
 public:
  using StatusCallback = folly::Function<void(Status)>;

  struct CheckpointingOptions {
    /*
     * The number of retries when synchronously writing checkpoints. folly::none
     * means that it will retry forever.
     */
    uint32_t num_retries = 10;
  };

  /*
   * See the params of CheckpointedReaderFactory creating functions.
   */
  CheckpointedReaderBase(const std::string& reader_name,
                         std::unique_ptr<CheckpointStore> store,
                         CheckpointingOptions opts);

  virtual ~CheckpointedReaderBase() = default;

  /*
   * Writes the passed checkpoints synchronously with retries specified in opts.
   *
   * @return status: Status::OK, if the checkpoints were successfully written
   *   within the limit of retries, otherwise see the getConfigSync return value
   *   in VersionedConfigStore class, as these are equivalent.
   */
  Status syncWriteCheckpoints(const std::map<logid_t, lsn_t>& checkpoints);

  /*
   * Same as the sync version, but doesn't retry on failure.
   * @param cb: to see the possible status, see the readModifyWriteConfig cb
   *   param description in VersionedConfigStore.
   */
  void asyncWriteCheckpoints(const std::map<logid_t, lsn_t>& checkpoints,
                             StatusCallback cb);

 protected:
  CheckpointingOptions options_;
  std::string reader_name_;
  std::unique_ptr<CheckpointStore> store_;
};

}} // namespace facebook::logdevice
