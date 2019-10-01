/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/CheckpointedReaderBase.h"

#include <chrono>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

CheckpointedReaderBase::CheckpointedReaderBase(
    const std::string& reader_name,
    std::unique_ptr<CheckpointStore> store,
    CheckpointingOptions opts)
    : options_(opts), reader_name_(reader_name), store_(std::move(store)) {}

Status CheckpointedReaderBase::syncWriteCheckpoints(
    const std::map<logid_t, lsn_t>& checkpoints) {
  Status return_status = Status::UNKNOWN;
  for (int retries = 0; retries < options_.num_retries; retries++) {
    return_status = store_->updateLSNSync(reader_name_, checkpoints);
    if (return_status == Status::OK) {
      return return_status;
    }
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Failed to write checkpoints for reader %s, status code "
                      "%s, trial number %d",
                      reader_name_.c_str(),
                      error_name(return_status),
                      retries);
  }
  return return_status;
}

void CheckpointedReaderBase::asyncWriteCheckpoints(
    const std::map<logid_t, lsn_t>& checkpoints,
    Callback cb) {
  auto update_cb = [cb = std::move(cb)](Status status,
                                        CheckpointStore::Version,
                                        std::string) mutable {
    // TODO: Implement versioning.
    cb(status);
  };
  store_->updateLSN(reader_name_, checkpoints, std::move(update_cb));
}

}} // namespace facebook::logdevice
