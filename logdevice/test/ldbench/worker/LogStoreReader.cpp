/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogStoreReader.h"

namespace facebook { namespace logdevice { namespace ldbench {
void LogStoreReader::setWorkerRecordCallback(
    logstore_record_callback_t record_callback) {
  worker_record_callback_ = record_callback;
}

void LogStoreReader::setWorkerGapCallback(
    logstore_gap_callback_t gap_callback) {
  worker_gap_callback_ = gap_callback;
}

void LogStoreReader::setWorkerDoneCallback(
    logstore_done_callback_t done_callback) {
  worker_done_callback_ = done_callback;
}
}}} // namespace facebook::logdevice::ldbench
