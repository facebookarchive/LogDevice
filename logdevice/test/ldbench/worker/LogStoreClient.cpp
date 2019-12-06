/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogStoreClient.h"

namespace facebook { namespace logdevice { namespace ldbench {

void LogStoreClient::setAppendStatsUpdateCallBack(
    write_stats_update_callback_t stats_update_cb) {
  writer_stats_update_cb_ = stats_update_cb;
}

void LogStoreClient::setReadStatsUpdateCallBack(
    read_stats_update_callback_t reader_stats_updates_cb) {
  reader_stats_updates_cb_ = reader_stats_updates_cb;
}

}}} // namespace facebook::logdevice::ldbench
