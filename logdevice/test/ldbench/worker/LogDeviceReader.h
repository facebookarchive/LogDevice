/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Client.h"
#include "logdevice/test/ldbench/worker/LogStoreReader.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogDeviceClient;

class LogDeviceReader : public LogStoreReader {
 public:
  explicit LogDeviceReader(LogDeviceClient* client);

  bool startReading(LogIDType logid,
                    LogPositionType start,
                    LogPositionType end) override;
  bool stopReading(LogIDType logid) override;
  bool resumeReading(LogIDType logid) override;

 private:
  std::unique_ptr<AsyncReader> async_reader_;
  LogDeviceClient* owner_client_;
};

}}} // namespace facebook::logdevice::ldbench
