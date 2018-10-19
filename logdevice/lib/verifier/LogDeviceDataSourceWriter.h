/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <folly/Random.h>
#include <folly/SharedMutex.h>

#include "logdevice/common/toString.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/DataSourceWriter.h"

namespace facebook { namespace logdevice {

// Wrapper class that contains all interactions with LogDevice Client API
class LogDeviceDataSourceWriter : public DataSourceWriter {
 public:
  explicit LogDeviceDataSourceWriter(std::shared_ptr<logdevice::Client> c);

  int append(logid_t logid,
             std::string payload,
             append_callback_t cb,
             AppendAttributes attrs = AppendAttributes()) override;

 private:
  std::shared_ptr<logdevice::Client> client_;
};
}} // namespace facebook::logdevice
