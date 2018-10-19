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

#include <folly/SharedMutex.h>

#include "folly/Random.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"
#include "logdevice/lib/verifier/DataSourceWriter.h"
#include "logdevice/lib/verifier/GenVerifyData.h"

namespace facebook { namespace logdevice {

class VerificationWriter {
 public:
  // TODO: add default values for mydatasource and mygvd here.
  VerificationWriter(std::unique_ptr<DataSourceWriter> mydatasource,
                     std::unique_ptr<GenVerifyData> mygvd);
  int append(logid_t logid,
             std::string payload,
             append_callback_t cb,
             AppendAttributes attrs = AppendAttributes());

  std::unique_ptr<DataSourceWriter> ds_;
  std::unique_ptr<GenVerifyData> gvd_;
};
}} // namespace facebook::logdevice
