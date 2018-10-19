/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/lib/verifier/LogDeviceDataSourceWriter.h"

#include <chrono>
#include <exception>
#include <unordered_map>

#include <boost/algorithm/string.hpp>
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

LogDeviceDataSourceWriter::LogDeviceDataSourceWriter(
    std::shared_ptr<logdevice::Client> c)
    : client_(c) {}

// Like ClientImpl::append, will return 0 if append succeeds, -1 otherwise.
int LogDeviceDataSourceWriter::append(logid_t logid,
                                      std::string payload,
                                      append_callback_t cb,
                                      AppendAttributes attrs) {
  return client_->append(logid, std::move(payload), cb, attrs);
}

}} // namespace facebook::logdevice
