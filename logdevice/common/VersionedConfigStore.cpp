/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/VersionedConfigStore.h"

#include <chrono>

#include <folly/synchronization/Baton.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

Status VersionedConfigStore::getConfigSync(
    std::string key,
    std::string* value_out,
    folly::Optional<version_t> base_version) const {
  folly::Baton<> b;
  Status ret_status = Status::OK;
  value_callback_t cb = [&b, &ret_status, value_out](
                            Status status, std::string value) {
    set_if_not_null(&ret_status, status);
    if (status == Status::OK) {
      set_if_not_null(value_out, std::move(value));
    }
    b.post();
  };

  getConfig(std::move(key), std::move(cb), std::move(base_version));
  b.wait();

  return ret_status;
}

Status
VersionedConfigStore::updateConfigSync(std::string key,
                                       std::string value,
                                       folly::Optional<version_t> base_version,
                                       version_t* version_out,
                                       std::string* value_out) {
  folly::Baton<> b;
  Status ret_status = Status::OK;
  write_callback_t cb =
      [&b, &ret_status, version_out, value_out](
          Status status, version_t current_version, std::string current_value) {
        set_if_not_null(&ret_status, status);
        if (status == Status::OK || status == Status::VERSION_MISMATCH) {
          set_if_not_null(version_out, current_version);
          set_if_not_null(value_out, std::move(current_value));
        }
        b.post();
      };

  updateConfig(std::move(key), std::move(value), base_version, std::move(cb));
  b.wait();
  return ret_status;
}

}} // namespace facebook::logdevice
