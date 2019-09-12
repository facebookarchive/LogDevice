/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/VersionedConfigStore.h"

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

void VersionedConfigStore::updateConfig(std::string key,
                                        std::string value,
                                        folly::Optional<version_t> base_version,
                                        write_callback_t callback) {
  auto opt = extract_fn_(value);
  if (!opt) {
    err = E::INVALID_PARAM;
    callback(E::INVALID_PARAM, version_t{}, "");
    return;
  }

  auto mutation_func =
      [this, key, write_value = std::move(value), base_version](
          folly::Optional<std::string> current_value) mutable
      -> std::pair<Status, std::string> {
    if (base_version.hasValue()) {
      if (current_value.hasValue()) {
        auto current_version_opt = extract_fn_(*current_value);
        if (!current_version_opt) {
          RATELIMIT_WARNING(std::chrono::seconds(10),
                            5,
                            "Failed to extract version from value read from "
                            "ZookeeperNodesConfigurationStore. key: \"%s\"",
                            key.c_str());
          return std::make_pair(E::BADMSG, "");
        }
        version_t current_version = current_version_opt.value();
        if (base_version != current_version) {
          return std::make_pair(E::VERSION_MISMATCH, std::move(*current_value));
        }
      } else {
        return std::make_pair(E::NOTFOUND, "");
      }
    }
    return std::make_pair(E::OK, std::move(write_value));
  };

  write_callback_t cb =
      [this, write_callback = std::move(callback)](
          Status status, version_t version, std::string value) mutable {
        if (status == E::VERSION_MISMATCH && value.size() > 0) {
          auto current_version_opt = extract_fn_(value);
          if (!current_version_opt) {
            write_callback(Status::BADMSG, version_t{}, "");
            return;
          }
          write_callback(status, current_version_opt.value(), std::move(value));
          return;
        }
        write_callback(status, version, std::move(value));
      };

  readModifyWriteConfig(key, mutation_func, std::move(cb));
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
