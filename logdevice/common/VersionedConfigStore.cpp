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

using Condition = VersionedConfigStore::Condition;

VersionedConfigStore::Condition::Condition(
    std::variant<version_t, OverwriteTag, CreateIfNotExistsTag> cond)
    : condition_(std::move(cond)) {}

VersionedConfigStore::Condition::Condition(version_t version)
    : condition_(version) {}

VersionedConfigStore::version_t
VersionedConfigStore::Condition::getVersion() const {
  ld_check(hasVersion());
  return std::get<version_t>(condition_);
}
bool VersionedConfigStore::Condition::hasVersion() const {
  return std::holds_alternative<version_t>(condition_);
}

/* static */ Condition VersionedConfigStore::Condition::createIfNotExists() {
  static const Condition ret{CreateIfNotExistsTag{}};
  return ret;
}
/* static */ Condition VersionedConfigStore::Condition::overwrite() {
  static const Condition ret{OverwriteTag{}};
  return ret;
}

bool VersionedConfigStore::Condition::isOverwrite() const {
  return std::holds_alternative<OverwriteTag>(condition_);
}

bool VersionedConfigStore::Condition::isCreateIfNotExists() const {
  return std::holds_alternative<CreateIfNotExistsTag>(condition_);
}

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
                                        Condition base_version,
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
    folly::Optional<version_t> current_version_opt;
    if (current_value.hasValue()) {
      current_version_opt = extract_fn_(*current_value);
      if (!current_version_opt) {
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          5,
                          "Failed to extract version from value read from "
                          "ZookeeperNodesConfigurationStore. key: \"%s\"",
                          key.c_str());
        return std::make_pair(E::BADMSG, "");
      }
    }
    if (auto status = isAllowedUpdate(base_version, current_version_opt);
        status != E::OK) {
      return std::make_pair(status, std::move(current_value).value_or(""));
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

Status VersionedConfigStore::updateConfigSync(std::string key,
                                              std::string value,
                                              Condition base_version,
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

Status VersionedConfigStore::isAllowedUpdate(
    Condition condition,
    folly::Optional<version_t> current_version) const {
  if (condition.isOverwrite()) {
    return E::OK;
  } else if (condition.isCreateIfNotExists()) {
    return current_version.hasValue() ? E::VERSION_MISMATCH : E::OK;
  } else if (condition.hasVersion()) {
    if (!current_version.hasValue()) {
      return E::NOTFOUND;
    }
    return current_version.value() == condition.getVersion()
        ? E::OK
        : E::VERSION_MISMATCH;
  }
  ld_check(false);
  return E::INTERNAL;
}

}} // namespace facebook::logdevice
