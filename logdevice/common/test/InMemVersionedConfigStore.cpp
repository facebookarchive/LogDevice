/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/test/InMemVersionedConfigStore.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

//////// InMemVersionedConfigStore ////////

void InMemVersionedConfigStore::getConfig(
    std::string key,
    value_callback_t cb,
    folly::Optional<version_t> base_version) const {
  std::string value{};
  Status status = getConfigSync(std::move(key), &value, base_version);
  cb(status, std::move(value));
}

Status InMemVersionedConfigStore::getConfigSync(
    std::string key,
    std::string* value_out,
    folly::Optional<version_t> base_version) const {
  {
    auto lockedConfigs = configs_.rlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      return Status::NOTFOUND;
    }

    if (base_version.hasValue()) {
      auto opt = extract_fn_(it->second);
      if (!opt) {
        return Status::BADMSG;
      }

      if (opt.value() <= base_version.value()) {
        *value_out = "";
        return Status::UPTODATE;
      }
    }

    if (value_out) {
      *value_out = it->second;
    }
  }
  return Status::OK;
}

void InMemVersionedConfigStore::getLatestConfig(std::string key,
                                                value_callback_t cb) const {
  getConfig(std::move(key), std::move(cb));
}

void InMemVersionedConfigStore::readModifyWriteConfig(std::string key,
                                                      mutation_callback_t mcb,
                                                      write_callback_t cb) {
  std::string current_value;
  auto status = getConfigSync(key, &current_value);
  if (status != E::OK && status != E::NOTFOUND) {
    cb(status, version_t{}, "");
    return;
  }
  folly::Optional<version_t> cur_ver = folly::none;
  if (status == E::OK) {
    auto curr_version_opt = extract_fn_(current_value);
    if (!curr_version_opt) {
      cb(E::BADMSG, version_t{}, "");
      return;
    }
    cur_ver = curr_version_opt.value();
  }
  auto status_value = (mcb)((status == E::NOTFOUND)
                                ? folly::none
                                : folly::Optional<std::string>(current_value));
  auto& write_value = status_value.second;
  if (status_value.first != E::OK) {
    cb(status_value.first, version_t{}, std::move(write_value));
    return;
  }

  version_t version;
  std::string value_out;
  status = updateConfigSync(
      std::move(key),
      std::move(write_value),
      cur_ver.hasValue() ? cur_ver.value()
                         : VersionedConfigStore::Condition::createIfNotExists(),
      &version,
      &value_out);

  cb(status, version, std::move(value_out));
}

Status InMemVersionedConfigStore::updateConfigSync(std::string key,
                                                   std::string value,
                                                   Condition base_version,
                                                   version_t* version_out,
                                                   std::string* value_out) {
  auto opt = extract_fn_(value);
  if (!opt) {
    return Status::INVALID_PARAM;
  }
  version_t value_version = opt.value();
  {
    auto lockedConfigs = configs_.wlock();

    folly::Optional<version_t> curr_version_opt;
    auto it = lockedConfigs->find(key);
    if (it != lockedConfigs->end()) {
      curr_version_opt = extract_fn_(it->second);
      if (!curr_version_opt) {
        return Status::BADMSG;
      }
    }

    // TODO: Add stricter enforcement of monotonic increment of version.
    if (curr_version_opt.hasValue() &&
        value_version.val() <= curr_version_opt->val()) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        5,
                        "Config value's version is not monitonically increasing"
                        "key: \"%s\". prev version: \"%lu\". version: \"%lu\"",
                        key.c_str(),
                        curr_version_opt->val(),
                        value_version.val());
    }

    if (auto status = isAllowedUpdate(base_version, curr_version_opt);
        status != E::OK) {
      // conditional update version mismatch
      // TODO: set err accordingly
      set_if_not_null(version_out, curr_version_opt.value_or(version_t(0)));
      set_if_not_null(value_out, curr_version_opt.hasValue() ? it->second : "");
      return status;
    }
    set_if_not_null(version_out, value_version);
    (*lockedConfigs)[std::move(key)] = std::move(value);
  }
  return Status::OK;
}

void InMemVersionedConfigStore::shutdown() {
  throw std::runtime_error("unimplemented");
}

/* static */ constexpr const folly::StringPiece TestEntry::kValue;
/* static */ constexpr const folly::StringPiece TestEntry::kVersion;

/* static */ VersionedConfigStore::version_t
TestEntry::extractVersionFn(folly::StringPiece buf) {
  return TestEntry::fromSerialized(buf).version();
}

/* static */
TestEntry TestEntry::fromSerialized(folly::StringPiece buf) {
  auto d = folly::parseJson(buf);
  version_t version{
      folly::to<typename version_t::raw_type>(d.at(kVersion).getString())};
  return TestEntry{version, d.at(kValue).getString()};
}

std::string TestEntry::serialize() const {
  folly::dynamic d = folly::dynamic::object(kValue, value_)(
      kVersion, folly::to<std::string>(version_.val()));
  return folly::toJson(d);
}
}} // namespace facebook::logdevice
