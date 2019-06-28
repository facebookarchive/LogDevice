/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/test/InMemVersionedConfigStore.h"

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

void InMemVersionedConfigStore::updateConfig(
    std::string key,
    std::string value,
    folly::Optional<version_t> base_version,
    write_callback_t cb) {
  version_t version;
  std::string value_out;
  Status status = updateConfigSync(
      std::move(key), std::move(value), base_version, &version, &value_out);

  cb(status, version, std::move(value_out));
}

Status InMemVersionedConfigStore::updateConfigSync(
    std::string key,
    std::string value,
    folly::Optional<version_t> base_version,
    version_t* version_out,
    std::string* value_out) {
  auto opt = extract_fn_(value);
  if (!opt) {
    return Status::INVALID_PARAM;
  }
  version_t value_version = opt.value();
  {
    auto lockedConfigs = configs_.wlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      if (base_version) {
        return Status::NOTFOUND;
      }
      set_if_not_null(version_out, value_version);
      auto res = lockedConfigs->emplace(std::move(key), std::move(value));
      ld_assert(res.second); // inserted
      return Status::OK;
    }

    auto curr_version_opt = extract_fn_(it->second);
    if (!opt) {
      return Status::INVALID_PARAM;
    }
    version_t curr_version = curr_version_opt.value();
    if (base_version && curr_version != base_version) {
      // conditional update version mismatch
      // TODO: set err accordingly
      set_if_not_null(version_out, curr_version);
      set_if_not_null(value_out, it->second);
      return Status::VERSION_MISMATCH;
    }
    set_if_not_null(version_out, value_version);
    it->second = std::move(value);
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
