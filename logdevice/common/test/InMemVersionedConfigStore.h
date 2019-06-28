/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/json.h>

#include "logdevice/common/VersionedConfigStore.h"

namespace facebook { namespace logdevice {

class InMemVersionedConfigStore : public VersionedConfigStore {
 public:
  explicit InMemVersionedConfigStore(extract_version_fn f)
      : configs_(), extract_fn_(std::move(f)) {}
  void getConfig(std::string key,
                 value_callback_t cb,
                 folly::Optional<version_t> base_version = {}) const override;

  Status
  getConfigSync(std::string key,
                std::string* value_out,
                folly::Optional<version_t> base_version = {}) const override;

  void getLatestConfig(std::string key, value_callback_t cb) const override;

  void updateConfig(std::string key,
                    std::string value,
                    folly::Optional<version_t> base_version,
                    write_callback_t cb = {}) override;

  Status updateConfigSync(std::string key,
                          std::string value,
                          folly::Optional<version_t> base_version,
                          version_t* version_out = nullptr,
                          std::string* value_out = nullptr) override;

  void shutdown() override;

 private:
  // TODO: switch to a more efficient map; avoid copying mapped_type; more
  // granular synchronization.
  folly::Synchronized<std::unordered_map<std::string, std::string>> configs_;
  extract_version_fn extract_fn_;
};

struct TestEntry {
 private:
  using version_t = VersionedConfigStore::version_t;

 public:
  constexpr static const folly::StringPiece kValue{"value"};
  constexpr static const folly::StringPiece kVersion{"version"};

  static version_t extractVersionFn(folly::StringPiece buf);

  explicit TestEntry(version_t version, std::string value)
      : version_(version), value_(std::move(value)) {}
  explicit TestEntry(version_t::raw_type version, std::string value)
      : version_(version), value_(std::move(value)) {}

  static TestEntry fromSerialized(folly::StringPiece buf);

  std::string serialize() const;

  version_t version() const {
    return version_;
  }

  std::string value() const {
    return value_;
  }

  friend std::ostream& operator<<(std::ostream& os, const TestEntry& entry) {
    return os << entry.serialize(); // whatever needed to print bar to os
  }

  bool operator==(const TestEntry& other) const {
    return version_ == other.version_ && value_ == other.value_;
  }

 private:
  version_t version_;
  std::string value_;
};
}} // namespace facebook::logdevice
