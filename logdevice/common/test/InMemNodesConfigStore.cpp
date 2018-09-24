/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/test/InMemNodesConfigStore.h"

namespace facebook { namespace logdevice { namespace configuration {

//////// InMemNodesConfigStore ////////

namespace {
template <typename T>
void setIfNotNull(std::decay_t<T>* output, T&& value) {
  if (output) {
    *output = std::forward<T>(value);
  }
}
} // namespace

int InMemNodesConfigStore::getConfig(std::string key,
                                     value_callback_t cb) const {
  Status status;
  std::string value{};
  int rv = getConfigSync(std::move(key), &status, &value);
  if (rv != 0) {
    return rv;
  }

  if (status == Status::OK || status == Status::NOTFOUND ||
      status == Status::ACCESS) {
    cb(status, value);
  }
  return 0;
}

int InMemNodesConfigStore::getConfigSync(std::string key,
                                         Status* status_out,
                                         std::string* value_out) const {
  {
    auto lockedConfigs = configs_.rlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      // TODO: set err accordingly
      setIfNotNull(status_out, Status::NOTFOUND);
      return -1;
    }
    if (value_out) {
      *value_out = it->second;
    }
  }
  setIfNotNull(status_out, Status::OK);
  return 0;
}

int InMemNodesConfigStore::updateConfig(std::string key,
                                        std::string value,
                                        folly::Optional<version_t> base_version,
                                        write_callback_t cb) {
  Status status;
  version_t version;
  std::string value_out;
  int rv = updateConfigSync(std::move(key),
                            &status,
                            std::move(value),
                            base_version,
                            &version,
                            &value_out);
  if (rv != 0) {
    return rv;
  }

  if (status == Status::OK || status == Status::NOTFOUND ||
      status == Status::VERSION_MISMATCH || status == Status::ACCESS ||
      status == Status::AGAIN) {
    cb(status, version, std::move(value_out));
  }
  return 0;
}

int InMemNodesConfigStore::updateConfigSync(
    std::string key,
    Status* status_out,
    std::string value,
    folly::Optional<version_t> base_version,
    version_t* version_out,
    std::string* value_out) {
  {
    auto lockedConfigs = configs_.wlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      if (base_version) {
        // TODO: set err accordingly
        setIfNotNull(status_out, Status::NOTFOUND);
        return -1;
      }
      setIfNotNull(version_out, extract_fn_(value));
      auto res = lockedConfigs->emplace(std::move(key), std::move(value));
      ld_assert(res.second); // inserted
      setIfNotNull(status_out, Status::OK);
      return 0;
    }

    auto curr_version = extract_fn_(it->second);
    if (base_version && curr_version != base_version) {
      // conditional update version mismatch
      // TODO: set err accordingly
      setIfNotNull(version_out, curr_version);
      setIfNotNull(status_out, Status::VERSION_MISMATCH);
      setIfNotNull(value_out, it->second);
      return -1;
    }
    setIfNotNull(version_out, extract_fn_(value));
    it->second = std::move(value);
  }
  setIfNotNull(status_out, Status::OK);
  return 0;
}
}}} // namespace facebook::logdevice::configuration
