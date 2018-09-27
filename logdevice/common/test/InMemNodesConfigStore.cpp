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
  std::string value{};
  Status status = getConfigSync(std::move(key), &value);

  if (status == Status::OK || status == Status::NOTFOUND ||
      status == Status::ACCESS || status == Status::AGAIN) {
    cb(status, value);
    return 0;
  }

  err = status;
  return -1;
}

Status InMemNodesConfigStore::getConfigSync(std::string key,
                                            std::string* value_out) const {
  {
    auto lockedConfigs = configs_.rlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      return Status::NOTFOUND;
    }
    if (value_out) {
      *value_out = it->second;
    }
  }
  return Status::OK;
}

int InMemNodesConfigStore::updateConfig(std::string key,
                                        std::string value,
                                        folly::Optional<version_t> base_version,
                                        write_callback_t cb) {
  version_t version;
  std::string value_out;
  Status status = updateConfigSync(
      std::move(key), std::move(value), base_version, &version, &value_out);

  if (status == Status::OK || status == Status::NOTFOUND ||
      status == Status::VERSION_MISMATCH || status == Status::ACCESS ||
      status == Status::AGAIN) {
    cb(status, version, std::move(value_out));
    return 0;
  }

  err = status;
  return -1;
}

Status
InMemNodesConfigStore::updateConfigSync(std::string key,
                                        std::string value,
                                        folly::Optional<version_t> base_version,
                                        version_t* version_out,
                                        std::string* value_out) {
  {
    auto lockedConfigs = configs_.wlock();
    auto it = lockedConfigs->find(key);
    if (it == lockedConfigs->end()) {
      if (base_version) {
        return Status::NOTFOUND;
      }
      setIfNotNull(version_out, extract_fn_(value));
      auto res = lockedConfigs->emplace(std::move(key), std::move(value));
      ld_assert(res.second); // inserted
      return Status::OK;
    }

    auto curr_version = extract_fn_(it->second);
    if (base_version && curr_version != base_version) {
      // conditional update version mismatch
      // TODO: set err accordingly
      setIfNotNull(version_out, curr_version);
      setIfNotNull(value_out, it->second);
      return Status::VERSION_MISMATCH;
    }
    setIfNotNull(version_out, extract_fn_(value));
    it->second = std::move(value);
  }
  return Status::OK;
}
}}} // namespace facebook::logdevice::configuration
