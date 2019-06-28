/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <type_traits>
#include <unordered_map>

#include <folly/Function.h>
#include <folly/Optional.h>
#include <folly/Synchronized.h>

#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

// Backing data source of NodesConfig, used both by the LogDevice server and by
// tooling. This is mostly owned and accessed by the NodesConfigurationManager
// singleton. This is just an API, implementations include
// VersionedNodesConfigurationStore and ServerBasedNodesConfigurationStore.
class NodesConfigurationStore {
 public:
  using version_t = VersionedConfigStore::version_t;
  using value_callback_t = VersionedConfigStore::value_callback_t;
  using write_callback_t = VersionedConfigStore::write_callback_t;
  using extract_version_fn = VersionedConfigStore::extract_version_fn;

  virtual ~NodesConfigurationStore() = default;

  // Read the documentation of VersionedConfigStore::getConfig.
  virtual void
  getConfig(value_callback_t cb,
            folly::Optional<version_t> base_version = {}) const = 0;

  // Read the documentation of VersionedConfigStore::getConfigSync.
  virtual Status
  getConfigSync(std::string* value_out,
                folly::Optional<version_t> base_version = {}) const = 0;

  // Read the documentation of VersionedConfigStore::getLatestConfig.
  virtual void getLatestConfig(value_callback_t cb) const = 0;

  // Read the documentation of VersionedConfigStore::updateConfig.
  virtual void updateConfig(std::string value,
                            folly::Optional<version_t> base_version,
                            write_callback_t cb = {}) = 0;

  // Read the documentation of VersionedConfigStore::updateConfigSync.
  virtual Status updateConfigSync(std::string value,
                                  folly::Optional<version_t> base_version,
                                  version_t* version_out = nullptr,
                                  std::string* value_out = nullptr) = 0;

  // Read the documentation of VersionedConfigStore::shutdown.
  virtual void shutdown() = 0;
};

// A NodesConfigurationStore implementation that's backed by a
// VersionedConfigStore.
template <class T>
class VersionedNodesConfigurationStore : public NodesConfigurationStore {
  static_assert(std::is_base_of<VersionedConfigStore, T>::value,
                "The generic type should be a VersionedConfigStore");

 public:
  template <typename... Args>
  VersionedNodesConfigurationStore(std::string path, Args&&... args)
      : store_(std::make_unique<T>(std::forward<Args>(args)...)),
        path_(std::move(path)) {}

  virtual ~VersionedNodesConfigurationStore() = default;

  virtual void
  getConfig(value_callback_t cb,
            folly::Optional<version_t> base_version = {}) const override {
    store_->getConfig(path_, std::move(cb), base_version);
  }

  virtual Status
  getConfigSync(std::string* value_out,
                folly::Optional<version_t> base_version = {}) const override {
    return store_->getConfigSync(path_, value_out, base_version);
  }

  virtual void getLatestConfig(value_callback_t cb) const override {
    store_->getLatestConfig(path_, std::move(cb));
  }

  virtual void updateConfig(std::string value,
                            folly::Optional<version_t> base_version,
                            write_callback_t cb = {}) override {
    store_->updateConfig(
        path_, std::move(value), std::move(base_version), std::move(cb));
  }

  virtual Status updateConfigSync(std::string value,
                                  folly::Optional<version_t> base_version,
                                  version_t* version_out = nullptr,
                                  std::string* value_out = nullptr) override {
    return store_->updateConfigSync(path_,
                                    std::move(value),
                                    std::move(base_version),
                                    version_out,
                                    value_out);
  }

  const std::string& getPath() {
    return path_;
  }

  void shutdown() override {
    store_->shutdown();
  }

 private:
  std::unique_ptr<T> store_;
  const std::string path_;
};

}}}} // namespace facebook::logdevice::configuration::nodes
