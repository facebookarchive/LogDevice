/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/NodesConfigStore.h"

namespace facebook { namespace logdevice { namespace configuration {

class InMemNodesConfigStore : public NodesConfigStore {
  using version_t = NodesConfigStore::version_t;

 public:
  explicit InMemNodesConfigStore(extract_version_fn f)
      : NodesConfigStore(std::move(f)) {}
  int getConfig(std::string key, value_callback_t cb) const override;

  Status getConfigSync(std::string key, std::string* value_out) const override;

  int updateConfig(std::string key,
                   std::string value,
                   folly::Optional<version_t> base_version,
                   write_callback_t cb = {}) override;

  Status updateConfigSync(std::string key,
                          std::string value,
                          folly::Optional<version_t> base_version,
                          version_t* version_out = nullptr,
                          std::string* value_out = nullptr) override;

 private:
  // TODO: switch to a more efficient map; avoid copying mapped_type; more
  // granular synchronization.
  folly::Synchronized<std::unordered_map<std::string, std::string>> configs_;
};
}}} // namespace facebook::logdevice::configuration
