/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include <folly/Function.h>
#include <folly/Optional.h>
#include <folly/Synchronized.h>

#include "logdevice/common/ZookeeperClientBase.h"
#include "logdevice/common/configuration/NodesConfigStore.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {

class ZookeeperNodesConfigStore : public NodesConfigStore {
 public:
  using version_t = membership::MembershipVersion::Type;

  explicit ZookeeperNodesConfigStore(extract_version_fn extract_fn,
                                     std::shared_ptr<ZookeeperClientBase> zk)
      : extract_fn_(
            std::make_shared<extract_version_fn>(std::move(extract_fn))),
        zk_(std::move(zk)) {
    ld_check(extract_fn_ != nullptr);
    ld_check(zk_ != nullptr);
  }

  // Note on destruction: while this class does not keep track of pending
  // callbacks, we take care to ensure that the callbacks in the async methods
  // do not captures state that would be otherwise invalid after the
  // ZookeeperNodesConfigStore instance gets destroyed. (This is why we store
  // the extraction function and Zookeeper client as shared_ptr-s.
  ~ZookeeperNodesConfigStore() override {}

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
  const std::shared_ptr<const extract_version_fn> extract_fn_;
  std::shared_ptr<ZookeeperClientBase> zk_;
};
}}} // namespace facebook::logdevice::configuration
