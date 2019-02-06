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

#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/common/ZookeeperClientBase.h"

namespace facebook { namespace logdevice {

class ZookeeperVersionedConfigStore : public VersionedConfigStore {
 public:
  explicit ZookeeperVersionedConfigStore(
      extract_version_fn extract_fn,
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
  // ZookeeperVersionedConfigStore instance gets destroyed. (This is why we
  // store the extraction function and Zookeeper client as shared_ptr-s.
  ~ZookeeperVersionedConfigStore() override {}

  void getConfig(std::string key, value_callback_t cb) const override;

  // Does a linearizable read to zookeeper by doing a sync() call first and
  // then the actual read.
  void getLatestConfig(std::string key, value_callback_t cb) const override;

  void updateConfig(std::string key,
                    std::string value,
                    folly::Optional<version_t> base_version,
                    write_callback_t cb = {}) override;

 private:
  const std::shared_ptr<const extract_version_fn> extract_fn_;
  std::shared_ptr<ZookeeperClientBase> zk_;
};
}} // namespace facebook::logdevice
