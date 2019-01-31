/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

// An implementation of a NodesConfigurationStore where the NodesConfiguration
// is fetched from random servers in the cluster.
class ServerBasedNodesConfigurationStore : public NodesConfigurationStore {
 public:
  ~ServerBasedNodesConfigurationStore() override = default;

  // Enqueues a ConfigurationFetchRequest request of type NodesConfiguration on
  // the same worker, with a request callback invoking the passed `cb`. The
  // config fetch request is sent to a random serer in the cluster.
  virtual void getConfig(value_callback_t cb) const override;

  // We can't support a sync config fetch easily. The `getConfig` methods posts
  // a ConfigurationFetchRequest which may end up scheduled on this worker
  // causing a deadlock.
  virtual Status getConfigSync(std::string* value_out) const override;

  // This is a read only NodesConfigStore. Updates are not supported. This
  // function calls the callback immediately with Status::NOTSUPPORTED.
  virtual void updateConfig(std::string value,
                            folly::Optional<version_t> base_version,
                            write_callback_t cb = {}) override;

  // This is a read-only NodesConfigStore. Updates are not supported. This
  // function returns immediately with Status::NOTSUPPORTED.
  virtual Status updateConfigSync(std::string value,
                                  folly::Optional<version_t> base_version,
                                  version_t* version_out = nullptr,
                                  std::string* value_out = nullptr) override;
};
}}}} // namespace facebook::logdevice::configuration::nodes
