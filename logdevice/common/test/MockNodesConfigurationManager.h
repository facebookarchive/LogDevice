/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfigurationAPI.h"

namespace facebook { namespace logdevice { namespace configuration {

/**
 * @file MockNodesConfigurationManager is an implementation to the
 * NodesConfigurationAPI that just persists the NodesConfiguration in memory.
 */
class MockNodesConfigurationManager : public NodesConfigurationAPI {
 public:
  explicit MockNodesConfigurationManager(
      const nodes::NodesConfiguration& initial_nc = nodes::NodesConfiguration{})
      : nc_{std::make_shared<const nodes::NodesConfiguration>(initial_nc)} {}
  virtual ~MockNodesConfigurationManager() override = default;

  virtual std::shared_ptr<const nodes::NodesConfiguration>
  getConfig() const override;

  virtual void update(nodes::NodesConfiguration::Update update,
                      CompletionCb callback) override;

  virtual void
  overwrite(std::shared_ptr<const nodes::NodesConfiguration> configuration,
            CompletionCb callback) override;

 private:
  std::shared_ptr<const nodes::NodesConfiguration> nc_;
};

}}} // namespace facebook::logdevice::configuration
