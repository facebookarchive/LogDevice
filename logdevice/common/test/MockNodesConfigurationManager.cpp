/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/MockNodesConfigurationManager.h"

namespace facebook { namespace logdevice { namespace configuration {

using namespace facebook::logdevice::configuration::nodes;

std::shared_ptr<const nodes::NodesConfiguration>
MockNodesConfigurationManager::getConfig() const {
  return nc_;
}

void MockNodesConfigurationManager::update(
    nodes::NodesConfiguration::Update update,
    CompletionCb callback) {
  auto new_nc = nc_->applyUpdate(update);
  if (new_nc == nullptr) {
    callback(err, nullptr);
    return;
  }
  nc_ = new_nc;
  callback(Status::OK, nc_);
}

void MockNodesConfigurationManager::overwrite(
    std::shared_ptr<const nodes::NodesConfiguration> configuration,
    CompletionCb callback) {
  nc_ = configuration;
  callback(Status::OK, nc_);
}

}}} // namespace facebook::logdevice::configuration
