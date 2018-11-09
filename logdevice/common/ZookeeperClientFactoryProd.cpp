/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ZookeeperClientFactoryProd.h"

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"

namespace facebook { namespace logdevice {

std::unique_ptr<ZookeeperClientBase>
zkFactoryProd(const configuration::ZookeeperConfig& config) {
  auto zookeeper_quorum = config.getQuorumString();
  if (zookeeper_quorum.empty()) {
    ld_error("\"zookeeper\" section is missing in config file");
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  try {
    return std::make_unique<ZookeeperClient>(
        zookeeper_quorum, config.getSessionTimeout());
  } catch (const ConstructorFailed&) {
    return nullptr;
  }
}

}} // namespace facebook::logdevice
