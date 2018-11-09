/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/BuiltinZookeeperClientFactory.h"

#include "logdevice/common/ZookeeperClientFactoryProd.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"

namespace facebook { namespace logdevice {

std::unique_ptr<ZookeeperClientBase> BuiltinZookeeperClientFactory::getClient(
    const configuration::ZookeeperConfig& config) {
  // For now, issue separate ZK clients, but eventually we should consolidate
  // all uses of ZK clients to one long-lived, shared ZK client (per config).
  return zkFactoryProd(config);
}

}} // namespace facebook::logdevice
