/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ZookeeperClientBase.h"

namespace facebook { namespace logdevice {

/**
 * Production Zookeper factory used to create ZookeeperClient instances,
 * which connect to Zookeeper servers.
 */
std::unique_ptr<ZookeeperClientBase>
zkFactoryProd(const configuration::ZookeeperConfig& config);

}} // namespace facebook::logdevice
