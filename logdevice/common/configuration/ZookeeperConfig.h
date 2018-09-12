/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <vector>

#include "logdevice/common/Sockaddr.h"

namespace facebook { namespace logdevice { namespace configuration {

struct ZookeeperConfig {
  ZookeeperConfig() {}

  // Addresses of all ZK servers in the quorum we use to store and increment
  // next epoch numbers for logs
  std::vector<Sockaddr> quorum;

  // sesstion timeout to pass to zookeeper_init
  std::chrono::milliseconds session_timeout{0};

  // maximum length of cluster name string
  static const size_t MAX_CLUSTER_NAME = 127;
};

}}} // namespace facebook::logdevice::configuration
