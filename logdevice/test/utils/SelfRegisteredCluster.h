/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

class SelfRegisteredCluster : public Cluster {
 public:
  /**
   * Augments the cluster factory with the settings needed to enable self
   * registration. It also prepares the enviornment needed for a successful
   * startup of the node.
   */
  static std::unique_ptr<Cluster>
  create(ClusterFactory&& factory = ClusterFactory());
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
