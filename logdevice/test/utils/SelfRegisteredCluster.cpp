/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/SelfRegisteredCluster.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

using namespace facebook::logdevice::configuration::nodes;

/* static */ std::unique_ptr<Cluster>
SelfRegisteredCluster::create(ClusterFactory&& factory) {
  return factory
      // Dummy metadata to satisfy the parser
      // TODO: Get rid of requiring the metadata config for NCM enabled
      // clusters.
      .setMetaDataLogsConfig(createMetaDataLogsConfig({1, 2}, 1))
      .setNodesConfigurationSourceOfTruth(
          IntegrationTestUtils::NodesConfigurationSourceOfTruth::NCM)
      .useHashBasedSequencerAssignment()
      .doNotSyncServerConfigToNodesConfiguration()
      .doNotPreProvisionNodesConfigurationStore()
      // TODO: If rebuilding is disabled the node doesn't mark itself as
      // provisioned.
      .enableSelfInitiatedRebuilding()
      .setParam("--enable-node-self-registration", "true")
      .create(0);
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
