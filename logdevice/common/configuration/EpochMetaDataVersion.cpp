/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/EpochMetaDataVersion.h"

#include "logdevice/common/configuration/ServerConfig.h"

namespace facebook { namespace logdevice { namespace epoch_metadata_version {

type versionToWrite(const std::shared_ptr<ServerConfig>& server_cfg) {
  ld_check(server_cfg);
  auto version_in_config =
      server_cfg->getMetaDataLogsConfig().metadata_version_to_write;
  type res = version_in_config.hasValue() ? version_in_config.value() : CURRENT;
  ld_check(validToWrite(res));
  return res;
}

}}} // namespace facebook::logdevice::epoch_metadata_version
