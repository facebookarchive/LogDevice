/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PluginPack.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

std::unique_ptr<SequencerLocator> PluginPack::createSequencerLocator(
    const std::shared_ptr<UpdateableConfig>& config) {
  return std::unique_ptr<SequencerLocator>(
      new HashBasedSequencerLocator(config->updateableServerConfig()));
}

std::unique_ptr<TraceLogger>
PluginPack::createTraceLogger(const std::shared_ptr<UpdateableConfig>& config) {
  return std::make_unique<NoopTraceLogger>(config);
}

}} // namespace facebook::logdevice
