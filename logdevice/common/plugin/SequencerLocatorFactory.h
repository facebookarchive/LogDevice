/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file `SequencerLocatorFactory` will be used to create a `SequencerLocator`
 * instance.
 */

class SequencerLocator;
class UpdateableConfig;

class SequencerLocatorFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::SEQUENCER_LOCATOR_FACTORY;
  }

  /**
   * Returns the SequencerLocator instance that will be used for placing
   * sequencers on servers and for clients to locate sequencers.
   */
  virtual std::unique_ptr<SequencerLocator>
  operator()(std::shared_ptr<UpdateableConfig> config) = 0;
};

}} // namespace facebook::logdevice
