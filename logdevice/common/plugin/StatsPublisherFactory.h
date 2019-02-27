/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/StatsPublisher.h"
#include "logdevice/common/plugin/Plugin.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file `StatsPublisherFactory` will be used to create a `StatsPublisher`
 * instance.
 */

struct Settings;
template <typename T>
class UpdateableSettings;

class StatsPublisherFactory : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::STATS_PUBLISHER_FACTORY;
  }

  /**
   * If this returns non-null, the client/server will also create a
   * StatsPublishingThread, periodically collect them and push to the
   * StatsPublisher object.
   */
  virtual std::unique_ptr<StatsPublisher>
  operator()(UpdateableSettings<Settings>, int num_db_shards) = 0;
};

}} // namespace facebook::logdevice
