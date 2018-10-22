/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file Provides location information. Used on the client to figure out where
 * the client is in relation to the nodes in terms of network topology.
 */

class LocationProvider : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::LOCATION_PROVIDER;
  }

  // Returns location in "region.datacenter.cluster.row.rack" format. For
  // example, "europe.uk1.c47.r12.hj"
  virtual std::string getMyLocation() const = 0;
};

}} // namespace facebook::logdevice
