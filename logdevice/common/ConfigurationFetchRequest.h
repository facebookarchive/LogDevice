/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file Fetches a config from a given node
 */

class ConfigurationFetchRequest : public Request {
 public:
  using ConfigType = CONFIG_FETCH_Header::ConfigType;

  explicit ConfigurationFetchRequest(NodeID node_id,
                                     ConfigType ct = ConfigType::MAIN_CONFIG)
      : Request(RequestType::CONFIGURATION_FETCH),
        node_id_(node_id),
        config_type_(ct) {}

  Request::Execution execute() override;

  ~ConfigurationFetchRequest() override {}

 private:
  NodeID node_id_;
  CONFIG_FETCH_Header::ConfigType config_type_;
};

}} // namespace facebook::logdevice
