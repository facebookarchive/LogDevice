/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/include/Err.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {
namespace parser {

bool parseNodes(const folly::dynamic& clusterMap,
                ServerConfig::NodesConfig& output);

bool parseHostString(const std::string& hostStr,
                     Sockaddr& addr_out,
                     const std::string& fieldName);

}}}} // namespace facebook::logdevice::configuration::parser
