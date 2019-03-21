/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

struct MockNodeServiceDiscovery : public NodeServiceDiscovery {};

}}}} // namespace facebook::logdevice::configuration::nodes
