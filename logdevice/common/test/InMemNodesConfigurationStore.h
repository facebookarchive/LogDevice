/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/test/InMemVersionedConfigStore.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using InMemNodesConfigurationStore =
    VersionedNodesConfigurationStore<InMemVersionedConfigStore>;

}}}} // namespace facebook::logdevice::configuration::nodes
