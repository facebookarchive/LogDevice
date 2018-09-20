/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_Message.h"

namespace facebook { namespace logdevice {
Message::Disposition
NODE_STATS_AGGREGATE_onReceived(NODE_STATS_AGGREGATE_Message* msg,
                                const Address& from);
}} // namespace facebook::logdevice
