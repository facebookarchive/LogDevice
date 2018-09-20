/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/GAP_Message.h"

namespace facebook { namespace logdevice {
void GAP_onSent(const GAP_Message& msg,
                Status st,
                const Address& to,
                const SteadyTimestamp enqueue_time);
}} // namespace facebook::logdevice
