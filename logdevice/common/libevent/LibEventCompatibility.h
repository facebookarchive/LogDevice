/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/libevent/EvBaseLegacy.h"
#include "logdevice/common/libevent/EvTimerLegacy.h"
#include "logdevice/common/libevent/EventLegacy.h"

namespace facebook { namespace logdevice {

using EvBase = EvBaseLegacy;
using Event = EventLegacy;
using EvTimer = EvTimerLegacy;

}} // namespace facebook::logdevice
