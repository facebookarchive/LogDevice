/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

__thread E err; // see Err.h

}} // namespace facebook::logdevice
