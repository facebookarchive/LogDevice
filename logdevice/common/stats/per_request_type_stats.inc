/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef STAT_DEFINE
#error STAT_DEFINE() macro not defined
#define STAT_DEFINE(...)
#endif

// Number of requests posted
STAT_DEFINE(post_request, SUM)
// Number of microseconds that workers spent processing requests of this type.
STAT_DEFINE(request_worker_usec, SUM)

#undef STAT_DEFINE
#undef RESETTING_STATS
