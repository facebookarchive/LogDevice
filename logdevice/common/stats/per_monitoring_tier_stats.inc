/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
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

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */
#ifndef RESETTING_STATS

STAT_DEFINE(num_read_streams, SUM)
STAT_DEFINE(read_streams_stuck, SUM)
STAT_DEFINE(read_streams_lagging, SUM)

#endif

#undef STAT_DEFINE
#undef RESETTING_STATS
