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

// Priority class max bytes per second limit defined according to the config.
STAT_DEFINE(max_bw, MAX)
// Priority class max burst bytes per second limit as per the config.
STAT_DEFINE(max_burst, MAX)
// Priority class guaranteed bytes per second limit as per the config.
STAT_DEFINE(guaranteed_bw, MAX)
// Per priority class bw consumed during an interval as messages are sent by the
// sender.
STAT_DEFINE(bwconsumed, SUM)
// If there are no credits left in the priority class bucket, and the
// client wants to send message on the socket, it won't be allowed to send.
// Instead, the callback or envelope will be added to a list belonging to the
// priority class/bucket. When credits are deposited to the bucket or when the
// deadline timer to run flow groups expires, we go over the list and drain
// messages.
// This uses up the new credits deposited or if credits are available in the
// priority queue bucket we use them to send message on the socket. This stat
// tracks whether there are messages still backlogged for a bucket after
// priorityq was drained
STAT_DEFINE(pq_backlog, MAX)


#undef STAT_DEFINE
#undef RESETTING_STATS
