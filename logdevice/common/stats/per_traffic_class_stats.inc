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

STAT_DEFINE(messages_sent, SUM)
STAT_DEFINE(messages_received, SUM)
STAT_DEFINE(bytes_sent, SUM)
STAT_DEFINE(bytes_received, SUM)

STAT_DEFINE(gap_messages_sent, SUM)

STAT_DEFINE(read_streams_created, SUM)

STAT_DEFINE(record_messages_sent, SUM)
// Payload bytes sent to clients in RECORDs
STAT_DEFINE(record_payload_bytes, SUM)

STAT_DEFINE(started_messages_sent, SUM)

// Number of STORE messages received
STAT_DEFINE(store_received, SUM)
// Payload bytes sent by sequencers to storage nodes in STORE messages
STAT_DEFINE(store_payload_bytes, SUM)

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */
#ifndef RESETTING_STATS

// Number of active server read streams
STAT_DEFINE(read_streams, SUM)
// Number of server read streams that are catching up
STAT_DEFINE(read_streams_num_catching_up, SUM)
// Number of server read streams waiting for their windows to slide
STAT_DEFINE(read_streams_num_window_end, SUM)

#endif // RESETTING_STATS

#undef STAT_DEFINE
#undef RESETTING_STATS
