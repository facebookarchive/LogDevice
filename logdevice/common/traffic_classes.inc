/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef TRAFFIC_CLASS
#error TRAFFIC_CLASS() macro not defined
#define TRAFFIC_CLASS(...)
#endif

TRAFFIC_CLASS(APPEND)             // Adding new records.
TRAFFIC_CLASS(FAILURE_DETECTOR)   // Node failure detection.
TRAFFIC_CLASS(HANDSHAKE)          // Connection negotiation.
TRAFFIC_CLASS(RSM)                // Reading or writing RSM logs.
TRAFFIC_CLASS(TRIM)               // Explicitly culling old records.
TRAFFIC_CLASS(READ_TAIL)          // Reading from in-core log data.
TRAFFIC_CLASS(READ_BACKLOG)       // Reading from disk or explicitly marked as
                                  // a low priority reader.
TRAFFIC_CLASS(REBUILD)            // Reading or writing a record to restore its
                                  // replication factor.
TRAFFIC_CLASS(RECOVERY)           // Sequencer recovery and purging of
                                  // unclean epochs.

#undef TRAFFIC_CLASS
