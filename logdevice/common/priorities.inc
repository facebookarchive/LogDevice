/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef PRIORITY
#error PRIORITY(name) macro not defined
#define PRIORITY(...)
#endif

// Highest possible priority. Should be used rarely - typically
// only in cases where a client's priority isn't yet known, or
// for operations whose completion blocks read or write
// availability.
PRIORITY(MAX)
// Realtime client priority
PRIORITY(CLIENT_HIGH)
// Default client priority
PRIORITY(CLIENT_NORMAL)
// Best effort client priority. (e.g. a batch backlog reader)
PRIORITY(CLIENT_LOW)
// Operations that should have minimal impact on response times
// to normal client traffic.
PRIORITY(BACKGROUND)
// Only run if nothing else can.
PRIORITY(IDLE)

#undef PRIORITY
