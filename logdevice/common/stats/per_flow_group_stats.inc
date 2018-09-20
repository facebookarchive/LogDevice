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

// Sender was able to release a registered message to the Socket without
// any traffic shaping induced delay.
STAT_DEFINE(direct_dispatched, SUM)
// Sender was unable to direct dispatch a registered message. It will
// either be released by consuming priority queue bandwidth at the end
// of the event loop, or when sufficient bandwidth is released to its
// priority class.
STAT_DEFINE(deferred, SUM)
// Sender was unable to direct dispatch a registered message, but a bandwidth
// available callback was provided. The callback will be registered
// (cbregistered stat will be incremented too) but this message will likely
// be discarded by the caller as it waits for its callback to fire.
STAT_DEFINE(discarded, SUM)
// A bandwidth available callback was registered via either the
// Sender::sendMessage() or Sender::canSendTo() APIs.
STAT_DEFINE(cbregistered, SUM)
// Bandwidth limit for the flow group, as defined by configuration
STAT_DEFINE(limit, SUM)
// Bandwidth thrown away because it was not used by any traffic classes for
// multiple iterations.
STAT_DEFINE(bwdiscarded, SUM)

#undef STAT_DEFINE
#undef RESETTING_STATS
