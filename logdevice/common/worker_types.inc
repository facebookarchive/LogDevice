/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef WORKER_TYPE
#error WORKER_TYPE() macro is not defined
#define WORKER_TYPE(...)
#endif

/*
 * The general worker pool for handling socket messages and most of logdevice
 * internal processing
 */
WORKER_TYPE(GENERAL, 'G')
/**
 * Speciailised worker pool (normally 1 worker) for handling gossip messages
 * between logdevice cluster nodes
 */
WORKER_TYPE(FAILURE_DETECTOR, 'F')
/**
 * A special worker pool designated for time-insensitive operations
 * and state machines
 */
WORKER_TYPE(BACKGROUND, 'B')

#undef WORKER_TYPE
