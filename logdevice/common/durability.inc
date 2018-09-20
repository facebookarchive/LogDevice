/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef DURABILITY
#error DURABILITY() macro not defined
#define DURABILITY(...)
#endif

DURABILITY(ALL)         // All durability types. Only allowed by APIs like
                        // LocalLogStore::sync() where a wildcard makes sense.

DURABILITY(MEMORY)      // Data is written to memory until evicted by memory
                        // pressure, a time trigger, or some other mechanism.
                        // Durability can be guaranteed by invoking
                        // LocalLogStore::sync() with Durability::MEMORY or
                        // Durablility::ALL.

DURABILITY(ASYNC_WRITE) // Data is passed to the kernel (i.e. write() syscall
                        // to durable media returns success).  Data will be
                        // persisted even if the application fails, but may
                        // not survive an OS kernel failure, power interruption,
                        // or a storage device failure.
                        // Durability can be guaranteed by invoking
                        // LocalLogStore::sync() with Durability::ASYNC_WRITE
                        // or Durability::ALL.

DURABILITY(SYNC_WRITE)  // Data is confirmed to have been written to durable
                        // media.

#undef DURABILITY
