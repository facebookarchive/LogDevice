/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef STORAGE_TASK_PRINCIPAL
#error STORAGE_TASK_PRINCIPAL() macro not defined
#define STORAGE_TASK_PRINCIPAL(...)
#endif

/*
 * Storage tasks Principals and their shares. See DRRScheduler.h for
 * details on how the shares work. Each principal gets a proportional
 * share of the underlying resource (in this case disk), scaled by a
 * 'quanta' if specified.
 * Try to map multiple IO streams to a few principals. Use principal
 * as a bucket for a set of IO streams that need reservation. E.g.,
 * all small size, random-access, storage tasks are currently mapped to
 * METADATA, including those related to rebuilding. But the main data
 * read rebuilding task has its own principal. A new scanning read, like
 * SCRUB, deserves its own principal. While a new infrequent random-access
 * internal read client could go to MISC.
 */
STORAGE_TASK_PRINCIPAL(MISC, read-misc, "1")
STORAGE_TASK_PRINCIPAL(COMPACTION_PARTIAL, read-compaction-partial, "1")
STORAGE_TASK_PRINCIPAL(REBUILD, read-rebuild, "3")
STORAGE_TASK_PRINCIPAL(COMPACTION_RETENTION, read-compaction-retention, "3")
STORAGE_TASK_PRINCIPAL(READ_BACKLOG, read-backlog, "5")
STORAGE_TASK_PRINCIPAL(READ_TAIL, read-tail, "8")
STORAGE_TASK_PRINCIPAL(READ_INTERNAL, read-internal, "10")
STORAGE_TASK_PRINCIPAL(METADATA, read-metadata, "10")
STORAGE_TASK_PRINCIPAL(FINDKEY, read-findkey, "10")

#undef STORAGE_TASK_PRINCIPAL
