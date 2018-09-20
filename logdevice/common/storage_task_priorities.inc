/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef STORAGE_TASK_PRIORITY
#error STORAGE_TASK_PRIORITY() macro not defined
#define STORAGE_TASK_PRIORITY(...)
#endif

STORAGE_TASK_PRIORITY(LOW, "low")
STORAGE_TASK_PRIORITY(MID, "mid")
STORAGE_TASK_PRIORITY(HIGH, "high")
STORAGE_TASK_PRIORITY(VERY_HIGH, "very-high")

#undef STORAGE_TASK_PRIORITY
