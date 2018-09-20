/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef DELTA_TYPE
#error DELTA_TYPE() macro not defined
#define DELTA_TYPE(...)
#endif

DELTA_TYPE(INVALID)
DELTA_TYPE(MK_DIRECTORY)
DELTA_TYPE(MK_LOG_GROUP)
DELTA_TYPE(REMOVE)
DELTA_TYPE(RENAME)
DELTA_TYPE(SET_ATTRS)
DELTA_TYPE(SET_LOG_RANGE)
DELTA_TYPE(BATCH)
DELTA_TYPE(SET_TREE)

#undef DELTA_TYPE
