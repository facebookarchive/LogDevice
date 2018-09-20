/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef LOG_METADATA_TYPE
#error LOG_METADATA_TYPE() macro not defined
#define LOG_METADATA_TYPE(...)
#endif


LOG_METADATA_TYPE(LAST_RELEASED, 0)
LOG_METADATA_TYPE(TRIM_POINT, 1)
LOG_METADATA_TYPE(SEAL, 2)
LOG_METADATA_TYPE(LAST_CLEAN, 3)
LOG_METADATA_TYPE(DEPRECATED_1, 4)
LOG_METADATA_TYPE(DEPRECATED_2, 5)
LOG_METADATA_TYPE(DEPRECATED_3, 6)
LOG_METADATA_TYPE(LOG_REMOVAL_TIME, 7)
LOG_METADATA_TYPE(REBUILDING_CHECKPOINT, 8)
LOG_METADATA_TYPE(SOFT_SEAL, 9)
LOG_METADATA_TYPE(MAX, 10)

#undef LOG_METADATA_TYPE
