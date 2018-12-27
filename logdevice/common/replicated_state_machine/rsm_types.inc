/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef RSM_TYPE
#error RSM_TYPE() macro not defined
#define RSM_TYPE(...)
#endif

// Args: enum_name, string_name

RSM_TYPE(EVENT_LOG_STATE_MACHINE, "eventlog")
RSM_TYPE(LOGS_CONFIG_STATE_MACHINE, "logsconfig")
RSM_TYPE(MAINTENANCE_LOG_STATE_MACHINE, "maintenancelog")

#undef RSM_TYPE
