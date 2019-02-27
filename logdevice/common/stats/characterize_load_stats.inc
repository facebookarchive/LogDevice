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

STAT_DEFINE(scuba_error, SUM)
STAT_DEFINE(data_size_failed, SUM)
STAT_DEFINE(data_size_overflows, SUM)
STAT_DEFINE(data_size_failed_to_post, SUM)
STAT_DEFINE(get_tail_LSN_failed, SUM)
STAT_DEFINE(get_tail_LSN_failed_to_post, SUM)
STAT_DEFINE(failed_to_start_reading, SUM)
STAT_DEFINE(found_no_records_log, SUM)
STAT_DEFINE(found_no_records, SUM)
STAT_DEFINE(run_started, SUM)
STAT_DEFINE(run_finished, SUM)

#undef STAT_DEFINE
