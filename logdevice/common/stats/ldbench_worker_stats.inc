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

STAT_DEFINE(workers_started, SUM)

STAT_DEFINE(readers_assigned, SUM)
STAT_DEFINE(readers_active, SUM)
STAT_DEFINE(reader_restarting_to_backlog, SUM)
STAT_DEFINE(reader_restarting_to_tail, SUM)
STAT_DEFINE(reader_max_readers_per_log, MAX)
STAT_DEFINE(reader_restart_backlog_depth_ms_sum, SUM) // milliseconds
STAT_DEFINE(reader_num_distinct_logs, SUM)
STAT_DEFINE(reader_records, SUM)
STAT_DEFINE(reader_bytes, SUM)
STAT_DEFINE(reader_backlog_bytes, SUM)
STAT_DEFINE(reader_restarts_skipped, SUM)
STAT_DEFINE(reader_records_without_writer_info, SUM)
STAT_DEFINE(reader_avg_fanout_target_1000x, MAX)
STAT_DEFINE(reader_avg_backlog_depth_target_ms, MAX)
STAT_DEFINE(reader_rate_limit_bytes_per_sec_per_worker, MAX)
STAT_DEFINE(reader_restart_period_target_ms, SUM)
STAT_DEFINE(reader_target_restart_backlog_probability_1000x, MAX)
STAT_DEFINE(reader_gap_ACCESS, SUM)
STAT_DEFINE(reader_gap_DATALOSS, SUM)
STAT_DEFINE(reader_gap_OTHER, SUM)

STAT_DEFINE(writer_appends_skipped, SUM)
STAT_DEFINE(writer_appends_skipped_bytes_in_flight, SUM)
STAT_DEFINE(writer_appends_skipped_appends_in_flight, SUM)
STAT_DEFINE(writer_appends_in_flight, SUM)
STAT_DEFINE(writer_append_bytes_in_flight, SUM)
STAT_DEFINE(writer_throughput_total_target, MAX)
STAT_DEFINE(writer_payload_target_avg_size, MAX)
STAT_DEFINE(writer_highest_target_log_throughput, MAX)
STAT_DEFINE(writer_largest_log_avg_payload_size, MAX)
STAT_DEFINE(writer_largest_payload_size, MAX)
STAT_DEFINE(writer_max_appends_in_flight, MAX)
STAT_DEFINE(writer_max_append_bytes_in_flight, MAX)
STAT_DEFINE(writer_target_client_compressibility_1000x, MAX)
STAT_DEFINE(writer_target_sequencer_compressibility_1000x, MAX)

#undef STAT_DEFINE
