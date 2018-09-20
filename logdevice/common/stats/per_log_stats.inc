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

// Number of active server read streams
STAT_DEFINE(read_streams, SUM)
// Number of appends that succeeded
STAT_DEFINE(append_success, SUM)
// Number of appends that failed (excluding preempted)
STAT_DEFINE(append_failed, SUM)
// Number of RECORD messages sent for the log
STAT_DEFINE(records_sent, SUM)
// Number of FINDKEY messages received for the log
STAT_DEFINE(findkey_received, SUM)
// Number of GET_LOG_INFO messages received for the log
STAT_DEFINE(get_log_info_received, SUM)
// Number of TRIM messages received for the log
STAT_DEFINE(trim_received, SUM)
// Payload bytes sent by clients in APPENDs
STAT_DEFINE(append_payload_bytes, SUM)
// Payload bytes sent to clients in RECORDs
STAT_DEFINE(record_payload_bytes, SUM)
// Number of failures to deliver a metadata log record upon request because of
// empty metadata log or malformed records
STAT_DEFINE(metadata_log_read_failed, SUM)
// Number of IS_LOG_EMPTY requests received
STAT_DEFINE(is_log_empty_received, SUM)
// Number of IS_LOG_EMPTY_REPLY responses sent with TRUE answer
STAT_DEFINE(is_log_empty_reply_true, SUM)
// Number of IS_LOG_EMPTY_REPLY responses sent with FALSE answer
STAT_DEFINE(is_log_empty_reply_false, SUM)
// Number of IS_LOG_EMPTY_REPLY responses sent with ERROR status
STAT_DEFINE(is_log_empty_reply_error, SUM)
// Number of DATA_SIZE_REPLY responses sent with a successful answer
STAT_DEFINE(data_size_reply, SUM)
// Number of DATA_SIZE_REPLY responses sent with ERROR status
STAT_DEFINE(data_size_reply_error, SUM)
// Number of GET_TRIM_POINT requests received
STAT_DEFINE(get_trim_point_received, SUM)
// Number of GET_TRIM_POINT response sent with OK status
STAT_DEFINE(get_trim_point_reply_ok, SUM)
// Number of GET_TRIM_POINT response sent with ERROR status_
STAT_DEFINE(get_trim_point_reply_error, SUM)
// Number of BYTEOFFSET messages received for the log
STAT_DEFINE(byte_offset_received, SUM)
// Number of GET_SEQ_STATE messages sent with UNRELEASED_RECORD context
STAT_DEFINE(get_seq_state_sent_context_unreleased_record, SUM)
// Number of GET_SEQ_STATE messages received with UNRELEASED_RECORD context
STAT_DEFINE(get_seq_state_received_context_unreleased_record, SUM)
// Number of DATA_SIZE requests received
STAT_DEFINE(data_size_received, SUM)

#undef STAT_DEFINE
