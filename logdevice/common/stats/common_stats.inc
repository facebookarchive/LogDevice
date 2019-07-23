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

// Stats shared between server and client

// Number of calls to postRequest
STAT_DEFINE(post_request_total, SUM)
// Number of postRequest failures.
STAT_DEFINE(post_request_failed, SUM)
// Number of Requests executed on Worker threads.
STAT_DEFINE(worker_requests_executed, SUM)
// Number of Requests > request_execution_delay_threshold in execute().
STAT_DEFINE(worker_slow_requests, SUM)
// Number of tasks on background thread that spent > 10 msec executing.
STAT_DEFINE(background_slow_requests, SUM)
// TaskQueue stats.
STAT_DEFINE(worker_choose_hi_pri_work, SUM)
STAT_DEFINE(worker_enqueued_hi_pri_work, SUM)
STAT_DEFINE(worker_enqueued_mid_pri_work, SUM)
STAT_DEFINE(worker_enqueued_lo_pri_work, SUM)
STAT_DEFINE(worker_executed_hi_pri_work, SUM)
STAT_DEFINE(worker_executed_mid_pri_work, SUM)
STAT_DEFINE(worker_executed_lo_pri_work, SUM)

// number of MetaDataLogReaders created
STAT_DEFINE(metadata_log_readers_created, SUM)
// number of MetaDataLogReaders started
STAT_DEFINE(metadata_log_readers_started, SUM)
// number of MetaDataLogReaders finished
STAT_DEFINE(metadata_log_readers_finalized, SUM)

// Number of failures to deliver a metadata log record upon request.
// _corruption: because of malformed records,
// _dataloss: because of dataloss gap in metadata log
// _other: because metadata log is empty or access is denied.
STAT_DEFINE(metadata_log_read_failed_corruption, SUM)
STAT_DEFINE(metadata_log_read_dataloss, SUM)
STAT_DEFINE(metadata_log_read_failed_other, SUM)


// Number of CONFIG_CHANGED_Messages received with Action::Reload.
STAT_DEFINE(config_changed_reload, SUM)
// Number of CONFIG_CHANGED_Messages ignored because the config is already
// up-to-date.
STAT_DEFINE(config_changed_ignored_uptodate, SUM)
// Number of CONFIG_CHANGED_Messages ignored because the config is not trusted.
STAT_DEFINE(config_changed_ignored_not_trusted, SUM)
// Number of CONFIG_CHANGED_Messages ignored because one with same or newer
// config is already being processed.
STAT_DEFINE(config_changed_ignored_update_in_progress, SUM)
// Number of config updates that occurred as a result of
// CONFIG_CHANGED_Messages.
STAT_DEFINE(config_changed_update, SUM)

// Number of times nodes configuration polling gets a success result
STAT_DEFINE(nodes_configuration_polling_success, SUM)
// Number of times nodes configuration polling gets a PARTIAL result
STAT_DEFINE(nodes_configuration_polling_partial, SUM)
// Number of times nodes configuration polling gets a failure result
STAT_DEFINE(nodes_configuration_polling_failed, SUM)

// Set to 1 once the node received valid config. It also resets to 0 if a bad
// config is received, so this is really the validity of the most recent config
// received.
STAT_DEFINE(last_config_invalid, SUM)
// Config update rejected by a hook
STAT_DEFINE(config_update_invalid, SUM)

// number of times nodeset finder got metadata from sequencer
STAT_DEFINE(nodeset_finder_read_from_sequencer, SUM)
// number of times nodeset finder got metadata from metadata log
STAT_DEFINE(nodeset_finder_read_from_metadata_log, SUM)
// number of times nodeset finder failed to get metadata from sequencer
// and fallback to read from metadata log
STAT_DEFINE(nodeset_finder_fallback_to_metadata_log, SUM)

// LogsConfigManager
// Number of updates sent to UpdateableLogsConfig by LogsConfigManager
STAT_DEFINE(logsconfig_manager_published_update, SUM)
// LogsConfig manager receiving updates from the state machine
STAT_DEFINE(logsconfig_manager_received_update, SUM)
// The version of the last processed delta/snapshot on this node
STAT_DEFINE(logsconfig_manager_tree_version, MAX)
// LogsConfig Manager Is Started
STAT_DEFINE(logsconfig_manager_started, MAX)
// LogsConfig Manager deserialization errors
STAT_DEFINE(logsconfig_manager_serialization_errors, SUM)
// LogsConfig number of deltas applied
STAT_DEFINE(logsconfig_manager_delta_applied, SUM)

// NodesConfigurationManager
// Number of new nodes configuration published
STAT_DEFINE(nodes_configuration_manager_config_published, SUM)
// Number of nodes configuration received
STAT_DEFINE(nodes_configuration_manager_config_received, SUM)
// The version of the staged nodes configuration
STAT_DEFINE(nodes_configuration_manager_staged_version, MAX)
// The version of the nodes configuration being processed
STAT_DEFINE(nodes_configuration_manager_pending_version, MAX)
// The version of the last published NodesConfig
STAT_DEFINE(nodes_configuration_manager_published_version, MAX)
// NodesConfigurationManager Is Started
STAT_DEFINE(nodes_configuration_manager_started, MAX)
// Number of time the NCM updates proposed
STAT_DEFINE(nodes_configuration_manager_updates_requested, SUM)
// Number of time the NCM overwrites requested
STAT_DEFINE(nodes_configuration_manager_overwrites_requested, SUM)
// NodesConfigurationManager deserialization errors
STAT_DEFINE(nodes_configuration_manager_serialization_errors, SUM)
// Read from NodesConfigurationStore failed
STAT_DEFINE(nodes_configuration_store_read_failed, SUM)
// Proposed advancement out of intermediary states failed
STAT_DEFINE(nodes_configuration_manager_advance_intermediary_state_failed, SUM)

// BufferedWriter stats
STAT_DEFINE(buffered_writer_bytes_in, SUM)
STAT_DEFINE(buffered_writer_bytes_batched, SUM)
STAT_DEFINE(buffered_writer_manual_flush, SUM)
STAT_DEFINE(buffered_writer_max_payload_flush, SUM)
STAT_DEFINE(buffered_writer_time_trigger_flush, SUM)
STAT_DEFINE(buffered_writer_size_trigger_flush, SUM)

// No. of times a recipient got a Message payload where the checksum
// in the ProtocolHeader does not match the checksum computed on the recipient
STAT_DEFINE(protocol_checksum_mismatch, SUM)

// No. of times a recipient got a Message payload with a checksum
// in the ProtocolHeader and it matched the checksum computed on the recipient
STAT_DEFINE(protocol_checksum_matched, SUM)

// GetSeqStateRequest stats
// Number of times a GetSeqStateRequest was created.
// This doesn't count the retries or the reuse of the same request id.
STAT_DEFINE(get_seq_state_unique, SUM)

// Total number of times GetSeqStateRequests were merged.
STAT_DEFINE(get_seq_state_merged_total, SUM)

// Number of times a new GetSeqStateRequest was
// merged into an existing one because the options
// matched b/w both requests.
STAT_DEFINE(get_seq_state_merge_into_existing, SUM)

// Number of times an existing request's options
// matched with the new request, but the new request
// wants to start a new state machine instead of using
// the existing state machine, e.g. rebuilding
STAT_DEFINE(get_seq_state_merge_into_new, SUM)

// Number of times a GetSeqStateRequest was resent
// because sequencer sent E::AGAIN.
STAT_DEFINE(get_seq_state_resent, SUM)

// Number of times a GetSeqStateRequest timedout
STAT_DEFINE(get_seq_state_timedout, SUM)

// Total number of attempts that were made to issue GetSeqStateRequest.
// This includes the requests which may not be sent out
// because there already was an outstanding request.
STAT_DEFINE(get_seq_state_attempts_context_unknown, SUM)
STAT_DEFINE(get_seq_state_attempts_context_catchup_queue, SUM)
STAT_DEFINE(get_seq_state_attempts_context_findkey_message, SUM)
STAT_DEFINE(get_seq_state_attempts_context_islogempty_message, SUM)
STAT_DEFINE(get_seq_state_attempts_context_rebuilding_thread, SUM)
STAT_DEFINE(get_seq_state_attempts_context_rocksdb_cf, SUM)
STAT_DEFINE(get_seq_state_attempts_context_start_message, SUM)
STAT_DEFINE(get_seq_state_attempts_context_store_message, SUM)
STAT_DEFINE(get_seq_state_attempts_context_gettrimpoint_message, SUM)
STAT_DEFINE(get_seq_state_attempts_context_sealstoragetask, SUM)
STAT_DEFINE(get_seq_state_attempts_context_get_tail_lsn, SUM)
STAT_DEFINE(get_seq_state_attempts_context_rebuilding_planner, SUM)
STAT_DEFINE(get_seq_state_attempts_context_sync_sequencer, SUM)
STAT_DEFINE(get_seq_state_attempts_context_get_tail_attributes, SUM)
STAT_DEFINE(get_seq_state_attempts_context_unreleased_record, SUM)
STAT_DEFINE(get_seq_state_attempts_context_metadata_util, SUM)
STAT_DEFINE(get_seq_state_attempts_context_historical_metadata, SUM)
STAT_DEFINE(get_seq_state_attempts_context_get_tail_record, SUM)
STAT_DEFINE(get_seq_state_attempts_context_reader_monitoring, SUM)
STAT_DEFINE(get_seq_state_attempts_context_is_log_empty_v2, SUM)

// No. of actual GetSeqStateRequests that were sent out
STAT_DEFINE(get_seq_state_unique_context_unknown, SUM)
STAT_DEFINE(get_seq_state_unique_context_catchup_queue, SUM)
STAT_DEFINE(get_seq_state_unique_context_findkey_message, SUM)
STAT_DEFINE(get_seq_state_unique_context_islogempty_message, SUM)
STAT_DEFINE(get_seq_state_unique_context_rebuilding_thread, SUM)
STAT_DEFINE(get_seq_state_unique_context_rocksdb_cf, SUM)
STAT_DEFINE(get_seq_state_unique_context_start_message, SUM)
STAT_DEFINE(get_seq_state_unique_context_store_message, SUM)
STAT_DEFINE(get_seq_state_unique_context_gettrimpoint_message, SUM)
STAT_DEFINE(get_seq_state_unique_context_sealstoragetask, SUM)
STAT_DEFINE(get_seq_state_unique_context_get_tail_lsn, SUM)
STAT_DEFINE(get_seq_state_unique_context_rebuilding_planner, SUM)
STAT_DEFINE(get_seq_state_unique_context_sync_sequencer, SUM)
STAT_DEFINE(get_seq_state_unique_context_get_tail_attributes, SUM)
STAT_DEFINE(get_seq_state_unique_context_unreleased_record, SUM)
STAT_DEFINE(get_seq_state_unique_context_metadata_util, SUM)
STAT_DEFINE(get_seq_state_unique_context_historical_metadata, SUM)
STAT_DEFINE(get_seq_state_unique_context_get_tail_record, SUM)
STAT_DEFINE(get_seq_state_unique_context_reader_monitoring, SUM)
STAT_DEFINE(get_seq_state_unique_context_is_log_empty_v2, SUM)

// Socket stats.
STAT_DEFINE(sock_read_events, SUM)
STAT_DEFINE(sock_time_spent_reading_message, SUM)
STAT_DEFINE(sock_num_messages_read, SUM)
STAT_DEFINE(sock_read_event_nobufs, SUM)

STAT_DEFINE(sock_write_events, SUM)
STAT_DEFINE(sock_time_spent_to_process_send_done, SUM)
STAT_DEFINE(sock_num_messages_sent, SUM)

STAT_DEFINE(sock_misc_socket_events, SUM)
STAT_DEFINE(sock_connect_event_proc_time, SUM)
STAT_DEFINE(sock_error_event_proc_time, SUM)
STAT_DEFINE(sock_peer_closed_event_proc_time, SUM)

// Timer Delays
STAT_DEFINE(wh_timer_sched_delay, SUM)

// Number of connection retries following a timeout.
STAT_DEFINE(connection_retries, SUM)
// Number of timeouts trying to connect a tcp socket.
STAT_DEFINE(connection_timeouts, SUM)
// Number of handshake timeouts.
STAT_DEFINE(handshake_timeouts, SUM)

// number of attempts to record error messages at severety CRITICAL
STAT_DEFINE(critical_errors, SUM)
// number of attempts to record error messages at severety ERROR
STAT_DEFINE(severe_errors, SUM)
// number of attempts to record error messages at severety INFO,NOTE,WARNING
STAT_DEFINE(production_notices, SUM)

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */
#ifndef RESETTING_STATS

/*
 * These stats will not be aggregated for destroyed threads.
 */
#ifndef DESTROYING_THREAD

// Counts config updates
STAT_DEFINE(updated_config, SUM)
// Counts config updates ignored because their version is older than the
// running config
STAT_DEFINE(config_update_old_version, SUM)
// Counts config updates that came in with same version but different hash
STAT_DEFINE(config_update_hash_mismatch, SUM)
// Counts config updates ignored because they were identical to running config
STAT_DEFINE(config_update_same_version, SUM)

#endif // DESTROYING_THREAD
#endif // RESETTING_STATS

#undef STAT_DEFINE
#undef RESETTING_STATS
#undef DESTROYING_THREAD
