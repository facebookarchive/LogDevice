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

// Total number of read requests
STAT_DEFINE(read_requests, SUM)
// Number of read requests that got kicked to storage threads
STAT_DEFINE(read_requests_to_storage, SUM)
// Number of epoch offset request that got kicked to storage threads
STAT_DEFINE(epoch_offset_to_storage, SUM)
// Number of records not written to RocksDB because their LSN <= trim point
STAT_DEFINE(skipped_record_lsn_before_trim_point, SUM)
// Number of write ops of all types submitted to the storage thread pool
STAT_DEFINE(write_ops, SUM)
// Number of multi-write requests passed to the local log store (RocksDB)
// write_ops/write_batches gives the batching factor for group commits
STAT_DEFINE(write_batches, SUM)
// Same for rebuilding writes.
STAT_DEFINE(write_ops_stallable, SUM)
STAT_DEFINE(write_batches_stallable, SUM)
// Number of write ops queued for WAL sync, but completed immediately
// because they were waiting on a previous sync batch that has completed.
STAT_DEFINE(write_ops_sync_already_done, SUM)
// How many times we attempted a non-blocking read
STAT_DEFINE(non_blocking_reads, SUM)
// How many times a non-blocking read did not return any records
STAT_DEFINE(non_blocking_reads_empty, SUM)
// Number of records removed from local log store by trimming.
// Doesn't include range skips.
STAT_DEFINE(records_trimmed_removed, SUM)
// Number of metadata log records removed from local log store by trimming
// Doesn't include range skips.
STAT_DEFINE(metadata_log_records_trimmed_removed, SUM)
// Number of per epoch log metadata records removed from local log store
// by trimming
STAT_DEFINE(per_epoch_log_metadata_trimmed_removed, SUM)

// Number of mutable per-epoch log metadata reads for the purpose of reading
// the LNG
STAT_DEFINE(last_known_good_from_metadata_reads, SUM)
// Number of mutable per-epoch log metadata reads that got kicked to storage,
// (i.e., blocking) for the purpose of reading the LNG
STAT_DEFINE(last_known_good_from_metadata_reads_to_storage, SUM)
// Number of data record reads for the purpose of reading the LNG, as fallback
// from failed mutable per-epoch log metadata reads
STAT_DEFINE(last_known_good_from_record_reads, SUM)
// Number of data record reads that got kicked to storage (i.e., blocking), for
// the purpose of reading the LNG, as fallback from failed mutable per-epoch
// log metadata reads
STAT_DEFINE(last_known_good_from_record_reads_to_storage, SUM)
// Number of mutable per-epoch log metadata writes (technically merges)
STAT_DEFINE(mutable_per_epoch_log_metadata_writes, SUM)

// Number of successful attempts to start reading past the global last-released
// LSN.
STAT_DEFINE(read_past_last_released_success, SUM)
// Number of failed attempts to start reading past the global last-released
// LSN. Failure is not an error, it just means a client attempted to start
// reading beyond the highest safe LSN.
STAT_DEFINE(read_past_last_released_failed, SUM)

// Number of appends received by the server.  This should be very close to
// `message_received.APPEND' however failed append probes (which come as
// APPEND_PROBE messages not APPEND) are also included.  The main purpose of
// this counter is to serve as a denominator for an "append success/failure
// ratio" graph that reflects what the client observers (timeouts
// notwithstanding).
//
// When sequencer batching is in use, these counters are still in terms of
// incoming APPEND messages, so before batching (and unbatching if incoming
// appends were also batched).  Again the aim is for the counters to line up
// with AppendRequest state machines on the client.
STAT_DEFINE(append_received, SUM)
// Number of appends that succeeded
STAT_DEFINE(append_success, SUM)
// Number of appends that were preempted
STAT_DEFINE(append_preempted, SUM)
// Number of appends that resulted in a redirect sent to the client
STAT_DEFINE(append_redirected, SUM)
// Number of appends that failed (excluding preempted and redirected)
STAT_DEFINE(append_failed, SUM)
// number of APPENDS that failed because runAppender() reported E::SYSLIMIT
// (included in append_failed)
STAT_DEFINE(append_syslimit, SUM)
// number of appends that end up rediretcing to a dead node (hence with
// REDIRECT_NOT_ALIVE flag)
STAT_DEFINE(append_redirected_not_alive, SUM)

// Payload bytes sent by clients in APPENDs
STAT_DEFINE(append_payload_bytes, SUM)
// Payload size in bytes incoming to sequencer batching (if active), after
// unbatching.  (These are the original client bytes if the client is using
// BufferedWriter.)
STAT_DEFINE(append_bytes_seq_batching_in, SUM)
// Payload size in bytes of the batched & possibly compressed record that's
// actually stored.  append_bytes_seq_batching_in /
// append_bytes_seq_batching_out gives the compression ratio, including any
// overheads involved for delimiters between the original records.  But not
// overhead for storing the record itself; it just compares Payloads vs
// Payloads.
STAT_DEFINE(append_bytes_seq_batching_out, SUM)
// How many of the bytes in the '_in' and '_out' stats were just passed
// through by sequencer batching (record already large enough, avoiding a
// compression cycle)
STAT_DEFINE(append_bytes_seq_batching_passthru, SUM)
// Payload bytes incoming to sequencer batching and sent to a BufferedWriter
// shard for uncompression and re-batching.
STAT_DEFINE(append_bytes_seq_batching_buffer_submitted, SUM)
// Incoming payload bytes freed in BufferedWriter after they have been
// re-batched.
STAT_DEFINE(append_bytes_seq_batching_buffer_freed, SUM)
// APPEND_PROBE messages that we replied to with E::OK
STAT_DEFINE(append_probes_passed, SUM)
// APPEND_PROBE messages that we responded to with an error, which instruct
// the client not to follow with a real APPEND message
STAT_DEFINE(append_probes_denied, SUM)
// Number of STORE messages demanding the write to be synced
STAT_DEFINE(store_synced, SUM)
// Number of STORE messages that were amends (had the AMEND flag)
STAT_DEFINE(store_received_amend, SUM)
// Number of StoreStorageTasks that timedout (i.e could not be
// executed before task_deadline_)
STAT_DEFINE(store_storage_task_timedout, SUM)

// Number of redirected appends that recevied LSNs from previous sequencer, and
// were subsequently replicated & released during recovery.
STAT_DEFINE(append_redirected_previously_stored, SUM)
// Number of redirected appends that recevied LSNs from previous sequencer, but
// recovery didn't find it and inserted a hole plug.
STAT_DEFINE(append_redirected_newly_stored, SUM)
// Number of redirected appends that recevied LSNs from previous sequencer, but
// recovery couldn't tell whether or not it was stored.
STAT_DEFINE(append_redirected_maybe_stored, SUM)

// Number of streams inserted to CatchupQueue to be processed immediately
STAT_DEFINE(catchup_queue_push_immediate, SUM)
// Number of streams inserted to CatchupQueue to be processed when it's
// ready to read newly released records
STAT_DEFINE(catchup_queue_push_delayed, SUM)
// Number of times a CatchupQueue activated its ping timer
STAT_DEFINE(catchup_queue_ping_timer_activations, SUM)

// Read stream ordering rules violations
STAT_DEFINE(read_stream_start_violations, SUM)
STAT_DEFINE(read_stream_started_violations, SUM)
STAT_DEFINE(read_stream_gap_violations, SUM)
STAT_DEFINE(read_stream_record_violations, SUM)

// How many times did we consult Read Throttling framework
STAT_DEFINE(read_throttling_num_throttle_checks, SUM)
// How many times a read attempt was throttled because of
// throttling limits set in the config
STAT_DEFINE(read_throttling_num_streams_throttled, SUM)
// For how many read streams ReadIoShapingCallback was called
STAT_DEFINE(read_throttling_num_streams_unthrottled, SUM)
// How many read streams closed before being unthrottled
STAT_DEFINE(read_throttling_num_streams_closed_in_throttle, SUM)
// How many bytes were read off ReadStorageTask that were administered by
// Read Throttling Framework
STAT_DEFINE(read_throttling_num_bytes_read, SUM)
STAT_DEFINE(read_throttling_excess_bytes_debited_from_meter, SUM)
STAT_DEFINE(read_throttling_excess_bytes_read_from_rocksdb, SUM)

// track outstanding drain() v/s onReadTaskDone
STAT_DEFINE(read_throttling_num_reads_throttled, SUM)
STAT_DEFINE(read_throttling_num_reads_allowed, SUM)
STAT_DEFINE(read_throttling_num_exact_debits, SUM)
STAT_DEFINE(read_throttling_overflow_bytes, SUM)

// How many bytes were read off ReadStorageTask irrespective of wheter
// throttling is enabled/disabled
STAT_DEFINE(num_bytes_read_via_read_task, SUM)
// How many read storage tasks are issued when Read Throttling is on
STAT_DEFINE(read_throttling_num_storage_tasks_issued, SUM)

// Age in milliseconds of the oldest running log recovery.
STAT_DEFINE(oldest_recovery_request, MAX)
// Number of scheduled log recovery requests. Includes queued requests and
// retries for failed recoveries.
STAT_DEFINE(recovery_scheduled, SUM)
// Number of completed log recoveries, regardless of the success of the
// operation.
STAT_DEFINE(recovery_completed, SUM)
// Number of log recoveries that are successfully completed
STAT_DEFINE(recovery_success, SUM)
// Log recovery requests that failed because a later sequencer sealed the log
// through a higher epoch number.
STAT_DEFINE(recovery_preempted, SUM)
// Number of failed log recovery requests.
STAT_DEFINE(recovery_failed, SUM)
// Number of holes in the log identified during the epoch recovery, excluding
// bridge records
STAT_DEFINE(num_hole_plugs, SUM)
// Number of bridge records mutated during epoch recovery
STAT_DEFINE(num_bridge_records, SUM)

// Number of holes or bridge records that recovery didn't plug because it had
// no authoritative digest.
STAT_DEFINE(num_holes_not_plugged, SUM)

// Number of times epoch recovery received a digest record with checksum error
STAT_DEFINE(epoch_recovery_digest_checksum_fail, SUM)

// number of times the tail record failed to appear in the recovery digest.
// Indicates dataloss, a log being trimmed before it was recovered, or a bug.
STAT_DEFINE(epoch_recovery_tail_record_not_in_digest, SUM)

// number of times tail record in digest is a hole plug due to bugs or data
// corruption
STAT_DEFINE(epoch_recovery_tail_record_hole_plug, SUM)

// number of times that epoch recovery updates its consensus LNG from STARTED
// replies
STAT_DEFINE(lng_update_digest_started, SUM)

// number of data log records mutated by epoch recovery as the best effort but
// not satisfying the replication requirement
STAT_DEFINE(epoch_recovery_record_underreplication_datalog, SUM)
// number of metadatadata log records mutated by epoch recovery as the best
// effort but not satisfying the replication requirement
STAT_DEFINE(epoch_recovery_record_underreplication_metadatalog, SUM)

// number of Mutators started by epoch recovery
STAT_DEFINE(mutation_started, SUM)
// Number of epochs that were recovered non-authoritatively.
STAT_DEFINE(non_auth_recovery_epochs, SUM)
// Number of times an EpochRecovery times out during mutation and cleaning.
STAT_DEFINE(recovery_mutation_and_cleaning_timeouts, SUM)
// Number of times an EpochRecovery successfully finishes
STAT_DEFINE(epoch_recovery_success, SUM)

// attempts to activate sequencers
STAT_DEFINE(sequencer_activations, SUM)

// sequencer gets activated with epoch metadata incompatible with the
// current configuration.
STAT_DEFINE(sequencer_activations_incompatible_metadata, SUM)

// sequencer is brought up on-demand since the node is the correct node
// to run sequencer
STAT_DEFINE(sequencer_activations_no_seq_correct_node, SUM)
// sequencer is brought up not because the node is the correct node, but
// because the no-redirect flag is set by the client
STAT_DEFINE(sequencer_activations_no_seq_flag_no_redirect, SUM)
// sequencer is reactivated because it is preempted by itself
STAT_DEFINE(sequencer_activations_preempted_self, SUM)
// sequencer is preempted by another node, but it needs to be reactivated
// because the client sets the REACTIVATE_IF_PREEMPTED flag
STAT_DEFINE(sequencer_activations_preempted_flag_reactivate, SUM)
// sequencer is preempted by another node, but it needs to be reactivated
// because the prempted node is considered dead
STAT_DEFINE(sequencer_activations_preempted_dead, SUM)
// sequencer is reactivated after a record is written in the metadata log
STAT_DEFINE(sequencer_activations_metadata_record_written, SUM)

// how many attempts to activate a sequencer have failed
STAT_DEFINE(sequencer_activation_failures, SUM)
// how many attempts to activate a sequencer failed because epoch store was
// empty, but timed out reading metadata log to see if it is empty too
STAT_DEFINE(sequencer_metadata_log_check_timeouts, SUM)
// how many attempts to activate a sequencer failed because epoch store was
// empty for the given log, but the metadata log was not
STAT_DEFINE(sequencer_activation_failed_metadata_inconsistency, SUM)

// sequencer gets activated with an epoch metadata that is inconsistent with the
// existing historical metadata maintained by the sequencer
STAT_DEFINE(sequencer_got_inconsistent_metadata, SUM)

// how many times that a sequencer become unavailable because the logid is removed
// from the config
STAT_DEFINE(sequencer_unavailable_log_removed_from_config, SUM)

// how many times a sequencer became unavailable because the node was isolated
STAT_DEFINE(sequencer_unavailable_node_isolated, SUM)

// how many times a sequencer became unavailable because admin command deactivated it
STAT_DEFINE(sequencer_unavailable_admin_deactivated, SUM)

// how many times a sequencer became unavailable because the node stopped being
// a sequencer node
STAT_DEFINE(sequencer_unavailable_not_sequencer_node, SUM)

// how many times sequencer activation gets a result of GRACEFUL_SUCCESS
STAT_DEFINE(graceful_reactivation_result_success, SUM)
// how many times sequencer activation gets a result of GRACEFUL_DEFERRED
STAT_DEFINE(graceful_reactivation_result_deferred, SUM)
// how many graceful reactivation completion procedure is started
STAT_DEFINE(graceful_reactivation_completion_started, SUM)

// How many times a job is enqueued to check whether a reactivation or
// epoch metadata update is needed. When the actual reactivation occurs
// then a different stat is bumped.
STAT_DEFINE(background_sequencer_reactivation_checks_scheduled, SUM)
// How many times such check was performed.
STAT_DEFINE(background_sequencer_reactivation_checks_completed, SUM)

// The check for sequencer reactivation resulted in no operations
// necessary
STAT_DEFINE(sequencer_reactivations_noop, SUM)

// Number of times that sequencer reactivations were delayed because there were
// non-substantial reconfigurations.
STAT_DEFINE(sequencer_reactivations_delayed, SUM)
// Number of times that delayed reactivations completed after the delay. This
// should match the previous stat.
STAT_DEFINE(sequencer_reactivations_delay_completed, SUM)

// Number of times a sequencer reactivation check recommended delaying
// but there was an already active timer that could be re-used.
STAT_DEFINE(sequencer_reactivations_delay_timer_reused, SUM)

// How many sequencers we reactivated in order to update epoch metadata
// (nodeset, replication factor etc).
STAT_DEFINE(sequencer_reactivations_for_metadata_update, SUM)
// How many times we updated epoch metadata without reactivating sequencer.
// This happens when nodes config changes in a way that might change the nodeset
// but actually doesn't.
STAT_DEFINE(metadata_updates_without_sequencer_reactivation, SUM)

// How many times SequencerBackgroundActivator changed nodeset size based on
// throughput. May be overestimated because the adjustment transaction can fail
// if current sequencer is active but stale.
STAT_DEFINE(nodeset_adjustments_done, SUM)
// How many times SequencerBackgroundActivator considered changing nodeset size
// based on throughput but decided against it.
STAT_DEFINE(nodeset_adjustments_skipped, SUM)
// How many times SequencerBackgroundActivator randomized nodeset seed.
// May be overestimated.
STAT_DEFINE(nodeset_randomizations_done, SUM)

//// Per-epoch sequencers

// number of times that sequencer did not finish draining an epoch within the
// configured time
STAT_DEFINE(sequencer_draining_timedout, SUM)

// how many times approximate findKey tried to be executed on worker thread
STAT_DEFINE(approximate_find_key_try_non_blocking, SUM)
// how many times approximate findKey tried to be executed on worker thread
// but end up doing seeks on disk
STAT_DEFINE(approximate_find_key_would_block, SUM)

// how many times strict findKey tried to be executed on worker thread
STAT_DEFINE(strict_find_key_try_non_blocking, SUM)
// how many times strict findKey tried to be executed on worker thread
// but end up doing seeks on disk
STAT_DEFINE(strict_find_key_would_block, SUM)

// How many times linear search in PartitionedRocksDBStore findTime iterated
STAT_DEFINE(strict_findtime_linear_search_iterations, SUM)
// How many times linear search in PartitionedRocksDBStore triggered
STAT_DEFINE(strict_findtime_linear_search_count, SUM)
// How many times linear search in PartitionedRocksDBStore findTime timed out
STAT_DEFINE(strict_findtime_linear_search_timedout, SUM)

// How many times findKey storage task timed out during execution
STAT_DEFINE(findkey_timedout_during_run, SUM)
// How many times findKey storage task timed out before execution
STAT_DEFINE(findkey_timedout_before_run, SUM)

// The total number of Appenders successfully inserted into appender buffers
STAT_DEFINE(appenderbuffer_appender_buffered, SUM)
// The total number of Appenders that get requeued when processing the buffer
STAT_DEFINE(appenderbuffer_appender_requeued, SUM)
// The number of APPEND requests that failed because an appender had to be
// inserted into a buffer queue, but the queue was full
STAT_DEFINE(appenderbuffer_appender_failed_queue_full, SUM)
// The number of pending Appenders that have been successfully buffered
// but failed again when it was processed for the second time
STAT_DEFINE(appenderbuffer_appender_failed_retry, SUM)
// The number of pending Appenders that have been successfully buffered
// but had to be failed since sequencer activation failed.
STAT_DEFINE(appenderbuffer_appender_failed_sequencer_activation, SUM)
// The number of APPEND requests that failed because an appender had to be
// inserted into a buffer queue, but the total size of appenders was above limit
STAT_DEFINE(appenderbuffer_appender_failed_size_limit, SUM)
// the total number of Appenders removed from appender buffers and successfully
// released to Sequenceres
STAT_DEFINE(appenderbuffer_appender_released, SUM)
// the subset of appenderbuffer_appender_released that were previoulsly stored,
// and therefore replied OK and delete the appender.
STAT_DEFINE(appenderbuffer_appender_deleted, SUM)

// (zookeeper epoch store only) number of times zookeeper epoch store encounters
// an internal consistency error
STAT_DEFINE(zookeeper_epoch_store_internal_inconsistency_error, SUM)

// PurgeUncleanEpochs instances created and started
STAT_DEFINE(purging_started, SUM)
// PurgeUncleanEpochs instances that started reading
STAT_DEFINE(purging_reading_started, SUM)
// PurgeUncleanEpochs instances that finished reading
STAT_DEFINE(purging_reading_done, SUM)
// PurgeUncleanEpochs instances that started deleting
STAT_DEFINE(purging_delete_started, SUM)
// PurgeUncleanEpochs instances that finished deleting
STAT_DEFINE(purging_delete_done, SUM)
// PurgeUncleanEpochs instances that completed
STAT_DEFINE(purging_done, SUM)

// PurgeUncleanEpochs instances that finished successfully
STAT_DEFINE(purging_success, SUM)
// PurgeUncleanEpochs instances that finished with failure
STAT_DEFINE(purging_failed, SUM)

// number of purge jobs created by release enqueued because the limit
// of concurrent purges are reached
STAT_DEFINE(purging_for_release_enqueued, SUM)
// number of purge jobs created by release dequeued
STAT_DEFINE(purging_for_release_dequeued, SUM)

// number of PurgeSingleEpoch state machine started
STAT_DEFINE(purging_v2_purge_epoch_started, SUM)
// number of PurgeSingleEpoch state machine completed
STAT_DEFINE(purging_v2_purge_epoch_completed, SUM)

// number of times PurgeSingleEpoch state machine is completed with purging
// skipped because all fully authoritative nodes have responded
STAT_DEFINE(purging_v2_purge_epoch_skipped_all_responded, SUM)
// number of times PurgeSingleEpoch state machine is completed with the epoch
// considered empty and purged because all fully authoritative nodes have
// responded including at least one of them with E::EMPTY
STAT_DEFINE(purging_v2_purge_epoch_empty_all_responsed, SUM)

// number of times PurgeSingleEpoch state machine not deleting records
// for the epoch because the epoch is found empty but has a non-zero local
// LNG
STAT_DEFINE(purge_epoch_skipped_empty_positive_local_lng, SUM)

// number of times purge delete records by their key range
STAT_DEFINE(purging_v2_delete_by_keys, SUM)
// number of times purge delete records by first reading the records
STAT_DEFINE(purging_v2_delete_by_reading_data, SUM)

// Records deleted while purging
STAT_DEFINE(purging_deleted_records, SUM)
STAT_DEFINE(purging_task_dropped, SUM)
// Number of storage tasks queued
STAT_DEFINE(storage_tasks_queued, SUM)
// Number of storage tasks dropped
STAT_DEFINE(storage_tasks_dropped_fast_time_sensitive, SUM)
STAT_DEFINE(storage_tasks_dropped_fast_stallable, SUM)
STAT_DEFINE(storage_tasks_dropped_slow, SUM)
STAT_DEFINE(storage_tasks_dropped_default, SUM)
// Number of storage tasks picked up by storage threads
STAT_DEFINE(storage_tasks_dequeued_fast_time_sensitive, SUM)
STAT_DEFINE(storage_tasks_dequeued_fast_stallable, SUM)
STAT_DEFINE(storage_tasks_dequeued_slow, SUM)
STAT_DEFINE(storage_tasks_dequeued_default, SUM)

// Number of failures forwarding a message in the delivery chain
STAT_DEFINE(store_forwarding_failed, SUM)

// Number of times some read iterator was invalidated due to inactivity
STAT_DEFINE(iterator_invalidations, SUM)

// number of waves of STORE messages appenders tried to send through chain
STAT_DEFINE(appender_wave_chain, SUM)
// number of waves appenders tried to send directly to all nodes
STAT_DEFINE(appender_wave_direct, SUM)
// Appender waves that hit a STORE timeout (and probably sent another wave)
STAT_DEFINE(appender_wave_timedout, SUM)
// Appender store timer was reset (because the sync replication scope
// came out of isolation)
STAT_DEFINE(appender_store_timer_reset, SUM)
// number of times that an Appender cannot find enough nodes to store copies
// for a record
STAT_DEFINE(appender_unable_pick_copyset, SUM)
// number of times that an Appender for draining fails to store on a store node
// because the store is preempted by soft seals only
STAT_DEFINE(appender_draining_soft_preempted, SUM)
// number of times that an Appender is started at the beginning of its epoch
// with EPOCH_BEGIN flag in its STORE header
STAT_DEFINE(appender_epoch_begin, SUM)

// two important stats above, but exclusively for metadata log appends
STAT_DEFINE(metadata_log_appender_wave_timedout, SUM)
STAT_DEFINE(metadata_log_appender_unable_pick_copyset, SUM)

// number of appenders are forcefully aborted because EpochSequencer for its
// epoch is dying
STAT_DEFINE(appender_aborted_epoch, SUM)

// Stats from WeightedCopySetSelector.
STAT_DEFINE(copyset_selected, SUM)
STAT_DEFINE(copyset_biased, SUM)
STAT_DEFINE(copyset_selection_failed, SUM)
STAT_DEFINE(copyset_selection_attempts, SUM)
STAT_DEFINE(copyset_selected_rebuilding, SUM)
STAT_DEFINE(copyset_biased_rebuilding, SUM)
STAT_DEFINE(copyset_selection_failed_rebuilding, SUM)
STAT_DEFINE(copyset_selection_attempts_rebuilding, SUM)

// Raw bytes and compressed bytes for a given block if the block was selected
// for compressio sampling. The fast stat is based on using a fast compression
// algorithm like lz4 and the slow stat is based on something like zstd.
STAT_DEFINE(sampled_blocks_raw_bytes_fast, SUM)
STAT_DEFINE(sampled_blocks_raw_bytes_slow, SUM)
STAT_DEFINE(sampled_blocks_compressed_bytes_fast, SUM)
STAT_DEFINE(sampled_blocks_compressed_bytes_slow, SUM)

// A set of stats on distribution of rocksdb block sizes.
STAT_DEFINE(num_sst_blocks_GT_1024KB, SUM)
STAT_DEFINE(num_sst_blocks_GT_512KB, SUM)
STAT_DEFINE(num_sst_blocks_GT_256KB, SUM)
STAT_DEFINE(num_sst_blocks_GT_128KB, SUM)
STAT_DEFINE(num_sst_blocks_GT_64KB, SUM)
STAT_DEFINE(num_sst_blocks_GT_32KB, SUM)
STAT_DEFINE(num_sst_blocks_LT_32KB, SUM)

// number of times that a storage node replied in STORED header
// with a status code other than E::OK
STAT_DEFINE(node_stored_unsuccessful_total, SUM)
// number of times that a storage node replied with E::PREEMPTED
// in STORED header
STAT_DEFINE(node_stored_preempted_sent, SUM)
// number of times that a storage node replied with E::DISABLED
// in STORED header
STAT_DEFINE(node_stored_disabled_sent, SUM)
// number of times that a storage node replied with E::DROPPED
// in STORED header
STAT_DEFINE(node_stored_dropped_sent, SUM)
// number of times that a storage node replied with E::NOSPC
// in STORED header
STAT_DEFINE(node_stored_out_of_space_sent, SUM)
// number of times that a storage node replied with E::LOW_ON_SPC
// in STORED header
STAT_DEFINE(node_stored_low_on_space_sent, SUM)
// number of times a sequencer received the notification from a
// storage node, that it has crossed lower watermark of free disk space.
STAT_DEFINE(node_low_on_space_received, SUM)
// number of times that a storage node replied with E::NOTSTORAGE
// in STORED header
STAT_DEFINE(node_stored_not_storage_sent, SUM)
// number of times that a storage node replied with E::REBUILDING
// in STORED header because the copyset contains a node in rebuilding
STAT_DEFINE(node_stored_rebuilding_sent, SUM)
// number of times that the sequencer received the report that a storage node
// is out of free disk space
STAT_DEFINE(node_out_of_space_received, SUM)
// number of times that a storage node replied with overloaded flag set
// in STORED header
STAT_DEFINE(node_overloaded_sent, SUM)
// number of times that the sequencer received the report that
// a storage node is overloaded
STAT_DEFINE(node_overloaded_received, SUM)
// number of times that the sequencer got E::UNROUTABLE when trying to
// connect to a storage node
STAT_DEFINE(node_unroutable_received, SUM)
// number of times that the sequencer got E::DROPPED in STORED header
STAT_DEFINE(node_dropped_received, SUM)
// number of times that the sequencer got E::DISABLED in STORED header
STAT_DEFINE(node_disabled_received, SUM)
// number of times that the sequencer got E::REBUILDING in STORED header
STAT_DEFINE(node_rebuilding_received, SUM)
// number of times that payloads were corrupted by either a sequencer or
// storage node. Bumped by storage nodes; see log to find which sequencer the
// store was from, then see its log to figure out which node caused it.
// Expected cause is bad hardware
STAT_DEFINE(payload_corruption, SUM)
// number of times a storage node reported corruption to a sequencer node, and
// the sequencer node ignored it. This is either because it is not verifying
// checksums that the client provides - meaning it is most likely the client's
// fault - or the sequencer was shown to corrupt records, but there are too
// many dead nodes to crash it.
STAT_DEFINE(payload_corruption_ignored, SUM)

// number of times a storage node does not store per-epoch recovery
// metadata since the epoch is empty according to the recovering sequencer
STAT_DEFINE(epoch_recovery_metadata_not_stored_epoch_empty, SUM)
// number of times a storage node stores or updates per-epoch recovery
// metadata
STAT_DEFINE(epoch_recovery_metadata_stored, SUM)

// number of times a storage node failed to send SEALED reply to a sequencer
// node running log recovery
STAT_DEFINE(sealed_reply_failed_to_send, SUM)

// number of zero copied records drained on disposal on their original worker
STAT_DEFINE(zero_copied_records_drained, SUM)

//// stats for record cache
STAT_DEFINE(record_cache_store_cached, SUM)
STAT_DEFINE(record_cache_store_not_cached, SUM)
STAT_DEFINE(record_cache_records_evicted, SUM)
STAT_DEFINE(record_cache_bytes_cached_estimate, SUM)
STAT_DEFINE(record_cache_epoch_created, SUM)
STAT_DEFINE(record_cache_epoch_evicted, SUM)
STAT_DEFINE(record_cache_epoch_evicted_by_reset, SUM)

// number of times that record cache received a record that cannot be stored
// with the current capacity
STAT_DEFINE(record_cache_record_out_of_capacity, SUM)

// number of times that record cache monitor thead performs size-based
// eviction because the number of bytes cached exceed the limit
STAT_DEFINE(record_cache_eviction_performed_by_monitor, SUM)
// estimate number of payload bytes evicted by the eviction monitor thread
STAT_DEFINE(record_cache_bytes_evicted_by_monitor, SUM)


// for calculating cache hit rate
STAT_DEFINE(epoch_recovery_digest_received, SUM)
STAT_DEFINE(record_cache_digest_hit, SUM)
STAT_DEFINE(record_cache_digest_miss, SUM)
STAT_DEFINE(log_recovery_seal_received, SUM)
STAT_DEFINE(record_cache_seal_hit, SUM)
STAT_DEFINE(record_cache_seal_miss, SUM)

STAT_DEFINE(record_cache_digest_hit_datalog, SUM)
STAT_DEFINE(record_cache_digest_miss_datalog, SUM)
STAT_DEFINE(record_cache_seal_hit_datalog, SUM)
STAT_DEFINE(record_cache_seal_miss_datalog, SUM)

// on the read side
STAT_DEFINE(record_cache_digest_created, SUM)
STAT_DEFINE(record_cache_digest_completed, SUM)
STAT_DEFINE(record_cache_digest_active, SUM)

STAT_DEFINE(record_cache_digest_record_sent, SUM)
STAT_DEFINE(record_cache_digest_payload_bytes_sent, SUM)

// Number of records written to storage with the sticky copyset bit set
STAT_DEFINE(csi_entry_writes, SUM)

// Number of records written to storage for the index
STAT_DEFINE(index_entry_writes, SUM)

// number of rejected APPENDS because the logid is not in the config of the
// sequencer node
STAT_DEFINE(append_rejected_not_in_server_config, SUM)
// number of rejected APPENDS because there are too many out-of-space nodes
STAT_DEFINE(append_rejected_nospace, SUM)
// number of rejected APPENDS because there are too many overloaded nodes
STAT_DEFINE(append_rejected_overloaded, SUM)
// number of rejected APPENDS because there are too many unroutable nodes
STAT_DEFINE(append_rejected_unroutable, SUM)
// number of rejected APPENDS because too many nodes are rebuilding
STAT_DEFINE(append_rejected_rebuilding, SUM)
// number of rejected APPENDS because too many nodes have persistent error
STAT_DEFINE(append_rejected_disabled, SUM)
// number of rejected APPENDS because sequencer was not found
STAT_DEFINE(append_rejected_nosequencer, SUM)
// number of rejected APPENDS because the sliding window of Appenders was full
STAT_DEFINE(append_rejected_window_full, SUM)
// number of rejected APPENDS because of Settings::max_total_appenders_size_hard
STAT_DEFINE(append_rejected_size_limit, SUM)
// number of rejected APPENDS because of too many pending appenders
STAT_DEFINE(append_rejected_pending_full, SUM)
// number of rejected APPENDS because the server was shutting down
STAT_DEFINE(append_rejected_shutdown, SUM)
// number of APPENDS that were rejected because they require sequencer to be
// reactivated, but the limit has been exceeded
STAT_DEFINE(append_rejected_reactivation_limit, SUM)
// number of appends that were rejected because the node itself is marked
// as unavailable by the failure detector
STAT_DEFINE(append_rejected_not_ready, SUM)
// number of appends that were rejected because the LogsConfig is not fully
// loaded yet. Client should retry.
STAT_DEFINE(append_rejected_logsconfig_not_ready, SUM)
// number of rejected APPENDS becasue of invalid permissions
STAT_DEFINE(append_rejected_permission_denied, SUM)
// number of appends that were rejected due to the seuqencer being isolated
// meaning that more than half of the nodes appear to be dead from a failure
// detector point of view.
STAT_DEFINE(append_rejected_isolated, SUM)
// number of APPENDS rejected because they were cancelled at some point.  The
// append may or may not have succeeded.
STAT_DEFINE(append_rejected_cancelled, SUM)

// number of rocksdb manual compaction performed
STAT_DEFINE(manual_compactions, SUM)

STAT_DEFINE(server_read_streams_created, SUM)

// Total number of records read by LocalLogStoreReader for all read streams.
STAT_DEFINE(read_streams_num_records_read, SUM)
STAT_DEFINE(read_streams_num_bytes_read, SUM)
STAT_DEFINE(read_streams_num_record_bytes_read, SUM)
STAT_DEFINE(read_streams_num_csi_bytes_read, SUM)
// Total size of rocksdb blocks read from disk by LocalLogStoreReader.
STAT_DEFINE(read_streams_block_bytes_read, SUM)

// Total number of records filtered by LocalLogStoreReader for all read streams.
STAT_DEFINE(read_streams_num_records_filtered, SUM)
// Total number of bytes filtered by LocalLogStoreReader for all read streams.
STAT_DEFINE(read_streams_num_bytes_filtered, SUM)
// Number of records filtered by LocalLogStoreReader for read streams used for
// rebuilding.
STAT_DEFINE(read_streams_num_records_filtered_rebuilding, SUM)
// Number of bytes filtered by LocalLogStoreReader for read streams used for
// rebuilding.
STAT_DEFINE(read_streams_num_bytes_filtered_rebuilding, SUM)
// Number of records filtered by LogRebuilding which required information
// only available after the full record was read.
STAT_DEFINE(read_streams_num_records_late_filtered_rebuilding, SUM)

// The number of copyset index entries that LocalLogStoreReader read.
STAT_DEFINE(read_streams_num_csi_entries_read, SUM)
// The number of copyset index entries that were filtered out by ReadFilter
// in LocalLogStoreReader
STAT_DEFINE(read_streams_num_csi_entries_filtered, SUM)
// The number of copyset index entries that passed the ReadFilter
// in LocalLogStoreReader
STAT_DEFINE(read_streams_num_csi_entries_sent, SUM)
// The number of rocksdb::Iterators created on the copyset index
STAT_DEFINE(read_streams_num_csi_iterators_created, SUM)
// The number of rocksdb::Iterators on the copyset index that were destroyed
// when CopySetIndexIterator got destroyed
STAT_DEFINE(read_streams_num_csi_iterators_destroyed, SUM)

// When considering real time reads, the number of cached records we dropped
// because they were from a different epoch than our current read pointer.
STAT_DEFINE(real_time_records_from_wrong_epoch, SUM)

// The approximate number of records evicted because our cache was too big,
// summed over all streams.
STAT_DEFINE(real_time_record_buffer_eviction, SUM)

// Number of sent records that came real time, i.e. on release were sent from
// the writer to the reader, and never read from RocksDB.
STAT_DEFINE(read_streams_records_real_time, SUM)
// Number of sent records that were read from RocksDB using non blocking reads.
STAT_DEFINE(read_streams_records_non_blocking, SUM)
// Number of sent records that came from blocking reads of RocksDB.
STAT_DEFINE(read_streams_records_blocking, SUM)
// Same but in bytes.
STAT_DEFINE(read_streams_bytes_real_time, SUM)
STAT_DEFINE(read_streams_bytes_non_blocking, SUM)
STAT_DEFINE(read_streams_bytes_blocking, SUM)

// Number of times the previous record sent did NOT come from the real time
// buffer, and the current record is from it.
STAT_DEFINE(real_time_switched_to_real_time, SUM)
// Number of times the previous record sent was read from RocksDB NOT using non
// blocking I/O, and the current record DID read from RocksDB using non blocking
// I/O.
STAT_DEFINE(real_time_switched_to_non_blocking, SUM)
// Number of times the previous record sent was read from RocksDB NOT using
// blocking I/O, and the current record DID use blocking I/O.
STAT_DEFINE(real_time_switched_to_blocking, SUM)

// Number of times, when we went to get records to send, that there were no real
// time records.
STAT_DEFINE(real_time_no_released_metadata, SUM)
STAT_DEFINE(real_time_no_released_regular, SUM)

// Number of times we got released records out-of-order.
STAT_DEFINE(real_time_out_of_order_metadata, SUM)
STAT_DEFINE(real_time_out_of_order_regular, SUM)

// Number of times the recently released records were newer than we needed.
STAT_DEFINE(real_time_too_new_metadata, SUM)
STAT_DEFINE(real_time_too_new_regular, SUM)

//////////////////////////RocksDB LocalLogStore stats///////////////////////////

#define ITERATOR_OP_STATS(op) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _reads, SUM) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _reads_from_disk, SUM) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _block_bytes_read_from_disk, SUM) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _blocks_read_from_disk, SUM) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _reads_from_block_cache, SUM) \
STAT_DEFINE(read_streams_rocksdb_locallogstore_ ## op ## _blocks_read_from_block_cache, SUM) \

// Stats for Seek() calls on the copyset index rocksdb::Iterator
ITERATOR_OP_STATS(csi_seek)
// Stats for Next() calls on the copyset index rocksdb::Iterator
ITERATOR_OP_STATS(csi_next)
// Stats for Prev() calls on the copyset index rocksdb::Iterator
ITERATOR_OP_STATS(csi_prev)

// Stats for Seek() calls on the record store rocksdb::Iterator
ITERATOR_OP_STATS(record_seek)
// Stats for Next() calls on the record store rocksdb::Iterator
ITERATOR_OP_STATS(record_next)
// Stats for Prev() calls on the record store index rocksdb::Iterator
ITERATOR_OP_STATS(record_prev)
////////////////////////////////////////////////////////////////////////////////

// TODO (#10357210): remove
STAT_DEFINE(data_key_format_migration_steps, SUM)
STAT_DEFINE(data_key_format_migration_merges, SUM)

// Number of calls to LocalLogStoreReader::read()
STAT_DEFINE(read_streams_num_ops, SUM)
STAT_DEFINE(read_streams_num_ops_rebuilding, SUM)
// Number of reads that required doing a seek() on an iterator
STAT_DEFINE(read_streams_num_ops_requiring_seeks, SUM)
// Number of times we encounter a transient errors while serving reads
STAT_DEFINE(read_streams_transient_errors, SUM)

// Number of bytes we enqueued to a reader while a storage task (for a different
// read stream) is outstanding.
STAT_DEFINE(bytes_queued_during_storage_task, SUM)

// Total number of successfully started WriteMetaDataRecord state machines
STAT_DEFINE(write_metadata_record_started, SUM)
// Total number of successfully finished WriteMetaDataRecord state machines
STAT_DEFINE(write_metadata_record_finished, SUM)

// Number of times a partition was created, dropped and compacted
STAT_DEFINE(partitions_created, SUM)
STAT_DEFINE(partitions_prepended, SUM)
STAT_DEFINE(partitions_dropped, SUM)
STAT_DEFINE(partitions_compacted, SUM)
// Total number of milliseconds spent compacting partitions.
STAT_DEFINE(partitions_compaction_time, SUM)
// Total number of milliseconds spent cleaning up directory after compaction.
STAT_DEFINE(partitions_compaction_cleanup_time, SUM)
// Total number of milliseconds spent partially compacting partitions.
STAT_DEFINE(partitions_partial_compaction_time, SUM)
// Total number of files partially compacted
STAT_DEFINE(partitions_partial_compaction_files, SUM)
STAT_DEFINE(partition_proactive_compactions, SUM)
STAT_DEFINE(partition_manual_compactions, SUM)
STAT_DEFINE(partition_partial_compactions, SUM)
// Partition Dirty State Tracking
STAT_DEFINE(partition_cleaner_scans, SUM)
STAT_DEFINE(partition_marked_clean, SUM)
STAT_DEFINE(partition_marked_dirty, SUM)
STAT_DEFINE(partition_dirty_data_updated, SUM)
STAT_DEFINE(partition_sync_write_promotion_for_timstamp, SUM)
STAT_DEFINE(triggered_manual_memtable_flush, SUM)

// A MEMORY or ASYNC_WRITE was promoted to a SYNC write due
// to lacking a record timestamp.
STAT_DEFINE(sync_write_promotion_no_timestamp, SUM)
// A MEMORY or ASYNC_WRITE was promoted to a SYNC write due
// to lacking coordinator (the node that issued the write) information.
STAT_DEFINE(sync_write_promotion_no_coordinator, SUM)

// Number of records that were stored in some partition other than the two most
// recent ones. Expect high values during rebuilding.
STAT_DEFINE(logsdb_writes_to_old_partitions, SUM)
STAT_DEFINE(logsdb_iterator_dir_seek_needed, SUM)
// Various relatively rare conditions in LogsDB.
STAT_DEFINE(logsdb_partition_pick_retries, SUM)
STAT_DEFINE(logsdb_directory_premature_flushes, SUM)
STAT_DEFINE(logsdb_writes_dir_key_decrease, SUM)
STAT_DEFINE(logsdb_writes_dir_key_add, SUM)
STAT_DEFINE(logsdb_skipped_writes, SUM)
STAT_DEFINE(logsdb_target_partition_clamped, SUM)
STAT_DEFINE(logsdb_iterator_dir_reseek_needed, SUM)
STAT_DEFINE(logsdb_iterator_partition_dropped, SUM)

// Number of append messages processed due to the NO_REDIRECT flag
STAT_DEFINE(append_no_redirect, SUM)
// Number of append messages processed due to the REACTIVATE_IF_PREEMPTED flag
STAT_DEFINE(append_reactivate_if_preempted, SUM)
// Number of append messages processed due to the preemptor node being dead
STAT_DEFINE(append_preempted_dead, SUM)
// Number of redirects sent to nodes that are not in the current config
STAT_DEFINE(append_redir_not_in_config, SUM)

// How many times a GET_SEQ_STATE message is received for a log the node is not
// running a sequencer for.
STAT_DEFINE(get_seq_state_nosequencer, SUM)
// Number of times redirect was sent as a reply to GET_SEQ_STATE
STAT_DEFINE(get_seq_state_redirect, SUM)
// Number of GET_SEQ_STATE messages received with NO_REDIRECT
STAT_DEFINE(get_seq_state_no_redirect, SUM)
// Number of GET_SEQ_STATE messages received with REACTIVATE_IF_PREEMPTED
STAT_DEFINE(get_seq_state_reactivate_if_preempted, SUM)
// Number of times GET_SEQ_STATE message reactivated a sequencer because
// its preemptor is dead
STAT_DEFINE(get_seq_state_reactivate_preemptor_dead, SUM)
// Number of times GET_SEQ_STATE message reactivated a sequencer because
// its preemptor is boycotted
STAT_DEFINE(get_seq_state_reactivate_preemptor_boycotted, SUM)

// Number of times a GET_SEQ_STATE would have returned result from
// a stale sequencer(older epoch), had it not sent out CheckSealRequest
// to determine if another sequencer is activated with a higher epoch
// somewhere else.
STAT_DEFINE(get_seq_state_stale_sequencer, SUM)

// Number of times a sequencer was found INACTIVE,
// but preempted_epoch_ was not set.
//
// Due to sending a CHECK_SEAL message after receiving a
// GET_SEQ_STATE, the preempted_epoch_ is now set.
STAT_DEFINE(get_seq_state_set_preempted_epoch, SUM)

// Number of times a CheckSealRequest timedout
STAT_DEFINE(check_seal_req_timedout, SUM)

// Number of times a CHECK_SEAL_Message had to recover
// LogStorageState from RocksdDB
STAT_DEFINE(check_seal_req_recover_seal, SUM)

// How many times a GET_SEQ_STATE has to be retried
// since copyset manager is not available.
STAT_DEFINE(check_seal_req_copysetmanager_invalid, SUM)

// Number of reactivations, meant to prevent out-of-order appends, caused by
// clients notifying us that a sequencer with a higher epoch exists (or
// existed), i.e. with seen_epoch higher than sequencer's current epoch
STAT_DEFINE(stale_reactivation, SUM)
// Number of reactivations due to exhastion of the ESN space within the current
// epoch.
STAT_DEFINE(epoch_end_reactivation, SUM)

// Number of read batches of records read for all read streams.
STAT_DEFINE(read_streams_batch_complete, SUM)
// Total number of microseconds spent processing batches.
// Dividing this by read_streams_batch_complete gives the average batch
// processing time.
STAT_DEFINE(read_streams_batch_processing_microsec, SUM)
// Total number of microseconds spent waiting on the catchup queue.
// Dividing this by read_streams_batch_complete gives the average time waiting
// in catchup queue before processing a batch.
STAT_DEFINE(read_streams_batch_queue_microsec, SUM)

STAT_DEFINE(wal_syncs, SUM)
STAT_DEFINE(wal_sync_microsec, SUM)
STAT_DEFINE(fsyncs, SUM)
STAT_DEFINE(fsync_microsec, SUM)
STAT_DEFINE(fdatasyncs, SUM)
STAT_DEFINE(fdatasync_microsec, SUM)

STAT_DEFINE(rebuilding_store_sent, SUM)
STAT_DEFINE(rebuilding_amend_sent, SUM)
STAT_DEFINE(rebuilding_donor_stored_ok, SUM)
STAT_DEFINE(rebuilding_donor_stored_ms, SUM)
STAT_DEFINE(rebuilding_donor_store_persisted_ms, SUM)
STAT_DEFINE(rebuilding_donor_amended_ok, SUM)
STAT_DEFINE(rebuilding_donor_amended_ms, SUM)
STAT_DEFINE(rebuilding_donor_amend_persisted_ms, SUM)
STAT_DEFINE(rebuilding_recipient_stored_ok, SUM)
STAT_DEFINE(rebuilding_recipient_amended_ok, SUM)
STAT_DEFINE(rebuilding_malformed_records, SUM)
STAT_DEFINE(rebuilding_bad_copysets, SUM)
STAT_DEFINE(record_rebuilding_retries, SUM)
STAT_DEFINE(record_rebuilding_store_timeouts, SUM)
STAT_DEFINE(record_rebuilding_amend_in_progress, SUM)
STAT_DEFINE(record_rebuilding_amend_timeouts, SUM)
STAT_DEFINE(record_rebuilding_amend_retries, SUM)
STAT_DEFINE(record_rebuilding_amend_failed, SUM)
STAT_DEFINE(log_rebuilding_record_durability_timeout, SUM)
STAT_DEFINE(log_rebuilding_restarted_by_rebuilding_coordinator, SUM)
STAT_DEFINE(record_rebuilding_timeouts, SUM)

// How many times we've seen an amend pseudorecord without an corresponding
// full record.
STAT_DEFINE(dangling_amends_seen, SUM)
// How many times we've seen a record with its corresponding CSI entry such
// that the copyset or flags in the CSI entry are different from those in the
// record.
STAT_DEFINE(csi_contents_mismatch, SUM)
// Number of times we saw a large range of records/csi-entries without
// corresponding csi-entries/records.
STAT_DEFINE(csi_unexpectedly_long_skips, SUM)
// Number of CSI entries that CSIWrapper has skipped because no matching record
// was found
STAT_DEFINE(read_streams_num_csi_skips_no_record, SUM)

STAT_DEFINE(gossips_received_on_worker_thread, SUM)
STAT_DEFINE(gossips_received_on_gossip_thread, SUM)

// How many gossip messages were delayed on recipient
// by atleast 1 second.
STAT_DEFINE(gossips_delayed_total, SUM)

// How many gossip messages were dropped since they were delayed
// by >= 'gossip_time_skew_threshold' on receiving side.
STAT_DEFINE(gossips_dropped_total, SUM)

// How many gossip messages were rejected because the incoming message
// contains a lower instance id for the sender node than the recipient
// already knows
STAT_DEFINE(gossips_rejected_instance_id, SUM)

// How many gossip messages were delayed because of sitting in Pipe
STAT_DEFINE(gossips_delayed_pipe, SUM)

// How many times the failure detector failed to send gossip messages
STAT_DEFINE(gossips_failed_to_send, SUM)

// How many times the failure detector failed to send gossip messages to an alive node
STAT_DEFINE(gossips_failed_to_send_to_alive_nodes, SUM)


// Total number of nodes expected to be seen (including self)
STAT_DEFINE(num_nodes, SUM)
// Effective number of nodes in the cluster, excluding disabled nodes
STAT_DEFINE(effective_num_nodes, SUM)

// How many nodes are marked DEAD by the FailureDetector.
// This measures whether the FailureDetector state is in
// sync across all nodes.
STAT_DEFINE(num_dead_nodes, SUM)

// Effective number of dead nodes, excluding disabled nodes
STAT_DEFINE(effective_dead_nodes, SUM)

// How many times the watchdog took more than
// expected time to enter stall detection loop.
STAT_DEFINE(watchdog_num_delays, SUM)

// Num workers detected as stalled by watchdog
STAT_DEFINE(num_stalled_workers, SUM)

// EventLoop delays.
STAT_DEFINE(event_loop_sched_delay, SUM)

// Number of records from the event log that EventLogReader has seen so far.
STAT_DEFINE(num_event_log_records_read, SUM)
STAT_DEFINE(malformed_event_log_records_read, SUM)

// Total size of records and copyset index entries written to rocksdb.
STAT_DEFINE(csi_bytes_written, SUM)
STAT_DEFINE(record_bytes_written, SUM)
STAT_DEFINE(index_bytes_written, SUM)

// Number and total size of all rocksdb blocks written to sst files.
// Only when RocksDBFlushBlockPolicy is used. In particular, metadata column
// family is excluded because it doesn't use RocksDBFlushBlockPolicy.
STAT_DEFINE(sst_blocks_written, SUM)
STAT_DEFINE(sst_blocks_bytes, SUM)
// Number and size of blocks consisting of data records.
STAT_DEFINE(sst_record_blocks_written, SUM)
STAT_DEFINE(sst_record_blocks_bytes, SUM)

// Approximate breakdown of the (uncompressed) size of sst files written by
// rocksdb (both flushes and compactions). Metadata column family excluded.
// Can be used to estimate space overhead of the various headers and indexes.
STAT_DEFINE(sst_bytes_payload, SUM)
STAT_DEFINE(sst_bytes_record_header, SUM)
STAT_DEFINE(sst_bytes_csi, SUM)
STAT_DEFINE(sst_bytes_index, SUM)
STAT_DEFINE(sst_bytes_other, SUM)

// Number of shards scheduled for rebuilding
STAT_DEFINE(shard_rebuilding_scheduled, SUM)
// Number of rebuilding triggered by the rebuilding supervisor
STAT_DEFINE(shard_rebuilding_triggered, SUM)
// Number of rebuilding *not* triggered by the rebuilding supervisor because
// - the node doesn't appear in the config
STAT_DEFINE(node_rebuilding_not_triggered_notinconfig, SUM)
// - the node is not a storage node
STAT_DEFINE(node_rebuilding_not_triggered_notstorage, SUM)
// - the node is alive
STAT_DEFINE(shard_rebuilding_not_triggered_nodealive, SUM)
// - rebuilding has already started
STAT_DEFINE(shard_rebuilding_not_triggered_started, SUM)
// Rebuilding supervisors that are throttled due to the number of triggers
// exceeding the trigger queue threshold
STAT_DEFINE(rebuilding_supervisor_throttled, SUM)

// LogsConfig RSM trimming requests
STAT_DEFINE(logsconfig_manager_trimming_requests, SUM)
// LogsConfig RSM snapshotting errors
STAT_DEFINE(logsconfig_manager_snapshotting_errors, SUM)
// LogsConfig number of deltas written in LogsConfig state machine
STAT_DEFINE(logsconfig_manager_delta_written, SUM)
// How many snapshots has been created
STAT_DEFINE(logsconfig_manager_snapshot_created, SUM)
// How many snapshots we have requested
STAT_DEFINE(logsconfig_manager_snapshot_requested, SUM)
// LogsConfigManager Request Stats
STAT_DEFINE(logsconfig_api_requests_received, SUM)
// Requests that was served successfully, This include legit error responses
// (e.g, the request ended up returning E::EXISTS)
STAT_DEFINE(logsconfig_api_requests_success, SUM)
// This accounts only errors that are unexpected (including NOTSUPPORTED)
STAT_DEFINE(logsconfig_api_requests_failed, SUM)
// The maximum payload size of a response, this should not exceed the
// Message::MAX_LEN
STAT_DEFINE(logsconfig_api_reply_payload_size, MAX)
// Messages that had a big payload that was too big and were dropped
STAT_DEFINE(logsconfig_api_response_toobig, SUM)
// Size of the logsconfig snapshot record
STAT_DEFINE(logsconfig_snapshot_size, MAX)

// Determines whether the nodes configuration diverged from the server config
// while having the same version. This means that either one of the NCs got
// corrupted, the ServerConfig -> NC conversion was incorrect or someone changed
// the NodesConfig section in configerator without bumping the version.
// The value here is either 0 or 1. 1 Means that they diverged and probably
// need immediate action.
STAT_DEFINE(nodes_configuration_server_config_diverged_with_same_version, MAX)

// Determines whether the nodes configuration diverged from the server config or
// not. This might be ok for short durations while the config propagates and is
// less severe than
// nodes_configuration_server_config_diverged_with_same_version.
STAT_DEFINE(nodes_configuration_server_config_diverged, MAX)

// the amount of boycotts active by a controller
STAT_DEFINE(boycotts_by_controller_active, SUM)
// the amount of boycotts made by a controller
STAT_DEFINE(boycotts_by_controller_total, SUM)
// the amount of boycotts being forwarded in the gossip by this node
STAT_DEFINE(boycotts_seen, SUM)
// the latest count of append outliers as reported by the AppendOutlierDetector
STAT_DEFINE(append_success_outliers_active, SUM)

// Event log RSM snapshotting errors
STAT_DEFINE(eventlog_snapshotting_errors, SUM)

// Size of the event log snapshot
STAT_DEFINE(eventlog_snapshot_size, MAX)

// Server message handler
// skip permission
STAT_DEFINE(server_message_dispatch_skip_permission, SUM)
// check permission
STAT_DEFINE(server_message_dispatch_check_permission, SUM)
// bypass permission
STAT_DEFINE(server_message_dispatch_bypass_permission, SUM)

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */
#ifndef RESETTING_STATS

// Number of partitions in all partitioned stores
STAT_DEFINE(partitions, SUM)
// Number of partitions that are being compacted now
STAT_DEFINE(partition_compactions_in_progress, SUM)

STAT_DEFINE(read_storage_tasks_allocated_records_bytes, SUM)
// Number of ReadStorageTasks that are in flight, ie they are executing on a
// storage thread or waiting to be sent back to the worker thread.
STAT_DEFINE(num_in_flight_read_storage_tasks, SUM)
// Number of read storage tasks being delayed because we reached the limit on
// the number of read storage tasks in flight.
STAT_DEFINE(read_storage_tasks_delayed, SUM)

// Current number of log recovery requests enqueued because the number of
// active running log recovery request reaches the limit
STAT_DEFINE(recovery_enqueued, SUM)

// Stats for rebuilding
STAT_DEFINE(num_in_flight_rebuilding_read_storage_tasks, SUM)
STAT_DEFINE(num_logs_rebuilding, SUM)
STAT_DEFINE(log_rebuilding_stall_timer_expired, SUM)
STAT_DEFINE(rebuilding_records_read_from_cache, SUM)

// Number of pending GetSeqStateRequests with UNRELEASED_RECORD context
STAT_DEFINE(get_seq_state_pending_context_unreleased_record, SUM)

// Number of logs with LogStorageState::permanent_error_ = true.
// We're likely to send errors to the client when trying to read these logs.
STAT_DEFINE(logs_with_permanent_error, SUM)

// set to 1 if the node is chosen as controller, 0 otherwise
STAT_DEFINE(is_controller, SUM)

/*
 * These stats will not be aggregated for destroyed threads.
 */
#ifndef DESTROYING_THREAD

// the current total size of all appender buffer queues, cannot be reset
STAT_DEFINE(appenderbuffer_pending_appenders, SUM)
// Number of accepted connections that are waiting for logdevice protocol
// negotiation
STAT_DEFINE(num_backlog_connections, SUM)
// Total number of open connections
STAT_DEFINE(num_connections, SUM)
// Total number of open connections using ssl
STAT_DEFINE(num_ssl_connections, SUM)
// Dropped connections due to limit/burst
STAT_DEFINE(dropped_connection_limit, SUM)
STAT_DEFINE(dropped_connection_burst, SUM)
// fd/connection limits
STAT_DEFINE(fd_limit, MAX)
STAT_DEFINE(num_reserved_fds, MAX)
STAT_DEFINE(max_incoming_connections, MAX)
STAT_DEFINE(max_external_connections, MAX)

// Number of tasks queued on the storage thread pool
STAT_DEFINE(num_storage_tasks_fast_time_sensitive, SUM)
STAT_DEFINE(num_storage_tasks_fast_stallable, SUM)
STAT_DEFINE(num_storage_tasks_slow, SUM)
STAT_DEFINE(num_storage_tasks_default, SUM)
// Total size of evbuffer pending bytes accross all workers
STAT_DEFINE(evbuffer_total_size, SUM)
// Max evbuffer size accross all workers
STAT_DEFINE(evbuffer_max_size, MAX)
// Count of flow groups runs that overran the configured max runtime
STAT_DEFINE(flow_groups_run_deadline_exceeded, SUM)
STAT_DEFINE(flow_groups_run_deadline_exceeded_rt, SUM)
// If this is > 0, there is a non authoritative rebuilding, which means too many
// shards lost data such that rebuilding could not restore all records, ie some
// records had all of their copies on these shards. When this happens, the
// shards are never transitioned to AUTHORITATIVE_EMPTY and readers may stall.
// This can be fixed by either restoring enough of these shards with their data
// intact or marking each shard "unrecoverable" which means "I know the data on
// this shard will never be restored, allow readers to make progress".
STAT_DEFINE(rebuilding_waiting_for_recoverable_shards, MAX)
// Number of storage tasks buffered accross all workers.
STAT_DEFINE(storage_task_buffer_size_fast_time_sensitive, SUM)
STAT_DEFINE(storage_task_buffer_size_fast_stallable, SUM)
STAT_DEFINE(storage_task_buffer_size_slow, SUM)
STAT_DEFINE(storage_task_buffer_size_default, SUM)
// Total number of appenders
STAT_DEFINE(num_appenders, SUM)
// Total size of appenders along with their append messages and payloads
STAT_DEFINE(total_size_of_appenders, SUM)
// Appenders started
STAT_DEFINE(appender_start, SUM)

// Current number of log recoveries that are reading sequencer metadata
// from its metadata log
STAT_DEFINE(num_recovery_reading_sequencer_metadata, SUM)

// Current total number of read streams that are in single copy delivery mode.
STAT_DEFINE(read_streams_num_scd, SUM)
// are in single copy delivery mode.
STAT_DEFINE(read_streams_total_known_down_size, SUM)
// Number of ServerReadStreams that see themselves in the SCD known down list.
STAT_DEFINE(read_streams_total_self_in_known_down, SUM)

// Number of active RecordRebuilding state machines.
STAT_DEFINE(record_rebuilding_in_progress, SUM)

// Number of logs configured for this cluster
STAT_DEFINE(num_logs_configured, MAX)

STAT_DEFINE(record_cache_repopulations_failed, SUM)
STAT_DEFINE(record_cache_repopulated_bytes, SUM)

// Number of replicated state machines that are stalled because they saw a TRIM
// or DATALOSS gap in the delta log and are waiting for a snapshot.
STAT_DEFINE(num_replicated_state_machines_stalled, SUM)

// How many times sequencer initiated trimming
STAT_DEFINE(sbt_num_seq_initiated_trims, SUM)

// Storage Node GrayListing
// 1. How many times a shard is added to the graylist, this may be more
//    than the number of nodes actually, because of
//    a) multiple store timeouts around the same time
//    b) the graylist is per NodeSetState(which is per log)
STAT_DEFINE(graylist_shard_added, SUM)

// 2. How many times graylist was reset because we couldn't pick enough nodes
//    to form a copyset
STAT_DEFINE(graylist_reset_cant_pick_copyset, SUM)

// 3. If number of nodes in graylist has reached or exceeded the threshold
//    percentage(of nodeset), then we clear the graylist
STAT_DEFINE(graylist_reset_on_threshold_reached, SUM)

// 4. If graylisting is disabled in settings for some reason, we need to
//    clear the existing graylist
STAT_DEFINE(graylist_reset_on_disable, SUM)

// 5. For the outlier based graylisting, we don't care about the reason of the
//    reset. So this counter is incremented (per worker) if the graylist is
//    cleared for any reason.
STAT_DEFINE(graylist_reseted, SUM)

// How many times does PrioritizedQueue scan all priorities but not find an
// element, even though there should be one?
STAT_DEFINE(prioritized_queue_cant_find_once, SUM)
STAT_DEFINE(prioritized_queue_cant_find_twice, SUM)
STAT_DEFINE(prioritized_queue_cant_find_thrice_or_more, SUM)
STAT_DEFINE(prioritized_queue_cant_find_max, MAX)

// how many time fail to recompute byteoffset during Epoch recovery
STAT_DEFINE(recompute_byteoffset_failed, SUM)

// stats related to admin command connections
STAT_DEFINE(command_port_connection_encrypted, SUM)
STAT_DEFINE(command_port_connection_plain, SUM)
STAT_DEFINE(command_port_connection_failed_ssl_upgrade, SUM)
STAT_DEFINE(command_port_connection_failed_ssl_required, SUM)

#endif // DESTROYING_THREAD
#endif // RESETTING_STATS


#undef STAT_DEFINE
#undef RESETTING_STATS
#undef DESTROYING_THREAD
