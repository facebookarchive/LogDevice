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

// The end of the rebuilding global window
STAT_DEFINE(rebuilding_global_window_end, MAX)
// Number of times the global window was slid
STAT_DEFINE(rebuilding_global_window_slide_num, SUM)
// The total number of milliseconds the global window slid by
STAT_DEFINE(rebuilding_global_window_slide_total, SUM)
// 1 if we are waiting on the global window, 0 otherwise
STAT_DEFINE(rebuilding_global_window_waiting_flag, SUM)
// total amount of milliseconds we waited for the global window to slide
// (updated when it finally slides)
STAT_DEFINE(rebuilding_global_window_waiting_total, SUM)
// The end of the rebuilding local window
STAT_DEFINE(rebuilding_local_window_end, MAX)
// Number of times the local window was slid
STAT_DEFINE(rebuilding_local_window_slide_num, SUM)
// The total number of milliseconds the local window slid by
STAT_DEFINE(rebuilding_local_window_slide_total, SUM)

// number of times that a storage node failed to write to the local log store
// (err is E::LOCAL_LOG_STORE_WRITE)
STAT_DEFINE(local_logstore_failed_writes, SUM)
// number of times that a storage node failed to access metadata on the local
// log store (err is E::LOCAL_LOG_STORE)
STAT_DEFINE(local_logstore_failed_metadata, SUM)
// Number of times we've seen rocksdb::Iterator::status().ok() == false.
STAT_DEFINE(iterator_errors, SUM)
// Number of records read by LocalLogStoreReader in rebuilding read streams
STAT_DEFINE(read_streams_num_records_read_rebuilding, SUM)
// Number of bytes read by LocalLogStoreReader in rebuilding read streams
STAT_DEFINE(read_streams_num_bytes_read_rebuilding, SUM)
// Number of record bytes read by LocalLogStoreReader in rebuilding read streams
STAT_DEFINE(read_streams_num_record_bytes_read_rebuilding, SUM)
// Number of CSI bytes read by LocalLogStoreReader in rebuilding read streams
STAT_DEFINE(read_streams_num_csi_bytes_read_rebuilding, SUM)
// Number of CSI entries read by LocalLogStoreReader in rebuilding read streams
STAT_DEFINE(read_streams_num_csi_entries_read_rebuilding, SUM)
// IO errors reported by rocksdb. Usually indicates hardware or filesystem
// problem. External tools can use this stat as a trigger for disk maintenance
// or swap.
STAT_DEFINE(disk_io_errors, SUM)
// Corruption errors reported by rocksdb.
STAT_DEFINE(corruption_errors, SUM)
// Number of times the MemTable window for shard moved
STAT_DEFINE(active_memtables_window_move, SUM)
// The total size of trash files
STAT_DEFINE(trash_size, SUM)

// For how long this shard was stalling low-pri writes.
STAT_DEFINE(write_stall_microsec, SUM)
// Total number of flushes per shard.
STAT_DEFINE(num_memtable_flush_completed, SUM)
// Total number of metadata memtable flushes for a shard.
STAT_DEFINE(num_metadata_memtable_flush_completed, SUM)
// Total number of memtables created on a shard.
STAT_DEFINE(num_memtables, SUM)
// Cumulative sum of age for all memtables created.
STAT_DEFINE(cumulative_memtable_age_ms, SUM)

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */
#ifndef RESETTING_STATS

// 1 if we failed to open a log store and opened a FailingLocalLogStore instead.
STAT_DEFINE(failing_log_stores, SUM)
// 1 if this shard encountered an error indicating partial loss of
// access or corruption causing the shard to enter "fail-safe mode".
// Reads can still be attempted, but writes will always be denied.
STAT_DEFINE(failed_safe_log_stores, SUM)

// 1 if the shard is waiting to be rebuilt or undrained. If the shard is only
// dirty (see shard_dirty stat), the shard will accept writes and reads.
// Otherwise writes and reads are disabled.
STAT_DEFINE(shard_missing_all_data, SUM)
// 1 if the shard was rebuilt but is not acking the rebuilding because the shard
// is marked as drained, waiting for SHARD_UNDRAIN event.
STAT_DEFINE(shard_waiting_for_undrain, SUM)
// 1 if, according to out view of the event log, this node is in rebuilding set
// for this shard
STAT_DEFINE(rebuilding_set_contains_myself, SUM)
// 1 if rebuilding_set_contains_myself, and the rebuilding mode is RESTORE.
// Not counting mini-rebuildings.
STAT_DEFINE(full_restore_set_contains_myself, SUM)
// 1 if shard has under-replicated regions due to an unsafe shutdown.
// Reads and writes are still accepted, and the node will schedule a
// mini-rebuilding to restore the replication factor.
STAT_DEFINE(shard_dirty, SUM)

// Derived stats (see Stats::deriveStats()).

STAT_DEFINE(shards_waiting_for_non_started_restore, SUM)
STAT_DEFINE(non_empty_shards_in_restore, SUM)
STAT_DEFINE(unwritable_non_restore_shards, SUM)
STAT_DEFINE(iterator_errors_in_writable_shards, SUM)
STAT_DEFINE(unhealthy_shards, SUM)

// How many times space based trimming decided to trim
// partitions on a particular shard.
STAT_DEFINE(sbt_num_storage_trims, SUM)

// The starting time of the oldest partition to keep, i.e.
// the data for which we are honoring time based retention.
// Must only be set from one thread: the low-pri logsdb background thread of
// this shard. Otherwise the values from different threads would be aggregated
// incorrectly.
STAT_DEFINE(sbt_effective_retention_seconds, SUM)

#endif // RESETTING_STATS


/*
 * These stats will not be aggregated for destroyed threads.
 */
#ifndef DESTROYING_THREAD

// No. of logs that are not in config, but are not being trimmed,
// because grace period hasn't expired.
STAT_DEFINE(logs_waiting_for_grace_period, SUM)

// How many partitions are waiting to be compacted.
STAT_DEFINE(pending_compactions, SUM)

#endif // DESTROYING_THREAD

#undef STAT_DEFINE
#undef RESETTING_STATS
#undef DESTROYING_THREAD
