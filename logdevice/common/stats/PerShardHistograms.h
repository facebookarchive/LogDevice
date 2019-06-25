/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <vector>

#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/HistogramBundle.h"

namespace facebook { namespace logdevice {

struct PerShardHistograms : public PerShardHistogramBundle {
  using compact_latency_histogram_t = ShardedHistogram<CompactLatencyHistogram>;
  using compact_size_histogram_t = ShardedHistogram<CompactSizeHistogram>;
  using compact_no_unit_histogram_t = ShardedHistogram<CompactNoUnitHistogram>;
  using latency_histogram_t = ShardedHistogram<LatencyHistogram>;
  using size_histogram_t = ShardedHistogram<SizeHistogram>;
  using record_age_histogram_t = ShardedHistogram<RecordAgeHistogram>;
  using no_unit_histogram_t = ShardedHistogram<NoUnitHistogram>;

  explicit PerShardHistograms() {}

  PerShardHistogramBundle::MapType getMap() override {
    // An entry must be added below if we ever create a new storage thread type.
    static_assert(static_cast<size_t>(StorageTaskThreadType::MAX) == 4,
                  "Please modify this file after adding a new storage "
                  "thread type");
    return {
        {"store_latency", &store_latency},
        {"store_latency_rebuilding", &store_latency_rebuilding},
        {"rocks_wal", &rocks_wal},
        {"rocks_memtable", &rocks_memtable},
        {"rocks_memtable_age", &rocks_memtable_age},
        {"rocks_delay", &rocks_delay},
        {"rocks_scheduling", &rocks_scheduling},
        {"rocks_pre_and_post", &rocks_pre_and_post},
        {"flushed_file_size", &flushed_file_size},
        {"compacted_file_size", &compacted_file_size},
        {"flushed_log_run_length", &flushed_log_run_length},
        {"compacted_log_run_length", &compacted_log_run_length},
        {"trimmed_record_age", &trimmed_record_age},

        // Rebuilding related histograms
        {"record_rebuilding", &record_rebuilding},
        {"log_rebuilding_batch_size", &log_rebuilding_batch_size},
        {"log_rebuilding_local_window_size", &log_rebuilding_local_window_size},
        {"log_rebuilding_partitions_per_batch",
         &log_rebuilding_partitions_per_batch},
        {"log_rebuilding_process_batch", &log_rebuilding_process_batch},
        {"log_rebuilding_read_batch", &log_rebuilding_read_batch},
        {"log_rebuilding_num_batches_per_window",
         &log_rebuilding_num_batches_per_window},

        // Queuing latencies for storage threads, one histogram per type of
        // storage thread.
        {"fast_storage_thread_queue_time",
         &storage_threads_queue_time[static_cast<int>(
             StorageTaskThreadType::FAST_TIME_SENSITIVE)]},
        {"fast_stallable_storage_thread_queue_time",
         &storage_threads_queue_time[static_cast<int>(
             StorageTaskThreadType::FAST_STALLABLE)]},
        {"slow_storage_thread_queue_time",
         &storage_threads_queue_time[static_cast<int>(
             StorageTaskThreadType::SLOW)]},
        {"default_storage_thread_queue_time",
         &storage_threads_queue_time[static_cast<int>(
             StorageTaskThreadType::DEFAULT)]},

    // Execution and queueing latencies for storage tasks.
    // See Stats::enumerate() to see how these are published
#define STORAGE_TASK_TYPE(name, class_name, _)                           \
  {class_name, &storage_tasks[static_cast<int>(StorageTaskType::name)]}, \
      {"queue_time." class_name,                                         \
       &storage_task_queue_time[static_cast<int>(StorageTaskType::name)]},
#include "logdevice/common/storage_task_types.inc"
    };
  }

  // Store latencies, measured from receiving STORE message to finishing
  // the store; doesn't count stores from rebuilding
  latency_histogram_t store_latency;

  // Same as store_latency but for rebuilding-related stores
  latency_histogram_t store_latency_rebuilding;

  // Time spent by RocksDB writing to WAL.
  latency_histogram_t rocks_wal;

  // Time spent by RocksDB inserting into memtables.
  latency_histogram_t rocks_memtable;

  // Age of rocksDB memtables.
  compact_no_unit_histogram_t rocks_memtable_age;

  // Time spent by RocksDB delaying writes.
  latency_histogram_t rocks_delay;

  // Time spent by RocksDB scheduling flushes and compactions on the write path.
  latency_histogram_t rocks_scheduling;

  // Time spent by RocksDB in the rest of write path.
  latency_histogram_t rocks_pre_and_post;

  // Sizes of sst files produced by flushes and compactions.
  compact_size_histogram_t flushed_file_size;
  compact_size_histogram_t compacted_file_size;

  // For each pair (log, sst file), amount of data for this log in this file.
  // Only logs that have at least one record in this file are considered.
  // This is uncompressed size.
  compact_size_histogram_t flushed_log_run_length;
  compact_size_histogram_t compacted_log_run_length;

  // The Histogram of trimmed records age, in seconds
  record_age_histogram_t trimmed_record_age;

  // Latency of RecordRebuilding state machine.
  compact_latency_histogram_t record_rebuilding;
  // Latency of Reading a batch in LogRebuilding.
  compact_latency_histogram_t log_rebuilding_read_batch;
  // Latency of processing (rebuilding records of) a batch in LogRebuilding.
  compact_latency_histogram_t log_rebuilding_process_batch;
  // Size of batches in LogRebuilding.
  compact_size_histogram_t log_rebuilding_batch_size;
  // Amount of data rebuilt in a local window in LogRebuilding.
  compact_size_histogram_t log_rebuilding_local_window_size;
  // Amount of partitions seeked per batch in LogRebuilding.
  compact_no_unit_histogram_t log_rebuilding_partitions_per_batch;
  // Amount of batches read per local window in LogRebuilding.
  compact_no_unit_histogram_t log_rebuilding_num_batches_per_window;

  // Execution latencies for storage tasks (by storage task type)
  compact_latency_histogram_t
      storage_tasks[static_cast<size_t>(StorageTaskType::MAX)];
  // Queueing latencies for storage tasks (by storage task type)
  compact_latency_histogram_t
      storage_task_queue_time[static_cast<size_t>(StorageTaskType::MAX)];
  // Queueing latencies for storage threads (by storage thread type)
  compact_latency_histogram_t storage_threads_queue_time[static_cast<size_t>(
      StorageTaskThreadType::MAX)];
};

}} // namespace facebook::logdevice
