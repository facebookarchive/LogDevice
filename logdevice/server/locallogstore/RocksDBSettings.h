/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/settings/UpdateableSettings.h"

static_assert(ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR >= 7),
              "LogDevice requires rocksdb 5.7 or higher");

#if ROCKSDB_MAJOR > 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR >= 14)
// These performance counters were added in rocksdb 5.14.
#define ROCKSDB_PERF_COUNTER_write_scheduling_flushes_compactions_time( \
    perf_context)                                                       \
  perf_context->write_scheduling_flushes_compactions_time
#define ROCKSDB_PERF_COUNTER_write_thread_wait_nanos(perf_context) \
  perf_context->write_thread_wait_nanos
#else
// For older versions use zeros.
#define ROCKSDB_PERF_COUNTER_write_scheduling_flushes_compactions_time( \
    perf_context)                                                       \
  0
#define ROCKSDB_PERF_COUNTER_write_thread_wait_nanos(perf_context) 0
#endif

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 1)
#define LOGDEVICED_ROCKSDB_HAS_AVOID_UNNECESSARY_BLOCKING_IO
#endif

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 2)
#define LOGDEVICE_ROCKSDB_HAS_INDEX_SHORTENING_MODE
#endif

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 3)
#define LOGDEVICE_ROCKSDB_HAS_CACHE_GET_CHARGE
#endif

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 4)
#define LOGDEVICE_ROCKSDB_HAS_FIRST_KEY_IN_INDEX
#endif

namespace boost { namespace program_options {
class options_description;
}} // namespace boost::program_options

namespace facebook { namespace logdevice {

class StatsHolder;

class RocksDBSettings : public SettingsBundle {
 public:
  const char* getName() const override {
    return "RocksDBSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  // Creates a RocksDBSettings instance with default settings suitable for
  // tests (most notably turns off allow_fallocate)
  static RocksDBSettings defaultTestSettings();

  // number of low-priority bg threads to run
  int num_bg_threads_lo;

  // number of high-priority bg threads to run
  int num_bg_threads_hi;

  // number of locks used to serialize some log metadata updates (e.g. trim
  // points)
  int num_metadata_locks;

  // enable RocksDB statistics collection
  bool statistics;

  // Specifies how many records following the last known position will be
  // examined while searching for a key in the memtable (skip list) before
  // resorting to a full-scale search.
  // This is an optimization for the tailing workload where Seek() is very
  // often called for a target key which is near the iterator's current
  // position.
  int skip_list_lookahead;

  // Do RangeSync() for WAL files in a background thread.
  bool background_wal_sync;

  // IO priority to request for lo-pri rocksdb threads.
  folly::Optional<std::pair<int, int>> low_ioprio;

  // Log a message if a blocking file deletion takes at least this long on a
  // Worker thread.
  std::chrono::milliseconds worker_blocking_io_threshold_;

  // If true, data will be partitioned by time and stored in multiple column
  // families, one per partition. Compaction is not necessary in this mode
  // (trimming is implemented by dropping complete partitions) and will be
  // disabled: all records are stored in level 0. Therefore, write amplification
  // is minimal in this mode. However, this mode is not a best fit when logs
  // have different retention policies, as dropping a partition is not possible
  // until all log strands within it have been marked as trimmed.
  bool partitioned;

  // Partitioned mode settings
  // -------------------------------------------------------------------

  // Whether partitioned log store should do background compactions.
  bool partition_compactions_enabled;

  // If x is present in this vector, each partition will be compacted when
  // all logs with backlog durations <= x are trimmed away from the partition.
  // If empty, all distinct backlog durations from config are used.
  // if unset, retention based compactions are disabled on this node.
  using compaction_schedule_t = std::vector<std::chrono::seconds>;
  folly::Optional<compaction_schedule_t> partition_compaction_schedule;

  // whether we're going to proactively compact all partitions
  // (besides two latest) that were never compacted.
  // Compacting will be done in low priority background thread
  bool proactive_compaction_enabled;

  // A new partition is created every time one of the following thresholds
  // reached hit for the latest partition:
  //  * age,
  std::chrono::seconds partition_duration_;
  //  * size in bytes,
  size_t partition_size_limit_;
  //  * number of level-0 files.
  size_t partition_file_limit_;

  // How much time to wait before trimming records for a log
  // that is no longer in the config.
  std::chrono::seconds unconfigured_log_trimming_grace_period_;

  // A partitions is considered old if it is older than the these
  // many hours, otherwise it is considered recent. The file num
  // threshold for partial compaction is appropriately applied
  // based on age.
  std::chrono::hours partition_partial_compaction_old_age_threshold_;

  // Consider a partition for partial compaction if it has at least this many
  // files of size lower than partition_partial_compaction_file_size_threshold_.
  // and the age of the partitions is greater than 1 day.
  size_t partition_partial_compaction_file_num_threshold_old_;

  // Consider a partition for partial compaction if it has at least this many
  // files of size lower than partition_partial_compaction_file_size_threshold_.
  // and the age of the partitions is less than 1 day.
  size_t partition_partial_compaction_file_num_threshold_recent_;

  // Max files to compact in one partial compaction.
  size_t partition_partial_compaction_max_files_;

  size_t partition_partial_compaction_max_num_per_loop_;
  size_t partition_partial_compaction_stall_trigger_;

  // The largest l0 files that it is beneficial to compact on their own. note
  // that we can still compact larger files than this if that enables us to
  // compact a longer range of consecutive files. e.g. if there are smaller
  // files before and after one that exceeds this threshold
  size_t partition_partial_compaction_file_size_threshold_;

  // The largest l0 file size to consider for partial compaction. If 0, will use
  // 2 * partition_partial_compaction_size_threshold_ instead
  size_t partition_partial_compaction_max_file_size_;

  // If a file in a partial compaction candidate range Is larger than this
  // proportion of the total size of the files in the range, we don't consider
  // the range for compaction. This limits the maximum number of times that a
  // record can be compacted in partial compactions.
  double partition_partial_compaction_largest_file_share_;

  // See .cpp
  size_t partition_count_soft_limit_;

  // Granularity of min and max timestamps for partitions.
  // This is how often timestamps for partition are updated.
  // This is also the duration by which time-based trimming lags because of
  // overestimated maximum timestamps.
  std::chrono::milliseconds partition_timestamp_granularity_;

  // See .cpp
  std::chrono::milliseconds new_partition_timestamp_margin_;

  // How often a background thread will check if new partition should be
  // created.
  std::chrono::milliseconds partition_hi_pri_check_period_;

  // How often a background thread will trim logs and check if old partitions
  // should be dropped or compacted, and do the drops and compactions.
  std::chrono::milliseconds partition_lo_pri_check_period_;

  std::chrono::milliseconds partition_flush_check_period_;

  // See .cpp
  std::chrono::milliseconds prepended_partition_min_lifetime_;

  // The minimum allowed interval between manual MemTable flushes
  // triggered by the high priority thread to push uncommitted data
  // to stable storage.
  std::chrono::milliseconds min_manual_flush_interval;

  // Whether we should flush MemTables according the cost function defined as
  // age of the memtable multiplied by its size. That *should* give us a good
  // balance between flushing old, but slow growing tables and new but big.
  // By default that flag is set to true. We can flip it to return back to just
  // use age as the decision maker.
  bool use_age_size_flush_heuristic;

  // Maximum allowed age of unflushed data.
  std::chrono::milliseconds partition_data_age_flush_trigger;

  // Maximum allowed time since last write to a paritition before flushing
  // its uncommitted data.
  std::chrono::milliseconds partition_idle_flush_trigger;

  // Minumum guaranteed time period for a node to re-dirty a partition after
  // a MemTable is flushed without incurring a syncronous write penalty to
  // update the partition dirty metadata
  std::chrono::milliseconds partition_redirty_grace_period;

  uint64_t bytes_written_since_throttle_eval_trigger;

  // See .cpp
  std::chrono::milliseconds metadata_compaction_period;

  // See .cpp
  std::chrono::milliseconds directory_consistency_check_period;

  // See .cpp for details.
  double free_disk_space_threshold_low{0};
  bool sbr_force{false};

  // Verify checksum on each store, reject with error if it fails (see .cpp)
  bool verify_checksum_during_store{true};

  // Disable the iterate_upper_bound optimization.
  // TODO(#8945358): Remove this option once #8945358 is fixed.
  bool disable_iterate_upper_bound;

  // When set to true, the read path will use the copyset index to skip records
  // that do not pass copyset filters
  bool use_copyset_index;

  // When set to true, the findTime operation will use the findTime index
  // instead of doing a binary search in the relevant partition.
  bool read_find_time_index;

  // If true, PartitionedRocksDBStore will be opened in read only mode.
  bool read_only;

  // If true, tracks iterator superversions for the info iterators admin command
  bool track_iterator_versions;

  // See cpp file for doc.
  rate_limit_t compaction_rate_limit_;

  enum class FlushBlockPolicyType {
    DEFAULT,
    EACH_LOG,
    EACH_COPYSET,
  };

  // approximate size of (uncompressed) user data packed per block
  size_t block_size_;

  // see RocksDBFlushBlockPolicy.h
  FlushBlockPolicyType flush_block_policy_;

#ifdef LOGDEVICE_ROCKSDB_HAS_INDEX_SHORTENING_MODE
  rocksdb::BlockBasedTableOptions::IndexShorteningMode index_shortening_;
#endif

#ifdef LOGDEVICE_ROCKSDB_HAS_FIRST_KEY_IN_INDEX
  bool first_key_in_index_;
#endif

  // ignored for FlushBlockPolicyType::DEFAULT
  size_t min_block_size_;

  // same for metadata column family (if partitioned = true)
  size_t metadata_block_size_;

  // size of uncompressed block cache
  size_t cache_size_;

  // width in bits of the number of shards into which to partition the
  // uncompressed block cache
  int cache_numshardbits_;

  size_t cache_small_block_threshold_for_high_priority_;

  // size of compressed block cache (disabled by default)
  size_t compressed_cache_size_;

  // width in bits of the number of shards into which to partition the
  // compressed block cache (not sharded by default)
  int compressed_cache_numshardbits_;

  // Size of the separate block cache for metadata (including the
  // (log, lsn) to partition mapping). If zero, block cache will be shared with
  // data partitions.
  size_t metadata_cache_size_;

  // Width in bits of the number of shards into which to partition the metadata
  // block cache. If zero, the default is used.
  int metadata_cache_numshardbits_;

  // Should we put index blocks into the block cache?  If true, index blocks
  // can get evicted, which can save memory but also change performance
  // patterns.  If false, need to hold index blocks for all files in memory
  // (roughly TOTAL_DB_SIZE/BLOCK_SIZE*50 bytes).
  //
  // NOTE: This sets `cache_index_and_filter_blocks' in rocksdb options but we
  // don't use filters.
  bool cache_index_;

  // See .cpp
  bool force_no_compaction_optimizations_;

  // Used for testing only. If true, a node will report all stores it receives
  // as corrupted.
  bool test_corrupt_stores{false};

  // Various settings related to the rocksdb flush policy.

  // Total size limit for all memtables in the system.
  size_t memtable_size_per_node;

  // See .cpp
  size_t memtable_size_low_watermark_percent;

  // Enable or disable management of memtable flushing within logdevice.
  bool ld_managed_flushes;

  bool print_details;

  std::vector<shard_index_t> io_tracing_shards;

  // When ld manages flushes, memory limit for the node and memtable
  // within rocksdb set to a very high value. rocksdb should never be
  // able to reach those limits and initiate a flush. This limit is a
  // constant set to 64GB which is max memtable size supported by rocksdb.
  // Memtable and per node limits are expected to be
  // atleast half of this value. If the current settings do not satisfy this we
  // fallback to rocksdb managing the flushes.
  static constexpr size_t INFINITE_MEMORY_LIMIT{1ull << 36};

  // See .cpp
  double low_pri_write_stall_threshold_percent;
  double pinned_memtables_limit_percent;

  // See .cpp
  std::chrono::milliseconds flush_trigger_check_interval;

  // Enable rocksdb insert hint optimization with data/metadata keys. May reduce
  // CPU usage for inserting keys into rocksdb and incur small memory overhead.
  bool enable_insert_hint_;

  // If Cache index and filter block in high pri pool of block cache, making
  // them less likely to be evicted than data blocks.
  bool cache_index_with_high_priority_;

  // Ratio of rocksdb block cache reserve for index and filter blocks.
  double cache_high_pri_pool_ratio_;

  // If greater than 0, will create a bitmap to estimate rocksdb read
  // amplification and expose the result through
  // READ_AMP_ESTIMATE_USEFUL_BYTES and READ_AMP_TOTAL_READ_BYTES stats.
  uint32_t read_amp_bytes_per_bit_;

  // See .cpp
  int bloom_bits_per_key_;
  int metadata_bloom_bits_per_key_;
  bool bloom_block_based_;

  uint32_t table_format_version_;

  std::chrono::seconds test_clamp_backlog;

  // rocksdb::Options
  // These settings are copied to a rocksdb::Options struct on DB creation and
  // thus should have the REQUIRES_RESTART flag.
  // TODO(T12783992): some of these are dynamically changeable through
  // SetOption() API
  // -------------------------------------------------------------------

  rocksdb::CompactionStyle compaction_style;
  rocksdb::CompressionType compression;
  uint64_t sample_for_compression;
  bool compaction_access_sequential;
  bool advise_random_on_open;
  bool update_stats_on_db_open;
  bool allow_fallocate;
  bool auto_create_shards;
  bool use_direct_reads;
  bool use_direct_io_for_flush_and_compaction;
  int max_open_files;
  uint64_t compaction_max_bytes_at_once;
  uint64_t bytes_per_sync;
  uint64_t wal_bytes_per_sync;
  size_t compaction_readahead_size;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  int max_background_jobs;
  int max_background_compactions;
  int max_background_flushes;
  uint64_t max_bytes_for_level_base;
  int max_bytes_for_level_multiplier;
  int max_write_buffer_number;
  int num_levels;
  uint64_t target_file_size_base;
  size_t write_buffer_size;
  uint64_t max_total_wal_size;
  size_t db_write_buffer_size;
  size_t arena_block_size;
  unsigned int uc_min_merge_width;
  unsigned int uc_max_merge_width;
  unsigned int uc_max_size_amplification_percent;
  unsigned int uc_size_ratio;
  uint64_t sst_delete_bytes_per_sec;

  // Applies the settings that directly correspond to rocksdb::Options fields.
  // Note that this is not the whole logic of making rocksdb::Options.
  // The nontrivial logic (like creating caches and table factory and
  // configuring flushes) is in RocksDBLogStoreConfig's constructor.
  rocksdb::Options passThroughRocksDBOptions() const;

  // rocksdb::BlockBasedTableOptions
  // These settings are copied to a rocksdb::BlockBasedTableOptions struct on DB
  // creation and thus should have the REQUIRES_RESTART flag.
  // -------------------------------------------------------------------

  int index_block_restart_interval;

 private:
  // Only UpdateableSettings can create this bundle.
  RocksDBSettings() {}
  friend class UpdateableSettingsRaw<RocksDBSettings>;

  friend struct RocksDBLogStoreConfig;
};

}} // namespace facebook::logdevice
