/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iterator>
#include <list>

#include <folly/Conv.h>
#include <folly/Likely.h>
#include <folly/Optional.h>
#include <folly/hash/Hash.h>
#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/convenience.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/sst_file_manager.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/MemtableFlushedRequest.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreFindKey.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreFindTime.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStoreIterators.h"
#include "logdevice/server/locallogstore/RocksDBCompactionFilter.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBListener.h"
#include "logdevice/server/locallogstore/RocksDBMemTableRep.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/LocalLogStoreUtils.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

using RocksDBKeyFormat::CustomIndexDirectoryKey;
using RocksDBKeyFormat::PartitionDirectoryKey;
using RocksDBKeyFormat::PartitionMetaKey;
using RocksDBKeyFormat::PerEpochLogMetaKey;
using DirectoryEntry = PartitionedRocksDBStore::DirectoryEntry;
using FlushEvaluator = PartitionedRocksDBStore::FlushEvaluator;
using CFData = FlushEvaluator::CFData;

const char* PartitionedRocksDBStore::METADATA_CF_NAME = "metadata";
const char* PartitionedRocksDBStore::UNPARTITIONED_CF_NAME = "unpartitioned";
const char* PartitionedRocksDBStore::SNAPSHOTS_CF_NAME = "snapshots";
static const int SCHEMA_VERSION = 2;
constexpr std::pair<node_index_t, DataClass>
    PartitionedRocksDBStore::DirtyState::SENTINEL_KEY;

int DirectoryEntry::fromIterator(const RocksDBIterator* it, logid_t log_id) {
  ld_check(it);
  ld_check(it->status().ok());
  if (!it->Valid() ||
      !RocksDBKeyFormat::PartitionDirectoryKey::valid(
          it->key().data(), it->key().size()) ||
      RocksDBKeyFormat::PartitionDirectoryKey::getLogID(it->key().data()) !=
          log_id) {
    id = PARTITION_INVALID;
    return 0;
  }
  first_lsn = RocksDBKeyFormat::PartitionDirectoryKey::getLSN(it->key().data());
  id = RocksDBKeyFormat::PartitionDirectoryKey::getPartition(it->key().data());
  if (id == PARTITION_INVALID) {
    ld_error("Invalid directory key: %s; value: %s",
             hexdump_buf(it->value().data(), it->value().size()).c_str(),
             hexdump_buf(it->key().data(), it->key().size()).c_str());
    return -1;
  }
  if (!PartitionDirectoryValue::valid(it->value().data(), it->value().size())) {
    ld_error("Invalid directory value %s for key %s",
             hexdump_buf(it->value().data(), it->value().size()).c_str(),
             hexdump_buf(it->key().data(), it->key().size()).c_str());
    return -1;
  }
  max_lsn = PartitionDirectoryValue::getMaxLSN(
      it->value().data(), it->value().size());
  flags =
      PartitionDirectoryValue::getFlags(it->value().data(), it->value().size());
  approximate_size_bytes = PartitionDirectoryValue::getApproximateSizeBytes(
      it->value().data(), it->value().size());
  return 0;
}

// Put()s this entry in rocksdb_batch.
void DirectoryEntry::doPut(logid_t log_id,
                           Durability durability,
                           rocksdb::ColumnFamilyHandle* cf,
                           rocksdb::WriteBatch& rocksdb_batch) {
  flags &= ~PartitionDirectoryValue::NOT_DURABLE;
  if (durability <= Durability::MEMORY) {
    flags |= PartitionDirectoryValue::NOT_DURABLE;
  }
  ld_check_ne(id, PARTITION_INVALID);
  PartitionDirectoryKey key(log_id, first_lsn, id);
  PartitionDirectoryValue value(max_lsn, flags, approximate_size_bytes);
  Slice value_slice = value.serialize();
  rocksdb_batch.Put(
      cf,
      rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
      rocksdb::Slice(
          reinterpret_cast<const char*>(value_slice.data), value_slice.size));
}

// Delete()s this entry in rocksdb_batch.
void DirectoryEntry::doDelete(logid_t log_id,
                              rocksdb::ColumnFamilyHandle* cf,
                              rocksdb::WriteBatch& rocksdb_batch) {
  ld_check_ne(id, PARTITION_INVALID);
  PartitionDirectoryKey key(log_id, first_lsn, id);
  rocksdb_batch.Delete(
      cf, rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
}

// String representation in format "<partition id> [<first lsn>,<last lsn>]"
std::string DirectoryEntry::toString() const {
  return (std::to_string(id) + " [" + lsn_to_string(first_lsn) + "," +
          lsn_to_string(max_lsn) + "]");
}

std::string DirectoryEntry::flagsToString() const {
  return PartitionDirectoryValue::flagsToString(flags);
}

namespace PartitionedDBKeyFormat {
partition_id_t getIdFromCFName(const std::string& name) {
  return folly::to<partition_id_t>(name);
}
std::string getNameFromId(partition_id_t id) {
  return folly::to<std::string>(id);
}
}; // namespace PartitionedDBKeyFormat

bool PartitionedRocksDBStore::MetadataMergeOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* /*logger*/) const {
  // This operator only applies to MutablePerEpochLogMetadata and the metadata
  // index entries for findKey. Make sure key is valid.
  const char header = key.size() > 0 ? key.data()[0] : '\0';
  if (key.size() == 0 ||
      (header !=
           PerEpochLogMetaKey::getHeader(PerEpochLogMetadataType::MUTABLE) &&
       !CustomIndexDirectoryKey::valid(key.data(), key.size()))) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Merge called on a metadata column family with an "
                    "invalid key header and not a CustomIndexDirectoryKey: %c",
                    header);
    return false;
  }

  // If there is no existing value, just assign the new value.
  if (existing_value == nullptr) {
    new_value->assign(value.data(), value.size());
    return true;
  }

  // Generic lambda for safely merging metadata.
  const auto merge = [new_value, left = *existing_value, right = value](
                         auto&& metadata) -> bool {
    Slice right_slice(static_cast<const char*>(right.data()), right.size());
    if (UNLIKELY(!metadata.valid(right_slice) ||
                 metadata.deserialize(Slice(left.data(), left.size())) != 0)) {
      return false; // invalid format
    }
    metadata.merge(right_slice);
    const Slice merged_slice(metadata.serialize());
    new_value->assign(
        static_cast<const char*>(merged_slice.data), merged_slice.size);
    return true;
  };

  if (header ==
      PerEpochLogMetaKey::getHeader(PerEpochLogMetadataType::MUTABLE)) {
    return merge(MutablePerEpochLogMetadata{});
  }

  // Handle the case for CustomIndexDirectoryKey. The metadata index used in the
  // findKey API maps (log_id, index_type, partition_id) to the minimum key in
  // the partition and the corresponding LSN, which is thus the value being
  // updated.
  if (!CustomIndexDirectoryValue::valid(
          existing_value->data(), existing_value->size()) ||
      !CustomIndexDirectoryValue::valid(value.data(), value.size())) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "At least one value is invalid; key: %s, old value: %s, "
        "new value: %s",
        hexdump_buf(key.data(), key.size()).c_str(),
        hexdump_buf(existing_value->data(), existing_value->size()).c_str(),
        hexdump_buf(value.data(), value.size()).c_str());
    return false;
  }

  if (CustomIndexDirectoryValue::compare(*existing_value, value)) {
    std::string old_key(reinterpret_cast<const char*>(existing_value->data()),
                        existing_value->size());
    new_value->assign(std::move(old_key));
  } else {
    std::string new_key(
        reinterpret_cast<const char*>(value.data()), value.size());
    new_value->assign(std::move(new_key));
  }

  return true;
}

class PartitionedRocksDBStore::Partition::TimestampUpdateTask
    : public StorageTask {
 public:
  TimestampUpdateTask(PartitionPtr p, RecordTimestamp min, RecordTimestamp max)
      : StorageTask(StorageTask::Type::UPDATE_PARTITION_TIMESTAMP),
        partition(p),
        min_timestamp(min),
        max_timestamp(max) {
    ld_check_ne(min_timestamp, RecordTimestamp::min());
    ld_check_ne(min_timestamp, RecordTimestamp::max());
    ld_check_ne(max_timestamp, RecordTimestamp::min());
    ld_check_ne(max_timestamp, RecordTimestamp::max());
  }

  void execute() override {
    // This task triggers and waits for a WAL sync. It should never
    // be executed.
    ld_check(false);
  }
  Durability durability() const override {
    return Durability::SYNC_WRITE;
  }

  void setSyncToken(FlushToken ft) {
    sync_token = ft;
  }

  FlushToken syncToken() const override {
    return sync_token;
  }

  void onSynced() override {
    // Take parititon write lock, etc.
    ld_debug("Durable timestamps updated from %s:%s to %s:%s",
             logdevice::toString(partition->min_durable_timestamp).c_str(),
             logdevice::toString(partition->max_durable_timestamp).c_str(),
             logdevice::toString(min_timestamp).c_str(),
             logdevice::toString(max_timestamp).c_str());
    partition->min_durable_timestamp.storeMin(min_timestamp);
    partition->max_durable_timestamp.storeMax(max_timestamp);
  }

  void onDone() override {}

  // Can never be dropped.
  void onDropped() override {
    ld_check(false);
  }
  bool isDroppable() const override {
    return false;
  }
  bool allowDelayingSync() const override {
    return false;
  }

  std::string toString() const {
    return std::string("Partition::TimestampUpdateTask:") +
        std::string(" partition: ") + logdevice::toString(partition->id_) +
        std::string(", min: ") + logdevice::toString(min_timestamp) +
        std::string(", max: ") + logdevice::toString(max_timestamp);
  }
  PartitionPtr partition;
  RecordTimestamp min_timestamp;
  RecordTimestamp max_timestamp;
  FlushToken sync_token = FlushToken_INVALID;
};

RecordTimeInterval PartitionedRocksDBStore::Partition::dirtyTimeInterval(
    const RocksDBSettings& settings,
    partition_id_t latest_id) const {
  RecordTimestamp dirty_min = min_durable_timestamp;
  RecordTimestamp dirty_max = max_durable_timestamp;

  // No timestamp updates durably recorded. Rely on starting timestamp.
  if (dirty_min == RecordTimestamp::max()) {
    dirty_min = starting_timestamp;
  }
  if (dirty_max == RecordTimestamp::min()) {
    dirty_max = dirty_min;
  }

  // The latest partition is assumed to be dirty for the configured
  // partition duration. This ensures that an idle latest partition
  // doesn't require a blocking metadata write as soon as writes resume.
  //
  // NOTE: partition_duration_ and partition_timestamp_granularity_ can change
  //       during runtime and between invocations of logdeviced. Changes for
  //       these values are rare; the settings are tuned once for the use case
  //       and typically never changed again. Correctness will be impacted
  //       if the values are decreased between LogDevice invocations,
  //       and timestamp updates to the WAL are lost. Dirty ranges will
  //       be potenailly underestimated by the amount these settings are
  //       reduced. Since both settings changes and the loss of the WAL are
  //       rare events, the complexity of plugging this hole isn't justified.
  if (id_ == latest_id) {
    dirty_max.storeMax(starting_timestamp + settings.partition_duration_);
  }
  return RecordTimeInterval(
      dirty_min -
          std::min(dirty_min.time_since_epoch(), // avoiding overflow
                   settings.partition_timestamp_granularity_),
      dirty_max +
          std::min(RecordTimestamp::max() - dirty_max,
                   settings.partition_timestamp_granularity_));
}

PartitionedRocksDBStore::PartitionedRocksDBStore(
    uint32_t shard_idx,
    uint32_t num_shards,
    const std::string& path,
    RocksDBLogStoreConfig rocksdb_config,
    const Configuration* config,
    StatsHolder* stats,
    IOTracing* io_tracing,
    DeferInit defer_init)
    : RocksDBLogStoreBase(shard_idx,
                          num_shards,
                          path,
                          std::move(rocksdb_config),
                          stats,
                          io_tracing),
      logs_(),
      data_cf_options_(rocksdb_config_.options_) {
  if (defer_init == DeferInit::NO) {
    init(config);
  }
}

void PartitionedRocksDBStore::init(const Configuration* config) {
  // Disable compactions for data partitions.
  // These should be less than 1<<30 because rocksdb sometimes multiplies them
  // by two as signed 32-bit ints.
  data_cf_options_.level0_file_num_compaction_trigger = 1 << 29;
  data_cf_options_.level0_slowdown_writes_trigger = 1 << 29;
  data_cf_options_.level0_stop_writes_trigger = 1 << 29;
  data_cf_options_.disable_auto_compactions = true;

  // Metadata column family, on the other hand, gets compacted and can have a
  // dedicated block cache. Partition directory is usually small, and 100%
  // cache hit ratio is expected most of the time. If someone accidentally
  // flushed out metadata cache, we would need to do synchronous reads before
  // writing records to older partitions.
  rocksdb::ColumnFamilyOptions meta_cf_options =
      rocksdb_config_.metadata_options_;
  meta_cf_options.disable_auto_compactions = false;

  // enable compaction filter for metadata CF so that per-epoch log metadata
  // can be trimmed as the data log trims. Use the same compaction filter for
  // data logs. Note that auto compaction does not need to hold the context
  // lock so it is fine to not have it.
  meta_cf_options.compaction_filter_factory =
      data_cf_options_.compaction_filter_factory;

  // Metadata column family keys can have a different format than data records
  // (a byte prefix followed by a log id), so don't use the same prefix
  // extractor.
  meta_cf_options.prefix_extractor.reset();

  // Metadata column family uses a merge operator for updates to max lsn for
  // each log.
  meta_cf_options.merge_operator.reset(new MetadataMergeOperator);

  // Grab the list of column families first. This is needed for Open() later
  // and is also used to map partition ids to ColumnFamilyHandles.
  std::vector<std::string> column_families;
  rocksdb::Status status = rocksdb::DB::ListColumnFamilies(
      rocksdb_config_.options_, db_path_, &column_families);

  if (!status.ok()) {
    // ListColumnFamilies() expects the db to exist. If the call failed, we'll
    // just assume this is a new db, relying on a subsequent open() failing in
    // case it's a more severe issue. Empty databases start with just the
    // default column family.
    column_families.push_back(rocksdb::kDefaultColumnFamilyName);
  }

  // Sort column families so that partition IDs are increasing.
  std::sort(column_families.begin(),
            column_families.end(),
            [](const std::string& a, const std::string& b) -> bool {
              // Compare lengths first, then strings.
              // For numbers it's equivalent to numeric comparison.
              return std::forward_as_tuple(a.size(), a) <
                  std::forward_as_tuple(b.size(), b);
            });

  ld_spew("Found %zd column families", column_families.size());

  if (!open(column_families, meta_cf_options, config) || !readDirectories()) {
    throw ConstructorFailed();
  }
  if (!getSettings()->read_only) {
    if (!finishInterruptedDrops() || !convertDataKeyFormat()) {
      throw ConstructorFailed();
    }
  }

  if (!getSettings()->read_only) {
    startBackgroundThreads();
    // Register flush callback
    createAndRegisterFlushCallback();
    // Memtables written as part of recovery do not have owner pointer
    // initialized as the cf_accessor map is still getting initialized. Flush
    // all the memtables created during recovery. Going forward we have all the
    // necessary information to initialize the memtable completely.
    // Wait for the flush to complete, so that system is started with clean
    // slate and is ready to accept writes.
    flushAllMemtables(/*wait*/ true);
  }
}

PartitionedRocksDBStore::~PartitionedRocksDBStore() {
  if (!shutdown_event_.signaled()) {
    joinBackgroundThreads();
  }

  STAT_SUB(stats_, partitions, partitions_.size());
}

void PartitionedRocksDBStore::startBackgroundThreads() {
  ld_check(!getSettings()->read_only);
  static_assert(
      (int)BackgroundThreadType::COUNT == 3,
      "update startBackgroundThreads() after changing BackgroundThreadType");
  ld_check(!background_threads_[0].joinable());
  ld_check(!background_threads_[1].joinable());
  ld_check(!background_threads_[2].joinable());

  background_threads_[(int)BackgroundThreadType::HI_PRI] =
      std::thread([&]() { hiPriBackgroundThreadRun(); });
  background_threads_[(int)BackgroundThreadType::LO_PRI] =
      std::thread([&]() { loPriBackgroundThreadRun(); });
  background_threads_[(int)BackgroundThreadType::FLUSH] =
      std::thread([&]() { flushBackgroundThreadRun(); });
}

void PartitionedRocksDBStore::createAndRegisterFlushCallback() {
  flushCallback_ = std::make_unique<MemtableFlushCallback>();
  // Note that some tests override registerOnFlushCallback() to do nothing.
  registerOnFlushCallback(*flushCallback_.get());
}

void PartitionedRocksDBStore::joinBackgroundThreads() {
  // Prevent threads from starting new work.
  shutdown_event_.signal();

  // Persist all in-core data and mark partitions clean.
  if (!(getSettings()->read_only || inFailSafeMode() ||
        db_->GetOptions().avoid_flush_during_shutdown)) {
    flushAllMemtables();
    ld_info("Shard %d flushed memtables one last time", getShardIdx());
    // NOTE: While we are guaranteed that all dirty MemTables have been
    //       flushed to stable storage, background compactions may still
    //       reference them. This prevents the MemTables from being reaped
    //       and advancing the sliding window of outstanding MemTables.
    //       The full fix is to track when MemTables are flushed instead
    //       of when they are destroyed. This will likely involve some
    //       RocksDB changes (e.g. something like D6295324). Until that
    //       happens, update dirty state as if the sliding window fully
    //       closed as a result of our flush.
    releaseDirtyHold(latest_.get());
    updateDirtyState(maxFlushToken());
  }

  // Cancel any in-progress operations, such as compactions, that our
  // threads may be blocked on. Compactions can take minutes to complete
  // and we do not want to delay shutdown. All of these operations will
  // be restarted and completed the next time logdeviced runs.
  //
  // Note: CancelAllBackgroundWork() has the effect of shutting down
  //       RocksDB. Write operations, including those performed by our
  //       background threads, will start to fail once this API
  //       is called. Our background threads are designed to tolerate
  //       these errors and to exit cleanly.
  rocksdb::CancelAllBackgroundWork(db_.get());

  if (getSettings()->read_only) {
    // no threads were started
    return;
  }
  for (std::thread& t : background_threads_) {
    t.join();
  }

  immutable_.store(true);
}

void PartitionedRocksDBStore::setProcessor(Processor* processor) {
  ld_check(!processor_.load());
  processor_.store(checked_downcast<ServerProcessor*>(processor));
}

std::unique_ptr<LocalLogStore::ReadIterator>
PartitionedRocksDBStore::read(logid_t log_id,
                              const LocalLogStore::ReadOptions& options) const {
  // Reading from partitioned store without a log_id is not supported
  if (isLogPartitioned(log_id)) {
    return std::make_unique<Iterator>(this, log_id, options);
  } else {
    return std::make_unique<RocksDBLocalLogStore::CSIWrapper>(
        this, log_id, options, unpartitioned_cf_->get());
  }
}

std::unique_ptr<LocalLogStore::AllLogsIterator>
PartitionedRocksDBStore::readAllLogs(
    const LocalLogStore::ReadOptions& options,
    const folly::Optional<std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>&
        logs) const {
  return std::make_unique<PartitionedAllLogsIterator>(this, options, logs);
}

RocksDBCFPtr PartitionedRocksDBStore::wrapAndRegisterCF(
    std::unique_ptr<rocksdb::ColumnFamilyHandle> handle) {
  ld_check(handle);
  auto cf_id = handle->GetID();
  auto cf_ptr = std::make_shared<RocksDBColumnFamily>(handle.release());
  cf_accessor_.wlock()->insert(std::make_pair(cf_id, cf_ptr));
  return cf_ptr;
}

RocksDBCFPtr PartitionedRocksDBStore::createColumnFamily(
    std::string name,
    const rocksdb::ColumnFamilyOptions& options) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  ld_check(db_);
  rocksdb::ColumnFamilyHandle* handle;
  rocksdb::Status status;
  {
    SCOPED_IO_TRACING_CONTEXT(getIOTracing(), "create-cf:{}", name);
    status = db_->CreateColumnFamily(options, name, &handle);
  }
  if (!status.ok()) {
    ld_error("Failed to create column family \'%s\': %s",
             name.c_str(),
             status.ToString().c_str());
    enterFailSafeIfFailed(status, "CreateColumnFamily()");
    return nullptr;
  }
  return wrapAndRegisterCF(
      std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle));
}
std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>>
PartitionedRocksDBStore::createColumnFamilies(
    const std::vector<std::string>& names,
    const rocksdb::ColumnFamilyOptions& options) {
  ld_check(!names.empty());
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  ld_check(db_);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status status;
  {
    SCOPED_IO_TRACING_CONTEXT(
        getIOTracing(),
        "create-cfs:{}",
        names.size() > 1 ? names.front() + "-" + names.back() : names.front());
    status = db_->CreateColumnFamilies(options, names, &handles);
  }
  if (!status.ok()) {
    ld_error("Failed to create %lu column families \'%s\' - \'%s\': %s",
             names.size(),
             names.front().c_str(),
             names.back().c_str(),
             status.ToString().c_str());
    enterFailSafeIfFailed(status, "CreateColumnFamilies()");
    return {};
  }
  std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> result;
  result.reserve(handles.size());
  for (auto handle : handles) {
    result.emplace_back(handle);
  }
  return result;
}

bool PartitionedRocksDBStore::open(
    const std::vector<std::string>& column_families,
    const rocksdb::ColumnFamilyOptions& meta_cf_options,
    const Configuration* /*config*/) {
  // build a list of CF descriptors
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
  for (const auto& name : column_families) {
    cf_descriptors.emplace_back(
        name,
        name == METADATA_CF_NAME ? meta_cf_options
                                 : name == UNPARTITIONED_CF_NAME
                ? rocksdb::ColumnFamilyOptions(rocksdb_config_.options_)
                : name == SNAPSHOTS_CF_NAME
                    ? rocksdb::ColumnFamilyOptions(rocksdb_config_.options_)
                    : data_cf_options_);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_raw;
  rocksdb::DB* db;

  rocksdb::Status status;
  bool read_only = getSettings()->read_only;
  if (read_only) {
    status = rocksdb::DB::OpenForReadOnly(rocksdb_config_.options_,
                                          db_path_,
                                          cf_descriptors,
                                          &cf_handles_raw,
                                          &db);
  } else {
    status = rocksdb::DB::Open(rocksdb_config_.options_,
                               db_path_,
                               cf_descriptors,
                               &cf_handles_raw,
                               &db);
  }

  if (!status.ok()) {
    ld_error("Couldn't open the partitioned db \"%s\"%s: %s",
             read_only ? " (read only)" : "",
             db_path_.c_str(),
             status.ToString().c_str());
    noteRocksDBStatus(status, "Open()/OpenForReadOnly()");
    return false;
  }

  std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> cf_handles(
      cf_handles_raw.size());
  for (size_t i = 0; i < cf_handles.size(); ++i) {
    cf_handles[i].reset(cf_handles_raw[i]);
  }

  db_.reset(db);

  partition_id_t latest_partition_id = PARTITION_INVALID;
  partition_id_t oldest_partition_id = PARTITION_MAX;

  ld_check_eq(cf_descriptors.size(), cf_handles.size());
  for (size_t i = 0; i < cf_descriptors.size(); ++i) {
    auto& desc = cf_descriptors[i];

    ld_spew("Found column family %s in database \"%s\"",
            desc.name.c_str(),
            db_path_.c_str());

    if (desc.name == rocksdb::kDefaultColumnFamilyName) {
      // Check that default column family is empty. Otherwise it's likely that
      // it contains data of a non-partitioned store, and we're misconfigured.
      RocksDBIterator it =
          newIterator(getDefaultReadOptions(), cf_handles[i].get());
      it.SeekToFirst();
      if (it.Valid()) {
        ld_error("Default column family is not empty. Are you trying to open"
                 " a non-partitioned log store as a partitioned one?");
        PER_SHARD_STAT_INCR(stats_, corruption_errors, shard_idx_);
        return false;
      }
      continue;
    } else if (desc.name == METADATA_CF_NAME) {
      metadata_cf_ = wrapAndRegisterCF(std::move(cf_handles[i]));
    } else if (desc.name == UNPARTITIONED_CF_NAME) {
      unpartitioned_cf_ = wrapAndRegisterCF(std::move(cf_handles[i]));
    } else if (desc.name == SNAPSHOTS_CF_NAME) {
      snapshots_cf_ = wrapAndRegisterCF(std::move(cf_handles[i]));
    } else {
      auto id = PartitionedDBKeyFormat::getIdFromCFName(desc.name);
      ld_check_gt(id, latest_partition_id);
      if (latest_partition_id != PARTITION_INVALID &&
          id > latest_partition_id + 1) {
        // There's a gap in partition IDs. This is possible if we crashed while
        // dropping multiple partitions.
        // finishInterruptedDrops() will get rid of this gap.
        ld_warning("Gap in partition IDs: %lu - %lu. This may be possible "
                   "after a hard crash but should be extremely rare.",
                   latest_partition_id + 1,
                   id);
        for (partition_id_t p = latest_partition_id + 1; p < id; ++p) {
          partitions_.push(p, nullptr);
        }
      }
      // readPartitionTimestamps() will initialize starting_timestamp.
      auto cf_ptr = wrapAndRegisterCF(std::move(cf_handles[i]));
      PartitionPtr partition =
          std::make_shared<Partition>(id, cf_ptr, RecordTimestamp::min());
      addPartitions({partition});
      latest_partition_id = id;
      oldest_partition_id = std::min(oldest_partition_id, id);
    }
  }

  // create the metadata column family unless it already exists
  if (!metadata_cf_ && !getSettings()->read_only) {
    metadata_cf_ = createColumnFamily(METADATA_CF_NAME, meta_cf_options);
  }
  if (!metadata_cf_) {
    ld_error("Couldn't find/create metadata column family");
    return false;
  }

  // Column family for unpartitioned logs behaves like RocksDBLocalLogStore.
  if (!unpartitioned_cf_ && !getSettings()->read_only) {
    unpartitioned_cf_ =
        createColumnFamily(UNPARTITIONED_CF_NAME, rocksdb_config_.options_);
  }
  if (!unpartitioned_cf_) {
    ld_error("Couldn't find/create unpartitioned column family");
    return false;
  }

  int rv = checkSchemaVersion(db, metadata_cf_->get(), SCHEMA_VERSION);
  if (rv != 0) {
    PER_SHARD_STAT_INCR(stats_, corruption_errors, shard_idx_);
    return false;
  }

  // Use per-partition dirty information to create or update
  // RebuildingRangeMetadata.
  RebuildingRangesMetadata range_meta;
  if (!getSettings()->read_only && getRebuildingRanges(range_meta) != 0) {
    return false;
  }

  // Read per-partition metadata. We'll need it below to decide if we need
  // to create more partitions.
  rocksdb::WriteBatch partition_updates;
  for (partition_id_t id = oldest_partition_id; id <= latest_partition_id;
       ++id) {
    PartitionPtr partition = partitions_.get(id);
    if (!partition) {
      // Can have gaps in partition numbering because of interrupted drops.
      continue;
    }

    if (!readPartitionTimestamps(partition)) {
      if (id != latest_partition_id || err != E::NOTFOUND) {
        return false;
      } else {
        // We were probably adding a new latest partition, had created the CF,
        // but crashed before we could write the partition timestamp.
        // That's fine, we can drop it and it'll be recreated later if need be.
        ld_warning("Latest partition %lu had no metadata; assuming we crashed "
                   "before we could write it. Dropping the CF.",
                   partition->id_);
        ld_check_ne(oldest_partition_id, id);
        status = db_->DropColumnFamilies({partition->cf_->get()});
        if (!status.ok()) {
          ld_error("Failed to drop column family %lu: %s",
                   partition->id_,
                   status.ToString().c_str());
          return false;
        }

        partitions_.pop_back();
        if (partitions_.empty()) {
          ld_error("Found incomplete latest partition %lu but no previous "
                   "partition; should be impossible!",
                   id);
          return false;
        } else {
          latest_partition_id = partitions_.back()->id_;
          if (latest_partition_id < id - 1) {
            ld_error("Found gap between incomplete-latest partition %lu and "
                     "second-latest partition %lu; should be impossible!",
                     id,
                     latest_partition_id);
            return false;
          }
        }
        break;
      }
    }
    if (!readPartitionDirtyState(partition)) {
      return false;
    }

    for (auto& ndd_kv : partition->dirty_state_.dirtied_by_nodes) {
      // Only persisting Appends for now.
      if (ndd_kv.first.second != DataClass::APPEND) {
        continue;
      }

      if (!ndd_kv.second.isClean()) {
        setUnderReplicated(partition);
        auto dti =
            partition->dirtyTimeInterval(*getSettings(), latest_partition_id);
        ld_info("Partition s%u:%lu found dirty: %s",
                shard_idx_,
                partition->id_,
                logdevice::toString(dti).c_str());
        range_meta.modifyTimeIntervals(
            TimeIntervalOp::ADD, DataClass::APPEND, dti);

        // Expand partition bounds to cover the dirty time range.
        if (dti.lower() < partition->min_timestamp) {
          partition->min_timestamp = dti.lower();
          partition->min_durable_timestamp = dti.lower();
          PartitionMetaKey key(
              PartitionMetadataType::MIN_TIMESTAMP, partition->id_);
          PartitionTimestampMetadata meta(
              PartitionMetadataType::MIN_TIMESTAMP, partition->min_timestamp);
          Slice value = meta.serialize();
          partition_updates.Put(
              metadata_cf_->get(),
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
              rocksdb::Slice(
                  reinterpret_cast<const char*>(value.data), value.size));
        }
        if (dti.upper() > partition->max_timestamp) {
          partition->max_timestamp = dti.upper();
          partition->max_durable_timestamp = dti.upper();
          PartitionMetaKey key(
              PartitionMetadataType::MAX_TIMESTAMP, partition->id_);
          PartitionTimestampMetadata meta(
              PartitionMetadataType::MAX_TIMESTAMP, partition->max_timestamp);
          Slice value = meta.serialize();
          partition_updates.Put(
              metadata_cf_->get(),
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
              rocksdb::Slice(
                  reinterpret_cast<const char*>(value.data), value.size));
        }

        // Since we don't currently persist coordinating node information,
        // handling a single unclean node is sufficient to record the time
        // range as dirty.
        break;
      }
    }

    // The loaded partition dirty state reflects the state of the previous
    // instance of LogDevice. Now that the ranges are accounted for in
    // range_meta, allow the partition to be cleaned by the cleaner.
    partition->dirty_state_.dirtied_by_nodes.clear();

    if (partition->isUnderReplicated()) {
      ld_info("Partition s%u:%lu is under-replicated: "
              "Start:%s, Min:%s, Max: %s",
              shard_idx_,
              partition->id_,
              logdevice::toString(partition->starting_timestamp).c_str(),
              logdevice::toString(partition->min_timestamp).c_str(),
              logdevice::toString(partition->max_timestamp).c_str());
    }
  }

  ld_info("Shard[%u] dirty ranges: %s",
          shard_idx_,
          logdevice::toString(range_meta).c_str());

  if (!getSettings()->read_only && !range_meta.empty()) {
    status = writeBatch(rocksdb::WriteOptions(), &partition_updates);
    if (!status.ok()) {
      return false;
    }
    rv = writeStoreMetadata(range_meta, WriteOptions());
    if (rv != 0) {
      return false;
    }
    syncWAL();
    // With the range metadata persisted, force a cleaner pass to persist
    // partition dirty metadata for partitions which are now clean in this
    // instance of LogDevice.
    cleaner_pass_requested_.store(true);
  }

  auto partition_duration = getSettings()->partition_duration_;

  if (latest_partition_id == PARTITION_INVALID) {
    // No data partitions exist yet. Create one partition.
    ld_check(partitions_.empty());
    if (getSettings()->read_only) {
      // can't create a partition in read only mode
      ld_error("No partitions exist, can't open in read only mode.");
      return false;
    }
    auto created = createPartitionsImpl(INITIAL_PARTITION_ID, currentTime(), 1);
    if (created.empty()) {
      return false;
    }

    oldest_partition_id = created[0]->id_;
  } else if (partition_duration.count() > 0 && !getSettings()->read_only) {
    // If the server was down for a long time, the latest partition p has
    // starting_timestamp far in the past. If the next partition we create
    // has starting_timestamp in the future, p will end up covering a long time
    // range and potentially taking too much data during rebuilding.
    // To prevent that, create some partitions to bridge the gap.
    PartitionPtr latest = partitions_.get(latest_partition_id);
    ld_check(latest);
    const size_t limit = getSettings()->partition_count_soft_limit_;
    const size_t existing = partitions_.getVersion()->size();
    const RecordTimestamp now = currentTime();
    size_t num_to_create = now <= latest->starting_timestamp
        ? 0ul
        : (now - latest->starting_timestamp) / partition_duration;
    RecordTimestamp first_new_timestamp =
        latest->starting_timestamp + partition_duration;
    if (num_to_create != 0 && existing + num_to_create > limit) {
      ld_check_gt(limit, 0);
      // Create at least one.
      size_t reduced_num_to_create = limit - std::min(limit - 1, existing);
      ld_warning(
          "The existing partitions are so old and/or numerous that we can't "
          "fully bridge the gap between now and them without exceeding "
          "max-partition-count setting. Existing partitions: %lu, last "
          "existing "
          "partition: [%lu] %s, max-partition-count: %lu, new partitions "
          "needed to fill the gap: %lu. Will create %lu partitions instead.",
          existing,
          latest->id_,
          latest->starting_timestamp.toString().c_str(),
          limit,
          num_to_create,
          reduced_num_to_create);
      num_to_create = reduced_num_to_create;
      // The last created partition will have starting timestamp equal to `now`.
      first_new_timestamp =
          now - partition_duration * (reduced_num_to_create - 1);
    }
    if (num_to_create != 0) {
      ld_info("Creating %lu partitions to cover time range %s - %s",
              num_to_create,
              first_new_timestamp.toString().c_str(),
              now.toString().c_str());
      auto created = createPartitionsImpl(
          latest->id_ + 1, first_new_timestamp, num_to_create);
      if (created.empty()) {
        return false;
      }
    }
  }

  oldest_partition_id_.store(oldest_partition_id);

  // Initialize latest_ to point to the latest partition.
  latest_partition_id = partitions_.nextID() - 1;
  PartitionPtr latest_partition = partitions_.get(latest_partition_id);
  ld_check(latest_partition);

  // The latest partition is always kept dirty so there is no
  // WAL sync incurred when an idle storage node starts taking
  // writes again. We do this by adding a sentinal node entry
  // that is removed once the partition is no longer the latest.
  if (!getSettings()->read_only) {
    // We removed dirty holds during the scan above...
    ld_check(!isHeldDirty(latest_partition));
    rv = holdDirty(latest_partition);
    if (rv == 0) {
      rv = syncWAL();
    }
    if (rv != 0) {
      return false;
    }
  }

  latest_.update(latest_partition);

  return true;
}

std::vector<PartitionedRocksDBStore::PartitionPtr>
PartitionedRocksDBStore::createPartitionsImpl(
    partition_id_t first_id,
    std::function<RecordTimestamp()> get_first_timestamp_func,
    size_t count,
    const DirtyState* pre_dirty_state,
    bool create_cfs_before_metadata) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  RecordTimestamp::duration partition_duration =
      getSettings()->partition_duration_;
  std::vector<RecordTimestamp> starting_timestamps;
  std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> cfs;

  // Creating column families can take a while (e.g. if another thread is
  // dropping lots of partitions) and some callers are time-sensitive. If
  // create_cfs_before_metadata is specified, we'll therefore do this before we
  // write the corresponding metadata, so the timestamp can be more accurate.
  // This is done when we're creating a new latest partition, since in all
  // other cases, we already know what the timestamp should be.
  auto create_cfs = [&]() {
    std::vector<std::string> column_family_names;
    column_family_names.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      partition_id_t id = first_id + i;
      column_family_names.push_back(PartitionedDBKeyFormat::getNameFromId(id));
    }
    cfs = createColumnFamilies(column_family_names, data_cf_options_);
    if (cfs.empty()) {
      return false;
    }
    return true;
  };

  auto write_metadata = [&]() {
    // Write STARTING_TIMESTAMP and DIRTY metadata.
    RecordTimestamp first_timestamp = get_first_timestamp_func();
    for (size_t i = 0; i < count; ++i) {
      partition_id_t id = first_id + i;
      RecordTimestamp starting_timestamp =
          first_timestamp + i * partition_duration;
      starting_timestamps.push_back(starting_timestamp);
      PartitionTimestampMetadata meta(
          PartitionMetadataType::STARTING_TIMESTAMP, starting_timestamp);
      LocalLogStore::WriteOptions write_options;
      int rv = writer_->writeMetadata(
          PartitionMetaKey(PartitionMetadataType::STARTING_TIMESTAMP, id),
          meta,
          write_options,
          metadata_cf_->get());
      if (rv != 0) {
        return false;
      }
      if (pre_dirty_state != nullptr &&
          !pre_dirty_state->dirtied_by_nodes.empty()) {
        rv = writer_->writeMetadata(
            PartitionMetaKey(PartitionMetadataType::DIRTY, id),
            pre_dirty_state->metadata(),
            write_options,
            metadata_cf_->get());
        if (rv != 0) {
          return false;
        }
      }
    }
    if (sync(Durability::ASYNC_WRITE) != 0) {
      return false;
    }
    return true;
  };

  if (create_cfs_before_metadata) {
    if (!create_cfs() || !write_metadata()) {
      return {};
    }
  } else {
    if (!write_metadata() || !create_cfs()) {
      return {};
    }
  }

  // Add the new partitions to the list.
  std::vector<PartitionPtr> new_partitions(count);
  ld_check_eq(starting_timestamps.size(), count);
  for (size_t i = 0; i < count; ++i) {
    auto cf_ptr = wrapAndRegisterCF(std::move(cfs[i]));
    new_partitions[i] = std::make_shared<Partition>(
        first_id + i, cf_ptr, starting_timestamps[i], pre_dirty_state);
  }
  addPartitions(new_partitions);

  STAT_ADD(stats_, partitions_created, count);

  return new_partitions;
}

PartitionedRocksDBStore::PartitionPtr
PartitionedRocksDBStore::createPartition() {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  std::lock_guard<std::mutex> lock(latest_partition_mutex_);

  // Store the previous latest as we are going to update it.
  PartitionPtr new_penultimate = latest_.get();
  ld_check(new_penultimate);
  partition_id_t new_partition_id = new_penultimate->id_ + 1;
  const auto margin = getSettings()->new_partition_timestamp_margin_;
  RecordTimestamp starting_timestamp;

  // Inherit append dirty state from previous partition so that a WAL sync
  // can be avoided when nodes transition their writes to the new
  // latest partition.
  ld_check(isHeldDirty(new_penultimate));
  folly::SharedMutex::ReadHolder rd_lock(new_penultimate->mutex_);
  DirtyState pre_dirty_state = new_penultimate->dirty_state_;
  rd_lock.unlock();
  for (auto dirty_kv = pre_dirty_state.dirtied_by_nodes.begin();
       dirty_kv != pre_dirty_state.dirtied_by_nodes.end();) {
    if (dirty_kv->first.second != DataClass::APPEND) {
      dirty_kv = pre_dirty_state.dirtied_by_nodes.erase(dirty_kv);
    } else {
      ++dirty_kv;
    }
  }

  // Create partition but create CF before deciding the starting time, since
  // that step can take longer time than the margin e.g. if another thread is
  // dropping lots of partitions at the same time.
  ld_info("Creating a new partition [%lu], shard %u pre-dirtied by %lu nodes",
          new_partition_id,
          shard_idx_,
          pre_dirty_state.dirtied_by_nodes.size());
  auto created = createPartitionsImpl(new_partition_id,
                                      [&] {
                                        starting_timestamp =
                                            currentTime() + margin;
                                        return starting_timestamp;
                                      },
                                      1,
                                      &pre_dirty_state);

  if (created.empty()) {
    return nullptr;
  }
  ld_check_eq(created.size(), 1);

  // Transition to new latest partition.
  latest_.update(created[0]);

  // Allow cleaning of the penultimate partition.
  releaseDirtyHold(new_penultimate);

  // The hold sentinel was copied from previous partition as part of
  // pre-dirtying.
  ld_check(isHeldDirty(latest_.get()));

  RecordTimestamp now = currentTime();
  if (now > starting_timestamp) {
    ld_warning("Partition [%lu] in shard %u took unexpectedly long to create. "
               "FindTime "
               "may return slightly inaccurate results for timestamps between "
               "%s and %s",
               new_partition_id,
               shard_idx_,
               starting_timestamp.toString().c_str(),
               now.toString().c_str());
  }

  return created[0];
}

std::vector<PartitionedRocksDBStore::PartitionPtr>
PartitionedRocksDBStore::prependPartitionsInternal(size_t count) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  PartitionPtr first_partition = partitions_.front();
  ld_check_gt(count, 0);
  ld_check_lt(count, first_partition->id_);

  // Create partitions.
  partition_id_t first_id = first_partition->id_ - count;
  auto partition_duration = getSettings()->partition_duration_;
  RecordTimestamp first_timestamp =
      first_partition->starting_timestamp - partition_duration * count;
  auto partitions = createPartitionsImpl(first_id, first_timestamp, count);
  if (partitions.empty()) {
    return {};
  }
  ld_check_eq(partitions.size(), count);

  oldest_partition_id_.store(partitions[0]->id_);

  // Grant the newly created partitions temporary immunity from drops.
  avoid_drops_until_ =
      currentSteadyTime() + getSettings()->prepended_partition_min_lifetime_;

  STAT_ADD(stats_, partitions_prepended, partitions.size());

  return partitions;
}

std::vector<PartitionedRocksDBStore::PartitionPtr>
PartitionedRocksDBStore::prependPartitions(size_t count) {
  std::lock_guard<std::mutex> lock(oldest_partition_mutex_);
  PartitionPtr first_partition = partitions_.front();

  count = std::min(count, first_partition->id_ - 1);
  if (count == 0) {
    err = E::EXISTS;
    return {};
  }

  auto partitions = prependPartitionsInternal(count);
  if (partitions.empty()) {
    err = E::LOCAL_LOG_STORE_WRITE;
  }
  return partitions;
}

bool PartitionedRocksDBStore::prependPartitionsIfNeeded(
    RecordTimestamp min_timestamp_to_cover,
    logid_t log,
    lsn_t lsn) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  auto all_partitions = partitions_.getVersion();
  PartitionPtr first_partition = all_partitions->front();
  if (min_timestamp_to_cover >= first_partition->starting_timestamp) {
    // min_timestamp_to_cover is already covered. This is the common case.
    return true;
  }

  const size_t max_partition_count = getSettings()->partition_count_soft_limit_;
  const std::chrono::milliseconds partition_duration_ms =
      getSettings()->partition_duration_;

  if (partition_duration_ms.count() == 0) {
    return true;
  }

  // Decides how many partitions to pre-create. Called twice: before and after
  // locking the mutex, to avoid locking if nothing needs to be done.
  auto get_count = [&](bool warn) -> size_t {
    if (min_timestamp_to_cover >= first_partition->starting_timestamp) {
      // min_timestamp_to_cover is already covered.
      return 0;
    }
    if (all_partitions->size() >= max_partition_count) {
      // Not allowed to create any more partitions.
      return 0;
    }

    std::chrono::milliseconds to_cover_ms =
        first_partition->starting_timestamp - min_timestamp_to_cover;
    ld_check_gt(to_cover_ms.count(), 0);
    size_t target_count =
        (size_t)((to_cover_ms.count() - 1) / partition_duration_ms.count()) + 1;
    size_t count = target_count;

    if (all_partitions->size() + count > max_partition_count) {
      // Don't create too many partitions.
      ld_check_lt(all_partitions->size(), max_partition_count);
      count = max_partition_count - all_partitions->size();
    }
    if (count >= first_partition->id_) {
      // Only create partitions with positive IDs.
      ld_check_gt(first_partition->id_, 0);
      count = first_partition->id_ - 1;
    }

    if (warn && count != target_count) {
      ld_check_lt(count, target_count);
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          2,
          "Record timestamp too old to cover with partitions. Record: log %lu, "
          "lsn %s, ts %s. Oldest partition: %lu, ts: %s. Covering the record "
          "would require creating %lu partitions. Creating %lu instead.",
          log.val_,
          lsn_to_string(lsn).c_str(),
          min_timestamp_to_cover.toString().c_str(),
          first_partition->id_,
          first_partition->starting_timestamp.toString().c_str(),
          target_count,
          count);
    }

    return count;
  };

  // Do a quick check before locking the mutex.
  if (get_count(false) == 0) {
    return true;
  }

  std::lock_guard<std::mutex> lock(oldest_partition_mutex_);
  all_partitions = partitions_.getVersion();
  first_partition = all_partitions->front();

  size_t count = get_count(true);
  if (count == 0) {
    return true;
  }

  ld_info("Creating %lu partitions to cover record: log %lu, lsn %s, ts %s. "
          "First existing partition: %lu %s",
          count,
          log.val_,
          lsn_to_string(lsn).c_str(),
          min_timestamp_to_cover.toString().c_str(),
          first_partition->id_,
          first_partition->starting_timestamp.toString().c_str());

  auto created = prependPartitionsInternal(count);
  ld_check(created.empty() || created.size() == count);
  return !created.empty();
}

int PartitionedRocksDBStore::seekToLastInDirectory(
    RocksDBIterator* it,
    logid_t log_id,
    lsn_t lsn,
    logid_t* next_log_out) const {
  ld_check(it != nullptr);
  ld_check(it->totalOrderSeek());

  // make sure that `it' first points to a record with a sequence number
  // that's strictly greater than lsn
  PartitionDirectoryKey data_key(
      lsn != LSN_MAX ? log_id : logid_t(log_id.val_ + 1),
      lsn != LSN_MAX ? lsn + 1 : 0,
      0);

  auto it_error = [&] {
    rocksdb::Status status = it->status();
    if (status.ok()) {
      return false;
    }
    err = status.IsIncomplete() ? E::WOULDBLOCK : E::LOCAL_LOG_STORE_READ;
    return true;
  };

  it->Seek(rocksdb::Slice(
      reinterpret_cast<const char*>(&data_key), sizeof(data_key)));
  if (it_error()) {
    return -1;
  }

  // True if after seeking to data_key we got a record from log log_id.
  bool has_next_record_for_log = false;

  if (it->Valid()) {
    rocksdb::Slice key = it->key();
    if (!PartitionDirectoryKey::valid(key.data(), key.size())) {
      // moved past the section containing keys of needed type
      if (next_log_out) {
        *next_log_out = LOGID_INVALID;
      }
    } else {
      logid_t it_log = PartitionDirectoryKey::getLogID(key.data());
      lsn_t it_lsn = PartitionDirectoryKey::getLSN(key.data());

      ld_check(it_log > log_id || (it_log == log_id && it_lsn > lsn));
      if (next_log_out) {
        *next_log_out = it_log;
      }

      has_next_record_for_log = it_log == log_id;
    }
    it->Prev();
  } else {
    it->SeekToLast();
    if (next_log_out) {
      *next_log_out = LOGID_INVALID;
    }
  }

  if (it_error()) {
    return -1;
  }

  // check that we're at the right position after moving one step back
  if (!it->Valid() ||
      !PartitionDirectoryKey::valid(it->key().data(), it->key().size()) ||
      PartitionDirectoryKey::getLogID(it->key().data()) != log_id) {
    // there's nothing of interest before data_key

    if (!has_next_record_for_log) {
      err = E::NOTFOUND;
      return -1;
    }

    it->Seek(rocksdb::Slice(
        reinterpret_cast<const char*>(&data_key), sizeof(data_key)));
    if (it_error()) {
      return -1;
    }
    // has_next_record_for_log=true means that we've seeked iterator to the
    // exact same key before, and it was Valid. Should be Valid this time too,
    // since it's a snapshot iterator.
    ld_check(it->Valid());
  } else {
    ld_check(PartitionDirectoryKey::getLSN(it->key().data()) <= lsn);
  }

  return 0;
}

bool PartitionedRocksDBStore::finishInterruptedDrops() {
  ld_check(!getSettings()->read_only);

  // Find gaps in IDs of non-dropped partitions.
  partition_id_t last_gap = PARTITION_INVALID;
  auto partitions = partitions_.getVersion();
  for (partition_id_t id = partitions->firstID(); id < partitions->nextID();
       ++id) {
    if (partitions->get(id) == nullptr) {
      last_gap = id;
    }
  }

  if (last_gap != PARTITION_INVALID) {
    ld_warning("Dropping partitions %lu - %lu assuming that they're leftovers "
               "from an interrupted create/drop. This should be very rare.",
               partitions->firstID(),
               last_gap + 1);
    int rv = dropPartitions(last_gap + 1, [&]() { return last_gap + 1; });
    ld_check_ne(rv, 0);
    return err == E::GAP;
  } else {
    cleanUpDirectory();
    cleanUpPartitionMetadataAfterDrop(oldest_partition_id_);
  }

  return true;
}

// Read all unpartitioned records and convert from old to new DataKey format.
// Unpartitioned data should be small enough that we can afford doing it on
// every startup. Partitioned data has short enough retention that we don't
// need to proactively convert it like this.
bool PartitionedRocksDBStore::convertDataKeyFormat() {
  ld_check(unpartitioned_cf_);
  auto start_time = std::chrono::steady_clock::now();
  size_t records_read = 0;
  size_t records_written = 0;
  size_t bytes_read = 0;
  size_t bytes_written = 0;

  rocksdb::WriteBatch batch;
  auto flush_batch = [this, &batch]() {
    if (batch.Count() == 0) {
      return true;
    }
    rocksdb::WriteOptions options;
    auto status = writeBatch(options, &batch);
    if (!status.ok()) {
      ld_error(
          "Failed to write converted records: %s", status.ToString().c_str());
      return false;
    }
    batch.Clear();
    return true;
  };

  auto options = getDefaultReadOptions();
  auto it = newIterator(options, unpartitioned_cf_->get());
  RocksDBKeyFormat::DataKey min_key(logid_t(0), lsn_t(0));
  rocksdb::Slice min_key_slice = min_key.sliceForWriting();
  for (it.Seek(min_key_slice); it.status().ok() && it.Valid() &&
       reinterpret_cast<const char*>(it.key().data())[0] ==
           RocksDBKeyFormat::DataKey::HEADER;
       it.Next()) {
    rocksdb::Slice key = it.key();
    rocksdb::Slice value = it.value();
    ++records_read;
    bytes_read += key.size() + value.size();
    if (!RocksDBKeyFormat::DataKey::valid(key.data(), key.size())) {
      ld_error(
          "Invalid DataKey: %s", hexdump_buf(key.data(), key.size()).c_str());
      continue;
    }
    if (RocksDBKeyFormat::DataKey::isInNewFormat(key.data(), key.size())) {
      // Common case: already in new format.
      continue;
    }

    // Convert: delete old key and write (merge) new one.
    rocksdb::Status status;
    status = batch.Delete(unpartitioned_cf_->get(), key);
    if (!status.ok()) {
      ld_error("Failed to add a Delete to a WriteBatch: %s",
               status.ToString().c_str());
      return false;
    }
    logid_t log = RocksDBKeyFormat::DataKey::getLogID(key.data());
    lsn_t lsn = RocksDBKeyFormat::DataKey::getLSN(key.data());
    RocksDBKeyFormat::DataKey new_key(log, lsn);
    rocksdb::Slice new_key_slice = new_key.sliceForWriting();
    ld_check(RocksDBKeyFormat::DataKey::isInNewFormat(
        new_key_slice.data(), new_key_slice.size()));
    // Prepend RocksDBWriterMergeOperator::DATA_MERGE_HEADER to the value
    // to make it a merge operand.
    rocksdb::Slice new_value_parts[2] = {rocksdb::Slice("d", 1), value};
    // If both old-format and new-format keys exist, we merge the old-format
    // value into the new-format one, as if the old-format value was written
    // later. The opposite would make more sense, but it doesn't matter much
    // because normally all unpartitioned keys will be in the same format.
    status = batch.Merge(unpartitioned_cf_->get(),
                         rocksdb::SliceParts(&new_key_slice, 1),
                         rocksdb::SliceParts(&new_value_parts[0], 2));
    if (!status.ok()) {
      ld_error("Failed to add a Merge to a WriteBatch: %s",
               status.ToString().c_str());
      return false;
    }

    ++records_written;
    bytes_written += key.size() + value.size();

    if (batch.GetDataSize() > 1e8) {
      // The write batch got big, flush it. It's ok to write while iterating
      // because the iterator uses a snapshot.
      if (!flush_batch()) {
        return false;
      }
    }
  }
  if (!it.status().ok()) {
    ld_error("Got rocksdb error: %s", it.status().ToString().c_str());
    return false;
  }
  if (!flush_batch()) {
    return false;
  }

  if (records_written != 0) {
    auto status = db_->SyncWAL();
    if (!status.ok()) {
      ld_error("Failed to sync WAL: %s", status.ToString().c_str());
      return false;
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  ld_info("DataKey format conversion took %.3fs. Read %lu records, %lu bytes. "
          "Wrote %lu records, %lu bytes.",
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                    start_time)
              .count(),
          records_read,
          bytes_read,
          records_written,
          bytes_written);
  return true;
}

bool PartitionedRocksDBStore::readDirectories() {
  const char key = PartitionDirectoryKey::HEADER;
  DirectoryEntry directory_entry;
  logid_t current_log_id = LOGID_INVALID;
  LogState* log_state = nullptr;
  // Track the below to fill in LogState of each log
  partition_id_t latest_partition = PARTITION_INVALID;
  lsn_t first_lsn_in_latest = LSN_INVALID;
  lsn_t max_lsn_in_latest = LSN_INVALID;

  ld_check(logs_.empty());

  RocksDBIterator it = createMetadataIterator();
  it.Seek(rocksdb::Slice(&key, sizeof(key)));

  auto finalizePreviousLogState = [&]() {
    if (log_state) {
      // Update LogState of previous log
      ld_check_eq(log_state->latest_partition.latest_partition.load(),
                  PARTITION_INVALID);
      ld_check_eq(
          log_state->latest_partition.first_lsn_in_latest.load(), LSN_INVALID);
      ld_check_eq(
          log_state->latest_partition.max_lsn_in_latest.load(), LSN_INVALID);
      log_state->latest_partition.store(
          latest_partition, first_lsn_in_latest, max_lsn_in_latest);
    }
  };

  while (true) {
    if (!it.status().ok()) {
      ld_error("Failed to read LogsDB directory! Iterator failed with status: "
               "%s",
               it.status().ToString().c_str());
      ld_check(!it.status().IsIncomplete());
      return false;
    }

    if (!it.Valid() || it.key().size() < 1 ||
        reinterpret_cast<const char*>(it.key().data())[0] != key) {
      // End of all directories.
      finalizePreviousLogState();
      break;
    }

    if (!PartitionDirectoryKey::valid(it.key().data(), it.key().size())) {
      ld_error("Found a corrupt entry while reading LogsDB directories! "
               "Key: %s, Previous read log: %lu",
               it.key().ToString().c_str(),
               current_log_id.val());
      return false;
    }

    logid_t log_id = PartitionDirectoryKey::getLogID(it.key().data());
    if (log_id == LOGID_INVALID) {
      ld_error("Found corrupted directory entry: log_id is LOGID_INVALID");
      return false;
    }

    if (log_id != current_log_id) {
      finalizePreviousLogState();

      // Start of a new log
      current_log_id = log_id;
      ld_check_ne(current_log_id, LOGID_INVALID);

      // Initialize LogState for this log
      auto res = logs_.emplace(log_id.val(), std::make_unique<LogState>());
      ld_check(res.second);
      log_state = res.first->second.get();
    }
    ld_check_ne(current_log_id, LOGID_INVALID);
    ld_check_ne(log_state, (LogState*)nullptr);

    if (directory_entry.fromIterator(&it, current_log_id) != 0) {
      return false;
    }
    log_state->directory.emplace(directory_entry.first_lsn, directory_entry);

    // Update latest_partition
    latest_partition = directory_entry.id;
    first_lsn_in_latest = directory_entry.first_lsn;
    max_lsn_in_latest = directory_entry.max_lsn;

    it.Next();
  }
  return true;
}

bool PartitionedRocksDBStore::readPartitionTimestamps(PartitionPtr partition) {
  partition_id_t id = partition->id_;

  for (auto it : {std::make_pair(PartitionMetadataType::MIN_TIMESTAMP,
                                 &partition->min_timestamp),
                  std::make_pair(PartitionMetadataType::MAX_TIMESTAMP,
                                 &partition->max_timestamp),
                  std::make_pair(PartitionMetadataType::LAST_COMPACTION,
                                 &partition->last_compaction_time)}) {
    PartitionTimestampMetadata meta(it.first);
    int rv = RocksDBWriter::readMetadata(
        this, PartitionMetaKey(it.first, id), &meta, metadata_cf_->get());
    if (rv != 0 && err != E::NOTFOUND) {
      return false;
    }
    if (rv == 0) {
      *it.second = meta.getTimestamp();
    }
  }

  partition->min_durable_timestamp.store(partition->min_timestamp);
  partition->max_durable_timestamp.store(partition->max_timestamp);

  {
    PartitionCompactedRetentionMetadata meta;
    int rv = RocksDBWriter::readMetadata(
        this,
        PartitionMetaKey(PartitionMetadataType::COMPACTED_RETENTION, id),
        &meta,
        metadata_cf_->get());
    if (rv != 0 && err != E::NOTFOUND) {
      return false;
    }
    if (rv == 0) {
      partition->compacted_retention = meta.getBacklogDuration();
    }
  }

  {
    PartitionTimestampMetadata meta(PartitionMetadataType::STARTING_TIMESTAMP);
    int rv = RocksDBWriter::readMetadata(
        this,
        PartitionMetaKey(PartitionMetadataType::STARTING_TIMESTAMP, id),
        &meta,
        metadata_cf_->get());
    if (rv != 0) {
      return false;
    }
    partition->starting_timestamp = meta.getTimestamp();
  }

  return true;
}

bool PartitionedRocksDBStore::readPartitionDirtyState(PartitionPtr partition) {
  ld_check(partition->dirty_state_.dirtied_by_nodes.empty());

  PartitionDirtyMetadata meta;
  int rv = RocksDBWriter::readMetadata(
      this,
      PartitionMetaKey(PartitionMetadataType::DIRTY, partition->id_),
      &meta,
      metadata_cf_->get());
  if (rv != 0 && err != E::NOTFOUND) {
    return false;
  }

  if (meta.isUnderReplicated()) {
    setUnderReplicated(partition);
  }

  size_t dci = 0;
  for (const auto& dnv : meta.getAllDirtiedBy()) {
    DataClass dc = static_cast<DataClass>(dci);
    for (auto node_idx : dnv) {
      auto result = partition->dirty_state_.dirtied_by_nodes.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(node_idx, dc),
          std::forward_as_tuple());
      result.first->second.markDirtyUntil(FlushToken_MIN);
    }
    dci++;
  };
  return true;
}

void PartitionedRocksDBStore::addPartitions(
    std::vector<PartitionPtr> partitions) {
  ld_check(!partitions.empty());
  STAT_ADD(stats_, partitions, partitions.size());
  partition_id_t id0 = partitions[0]->id_;
  if (partitions_.empty()) {
    partitions_.setBaseID(id0);
  }
  if (id0 == partitions_.nextID()) {
    partitions_.append(std::move(partitions));
  } else if (id0 + partitions.size() == partitions_.firstID()) {
    partitions_.prepend(std::move(partitions));
  } else {
    ld_check(false);
  }
}

void PartitionedRocksDBStore::getLogsDBDirectories(
    std::vector<partition_id_t> partitions,
    const std::vector<logid_t>& logs,
    std::vector<std::pair<logid_t, DirectoryEntry>>& out) const {
  // Verify partitions and logs are sorted and don't contain duplicates
  ld_assert(std::is_sorted(partitions.begin(), partitions.end()));
  ld_assert(std::unique(partitions.begin(), partitions.end()) ==
            partitions.end());
  // Prevent dropping partitions for simplicity
  std::lock_guard<std::mutex> drop_lock(oldest_partition_mutex_);

  auto add_log_dir = [&](const logid_t log_id,
                         const LogStateMap::const_iterator& logs_it) {
    LogState* log_state = logs_it->second.get();
    std::unique_lock<std::mutex> log_lock(log_state->mutex);

    if (partitions.empty()) {
      for (auto& mem_it : log_state->directory) {
        out.emplace_back(log_id, mem_it.second);
      }
    } else {
      auto partitions_it = partitions.cbegin();
      ld_assert(partitions_it != partitions.cend());

      for (auto& mem_it : log_state->directory) {
        // If no directory entry exists for a partition, move on to next one
        for (; partitions_it != partitions.cend() &&
             mem_it.second.id > *(partitions_it);
             ++partitions_it) {
        }

        if (partitions_it == partitions.cend()) {
          break;
        }
        if (*partitions_it == mem_it.second.id) {
          out.emplace_back(log_id, mem_it.second);
          ++partitions_it;
        }
      }
    }
  };

  if (logs.empty()) {
    for (auto it = logs_.cbegin(); it != logs_.cend(); ++it) {
      ld_check_ne(it->first, LOGID_INVALID.val_);
      add_log_dir(logid_t(it->first), it);
    }
  } else {
    for (logid_t log_id : logs) {
      auto logs_it = logs_.find(log_id.val_);
      if (logs_it == logs_.cend()) {
        continue;
      }
      add_log_dir(log_id, logs_it);
    }
  }
}

PartitionedRocksDBStore::GetWritePartitionResult
PartitionedRocksDBStore::getWritePartition(
    logid_t log_id,
    lsn_t lsn,
    Durability durability,
    folly::Optional<RecordTimestamp> timestamp,
    partition_id_t* min_allowed_partition,
    rocksdb::WriteBatch& rocksdb_batch,
    rocksdb::WriteBatch& durable_batch,
    PartitionPtr* out_partition,
    size_t payload_size_bytes,
    LocalLogStoreRecordFormat::flags_t flags) {
  ld_check(!getSettings()->read_only);
  partition_id_t latest_partition_id = latest_.get()->id_;
  partition_id_t target_partition = timestamp.hasValue()
      ? getPreferredPartition(timestamp.value())
      : PARTITION_MAX;
  target_partition = std::min(target_partition, latest_partition_id);

  auto logs_it = logs_.find(log_id.val_);
  // LogLocks should have created the LogStates
  ld_check(logs_it != logs_.cend());
  LogState* log_state = logs_it->second.get();
  auto& log_directory = log_state->directory;

  // log_directory, latest_partition, first_lsn_in_latest and max_lsn_in_latest
  // must all agree on whether or not the log is empty.
  const partition_id_t max_used_partition = log_directory.empty()
      ? PARTITION_INVALID
      : (--log_directory.cend())->second.id;
  ld_check_eq(
      max_used_partition, log_state->latest_partition.latest_partition.load());
  ld_check_eq(
      log_directory.empty(),
      (log_state->latest_partition.first_lsn_in_latest.load() == LSN_INVALID));
  ld_check_eq(
      log_directory.empty(),
      (log_state->latest_partition.max_lsn_in_latest.load() == LSN_INVALID));

  // We need to find the range of partitions which we can use as target without
  // breaking directory invariants. For this we need to find two consecutive
  // directory entries:
  //  - first entry with first_lsn > lsn, let's call it next_partition, and
  //  - last entry with first_lsn <= lsn, let's call it current_partition.
  // One or both may be missing (DirectoryEntry::id = PARTITION_INVALID).

  DirectoryEntry* next_partition = nullptr;
  DirectoryEntry* current_partition = nullptr;

  // Iterators to lower and upper bound of allowed target partition, if found
  std::map<partition_id_t, DirectoryEntry>::iterator current_it, next_it;

  // Find first partition with first_lsn > lsn (next); then previous, if any,
  // is last partition with first_lsn <= lsn (current)
  next_it = log_directory.upper_bound(lsn);

  // First partition with first_lsn > lsn
  if (next_it != log_directory.end()) {
    ld_check_lt(lsn, next_it->first);
    ld_check_eq(next_it->first, next_it->second.first_lsn);
    next_partition = &next_it->second;
    ld_check_lt(lsn, next_partition->first_lsn);
  }

  // Last partition with first_lsn <= lsn
  if (next_it != log_directory.begin()) {
    current_it = next_it;
    --current_it;
    ld_check_le(current_it->first, lsn);
    current_partition = &current_it->second;
  }

  // Find the range of partitions that we can pick as target without violating
  // directory invariants.

  // The range of target partitions that we can choose without violating
  // the directory invariants. min_target can be PARTITION_INVALID.
  partition_id_t min_target =
      current_partition ? current_partition->id : PARTITION_INVALID;
  partition_id_t max_target;
  if (current_partition && lsn <= current_partition->max_lsn) {
    max_target = current_partition->id;
  } else if (next_partition) {
    max_target = next_partition->id;
  } else {
    max_target = PARTITION_MAX;
  }

  // Clamp target_partition to the allowed range. After that it's final.
  ld_check_le(min_target, max_target);
  if (target_partition < min_target || target_partition > max_target) {
    STAT_INCR(stats_, logsdb_target_partition_clamped);
    target_partition = std::max(min_target, target_partition);
    target_partition = std::min(max_target, target_partition);
  }

  if (target_partition < *min_allowed_partition) {
    // The target partition is outside the "safe" range - we can't be sure
    // that it won't be dropped while we're writing directory entry for it.
    // We can't even be sure that it hasn't been dropped already.
    // Tell the caller to prevent dropping this partition and try again.
    *min_allowed_partition = target_partition;
    return GetWritePartitionResult::RETRY;
  }

  // Update directory. In all 3 cases we only touch one directory entry,
  // and leave its final value in current_partition.

  if (current_partition && target_partition == current_partition->id) {
    // The most frequent case. No directory keys need updating, but max_lsn
    // in a directory value probably needs to be increased.

    // Update data size
    current_partition->approximate_size_bytes += payload_size_bytes;

    // Unset PSEUDORECORDS_ONLY flag if we're writing the first real record to
    // the given partition for this log.
    bool unset_pseudorecords_only_flag = current_partition->flags &
            PartitionDirectoryValue::PSEUDORECORDS_ONLY &&
        (flags & LocalLogStoreRecordFormat::PSEUDORECORD_MASK) == 0;
    if (unset_pseudorecords_only_flag) {
      current_partition->flags &= ~PartitionDirectoryValue::PSEUDORECORDS_ONLY;
    }

    if (lsn > current_partition->max_lsn) {
      if (!timestamp.hasValue()) {
        // This is a delete/amend operation, and record with this LSN is known
        // to not exist. Drop this operation.
        return GetWritePartitionResult::SKIP;
      }
      current_partition->max_lsn = lsn;
      current_partition->doPut(
          log_id, durability, metadata_cf_->get(), rocksdb_batch);
      if (current_partition->id == max_used_partition) {
        // New max lsn of latest used partition, update LogState
        log_state->latest_partition.updateMaxLSN(lsn);
      }
    } else if ((durability > Durability::MEMORY &&
                (current_partition->flags &
                 PartitionDirectoryValue::NOT_DURABLE)) ||
               payload_size_bytes != 0 || unset_pseudorecords_only_flag) {
      // Upgrade to a durable entry.
      current_partition->doPut(
          log_id, durability, metadata_cf_->get(), rocksdb_batch);
    }
  } else if (next_partition && target_partition == next_partition->id) {
    // Need to decrease first_lsn of next_partition.
    if (!timestamp.hasValue()) {
      return GetWritePartitionResult::SKIP;
    }

    // Replace next partition with a copy with decreased first_lsn
    DirectoryEntry new_next_partition = *next_partition;
    next_partition->doDelete(log_id, metadata_cf_->get(), durable_batch);
    ld_check_lt(lsn, new_next_partition.first_lsn);
    new_next_partition.first_lsn = lsn;
    // Update data size
    new_next_partition.approximate_size_bytes += payload_size_bytes;
    new_next_partition.doPut(
        log_id, Durability::ASYNC_WRITE, metadata_cf_->get(), durable_batch);
    STAT_INCR(stats_, logsdb_writes_dir_key_decrease);

    // Apply same as above in in-memory directory metadata
    auto hint_it = log_directory.erase(next_it);
    current_partition =
        &log_directory.emplace_hint(hint_it, lsn, new_next_partition)->second;
    if (current_partition->id == max_used_partition) {
      // New first lsn of latest partition, update LogState
      log_state->latest_partition.store(
          max_used_partition, lsn, new_next_partition.max_lsn);
    }
  } else {
    // Need to add a new directory entry. Includes the frequent case of adding
    // an entry for the latest partition soon after it's created.
    if (!timestamp.hasValue()) {
      return GetWritePartitionResult::SKIP;
    }

    PartitionDirectoryValue::flags_t directory_entry_flags =
        flags & LocalLogStoreRecordFormat::PSEUDORECORD_MASK
        ? PartitionDirectoryValue::PSEUDORECORDS_ONLY
        : 0;

    DirectoryEntry new_partition{
        target_partition,      // partition id
        lsn,                   // first lsn
        lsn,                   // max lsn
        directory_entry_flags, // flags
        payload_size_bytes     // approximate size in bytes
    };
    new_partition.doPut(log_id, durability, metadata_cf_->get(), rocksdb_batch);
    STAT_INCR(stats_, logsdb_writes_dir_key_add);

    // Add to in-memory directory metadata
    current_partition =
        &log_directory.emplace_hint(next_it, lsn, new_partition)->second;
    if (target_partition > max_used_partition) {
      // New latest partition, update LogState
      log_state->latest_partition.store(target_partition, lsn, lsn);
    }
  }

  ld_check_eq(current_partition->id, target_partition);

  // Get the partition by ID.

  bool ok = getPartition(target_partition, out_partition);
  // target_partition >= *min_allowed_partition, so it can't have been dropped.
  ld_check(ok);

  if (target_partition < latest_partition_id - 1) {
    STAT_INCR(stats_, logsdb_writes_to_old_partitions);
  }

  return GetWritePartitionResult::OK;
}

partition_id_t
PartitionedRocksDBStore::getPreferredPartition(RecordTimestamp timestamp,
                                               bool warn_if_old) {
  // Get a frozen version of partition list.
  partition_id_t latest_id = latest_.get()->id_;
  auto partitions = partitions_.getVersion();
  ld_check_gt(partitions->nextID(), latest_id);

  return getPreferredPartition(
      timestamp, warn_if_old, partitions, latest_id, partitions->firstID());
}

partition_id_t
PartitionedRocksDBStore::getPreferredPartition(RecordTimestamp timestamp,
                                               bool warn_if_old,
                                               PartitionList partitions,
                                               partition_id_t latest_id,
                                               partition_id_t oldest_id) {
  // Do a binary search to find the last partition that has
  // starting_timestamp <= timestamp.

  // A partition with starting_timestamp <= timestamp.
  partition_id_t less_equal = oldest_id - 1;
  // A partition with starting_timestamp > timestamp.
  partition_id_t greater = latest_id + 1;

  while (greater > less_equal + 1) {
    ld_check(greater == latest_id + 1 ||
             partitions->get(greater)->starting_timestamp > timestamp);
    ld_check(less_equal == oldest_id - 1 ||
             partitions->get(less_equal)->starting_timestamp <= timestamp);
    partition_id_t mid = less_equal + ((greater - less_equal) / 2);
    ld_check_gt(mid, less_equal);
    ld_check_lt(mid, greater);
    PartitionPtr p = partitions->get(mid);
    ld_check(p);
    if (p->starting_timestamp > timestamp) {
      greater = mid;
    } else {
      less_equal = mid;
    }
  }

  if (less_equal < oldest_id) {
    if (warn_if_old) {
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Writing a record with timestamp %s, which is older than the oldest "
          "partition %s",
          format_time(timestamp).c_str(),
          format_time(partitions->get(greater)->starting_timestamp).c_str());
    }
    return oldest_id;
  }

  return less_equal;
}

bool PartitionedRocksDBStore::getPartition(partition_id_t id,
                                           PartitionPtr* out_partition) const {
  *out_partition = partitions_.get(id);
  return *out_partition != nullptr;
}

PartitionedRocksDBStore::PartitionPtr
PartitionedRocksDBStore::getRelativePartition(ssize_t offset) const {
  auto partitions = getPartitionList();
  if (partitions->empty()) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  partition_id_t target_id;
  if (offset <= 0) {
    target_id = (partitions->nextID() - 1) + offset;
  } else {
    target_id = (partitions->firstID() - 1) + offset;
  }

  PartitionPtr partition = partitions->get(target_id);
  if (!partition) {
    if (target_id >= partitions->firstID() &&
        target_id < partitions->nextID()) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "Partition %lu hasn't been "
                         "found in the map and is not dropped",
                         target_id);
      err = E::INTERNAL;
      ld_check(false);
    } else {
      err = E::INVALID_PARAM;
    }
  }
  return partition;
}

std::unique_ptr<PartitionedRocksDBStore::Partition::TimestampUpdateTask>
PartitionedRocksDBStore::updatePartitionTimestampsIfNeeded(
    PartitionPtr partition,
    RecordTimestamp new_timestamp,
    rocksdb::WriteBatch& rocksdb_batch) {
  // Skip partition timestamp updates for records lacking a reasonable
  // timestamp - for now the bridge records that use RecordTimestamp::zero().
  // These records are promoted to the SYNC_WRITE durability class earlier
  // in the write path, so correctness is maintained without attempting to
  // use their timestamp for dirty range tracking.
  if (new_timestamp == RecordTimestamp::zero()) {
    return nullptr;
  }

  ld_check(!getSettings()->read_only);
  // If timestamps in partition are uninitialized, extreme values of
  // new_timestamp would update only one of them. We need to write both.
  if (new_timestamp == RecordTimestamp::max()) {
    new_timestamp--;
  }
  if (new_timestamp == RecordTimestamp::min()) {
    new_timestamp++;
  }

  // Over-extend the timestamp range by partition_timestamp_granularity_:
  //  if (new_timestamp > max_timestamp)
  //    max_timestamp = new_timestamp + partition_timestamp_granularity_;
  auto old_max = partition->max_timestamp.storeConditional(
      new_timestamp + // avoiding overflow
          std::min(RecordTimestamp::max() - new_timestamp,
                   getSettings()->partition_timestamp_granularity_),
      new_timestamp,
      std::less<>());

  // Same for min.
  auto old_min = partition->min_timestamp.storeConditional(
      new_timestamp -
          std::min(new_timestamp.time_since_epoch(), // avoiding overflow
                   getSettings()->partition_timestamp_granularity_),
      new_timestamp,
      std::greater<>());

  // NOTE: There's a race condition here: if two threads update the timestamp
  // almost simultaneously, the one with the smaller timestamp might be the
  // last to write metadata, overwriting metadata with greater timestamp.
  // To make it more unlikely, we do max_timestamp.load() again here.
  // Note that LogState::mutex doesn't help with this since writes for
  // different logs may modify the same piece of per-partition metadata.
  // TODO: This can be fixed with merge operator.
  auto new_max = old_max;
  auto new_min = old_min;
  if (new_timestamp > old_max) {
    new_max = partition->max_timestamp;
    PartitionMetaKey key(PartitionMetadataType::MAX_TIMESTAMP, partition->id_);
    PartitionTimestampMetadata meta(
        PartitionMetadataType::MAX_TIMESTAMP, new_max);
    Slice value = meta.serialize();
    rocksdb_batch.Put(
        metadata_cf_->get(),
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
        rocksdb::Slice(reinterpret_cast<const char*>(value.data), value.size));
  }
  if (new_timestamp < old_min) {
    new_min = partition->min_timestamp;
    PartitionMetaKey key(PartitionMetadataType::MIN_TIMESTAMP, partition->id_);
    PartitionTimestampMetadata meta(
        PartitionMetadataType::MIN_TIMESTAMP, new_min);
    Slice value = meta.serialize();
    rocksdb_batch.Put(
        metadata_cf_->get(),
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
        rocksdb::Slice(reinterpret_cast<const char*>(value.data), value.size));
  }
  if (new_max != old_max || new_min != old_min) {
    return std::make_unique<Partition::TimestampUpdateTask>(
        partition, new_min, new_max);
  }
  return nullptr;
}

int PartitionedRocksDBStore::findPartition(RocksDBIterator* it,
                                           logid_t log_id,
                                           lsn_t lsn,
                                           PartitionPtr* out_partition) const {
  ld_check(it);

  // NOTE: The case of multiple directory entries for the same (log_id, lsn)
  //       pair is handled correctly.
  int rv = seekToLastInDirectory(it, log_id, lsn);
  if (rv != 0) {
    ld_check(err == E::NOTFOUND || err == E::WOULDBLOCK ||
             err == E::LOCAL_LOG_STORE_READ);
    return -1;
  }

  ld_check(it->Valid());
  ld_check(PartitionDirectoryKey::valid(it->key().data(), it->key().size()));
  ld_check_eq(PartitionDirectoryKey::getLogID(it->key().data()), log_id);

  // When dropping partition, directory might briefly contain references to
  // partitions that are already removed. In this case move iterator forward
  // until we find an existing partition.
  while (true) {
    partition_id_t partition_id =
        PartitionDirectoryKey::getPartition(it->key().data());
    if (getPartition(partition_id, out_partition)) {
      return 0;
    }

    ld_spew("Stale directory entry for partition %lu for record %s of "
            "log %lu",
            partition_id,
            lsn_to_string(lsn).c_str(),
            log_id.val_);

    it->Next();

    if (!it->status().ok()) {
      err =
          it->status().IsIncomplete() ? E::WOULDBLOCK : E::LOCAL_LOG_STORE_READ;
      return -1;
    }
    if (!it->Valid()) {
      err = E::NOTFOUND;
      return -1;
    }

    rocksdb::Slice key = it->key();
    if (!PartitionDirectoryKey::valid(key.data(), key.size()) ||
        PartitionDirectoryKey::getLogID(key.data()) != log_id) {
      err = E::NOTFOUND;
      return -1;
    }
  }
}

int PartitionedRocksDBStore::dataSize(logid_t log_id,
                                      std::chrono::milliseconds lo,
                                      std::chrono::milliseconds hi,
                                      size_t* out) {
  return dataSize(log_id, RecordTimestamp(lo), RecordTimestamp(hi), out);
}

int PartitionedRocksDBStore::dataSize(logid_t log_id,
                                      RecordTimestamp lo_timestamp,
                                      RecordTimestamp hi_timestamp,
                                      size_t* out) {
  ld_check_ne(out, nullptr);
  *out = 0;

  RecordTimestamp now(currentTime());
  if (hi_timestamp > now) {
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    1,
                    "High timestamp is in the future; clamping to current "
                    "time");
    hi_timestamp = now;
  }
  if (lo_timestamp >= hi_timestamp) {
    return 0;
  }

  // Grab logstate mutex
  auto logs_it = logs_.find(log_id.val_);
  if (logs_it == logs_.cend()) {
    return 0;
  }

  LogState* log_state = logs_it->second.get();
  std::unique_lock<std::mutex> log_lock(log_state->mutex);
  auto& log_directory = log_state->directory;
  if (log_directory.empty()) {
    return 0;
  }
  auto partitions = getPartitionList();

  // Find the directory entry for the first partition which spans any
  // timestamps >= lo_timestamp -- that's our starting point.
  // Use lower_bound to binary search on LSN.
  // Let's define partition as "good" if either it has

  // starting_timestamp <= lo_timestamp or it's the first partition.
  // For calculating data size, the first partition we're interested in is the
  // last good partition.
  // Find it using binary search by LSN.

  // Invariant: `min_it` points to a good partition,
  // `max` is greater than first_lsn of every good partition.
  auto min_it = log_directory.cbegin();
  lsn_t max = log_directory.crbegin()->first;
  max = 1 + std::min(max, LSN_MAX - 1);

  while (max > min_it->second.max_lsn + 1) {
    ld_check(min_it->second.max_lsn >= min_it->first);
    lsn_t mid = min_it->second.max_lsn + (max - min_it->second.max_lsn) / 2;
    ld_check(mid < max && mid > min_it->first);
    auto mid_it = log_directory.lower_bound(mid);
    // This shouldn't be possible, since mid < max and
    // max <= log_directory.crbegin()->first + 1.
    ld_check(mid_it != log_directory.cend());

    PartitionPtr partition = partitions->get(mid_it->second.id);
    ld_check(partition != nullptr);

    if (partition->starting_timestamp <= lo_timestamp) {
      // Good partition, advance min_it to it.
      min_it = mid_it;
    } else {
      // Not good partition, decrease max.
      max = mid;
    }
  }

  // Keep going through partitions and add their sizes (to whatever degree we
  // estimate that they are covered) until we go above hi_timestamp.
  for (auto dir_it = min_it; dir_it != log_directory.cend(); ++dir_it) {
    partition_id_t partition_id = dir_it->second.id;
    PartitionPtr partition = partitions->get(partition_id);
    ld_check(partition != nullptr);

    if (partition->starting_timestamp > hi_timestamp) {
      // Done.
      break;
    }

    size_t bytes_covered = dir_it->second.approximate_size_bytes;

    RecordTimestamp partition_end_ts;
    if (partition_id >= partitions->nextID() - 1) {
      partition_end_ts = now;
    } else {
      partition_end_ts = partitions->get(partition_id + 1)->starting_timestamp;
    }

    if (partition_end_ts < lo_timestamp) {
      // A partition being selected by the binary search above means that it's
      // the last one with data for this log that starts before lo_timestamp,
      // but it might end before lo_timestamp, probably because there's no
      // directory entry for the following partition. Following directory
      // entries may still match the given range.
      continue;
    } else if (partition->starting_timestamp < lo_timestamp ||
               partition_end_ts > hi_timestamp) {
      // This partition is only partially covered; est. by linear interpolation
      uint64_t partition_time_range = partition_end_ts.toSeconds().count() -
          partition->starting_timestamp.toSeconds().count();
      ld_check_ge(std::min(hi_timestamp.toSeconds().count(),
                           partition_end_ts.toSeconds().count()),
                  std::max(lo_timestamp.toSeconds().count(),
                           partition->starting_timestamp.toSeconds().count()));
      uint64_t time_range_covered =
          std::min(hi_timestamp.toSeconds().count(),
                   partition_end_ts.toSeconds().count()) -
          std::max(lo_timestamp.toSeconds().count(),
                   partition->starting_timestamp.toSeconds().count());
      double fraction_in_range = partition_time_range == 0
          ? 1
          : (double(time_range_covered) / double(partition_time_range));
      bytes_covered = (uint64_t)round(bytes_covered * fraction_in_range);
    }

    *out += bytes_covered;
  }

  return 0;
}

bool PartitionedRocksDBStore::isLogEmpty(logid_t log_id) {
  auto logs_it = logs_.find(log_id.val_);
  if (logs_it == logs_.cend()) {
    return true;
  }

  // Grab logstate mutex
  LogState* log_state = logs_it->second.get();
  std::unique_lock<std::mutex> log_lock(log_state->mutex);
  auto& log_directory = log_state->directory;
  if (log_directory.empty()) {
    return true;
  }

  // Log has some record/s in some partition/s. If all of them are
  // pseudorecords, such as bridge records, we can declare the log empty.
  for (const auto directory_it : log_directory) {
    if (~directory_it.second.flags &
        PartitionDirectoryValue::PSEUDORECORDS_ONLY) {
      return false;
    }
  }

  return true;
}

int PartitionedRocksDBStore::findTime(
    logid_t log_id,
    std::chrono::milliseconds timestamp,
    lsn_t* lo,
    lsn_t* hi,
    bool approximate,
    bool allow_blocking_io,
    std::chrono::steady_clock::time_point deadline) const {
  FindTime findtime(*this,
                    log_id,
                    timestamp,
                    *lo,
                    *hi,
                    approximate,
                    allow_blocking_io,
                    deadline);
  return findtime.execute(lo, hi);
}

int PartitionedRocksDBStore::findKey(logid_t log_id,
                                     std::string key,
                                     lsn_t* lo,
                                     lsn_t* hi,
                                     bool approximate,
                                     bool allow_blocking_io) const {
  FindKey findkey(
      *this, log_id, std::move(key), approximate, allow_blocking_io);
  return findkey.execute(lo, hi);
}

rocksdb::Status
PartitionedRocksDBStore::writeBatch(const rocksdb::WriteOptions& options,
                                    rocksdb::WriteBatch* batch) {
  SCOPED_IO_TRACING_CONTEXT(getIOTracing(), "write-batch");
  uint64_t batch_size = batch->GetDataSize();
  uint64_t total_bytes_written =
      bytes_written_since_flush_eval_.fetch_add(batch_size);
  size_t throttling_reeval_trigger =
      getSettings()->bytes_written_since_throttle_eval_trigger;

  if ((total_bytes_written + batch_size) / throttling_reeval_trigger >
      total_bytes_written / throttling_reeval_trigger) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Shard: %d: reevaluating throttling. Bytes written since "
                   "last evaluation: %.3fM",
                   getShardIdx(),
                   total_bytes_written / 1e6);
    std::lock_guard<std::mutex> lock(throttle_eval_mutex_);
    auto extrapolated_buf_stats = last_flush_eval_stats_;
    extrapolated_buf_stats.active_memory_usage +=
        total_bytes_written + batch_size;
    throttleIOIfNeeded(extrapolated_buf_stats,
                       getSettings()->memtable_size_per_node / num_shards_);
  }

  return RocksDBLogStoreBase::writeBatch(options, batch);
}

void PartitionedRocksDBStore::findPartitionsMatchingIntervals(
    RecordTimeIntervals& rtis,
    std::function<void(PartitionPtr, RecordTimeInterval)> cb,
    bool include_empty) const {
  auto partitions = getPartitionList();
  for (PartitionPtr partition : *partitions) {
    if (partition->min_timestamp == RecordTimestamp::max() && !include_empty) {
      // Skip empty partitions.
      continue;
    }
    RecordTimeInterval pi(
        // FindTime relies on starting_timestamp, not min_timestamp.
        // If record timestamps do not strictly increase in LSN order
        // it is possible for min_timestamp to be earlier than
        // starting_timestamp. It is also possible for min_timestamp to be later
        // than starting_timestamp.
        //
        // Ensure that FindTime will return a result that is never higher than
        // the first lsn in this partition even if performing the partition
        // search based on min_timestamp's value would return a later partition.
        std::min(partition->starting_timestamp,
                 RecordTimestamp(partition->min_timestamp)),
        // max_timestamp == RecordTimestamp::min() if the partition is
        // currently empty.
        std::max(partition->starting_timestamp,
                 RecordTimestamp(partition->max_timestamp)));
    if (boost::icl::intersects(rtis, pi)) {
      cb(partition, pi);
    }
  }
}

void PartitionedRocksDBStore::normalizeTimeRanges(
    RecordTimeIntervals& rtis) const {
  RecordTimeIntervals partition_intervals;
  ld_check(!rtis.empty());
  if (rtis.empty()) {
    return;
  }
  if (*rtis.begin() == allRecordTimeInterval()) {
    // Common case of full shard being rebuilt.
    return;
  }
  findPartitionsMatchingIntervals(
      rtis, [&](PartitionPtr /* partition */, RecordTimeInterval pi) {
        partition_intervals.insert(pi);
      });
  for (auto& pi : partition_intervals) {
    rtis.insert(pi);
  }
}

RocksDBIterator
PartitionedRocksDBStore::createMetadataIterator(bool allow_blocking_io) const {
  ld_check(metadata_cf_);

  auto options = getDefaultReadOptions();
  options.read_tier =
      (allow_blocking_io ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier);

  return newIterator(options, metadata_cf_->get());
}

bool PartitionedRocksDBStore::flushPartitionAndDependencies(
    PartitionPtr partition) {
  ld_check(!immutable_.load());

  FlushToken flushed_through = partition->dirty_state_.max_flush_token;

  if (!flushMemtable(partition->cf_, /*wait*/ true)) {
    return false;
  }

  // We flush the unpartitioned data to ensure any dependencies
  // on unpartitioned data are retired.
  if (flushUnpartitionedMemtables(/*wait*/ true) != 0) {
    return false;
  }

  // Lock out dirty state changes from other threads.
  std::lock_guard<std::mutex> drop_lock(oldest_partition_mutex_);

  updatePartitionDirtyState(partition, flushed_through);
  syncWAL();

  return true;
}

bool PartitionedRocksDBStore::shouldCreatePartition() {
  const size_t partition_count = getPartitionList()->size();
  const size_t max_partition_count = getSettings()->partition_count_soft_limit_;
  bool partition_limit_crossed = false;

  if (partition_count >= max_partition_count) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Number of partitions (%lu) is above the soft limit (%lu). Adjusting "
        "partition creation triggers to limit the growth.",
        partition_count,
        max_partition_count);
    partition_limit_crossed = true;
  }

  // Prevent the latest partition from becoming non-latest, and therefore from
  // being dropped.
  std::lock_guard<std::mutex> lock(latest_partition_mutex_);

  auto partition = latest_.get();
  ld_check(partition);

  auto partition_duration = getSettings()->partition_duration_;
  if (partition_limit_crossed) {
    partition_duration *= 3;
  }
  if (partition_duration.count() > 0) {
    // If latest partition is old enough, start a new one.
    // In particular, if we didn't get any data for a long time, we want
    // a new empty partition to be created, so that all data can be trimmed.

    auto now = currentTime();

    if (now >= partition->starting_timestamp &&
        now - partition->starting_timestamp >= partition_duration) {
      return true;
    }
  }

  auto file_limit = getSettings()->partition_file_limit_;
  if (!partition_limit_crossed && file_limit > 0 &&
      getNumL0Files(partition->cf_->get()) >= file_limit) {
    return true;
  }

  auto size_limit = getSettings()->partition_size_limit_;
  if (size_limit > 0 &&
      getApproximatePartitionSize(partition->cf_->get()) >= size_limit) {
    return true;
  }

  return false;
}

int PartitionedRocksDBStore::writeMulti(
    const std::vector<const WriteOp*>& writes_in,
    const WriteOptions& options) {
  SCOPED_IO_TRACING_CONTEXT(getIOTracing(), "writeMulti");

  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  // List of logs whose partition directory we'll be using.
  FixedKeysMapKeys<logid_t> partitioned_logs;
  // Smallest timestamp among writes to partitioned logs.
  // Needed to estimate the first target partition.
  RecordTimestamp min_timestamp = RecordTimestamp::max();

  // Smallest timestamp among non-recovery writes to partitioned logs.
  // Needed to decide if we need to prepend partitions. Writes from recovery
  // are exempted because they might have very old timestamps if the log
  // hasn't been used for a long time (or recovery was stuck for a very long
  // time, which shouldn't happen).
  //
  // Why is it ok to exempt recovery? What if we get an old recovery record,
  // put it in a recent partition, and then get lots of old rebuilding records
  // above the recovery record? They would have to go to the recent partition
  // as well, potentially bloating the partition, as well as getting trimmed
  // later than retention says. This situation shouldn't be a problem in
  // practice: if a newly written but very old recovery record is followed by
  // lots of old records, it means that the recovery was stuck for a long time
  // while a sequencer in a higher epoch was successfully taking writes; if
  // this happens for many of logs at once, we have bigger problems than
  // a bloated partition.
  RecordTimestamp min_prependable_timestamp = RecordTimestamp::max();

  logid_t min_prependable_ts_log = LOGID_INVALID;
  lsn_t min_prependable_ts_lsn = LSN_INVALID;
  for (const auto write : writes_in) {
    logid_t log = LOGID_INVALID;
    switch (write->getType()) {
      case WriteType::PUT: {
        const PutWriteOp* op = static_cast<const PutWriteOp*>(write);
        log = op->log_id;

        if (isLogPartitioned(log)) {
          std::chrono::milliseconds timestamp_ms;
          int rv = LocalLogStoreRecordFormat::parseTimestamp(
              op->record_header, &timestamp_ms);
          RecordTimestamp timestamp(timestamp_ms);
          if (rv == 0 && timestamp < min_prependable_timestamp) {
            min_timestamp = std::min(min_timestamp, timestamp);

            bool written_by_recovery;
            rv = LocalLogStoreRecordFormat::isWrittenByRecovery(
                op->record_header, &written_by_recovery);
            if (rv == 0 && !written_by_recovery) {
              min_prependable_timestamp = timestamp;
              min_prependable_ts_log = log;
              min_prependable_ts_lsn = op->lsn;
            }
          } else {
            // If rv != 0 getWritePartition() will log a warning, so no need
            // to log anything here.
          }
        }
        break;
      }
      case WriteType::DELETE: {
        const RecordWriteOp* op = static_cast<const RecordWriteOp*>(write);
        log = op->log_id;
        break;
      }
      case WriteType::DUMP_RELEASE_STATE:
      case WriteType::PUT_LOG_METADATA:
      case WriteType::PUT_SHARD_METADATA:
      case WriteType::DELETE_LOG_METADATA:
      case WriteType::MERGE_MUTABLE_PER_EPOCH_METADATA:
        break;
        // Let compiler check that all enum values are handled.
    }
    if (log != LOGID_INVALID && isLogPartitioned(log)) {
      partitioned_logs.add(log);
    }
  }
  partitioned_logs.finalize();

  ld_check_ge(min_prependable_timestamp, min_timestamp);
  if (!prependPartitionsIfNeeded(min_prependable_timestamp,
                                 min_prependable_ts_log,
                                 min_prependable_ts_lsn)) {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  // This retry loop solves a sort of chicken and egg problem:
  //  * To know the exact target partition for each write, we need to look at
  //    the partition directory.
  //  * Before looking at the directory corresponding to some partition,
  //    we need to lock a mutex to make sure this partition is not being
  //    dropped. Otherwise we would get some complicated race conditions and
  //    ABA problems if a partition is dropped and quickly created again while
  //    we're trying to write to it. The mutex we need to lock is
  //    Partition::mutex_ of some partition with ID <= the IDs of all the target
  //    partitions for our writes.
  // So, we need to lock the mutex_ of a partition with ID <= the IDs of
  // all target partitions, and we need to lock it before we know exactly what
  // the target partitions are. Let's estimate the min target partition ID
  // based on timestamps. If writeMultiImpl() then finds that the exact min
  // target partition (based on directory) is smaller than our estimate,
  // it will fail with E::AGAIN and will give us a better estimate, and we'll
  // try again using the new estimate. This should almost never happen.
  // Note that the estimate can theoretically decrease more than once if
  // partitions were prepended between attempts.
  partition_id_t min_target_partition =
      getPreferredPartition(min_timestamp, false);
  int rv;
  while (true) {
    partition_id_t old_min_target_partition = min_target_partition;
    rv = writeMultiImpl(
        writes_in, options, &partitioned_logs, &min_target_partition);
    if (rv == 0 || err != E::AGAIN) {
      // Succeeded or failed, stop retrying. This is almost always hit on the
      // first iteration.
      break;
    }
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        2,
        "Retrying writeMultiImpl() because target partition was overestimated "
        "or dropped. This should be rare. Old target partition: %lu, new "
        "target partition: %lu, min ts in batch: %s, min non-recovery ts: %s "
        "(log: %lu, lsn: %s)",
        old_min_target_partition,
        min_target_partition,
        min_timestamp.toString().c_str(),
        min_prependable_timestamp.toString().c_str(),
        min_prependable_ts_log.val_,
        lsn_to_string(min_prependable_ts_lsn).c_str());
    STAT_INCR(stats_, logsdb_partition_pick_retries);
  }

  return rv;
}

int PartitionedRocksDBStore::writeMultiImpl(
    const std::vector<const WriteOp*>& writes_in,
    const WriteOptions& options,
    const LogLocks::Keys* partitioned_logs,
    partition_id_t* min_target_partition_est) {
  PartitionPtr min_target_partition;
  if (!getPartition(*min_target_partition_est, &min_target_partition)) {
    // Partition is dropped.
    *min_target_partition_est = oldest_partition_id_.load();
    err = E::AGAIN;
    return -1;
  }

  // Disallow dropping target partitions. Only locking the oldest of them is
  // sufficient because dropPartitions() locks all the partitions it drops,
  // from oldest to newest.
  folly::SharedMutex::ReadHolder cf_lock(min_target_partition->mutex_);
  if (min_target_partition->is_dropped) {
    // Partition is dropped.
    *min_target_partition_est = oldest_partition_id_.load();
    err = E::AGAIN;
    return -1;
  }

  // Lock LogState mutexes.
  LogLocks log_locks(partitioned_logs);
  // Note that FixedKeysMap iterator iterates in order of increasing key
  // (log ID) and has no duplicates.
  // Both properties are needed to prevent deadlocks.
  for (auto it : log_locks) {
    auto res = logs_.emplace(it.first.val(), std::make_unique<LogState>());
    auto log_state = res.first->second.get();
    *it.second = std::unique_lock<std::mutex>(log_state->mutex);
  }

  partition_id_t latest_partition_id = latest_.get()->id_;

  rocksdb::WriteBatch wal_batch;
  rocksdb::WriteBatch mem_batch;
  std::vector<const WriteOp*> writes;
  // Collect column family pointers to which keys will be written or deleted.
  std::vector<RocksDBCFPtr> cf_ptrs;

  // Record write operations that are being buffered in memory and thus
  // are leaving this RocksDB instance "dirty".
  std::vector<DirtyOp> dirty_ops;

  // Tasks to queue for WAL sync after partition timestamp updates.
  std::vector<std::unique_ptr<Partition::TimestampUpdateTask>>
      timestamp_update_tasks;

  // If a log has dir_updates_pending[log] > dir_updates_flushed, the log may
  // have some directory updates in rocksdb_batch. We need to flush these
  // updates before calling getWritePartition() again for this log.
  FixedKeysMap<logid_t, int> dir_updates_pending(log_locks.keys());
  int dir_updates_flushed = 0;

  bool unpartitioned_dirtied = false;

  const size_t expected_batch_size = writes_in.size();

  writes.reserve(expected_batch_size);
  cf_ptrs.reserve(expected_batch_size);
  dirty_ops.reserve(expected_batch_size);

  const bool skip_rebuilding = getRebuildingSettings()->read_only ==
      RebuildingReadOnlyOption::ON_RECIPIENT;

  // Writes and clears rocksdb_batch. Used for flushing directory updates
  // between calls to getWritePartition() for the same log.
  // Note: Partition timestamp updates can be flushed as part of this. Make sure
  // writes only to metadata column family are flushed in here, data column
  // family writes need to note down dependent metadata memtable and
  // hence should not be included in this update.
  auto flush_dir_updates = [&]() {
    rocksdb::WriteOptions rocksdb_options;
    for (auto* batch : {&wal_batch, &mem_batch}) {
      if (batch->Count()) {
        auto status = writeBatch(rocksdb_options, batch);
        if (!status.ok()) {
          ld_error("Failed to write directory updates to RocksDB: %s",
                   status.ToString().c_str());
          err = E::LOCAL_LOG_STORE_WRITE;
          return false;
        }
        STAT_INCR(stats_, logsdb_directory_premature_flushes);
        batch->Clear();
      }
      rocksdb_options.disableWAL = true;
    }

    // Clear dir_updates_pending.
    ++dir_updates_flushed;
    return true;
  };

  ld_spew("------------- Write Batch Begin --------------");
  for (const auto write : writes_in) {
    // When testing, we may want to ignore rebuilding related writes.
    if (skip_rebuilding && write->getType() == WriteType::PUT &&
        static_cast<const PutWriteOp*>(write)->isRebuilding()) {
      continue;
    }

    RocksDBCFPtr cf_ptr;
    bool skip_op = false;

    switch (write->getType()) {
      case WriteType::PUT:
        // Verify checksums before attempting to actually write the records,
        // so that a corrupt record does not affect log state or directory.
        if (getSettings()->verify_checksum_during_store) {
          // Reject to store malformed records.
          const PutWriteOp* op = static_cast<const PutWriteOp*>(write);
          int rv = LocalLogStoreRecordFormat::checkWellFormed(
              op->record_header, op->data);
          if (rv != 0) {
            RATELIMIT_ERROR(
                std::chrono::seconds(10),
                10,
                "checksum mismatch: refusing to write malformed record %lu%s. "
                "Header: %s, data: %s",
                op->log_id.val_,
                lsn_to_string(op->lsn).c_str(),
                hexdump_buf(op->record_header, 500).c_str(),
                hexdump_buf(op->data, 500).c_str());
            // Fail and send an error to sequencer. The record was corrupted
            // or malformed by this or sequencer's node, likely due to bad
            // hardware or bug.
            flush_dir_updates();
            err = E::CHECKSUM_MISMATCH;
            return -1;
          } else if (getSettings()->test_corrupt_stores) {
            RATELIMIT_INFO(std::chrono::seconds(60),
                           5,
                           "checksum mismatch: Pretending record %lu%s is "
                           "corrupt. Header: %s, "
                           "data: %s",
                           op->log_id.val_,
                           lsn_to_string(op->lsn).c_str(),
                           hexdump_buf(op->record_header, 500).c_str(),
                           hexdump_buf(op->data, 500).c_str());
            flush_dir_updates();
            err = E::CHECKSUM_MISMATCH;
            return -1;
          }
        }
      case WriteType::DELETE: {
        const RecordWriteOp* op = static_cast<const RecordWriteOp*>(write);

        if (!isLogPartitioned(op->log_id)) {
          // Unpartitioned writes aren't tracked in the partition data
          // structures, so we can't currently maintain dirty state for
          // them. This could change in the future (e.g. keep in-core
          // partitions to track unpartitioned time ranges that are older
          // than the maximum retention period for partitioned logs. These
          // partitions wouldn't maintain column family information).
          write->increaseDurabilityTo(Durability::ASYNC_WRITE);
          cf_ptr = unpartitioned_cf_;
          unpartitioned_dirtied = true;
          break;
        }

        if (dir_updates_pending[op->log_id] > dir_updates_flushed) {
          if (!flush_dir_updates()) {
            return -1;
          }
        }

        PartitionPtr partition;

        // Get record timestamp.
        folly::Optional<RecordTimestamp> timestamp;
        if (write->getType() == WriteType::PUT) {
          const PutWriteOp* put_op = static_cast<const PutWriteOp*>(write);

          std::chrono::milliseconds record_timestamp;
          int rv = LocalLogStoreRecordFormat::parseTimestamp(
              put_op->record_header, &record_timestamp);

          if (rv == 0) {
            timestamp = RecordTimestamp::from(record_timestamp);
          } else {
            RATELIMIT_ERROR(
                std::chrono::seconds(10),
                10,
                "Failed to parse timestamp, will use current time. %s",
                write->toString().c_str());
            timestamp = RecordTimestamp(currentTime());
          }
        }
        // For DELETE ops we keep timestamp unset, so that getWritePartition()
        // doesn't create a directory entry for it. Creating a directory entry
        // because of a DELETE op is pointless:
        //  - if record that we're processing a DELETE for was already stored,
        //    this write will not create a directory entry anyway
        //    (since we already saw a record with the same LSN),
        //  - if delete and store got reordered (unlikely but possible),
        //    DELETE will be a no-op anyway; since we don't have a record
        //    timestamp in DELETE op, it's better to not create directory
        //    entry.
        size_t payload_size_bytes = 0;
        LocalLogStoreRecordFormat::flags_t flags = 0;
        if (write->getType() == WriteType::PUT) {
          auto* put_write_op = static_cast<const PutWriteOp*>(write);
          int rv = LocalLogStoreRecordFormat::parseFlags(
              put_write_op->record_header, &flags);
          if (rv != 0) {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "Failed to parse flags for record %s of log %lu; "
                            "will assume it's not an amend",
                            lsn_to_string(put_write_op->lsn).c_str(),
                            put_write_op->log_id.val());
            payload_size_bytes = put_write_op->data.size;
          } else if (flags & LocalLogStoreRecordFormat::FLAG_AMEND) {
            payload_size_bytes = 0;
          } else {
            payload_size_bytes = put_write_op->data.size;
          }
        }
        auto res = getWritePartition(
            op->log_id,
            op->lsn,
            op->durability(),
            timestamp,
            min_target_partition_est,
            op->durability() <= Durability::MEMORY ? mem_batch : wal_batch,
            wal_batch,
            &partition,
            payload_size_bytes,
            flags);
        switch (res) {
          case GetWritePartitionResult::OK:
            break;
          case GetWritePartitionResult::RETRY:
            // If previous calls to getWritePartition() prepared some directory
            // updates, we must flush them to keep directory consistent with
            // LogState.
            if (!flush_dir_updates()) {
              return -1;
            }

            err = E::AGAIN;
            ld_check_lt(*min_target_partition_est, min_target_partition->id_);
            return -1;
          case GetWritePartitionResult::SKIP:
            ld_check_ne(write->getType(), WriteType::PUT);
            skip_op = true;
            STAT_INCR(stats_, logsdb_skipped_writes);
            break;
          case GetWritePartitionResult::ERROR:
            err = E::LOCAL_LOG_STORE_WRITE;
            return -1;
        }

        if (skip_op) {
          break;
        }

        // Track dirty regions for all non-synchronous writes.
        if (write->getType() == WriteType::PUT &&
            write->durability() < Durability::SYNC_WRITE) {
          const PutWriteOp* put_op = static_cast<const PutWriteOp*>(write);

          if (timestamp == RecordTimestamp::zero()) {
            // We cannot rely on dirty time range tracking to guarantee
            // record durbility without valid timestamps (e.g. some
            // instances of bridge records).  Promote to SYNC_WRITE.
            put_op->increaseDurabilityTo(Durability::SYNC_WRITE);
            STAT_INCR(stats_, sync_write_promotion_no_timestamp);
          } else if (!put_op->coordinator.hasValue()) {
            // Recovery from MEMORY stores requires coordinator information.
            // Promote to a synchronous write if this isn't available.
            put_op->increaseDurabilityTo(Durability::SYNC_WRITE);
            STAT_INCR(stats_, sync_write_promotion_no_coordinator);
          } else {
            ld_spew("Partition %lu %s dirtied by N%d",
                    partition->id_,
                    put_op->isRebuilding() ? "rebuild" : "append",
                    put_op->coordinator.value());
            ld_check(timestamp.hasValue());
            dirty_ops.emplace_back(partition,
                                   timestamp.value(),
                                   put_op,
                                   put_op->coordinator.value(),
                                   put_op->isRebuilding() ? DataClass::REBUILD
                                                          : DataClass::APPEND);
          }
        }

        // Pessimistically assume getWritePartition() always updates directory.
        // It indeed does when the writes arrive in order of increasing LSN.
        dir_updates_pending[op->log_id] = dir_updates_flushed + 1;

        cf_ptr = partition->cf_;

        // Complain about suspicious writes.
        if (write->getType() != WriteType::PUT ||
            !(static_cast<const PutWriteOp*>(write))->isRebuilding()) {
          // consider all partitions except two latest as old partitions
          if (partition && partition->id_ < latest_partition_id - 1) {
            RATELIMIT_INFO(std::chrono::seconds(10),
                           10,
                           "The following write is going to be written "
                           "to old partition %lu: %s",
                           partition->id_,
                           write->toString().c_str());
          }
        }
        // Complain about another kind of suspicious writes.
        if (timestamp.hasValue()) {
          auto dt = timestamp.value() - partition->starting_timestamp;
          auto dur = getSettings()->partition_duration_;
          // It's expected that 0 <= dt < dur, but the check below has some
          // margins to allow:
          //  - a small clock skew (dt slightly out of range),
          //  - a slight delay in creating new partitions
          //    (dt slightly above dur), or
          //  - up to 3x decrease of partition_duration_ setting
          //    (dur < dt < dur*3).
          if (dur.count() > 0 && (dt > dur * 3 || dt < -dur)) {
            RATELIMIT_INFO(
                std::chrono::seconds(10),
                10,
                "Timestamp of a record is way off compared to timestamp of "
                "target partition. Partition: %lu, partition timestamp: %s, "
                "record timestamp: %s, write: %s",
                partition->id_,
                format_time(partition->starting_timestamp).c_str(),
                format_time(timestamp.value()).c_str(),
                write->toString().c_str());
          }
        }

        if (write->getType() == WriteType::PUT) {
          // Maybe update partition metadata.
          auto update_task = updatePartitionTimestampsIfNeeded(
              partition, timestamp.value(), wal_batch);
          if (update_task != nullptr) {
            timestamp_update_tasks.emplace_back(std::move(update_task));
            ld_check(update_task == nullptr);
          }

          auto& rocksdb_batch =
              write->durability() <= Durability::MEMORY ? mem_batch : wal_batch;
          // Write "custom logsdb directory", i.e. mapping
          // (log_id, partition_id) => min_key.
          // This goes into metadata column family. There's also per-record
          // index written into the partition's column family by
          // RocksDBWriter. These updates can be flushed before the actual data
          // alongwith directory updates.
          const PutWriteOp* put_op = static_cast<const PutWriteOp*>(write);
          for (auto it = put_op->index_key_list.begin();
               it != put_op->index_key_list.end();
               ++it) {
            if (it->first == FIND_KEY_INDEX) {
              CustomIndexDirectoryKey key(
                  put_op->log_id, FIND_KEY_INDEX, partition->id_);
              rocksdb::Slice key_slice(
                  reinterpret_cast<const char*>(&key), sizeof key);

              auto value = CustomIndexDirectoryValue::create(
                  rocksdb::Slice(it->second.data(), it->second.size()),
                  op->lsn);
              rocksdb::Slice value_slice(value.data(), value.size());

              rocksdb_batch.Merge(metadata_cf_->get(), key_slice, value_slice);
            }
          }
        }

        break;
      }
      case WriteType::DUMP_RELEASE_STATE:
      case WriteType::PUT_LOG_METADATA:
      case WriteType::DELETE_LOG_METADATA:
      case WriteType::PUT_SHARD_METADATA:
      case WriteType::MERGE_MUTABLE_PER_EPOCH_METADATA:
        break;
        // Let compiler check that all enum values are handled.
    }

    if (!skip_op) {
      writes.push_back(write);
      cf_ptrs.push_back(cf_ptr);
    }
  }

  ld_check_eq(writes.size(), cf_ptrs.size());
  ld_check_eq(*min_target_partition_est, min_target_partition->id_);
  ld_check(!min_target_partition->is_dropped);

  // Go over all holders and mark beginning of write on the partition.
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  cf_handles.reserve(cf_ptrs.size());
  for (auto& cf_ptr : cf_ptrs) {
    if (cf_ptr != nullptr) {
      cf_ptr->beginWrite();
      cf_handles.push_back(cf_ptr->get());
    } else {
      cf_handles.push_back(nullptr);
    }
  }

  // Actually write the records to rocksdb.
  int rv = writer_->writeMulti(writes,
                               options,
                               metadata_cf_->get(),
                               &cf_handles,
                               wal_batch,
                               mem_batch,
                               /*skip_checksum_verification=*/true);
  if (rv != 0) {
    ld_check_in(err, ({E::INTERNAL, E::LOCAL_LOG_STORE_WRITE}));
    return -1;
  }

  auto metadata_cf_flush_token = metadata_cf_->activeMemtableFlushToken();
  auto timestamp_wal_flush_token = maxWALSyncToken();
  auto now = currentSteadyTime();
  auto max_flush_token = maxFlushToken();

  // Go over all the holders and mark that write finished on the partition.
  for (auto& cf_ptr : cf_ptrs) {
    if (cf_ptr == nullptr) {
      continue;
    }
    cf_ptr->endWrite(metadata_cf_flush_token);
  }

  // The rest of this method updates dirty state.

  // Unlock LogState::mutex'es, we don't need them anymore.
  // The DirtyState stuff is synchronized through Partition::mutex_.
  for (auto it : log_locks) {
    it.second->unlock();
  }

  if (unpartitioned_dirtied || !dirty_ops.empty()) {
    // Start tracking data idleness if we aren't already doing
    // so due to an earlier write.

    if (unpartitioned_dirtied) {
      unpartitioned_dirty_state_.noteDirtied(max_flush_token, now);
    }
  }

  if (!dirty_ops.empty()) {
    // Sort ops by partition/node/data class tuple so similar ops
    // can be processed in batches.
    std::sort(dirty_ops.begin(), dirty_ops.end());

    using DirtiedPartition =
        std::pair<PartitionPtr, folly::SharedMutex::WriteHolder>;
    std::vector<DirtiedPartition> dirtied_partitions;

    // Loop Invariant: cur_partition's mutex_ is locked either by cf_lock or
    //                 by dirtied_partitions.back().
    PartitionPtr cur_partition = min_target_partition;
    for (auto op = dirty_ops.begin(); op != dirty_ops.end(); ++op) {
      if (op->partition->id_ != cur_partition->id_) {
        cf_lock = folly::SharedMutex::ReadHolder(op->partition->mutex_);
        cur_partition = op->partition;
      }

      auto flush_token = cur_partition->cf_->getMostRecentMemtableFlushToken();

      // Mark the flush token on which the writers should depend to check
      // whether data is retired.
      op->write_op->setFlushToken(flush_token);

      auto& dirty_state = cur_partition->dirty_state_;
      dirty_state.noteDirtied(flush_token, now);

      // To simplify some logic, use the result type from emplace() to
      // track lookup/update/emplace operations on the node dirty data.
      DirtiedByKey key(op->node_idx, op->data_class);
      std::pair<DirtiedByMap::iterator, bool> result;
      auto& ndd_kv = result.first;
      auto& emplaced = result.second;
      ndd_kv = dirty_state.dirtied_by_nodes.find(key);
      emplaced = false;

      if (ndd_kv == dirty_state.dirtied_by_nodes.end()) {
        bool clean_partition;
        if (dirtied_partitions.empty() ||
            cur_partition->id_ != dirtied_partitions.back().first->id_) {
          // Upgrade to write lock.
          cf_lock.unlock();
          folly::SharedMutex::WriteHolder partition_lock(op->partition->mutex_);
          if (op->partition->is_dropped) {
            // Trimmed away. Don't care about tracking dirty.
            //
            // Downgrade to shared mode (to ensure the loop locking invariant)
            // and skip over any ops for trimmed partitions.
            cf_lock = folly::SharedMutex::ReadHolder(std::move(partition_lock));
            auto next_op = op;
            while (++next_op != dirty_ops.end() &&
                   next_op->partition->is_dropped) {
              op = next_op;
            }
            continue;
          }
          clean_partition = dirty_state.dirtied_by_nodes.empty();
          result = dirty_state.dirtied_by_nodes.emplace(
              std::piecewise_construct,
              std::forward_as_tuple(op->node_idx, op->data_class),
              std::forward_as_tuple());
          if (!emplaced) {
            // Another thread has created the node. Proceed in shared mode.
            ld_spew("Partition s%u:%ld lost race to mark N%d dirty",
                    getShardIdx(),
                    op->partition->id_,
                    op->node_idx);
            cf_lock = folly::SharedMutex::ReadHolder(std::move(partition_lock));
          } else {
            dirtied_partitions.emplace_back(
                op->partition, std::move(partition_lock));
            ld_spew("Partition s%u:%ld marked N%d:%s dirty",
                    getShardIdx(),
                    op->partition->id_,
                    op->node_idx,
                    toString(op->data_class).c_str());
          }
        } else {
          // A previous iteration of this loop locked and dirtied this
          // partition. It should not be clean.
          clean_partition = dirty_state.dirtied_by_nodes.empty();
          ld_check(!clean_partition);

          result = dirty_state.dirtied_by_nodes.emplace(
              std::piecewise_construct,
              std::forward_as_tuple(op->node_idx, op->data_class),
              std::forward_as_tuple());
          ld_check(emplaced);
        }
        if (clean_partition) {
          ld_spew("Partition s%u:%ld first marked dirty by N%d",
                  getShardIdx(),
                  op->partition->id_,
                  op->node_idx);
          STAT_INCR(stats_, partition_marked_dirty);
        }
      }

      op->newly_dirtied = ndd_kv->second.markDirtyUntil(flush_token);
      ld_check_eq(op->newly_dirtied, emplaced);

      // If the write op has to wait for updated PartitionDirtyMetadata
      // to sync out, the caller must treat it as a synchronous write
      // and wait for a WAL sync to make our metadata durable.
      Durability min_durability = Durability::MEMORY;
      FlushToken min_sync_token = FlushToken_INVALID;
      switch (op->data_class) {
        case DataClass::APPEND:
          // Any node that is dirty for appends is enough to ensure
          // that we rebuild all append data for the partition.
          min_sync_token = dirty_state.append_dirtied_wal_token.load();
          if (min_sync_token == FlushToken_MAX) {
            // Unconditional sync and wait.
            min_sync_token = FlushToken_INVALID;
            min_durability = Durability::SYNC_WRITE;
          } else if (min_sync_token > walSyncedUpThrough()) {
            // Wait for in-progress sync.
            min_durability = Durability::SYNC_WRITE;
          }
          break;
        case DataClass::REBUILD:
          if (op->newly_dirtied ||
              ndd_kv->second.syncingUntil() > walSyncedUpThrough()) {
            min_durability = Durability::SYNC_WRITE;
            // Note: If newly_dirtied is true, syncingUntil() will return
            //       FlushToken_INVALID. This requests an unconditional sync
            //       by SyncingStorageThread.  The WAL FlushToken in effect
            //       after our write will be recorded below for reference by
            //       future write operations that find the parition already
            //       dirty.
            min_sync_token = ndd_kv->second.syncingUntil();
          }
          break;
        case DataClass::MAX:
        case DataClass::INVALID:
          ld_check(false);
          break;
      }

      bool wait_for_timestamp_update = !boost::icl::contains(
          op->partition->dirtyTimeInterval(*getSettings(), latest_.get()->id_),
          op->timestamp);
      if (wait_for_timestamp_update &&
          min_durability < Durability::SYNC_WRITE) {
        STAT_INCR(stats_, partition_sync_write_promotion_for_timstamp);
        min_durability = Durability::SYNC_WRITE;
        min_sync_token = timestamp_wal_flush_token;
      }

      // Complete processing of all dirty ops (including this first one) that
      // dirty the same Node and DataClass.
      auto next_op = op;
      while (next_op != dirty_ops.end() && op->canMergeWith(*next_op)) {
        op = next_op++;
        // Mark the flush token on the write_op for ops of same type.
        op->write_op->setFlushToken(flush_token);
        op->write_op->increaseDurabilityTo(min_durability, min_sync_token);
      }
    }

    if (!dirtied_partitions.empty()) {
      ld_spew("Shard %u: Updated %zu partitions",
              getShardIdx(),
              dirtied_partitions.size());
      STAT_ADD(stats_, partition_dirty_data_updated, dirtied_partitions.size());

      rocksdb::WriteBatch dirty_batch;
      for (auto& dpd : dirtied_partitions) {
        PartitionPtr& partition = dpd.first;
        writePartitionDirtyState(partition, dirty_batch);
      }
      auto status = writeBatch(rocksdb::WriteOptions(), &dirty_batch);
      if (!status.ok()) {
        // Should have entered fail-safe mode. Fail all writes.
        ld_check_in(acceptingWrites(), ({E::DISABLED, E::NOSPC}));
        err = E::LOCAL_LOG_STORE_WRITE;
        return -1;
      } else {
        FlushToken wal_token = maxWALSyncToken();
        for (const auto& op : dirty_ops) {
          if (op.newly_dirtied) {
            auto& dirty_state = op.partition->dirty_state_;

            // At least one append entry is recorded in the partition
            // dirty state we have written.
            if (op.data_class == DataClass::APPEND) {
              atomic_fetch_min(dirty_state.append_dirtied_wal_token, wal_token);
            }

            DirtiedByKey key(op.node_idx, op.data_class);
            auto ndd_kv = dirty_state.dirtied_by_nodes.find(key);
            ld_check(ndd_kv != dirty_state.dirtied_by_nodes.end());
            if (ndd_kv != dirty_state.dirtied_by_nodes.end()) {
              ndd_kv->second.markSyncingUntil(wal_token);
            }
          }
        }
      }
    }
  }

  if (!timestamp_update_tasks.empty()) {
    ServerWorker* worker = ServerWorker::onThisThread(/*enforce*/ false);
    if (worker == nullptr) {
      // This is an unit test. Make the timestamps durable inline.
      syncWAL();
      for (auto& task : timestamp_update_tasks) {
        task->onSynced();
        task->onDone();
      }
    } else {
      auto& storage_thread_pool =
          worker->getStorageThreadPoolForShard(shard_idx_);
      for (auto& task : timestamp_update_tasks) {
        task->setSyncToken(timestamp_wal_flush_token);
        task->setStorageThreadPool(&storage_thread_pool);
        storage_thread_pool.enqueueForSync(std::move(task));
      }
    }
  }

  ld_spew("------------- Write Batch End: FlushToken %jx --------------",
          static_cast<uintmax_t>(max_flush_token));

  return 0;
}

int PartitionedRocksDBStore::writeStoreMetadata(
    const StoreMetadata& metadata,
    const WriteOptions& write_options) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  int rv =
      writer_->writeStoreMetadata(metadata, write_options, metadata_cf_->get());
  if (rv == 0 && metadata.getType() == StoreMetadataType::REBUILDING_RANGES) {
    auto& rrm = static_cast<const RebuildingRangesMetadata&>(metadata);
    if (rrm.empty()) {
      // Any under-replicated data has been rebuilt. Persistenty clear
      // partition under-relication flag.
      min_under_replicated_partition_.store(PARTITION_MAX);
      max_under_replicated_partition_.store(PARTITION_INVALID);
      auto partitions = getPartitionList();
      for (PartitionPtr partition : *partitions) {
        if (partition->isUnderReplicated()) {
          partition->setUnderReplicated(false);

          // If not dirty already, mark the partition dirty so that it
          // will be flushed the next time our background thread sees
          // the oldest MemTable retired. If no write activity occurs
          // before shutdown, these records will be flushed at shutdown.
          partition->dirty_state_.noteDirtied(
              FlushToken_MIN, currentSteadyTime());
        }
      }
    }
  }
  return rv;
}

void PartitionedRocksDBStore::writePartitionDirtyState(
    PartitionPtr partition,
    rocksdb::WriteBatch& batch) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  PartitionMetaKey key(PartitionMetadataType::DIRTY, partition->id_);
  PartitionDirtyMetadata meta = partition->dirty_state_.metadata();
  Slice value = meta.serialize();
  batch.Put(
      metadata_cf_->get(),
      rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
      rocksdb::Slice(reinterpret_cast<const char*>(value.data), value.size));
}

int PartitionedRocksDBStore::readAllLogSnapshotBlobs(
    LogSnapshotBlobType type,
    LogSnapshotBlobCallback callback) {
  std::lock_guard<std::mutex> snapshots_lock(snapshots_cf_lock_);
  if (!snapshots_cf_) {
    ld_info("Snapshots column family does not exist");
    return 0;
  }
  return readAllLogSnapshotBlobsImpl(type, callback, snapshots_cf_->get());
}

int PartitionedRocksDBStore::writeLogSnapshotBlobs(
    LogSnapshotBlobType snapshots_type,
    const std::vector<std::pair<logid_t, Slice>>& snapshots) {
  if (getSettings()->read_only) {
    return -1;
  }
  ld_check(!immutable_.load());

  std::lock_guard<std::mutex> snapshots_lock(snapshots_cf_lock_);
  if (!snapshots_cf_) {
    snapshots_cf_ =
        createColumnFamily(SNAPSHOTS_CF_NAME, rocksdb_config_.options_);
    if (!snapshots_cf_) {
      ld_error("Failed to create column family for log snapshot blobs");
      return -1;
    }
  }

  auto s = writer_->writeLogSnapshotBlobs(
      snapshots_cf_->get(), snapshots_type, snapshots);

  if (s == 0 && !flushMemtable(snapshots_cf_, /* wait */ false)) {
    return -1;
  }

  return s;
}

int PartitionedRocksDBStore::deleteAllLogSnapshotBlobs() {
  if (getSettings()->read_only) {
    return -1;
  }
  ld_check(!immutable_.load());

  std::lock_guard<std::mutex> snapshots_lock(snapshots_cf_lock_);

  if (!snapshots_cf_) {
    return 0;
  }

  auto status = db_->DropColumnFamily(snapshots_cf_->get());
  snapshots_cf_.reset();
  return status.ok() ? 0 : -1;
}

PartitionedRocksDBStore::PartitionList
PartitionedRocksDBStore::getPartitionList() const {
  auto res = partitions_.getVersion();
  size_t iter = 0;
  while (res->nextID() > latest_.get()->id_ + 1) {
    // partitions_ can brefly contain a partition after latest_. Since latest_
    // is updated right after adding to partitions_, we can afford to just busy
    // wait for latest_ to be updated.
    std::this_thread::yield();
    if (!(++iter & ((1 << 14) - 1))) { // every 16384 iterations
      ld_error("Busy-waited for latest_ to be updated for %lu iterations. "
               "Something's %s wrong.",
               iter,
               iter < 10000000 ? "probably" : "clearly");
    }
  }
  return res;
}

PartitionedRocksDBStore::PartitionPtr
PartitionedRocksDBStore::getLatestPartition() const {
  return latest_.get();
}

RecordTimestamp
PartitionedRocksDBStore::getPartitionTimestampForLSNIfReadilyAvailable(
    logid_t log_id,
    lsn_t lsn) const {
  RocksDBIterator it = createMetadataIterator(false);
  PartitionPtr out_partition;
  if (findPartition(&it, log_id, lsn, &out_partition) == 0) {
    return out_partition->starting_timestamp;
  }
  return RecordTimestamp::zero();
}

int PartitionedRocksDBStore::dropPartitions(
    partition_id_t oldest_to_keep_est,
    std::function<partition_id_t()> get_oldest_to_keep) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  // All partition drops are serialized for simplicity.
  std::lock_guard<std::mutex> drop_lock(oldest_partition_mutex_);

  partition_id_t prev_oldest = partitions_.firstID();
  ld_check(prev_oldest == oldest_partition_id_.load());

  E error = E::OK;
  std::vector<PartitionPtr> partitions;
  std::vector<folly::SharedMutex::WriteHolder> partition_locks;

  // The order of steps 1 - 4 below is important for correctness.
  // The order of steps 4a - 4d isn't.

  // 1) Acquire locks to disallow writing to our partitions.
  for (partition_id_t id = prev_oldest; id < oldest_to_keep_est; ++id) {
    PartitionPtr partition = partitions_.get(id);
    if (partition == nullptr) {
      ld_warning("Gap in partition numbers: partition %lu doesn't exist", id);
      error = E::GAP;
      continue;
    }

    partition_locks.emplace_back(partition->mutex_);
    partitions.push_back(partition);
  }

  // 2) Decide how many to drop.
  partition_id_t oldest_to_keep =
      std::min(oldest_to_keep_est, get_oldest_to_keep());

  // Nothing to drop.
  if (oldest_to_keep <= prev_oldest) {
    return 0;
  }

  // If oldest_to_keep_est is an overestimate, release unneeded locks.
  while (!partitions.empty() && partitions.back()->id_ >= oldest_to_keep) {
    partition_locks.pop_back();
    partitions.pop_back();
  }
  ld_check(!partitions.empty());

  ld_info("Dropping partitions [%lu, %lu), shard %u",
          prev_oldest,
          oldest_to_keep,
          shard_idx_);

  if (shutdown_event_.signaled()) {
    err = E::SHUTDOWN;
    return -1;
  }

  // 3) Mark partitions as officially dropped. After that no one will try to
  // write to our partitions. Someone might still successfully read from them,
  // but no one will complain if they can't.

  oldest_partition_id_.store(oldest_to_keep);

  for (auto partition : partitions) {
    partition->is_dropped = true;
  }

  // 4a) Remove obsolete metadata. It's slightly better to do this before
  // removing partition from partitions_ and releasing partition_locks.
  // Otherwise a writing thread may see directory entries pointing to a dropped
  // partition, which would make it busy-wait (retrying writeMultiImpl())
  // until directory is cleaned up.
  cleanUpDirectory();

  // 4b) Remove partitions from the list,
  // RocksDB will only remove data when ColumnFamilyHandles are destroyed.
  // Removes column family ptr from the accessor map as well.
  cf_accessor_.withWLock([&partitions](auto& locked_accessor) {
    for (const auto& partition : partitions) {
      locked_accessor.erase(partition->cf_->getID());
    }
  });

  partitions_.popUpTo(oldest_to_keep);

  // 4c) Now we can release threads trying to write records to our partitions.
  // They will see that partition is dropped and choose another partition.
  partition_locks.clear();

  // 4d) Drop column families.
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  cf_handles.reserve(partitions.size());
  for (auto& partition : partitions) {
    cf_handles.push_back(partition->cf_->get());
  }

  rocksdb::Status status;
  {
    SCOPED_IO_TRACING_CONTEXT(
        getIOTracing(), "drop-cfs:{}-{}", prev_oldest, oldest_to_keep - 1);
    status = db_->DropColumnFamilies(cf_handles);
  }

  if (!status.ok()) {
    // Ouch, we've already promised that it will be dropped
    // (by setting is_dropped = true).
    ld_error("Failed to drop column families [%lu, %lu]: %s",
             partitions.front()->id_,
             partitions.back()->id_,
             status.ToString().c_str());
    enterFailSafeIfFailed(status, "DropColumnFamilies()");
    error = E::LOCAL_LOG_STORE_WRITE;
    // Try to cleanup anyway.
  }

  if (shutdown_event_.signaled()) {
    err = E::SHUTDOWN;
    return -1;
  }

  cleanUpPartitionMetadataAfterDrop(oldest_to_keep);
  trimRebuildingRangesMetadata();

  STAT_ADD(stats_, partitions_dropped, partitions.size());
  STAT_SUB(stats_, partitions, partitions.size());

  ld_debug("Dropped partitions [%lu, %lu)", prev_oldest, oldest_to_keep);

  if (error != E::OK) {
    err = error;
    return -1;
  }

  return 0;
}

bool PartitionedRocksDBStore::cleanDirtyState(DirtyState& ds,
                                              FlushToken flushed_up_through) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  bool data_retired = ds.noteMemtableFlushed(
      flushed_up_through, oldestUnflushedDataTimestamp(), currentSteadyTime());
  return data_retired;
}

void PartitionedRocksDBStore::updateDirtyState(FlushToken flushed_up_through) {
  STAT_INCR(stats_, partition_cleaner_scans);

  // Prevent partition drops for simplicity.
  std::lock_guard<std::mutex> drop_lock(oldest_partition_mutex_);
  cleanDirtyState(unpartitioned_dirty_state_, flushed_up_through);

  auto partitions = getPartitionList();
  for (auto& partition : *partitions) {
    updatePartitionDirtyState(partition, flushed_up_through);
  }
}

void PartitionedRocksDBStore::updatePartitionDirtyState(
    PartitionPtr partition,
    FlushToken flushed_up_through) {
  auto& dirty_state = partition->dirty_state_;

  // Exclude clean partitions.
  if (dirty_state.min_flush_token.load() == FlushToken_MAX) {
    ld_spew("FUT(%ju): Skipping clean partition s%u:%lu",
            (uintmax_t)flushed_up_through,
            getShardIdx(),
            partition->id_);
    return;
  }

  // Only process dirty partitions that were first dirtied with
  // this or an earlier FlushToken.
  if (cleanDirtyState(dirty_state, flushed_up_through)) {
    ld_debug("FUT(%ju): Processing partition s%u:%lu",
             (uintmax_t)flushed_up_through,
             getShardIdx(),
             partition->id_);

    // Take a write lock under the assumption that we'll need to commit
    // updated dirty state.
    folly::SharedMutex::WriteHolder partition_lock(partition->mutex_);

    bool update_partition_dirty_state = false;
    bool append_dirtied = false;
    auto& nodes = dirty_state.dirtied_by_nodes;
    for (auto kv = nodes.begin(); kv != nodes.end();) {
      // Can't clean the node entry used to hold this
      // partition dirty.
      if (kv->first == DirtyState::SENTINEL_KEY) {
        ++kv;
        continue;
      }
      node_index_t nidx;
      DataClass dc;
      std::tie(nidx, dc) = kv->first;
      NodeDirtyData& ndd = kv->second;
      ld_spew("FUT(%ju): Partition s%u:%lu Before N%d:%s%s",
              (uintmax_t)flushed_up_through,
              getShardIdx(),
              partition->id_,
              nidx,
              toString(dc).c_str(),
              ndd.toString().c_str());
      bool modified_node = ndd.markCleanUpThrough(flushed_up_through);
      ld_spew("FUT(%ju): Partition s%u:%lu After N%d:%s%s",
              (uintmax_t)flushed_up_through,
              getShardIdx(),
              partition->id_,
              nidx,
              toString(dc).c_str(),
              ndd.toString().c_str());
      if (modified_node) {
        ld_debug("FUT(%ju): Partition s%u:%lu N%d:%s reset",
                 (uintmax_t)flushed_up_through,
                 getShardIdx(),
                 partition->id_,
                 nidx,
                 toString(dc).c_str());
        update_partition_dirty_state = true;
      }
      if (modified_node && ndd.isClean()) {
        ld_debug("FUT(%ju): Partition s%u:%lu N%d:%s cleaned",
                 (uintmax_t)flushed_up_through,
                 getShardIdx(),
                 partition->id_,
                 nidx,
                 toString(dc).c_str());
        kv = nodes.erase(kv);
      } else {
        if (dc == DataClass::APPEND) {
          append_dirtied = true;
        }
        ++kv;
      }
    }
    if (nodes.empty()) {
      update_partition_dirty_state = true;
      ld_debug("FUT(%ju): Partition s%u:%lu clean",
               (uintmax_t)flushed_up_through,
               getShardIdx(),
               partition->id_);
      STAT_INCR(stats_, partition_marked_clean);
    } else {
      ld_spew("FUT(%ju): Partition s%u:%lu not clean(%zd)",
              (uintmax_t)flushed_up_through,
              getShardIdx(),
              partition->id_,
              nodes.size());
    }
    if (update_partition_dirty_state) {
      if (!append_dirtied) {
        dirty_state.append_dirtied_wal_token = FlushToken_MAX;
      }

      // We don't batch to minimize partition lock hold time.
      LocalLogStore::WriteOptions write_options;
      int rv = writer_->writeMetadata(
          PartitionMetaKey(PartitionMetadataType::DIRTY, partition->id_),
          dirty_state.metadata(),
          write_options,
          metadata_cf_->get());
      if (rv != 0) {
        ld_warning("Failed to update PartitionDirtyMetadata for partition "
                   "s%u:%lu; ignoring",
                   getShardIdx(),
                   partition->id_);
      } else {
        ld_spew("FUT(%ju): Partition s%u:%lu data updated",
                (uintmax_t)flushed_up_through,
                getShardIdx(),
                partition->id_);
      }
      STAT_INCR(stats_, partition_dirty_data_updated);
    }
  }
}

bool PartitionedRocksDBStore::isHeldDirty(PartitionPtr partition) const {
  folly::SharedMutex::ReadHolder partition_lock(partition->mutex_);
  auto& dirty_nodes = partition->dirty_state_.dirtied_by_nodes;
  auto kv_it = dirty_nodes.find(DirtyState::SENTINEL_KEY);
  return kv_it != dirty_nodes.end();
}

int PartitionedRocksDBStore::holdDirty(PartitionPtr partition) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());

  folly::SharedMutex::WriteHolder partition_lock(partition->mutex_);
  auto result = partition->dirty_state_.dirtied_by_nodes.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(DirtyState::SENTINEL_KEY),
      std::forward_as_tuple());
  result.first->second.markDirtyUntil(FlushToken_MAX);

  LocalLogStore::WriteOptions write_options;
  return writer_->writeMetadata(
      PartitionMetaKey(PartitionMetadataType::DIRTY, partition->id_),
      partition->dirty_state_.metadata(),
      write_options,
      metadata_cf_->get());
}

void PartitionedRocksDBStore::releaseDirtyHold(PartitionPtr partition) {
  folly::SharedMutex::WriteHolder partition_lock(partition->mutex_);
  auto& dirty_state = partition->dirty_state_;
  if (dirty_state.dirtied_by_nodes.erase(DirtyState::SENTINEL_KEY) != 0) {
    // Reset dirty state min flush token so that the cleaner will process
    // this partition.
    atomic_fetch_min(dirty_state.min_flush_token, FlushToken_MIN);
    dirty_state.noteDirtied(flushedUpThrough(), currentSteadyTime());
    cleaner_pass_requested_.store(true);
  }
}

int PartitionedRocksDBStore::modifyUnderReplicatedTimeRange(
    TimeIntervalOp op,
    DataClass dc,
    RecordTimeInterval interval) {
  // Lock out dirty state changes from other threads.
  std::lock_guard<std::mutex> drop_lock(oldest_partition_mutex_);

  LocalLogStore::WriteOptions options;
  RebuildingRangesMetadata range_metadata;
  if (getRebuildingRanges(range_metadata) != 0) {
    err = E::FAILED;
    return -1;
  }

  if (op == TimeIntervalOp::REMOVE) {
    if (range_metadata.empty()) {
      // No-op.
      return 0;
    }
  }

  // Add/Remove dirty time ranges to durable store metadata.
  // NOTE: We allow intervals to be added even if they do not match
  //       existing partitions. This allows the administrator to force
  //       the cluster to re-replicate ranges where local log store data
  //       has been corrupted or partially lost. The node will publish the
  //       added ranges, causing any data in the ranges available on other
  //       nodes to be re-replicated. However, the node will not self
  //       identify under-replication to readers for ranges that are not
  //       covered by partitions.
  range_metadata.modifyTimeIntervals(op, dc, interval);

  if (range_metadata.empty() && (op == TimeIntervalOp::REMOVE)) {
    // If removing the interval makes the entire shard clean, then
    // expand the interval to cover all time so that even partitions that
    // are only partially covered by the original interval will be marked
    // clean.
    interval =
        RecordTimeInterval(RecordTimestamp::min(), RecordTimestamp::max());
  }

  auto partitions = getPartitionList();
  rocksdb::WriteBatch dirty_batch;
  RecordTimeIntervals intervals{interval};
  findPartitionsMatchingIntervals(
      intervals,
      [&](PartitionPtr partition,
          RecordTimeInterval /* partition's time interval */) {
        folly::SharedMutex::WriteHolder partition_lock(partition->mutex_);
        if (op == TimeIntervalOp::ADD) {
          setUnderReplicated(partition);
          writePartitionDirtyState(partition, dirty_batch);
        } else if (partition->max_timestamp != RecordTimestamp::min()) {
          auto dirty_range = RecordTimeInterval(
              partition->min_timestamp, partition->max_timestamp);
          // Can we fully clear the under-replicatedness of this partition?
          if ((interval & dirty_range) == dirty_range) {
            setUnderReplicated(partition, false);
            writePartitionDirtyState(partition, dirty_batch);
          }
        }
      },
      /*include empty partitions*/ true);

  if (writeRebuildingRanges(range_metadata) != 0) {
    err = E::FAILED;
    return -1;
  }
  auto status = writeBatch(rocksdb::WriteOptions(), &dirty_batch);
  if (!status.ok()) {
    ld_error("Failed to write partition dirty data for shard %u: %s",
             getShardIdx(),
             status.ToString().c_str());
    err = E::FAILED;
    return -1;
  }
  return 0;
}

int PartitionedRocksDBStore::trimLogsToExcludePartitions(
    partition_id_t oldest_to_keep) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  ServerProcessor* processor = processor_.load();

  if (!processor) {
    // We're not fully initialized yet.
    err = E::AGAIN;
    return -1;
  }

  LocalLogStoreUtils::TrimPointUpdateMap new_trim_points;

  PartitionDirectoryIterator iterator(*this);
  while (iterator.nextLog()) {
    lsn_t trim_point = LSN_INVALID;
    while (iterator.nextPartition() &&
           iterator.getPartitionID() < oldest_to_keep) {
      trim_point = iterator.getLastLSN();
    }
    ld_debug("Advancing trim point for log %lu to %s",
             iterator.getLogID().val_,
             lsn_to_string(trim_point).c_str());
    new_trim_points[iterator.getLogID()] = trim_point;
  }

  if (iterator.error()) {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  return LocalLogStoreUtils::updateTrimPoints(new_trim_points,
                                              processor,
                                              *this,
                                              /* sync */ true,
                                              stats_);
}

bool PartitionedRocksDBStore::PartialCompactionEvaluator::evaluateAll(
    std::vector<PartitionToCompact>* out_to_compact,
    size_t max_results) {
  ld_check(out_to_compact);
  if (max_results == 0) {
    return false;
  }
  // First we make a list of all file ranges it makes sense to compact in
  // every partition.
  for (size_t i = 0; i < partitions_.size(); ++i) {
    std::chrono::hours age_hours = RecordTimestamp::now().toHours() -
        partitions_[i]->starting_timestamp.toHours();
    // There are 3 cases here:
    // 1. The last two partitions are likely to be in use. Rather than partially
    // compacting them now, it's better to wait and give them a chance to
    // accumulate more small files: new_partition
    // 2. Older partitions (e.g., >= 1 day old) are less likley to be in the
    // read path and so can be compacted at a higher threshold.
    // 3. But if the partition is recent (e.g., < 1 day old) then use a lower
    // threshold for num-files to compact: recent_partition.
    bool new_partition = i + 2 >= partitions_.size();
    bool recent_partition = age_hours.count() < old_age_hours_.count();

    auto metadata = std::make_shared<rocksdb::ColumnFamilyMetaData>();
    deps_->getColumnFamilyMetaData(partitions_[i]->cf_->get(), metadata.get());
    // l0 files for a partition
    ld_check_gt(metadata->levels.size(), 0);
    auto& level0 = metadata->levels[0];

    // TODO: optimize this
    size_t min_num = min_files_old_;
    if (new_partition) {
      min_num = std::max(min_num, (size_t)(max_files_ * 0.7));
    } else if (recent_partition) {
      min_num = min_files_recent_;
    }
    for (size_t start_idx = 0; start_idx + min_num <= level0.files.size();
         ++start_idx) {
      size_t max_num = std::min(max_files_, level0.files.size() - start_idx);
      evaluate(metadata, i, start_idx, min_num, max_num);
    }
  }

  // Making a heap out of candidates to select the best ones
  auto cmp = [](const CompactionCandidate& a, const CompactionCandidate& b) {
    return a.value < b.value;
  };
  std::priority_queue<CompactionCandidate,
                      std::vector<CompactionCandidate>,
                      decltype(cmp)>
      candidate_heap(cmp, std::move(candidates_));

  // Picking the final set of non-overlapping compactions
  std::unordered_multimap<size_t, CompactionCandidate> idx; // indexed by
                                                            // partition

  auto candidates_overlap = [](const CompactionCandidate& a,
                               const CompactionCandidate& b) {
    return a.start_idx < b.end_idx() && b.start_idx < a.end_idx();
  };
  while (!candidate_heap.empty()) {
    auto& candidate = candidate_heap.top();
    // check if this overlaps with any of the ranges we already selected
    auto its = idx.equal_range(candidate.partition_offset);
    bool overlap = false;
    for (auto it = its.first; it != its.second; ++it) {
      if (candidates_overlap(candidate, it->second)) {
        overlap = true;
        break;
      }
    }
    // Only add candidate to result if there is no overlap with others in
    // the result set
    if (!overlap) {
      PartitionToCompact p(partitions_[candidate.partition_offset],
                           PartitionToCompact::Reason::PARTIAL);
      for (size_t file_offset = candidate.start_idx;
           file_offset < candidate.end_idx();
           ++file_offset) {
        ld_check_lt(file_offset, candidate.metadata->levels[0].files.size());
        p.partial_compaction_filenames.push_back(
            candidate.metadata->levels[0].files[file_offset].name);
        p.partial_compaction_file_sizes.push_back(
            candidate.metadata->levels[0].files[file_offset].size);
      }
      idx.emplace(candidate.partition_offset, candidate);
      out_to_compact->push_back(std::move(p));

      if (idx.size() == max_results) {
        // Enough candidates - stop looping through the heap
        break;
      }
    }
    candidate_heap.pop();
  }
  return !candidate_heap.empty();
}
void PartitionedRocksDBStore::PartialCompactionEvaluator::evaluate(
    std::shared_ptr<rocksdb::ColumnFamilyMetaData>& metadata,
    size_t partition,
    size_t start_idx,
    size_t min_num_files,
    size_t max_num_files) {
  size_t num_files;
  double value = compactionValue(&metadata->levels[0].files[start_idx],
                                 min_num_files,
                                 max_num_files,
                                 &num_files);
  if (value > 0.0) {
    candidates_.push_back({value, metadata, partition, start_idx, num_files});
  }
}

double PartitionedRocksDBStore::PartialCompactionEvaluator::compactionValue(
    const rocksdb::SstFileMetaData* sst_files,
    size_t min_num,
    size_t max_num,
    size_t* res_num) {
  ld_check(res_num);
  *res_num = 0;

  size_t largest_file_size = 0;
  size_t total_size = 0;
  double value = 0.0;
  double best_value = 0.0;
  for (size_t i = 0; i < max_num; ++i) {
    auto size = sst_files[i].size;
    if (size > max_file_size_) {
      return best_value;
    }
    // This is the core of the value calculation - it's
    // (max sensible compaction size) - (this file's size)
    value += double(max_avg_file_size_) - double(size);
    total_size += size;
    if (size > largest_file_size) {
      largest_file_size = size;
    }
    if (largest_file_size > max_largest_file_share_ * total_size) {
      continue;
      ;
    }
    if (i >= min_num - 1) {
      // consider files if their total number is at least min_num
      if (value > best_value) {
        best_value = value;
        *res_num = i + 1;
      }
    }
  }
  return best_value;
}

bool PartitionedRocksDBStore::getPartitionsForPartialCompaction(
    std::vector<PartitionToCompact>* out_to_compact,
    size_t max_results) {
  ld_check(out_to_compact);
  auto start = SteadyTimestamp::now();
  size_t results_before = out_to_compact->size();
  SCOPE_EXIT {
    auto now = SteadyTimestamp::now();
    auto secs = SteadyTimestamp(now - start).toSeconds();
    if (secs.count() > 10) {
      ld_warning("Evaluating partitions for partial compaction took %lu "
                 "seconds. %lu results returned.",
                 secs.count(),
                 out_to_compact->size() - results_before);
    }
  };

  auto partitions = getPartitionList();

  auto settings = getSettings();

  PartialCompactionEvaluator evaluator(
      std::vector<PartitionPtr>(partitions->begin(), partitions->end()),
      settings->partition_partial_compaction_old_age_threshold_,
      settings->partition_partial_compaction_file_num_threshold_old_,
      settings->partition_partial_compaction_file_num_threshold_recent_,
      settings->partition_partial_compaction_max_files_,
      settings->partition_partial_compaction_file_size_threshold_,
      settings->partition_partial_compaction_max_file_size_ > 0
          ? settings->partition_partial_compaction_max_file_size_
          : settings->partition_partial_compaction_file_size_threshold_ * 2,
      settings->partition_partial_compaction_largest_file_share_,
      std::make_unique<PartialCompactionEvaluator::DBDeps>(db_.get()));
  return evaluator.evaluateAll(out_to_compact, max_results);
}

void PartitionedRocksDBStore::getPartitionsForProactiveCompaction(
    std::vector<PartitionToCompact>* out_to_compact) {
  ld_check(out_to_compact);

  bool proactive_enabled = getSettings()->proactive_compaction_enabled;
  partition_id_t latest = latest_.get()->id_;

  auto partitions = getPartitionList();
  // We do not compact proactively two latest partitions because they might be
  // in use.
  for (PartitionPtr partition : *partitions) {
    if (partition->id_ + 1 >= latest) {
      // Don't compact two latest partitions.
      break;
    }

    if (proactive_enabled &&
        partition->last_compaction_time == RecordTimestamp::min()) {
      out_to_compact->emplace_back(
          partition, PartitionToCompact::Reason::PROACTIVE);
    }
  }
}

bool PartitionedRocksDBStore::getPartitionsForManualCompaction(
    std::vector<PartitionToCompact>* out_to_compact,
    size_t count,
    bool hi_pri) {
  ld_check(out_to_compact);
  std::lock_guard<std::mutex> lock(manual_compaction_mutex_);
  auto& target_list =
      hi_pri ? hi_pri_manual_compactions_ : lo_pri_manual_compactions_;
  auto partitions = getPartitionList();
  auto it = target_list.begin();
  while (it != target_list.end() && count > 0) {
    partition_id_t partition_id = *it;
    auto partition = partitions->get(partition_id);
    if (partition) {
      out_to_compact->emplace_back(
          partition, PartitionToCompact::Reason::MANUAL);
      --count;
    }
    ++it;
  }
  return !target_list.empty();
}

void PartitionedRocksDBStore::scheduleManualCompaction(
    partition_id_t partition_id,
    bool hi_pri) {
  ld_check_ne(partition_id, PARTITION_INVALID);
  std::lock_guard<std::mutex> lock(manual_compaction_mutex_);
  // cancelling any other manual compactions for this partition if they are
  // scheduled
  cancelManualCompactionImpl(lock, partition_id);
  auto& target_list =
      hi_pri ? hi_pri_manual_compactions_ : lo_pri_manual_compactions_;
  auto list_it = target_list.insert(target_list.end(), partition_id);
  auto insert_res =
      manual_compaction_index_.insert({partition_id, {hi_pri, list_it}});
  // there should be no other elements with this key, so the insert should've
  // been successful
  ld_check(insert_res.second);
}

void PartitionedRocksDBStore::cancelManualCompactionImpl(
    std::lock_guard<std::mutex>& /*lock*/,
    partition_id_t partition_id) {
  if (partition_id == PARTITION_INVALID) {
    // Cancelling all manual compactions on this shard
    lo_pri_manual_compactions_.clear();
    hi_pri_manual_compactions_.clear();
    manual_compaction_index_.clear();
    return;
  }
  auto index_it = manual_compaction_index_.find(partition_id);
  if (index_it != manual_compaction_index_.end()) {
    bool hi_pri = index_it->second.first;
    auto& list_iterator = index_it->second.second;
    auto& target_list =
        hi_pri ? hi_pri_manual_compactions_ : lo_pri_manual_compactions_;
    target_list.erase(list_iterator);
    manual_compaction_index_.erase(index_it);
  }
}

void PartitionedRocksDBStore::cancelManualCompaction(
    partition_id_t partition_id) {
  std::lock_guard<std::mutex> lock(manual_compaction_mutex_);
  cancelManualCompactionImpl(lock, partition_id);
}

std::list<std::pair<partition_id_t, bool>>
PartitionedRocksDBStore::getManualCompactionList() {
  std::lock_guard<std::mutex> lock(manual_compaction_mutex_);
  std::list<std::pair<partition_id_t, bool>> res;
  auto populate = [&](std::list<partition_id_t>& list, bool hi_pri) {
    for (partition_id_t p : list) {
      res.emplace_back(p, hi_pri);
    }
  };
  populate(hi_pri_manual_compactions_, true);
  populate(lo_pri_manual_compactions_, false);
  return res;
}

folly::Optional<std::chrono::seconds>
PartitionedRocksDBStore::getEffectiveBacklogDuration(
    logid_t log_id,
    size_t* out_logs_in_grace_period) {
  folly::Optional<std::chrono::seconds> backlog;
  ServerProcessor* processor = processor_.load();
  if (!processor) {
    // We're not fully initialized yet.
    return folly::none;
  }

  auto config = processor->config_->get();
  const auto settings = getSettings();
  const auto now = currentTime().toSeconds();
  auto test_override = settings->test_clamp_backlog;

  LogStorageStateMap& state_map = processor->getLogStorageStateMap();
  LogStorageState* log_state = state_map.insertOrGet(log_id, getShardIdx());
  if (!log_state) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "LogStorageState for log:%lu couldn't be created",
                      log_id.val());
    return folly::none; // be conservative in deleting data
  }

  std::chrono::seconds grace_period =
      settings->unconfigured_log_trimming_grace_period_;
  if (test_override.count() != 0) {
    grace_period = std::min(grace_period, test_override);
  }

  std::chrono::seconds log_removal_time = log_state->getLogRemovalTime();
  if (log_removal_time == std::chrono::seconds(0)) {
    // This field of LogStorageState is set to '0' by default to indicate
    // that some other state machine may have put the entry into the map,
    // without reading LogRemovalTimeMetadata, allowing this code to
    // read it only once by setting it to either std::chrono::seconds::max()
    // or some other timestamp.
    LogRemovalTimeMetadata meta;
    readLogMetadata(log_id, &meta);
    log_removal_time = meta.log_removal_time_;
    log_state->setLogRemovalTime(log_removal_time);
  }

  const std::shared_ptr<LogsConfig::LogGroupNode> log_config =
      config->getLogGroupByIDShared(log_id);
  if (!log_config) {
    std::chrono::seconds no_trim_until =
        std::chrono::seconds::max() == log_removal_time
        ? log_removal_time
        : log_removal_time + grace_period;
    // log no longer in config, but data is present in store
    if (log_removal_time == std::chrono::seconds::max()) {
      // start grace period
      LocalLogStore::WriteOptions options;
      writeLogMetadata(log_id, LogRemovalTimeMetadata(now), options);
      log_state->setLogRemovalTime(now);
      (*out_logs_in_grace_period)++;
      return folly::none;
    } else if (no_trim_until >= now) {
      (*out_logs_in_grace_period)++;
      ld_spew("grace period(%lds) hasn't expired yet(now=%lds) for log:%lu",
              no_trim_until.count(),
              now.count(),
              log_id.val());
      return folly::none;
    } else {
      // at this stage, log has been absent from logs config long enough
      // (grace period expired), its ok to trim the log if required.
      ld_spew("grace period(%lds) for log:%lu expired, now:%lds",
              no_trim_until.count(),
              log_id.val_,
              now.count());
      backlog = std::chrono::seconds(0);
    }
  } else { // check if log reappeared
    if (log_removal_time != std::chrono::seconds::max()) {
      LocalLogStore::WriteOptions options;
      writeLogMetadata(
          log_id, LogRemovalTimeMetadata(std::chrono::seconds::max()), options);
      log_state->setLogRemovalTime(std::chrono::seconds::max());
    }
    backlog = log_config->attrs().backlogDuration().value();

    if (test_override.count() != 0 && backlog.hasValue()) {
      backlog = std::min(backlog.value(), test_override);
    }
  }

  return backlog;
}

int PartitionedRocksDBStore::trimLogsBasedOnTime() {
  size_t logs_in_grace_period = 0;
  return trimLogsBasedOnTime(nullptr, nullptr, &logs_in_grace_period);
}

int PartitionedRocksDBStore::trimLogsBasedOnTime(
    partition_id_t* out_oldest_to_keep,
    std::vector<PartitionToCompact>* out_to_compact,
    size_t* out_logs_in_grace_period) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  ServerProcessor* processor = processor_.load();

  if (!processor) {
    // We're not fully initialized yet.
    err = E::AGAIN;
    return -1;
  }

  auto config = processor->config_->get();
  if (!config->logsConfig()->isFullyLoaded()) {
    // LogsConfig is not fully loaded yet.
    err = E::AGAIN;
    return -1;
  }

  LogStorageStateMap& state_map = processor->getLogStorageStateMap();
  LocalLogStoreUtils::TrimPointUpdateMap new_trim_points;

  // Data maintained for each distinct backlog duration. Needed to decide
  // which partitions need compaction.
  // If RocksDBSettings::partition_compaction_schedule is given, use durations
  // from it instead.
  struct DataForBacklogDuration {
    // First partition that may have non-trimmed records with this backlog,
    // i.e. partition p so that:
    //   starting_timestamp[p]  <= now - backlog_duration, and
    //   starting_timestamp[p+1] > now - backlog_duration.
    // This is only used for compactions, not for trim points.
    partition_id_t oldest_non_trimmed;

    // Partitions below oldest_non_trimmed that likely have trimmed records
    // for logs with this backlog duration. These are the partitions we're
    // going to compact.
    std::set<partition_id_t> partitions_with_trimmed_records;
  };
  std::map<std::chrono::seconds, DataForBacklogDuration> backlog_durations;

  const partition_id_t latest = latest_.get()->id_;
  const auto partitions = partitions_.getVersion();
  const partition_id_t oldest_existing = partitions->firstID();
  const auto settings = getSettings();
  const auto now = currentTime().toSeconds();
  partition_id_t oldest_to_keep = latest;

  PartitionDirectoryIterator iterator(*this);
  while (iterator.nextLog()) {
    logid_t log_id = iterator.getLogID();
    folly::Optional<std::chrono::seconds> backlog =
        getEffectiveBacklogDuration(log_id, out_logs_in_grace_period);

    // For each log we need the following:
    //  - updated trim point for this log
    //  - first partition that has non-trimmed records for this log,
    //  - all partitions that have trimmed records for this log
    //    (we'll want to run compaction on them).

    DataForBacklogDuration* duration_data = nullptr;
    if (backlog.hasValue() && out_to_compact != nullptr) {
      auto backlog_bucket = backlog;

      // settings->partition_compaction_schedule has the effect of rounding
      // up backlog durations to the nearest element of the list.
      // This only affects compaction decisions, not trim points.
      auto partition_compaction_schedule =
          settings->partition_compaction_schedule;
      if (partition_compaction_schedule.hasValue() &&
          !partition_compaction_schedule.value().empty()) {
        auto it =
            std::lower_bound(partition_compaction_schedule.value().begin(),
                             partition_compaction_schedule.value().end(),
                             backlog.value());
        if (it == partition_compaction_schedule.value().end()) {
          backlog_bucket.clear();
        } else {
          backlog_bucket = *it;
        }
      }

      if (backlog_bucket.hasValue()) {
        bool new_duration = !backlog_durations.count(backlog_bucket.value());
        duration_data = &backlog_durations[backlog_bucket.value()];
        if (new_duration) {
          // If we haven't seen this backlog duration yet, find its cutoff
          // partition.
          duration_data->oldest_non_trimmed = getPreferredPartition(
              RecordTimestamp(now - std::min(now, backlog_bucket.value())),
              false,
              partitions,
              latest,
              oldest_existing);
        }
      }
    }

    RecordTimestamp cutoff_timestamp = RecordTimestamp::min();
    if (backlog.hasValue() && backlog.value() < now) {
      cutoff_timestamp = RecordTimestamp::from(now - backlog.value());
    }

    // Get existing trim point

    LogStorageState* log_state = state_map.insertOrGet(log_id, getShardIdx());
    ld_check(log_state != nullptr);

    if (!log_state->getTrimPoint().hasValue()) {
      // If the trim point is not known yet, just read it from metadata CF.
      TrimMetadata meta{LSN_INVALID};
      int rv = readLogMetadata(log_id, &meta);
      if (rv == 0 || err == E::NOTFOUND) {
        log_state->updateTrimPoint(meta.trim_point_);
      } else {
        enterFailSafeMode("PartitionedRocksDBStore::trimLogsBasedOnTime()",
                          "Failed to read TrimMetadata");
        log_state->notePermanentError(
            "Reading trim point (in trimLogsBasedOnTime)");
        err = E::LOCAL_LOG_STORE_READ;
        return -1;
      }
    }
    lsn_t existing_trim_point = log_state->getTrimPoint().value();

    // Iterate over partitions.

    lsn_t new_trim_point = LSN_INVALID;

    while (iterator.nextPartition()) {
      partition_id_t partition = iterator.getPartitionID();

      // A paranoid check in case there's a bug that would cause the LSN_MAX
      // from PartitionDirectoryValue::getMaxLSN(blob, size, false) to end up
      // here.
      if (!dd_assert(iterator.getLastLSN() != LSN_MAX,
                     "Tried to trim up to LSN_MAX. There's a bug somewhere. "
                     "Not trimming anything.")) {
        err = E::INTERNAL;
        return -1;
      }

      // If directory entry points to partition that appears to be dropped,
      // it may be a partition that was created by another thread after
      // we snapshotted the list of partitions.
      partition_id_t next_partition = std::max(oldest_existing, partition + 1);

      if (iterator.getLastLSN() > existing_trim_point &&
          (next_partition > latest ||
           partitions->get(next_partition)->starting_timestamp >
               cutoff_timestamp)) {
        // This is first partition that has non-trimmed records for this log.
        oldest_to_keep = std::min(oldest_to_keep, partition);
        break;
      }

      new_trim_point = iterator.getLastLSN();
      if (duration_data != nullptr &&
          partition < duration_data->oldest_non_trimmed) {
        duration_data->partitions_with_trimmed_records.insert(partition);
      }
    }

    if (new_trim_point > existing_trim_point) {
      new_trim_points[log_id] = new_trim_point;
    }
  }

  if (iterator.error()) {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  if (out_to_compact != nullptr || out_oldest_to_keep != nullptr) {
    // Prevent empty dirty partitions from being trimmed away. We need
    // them to exist so that under-replicated regions are properly reported
    // to iterators.
    //
    // NOTE: Since we may update oldest_to_keep, this can increase the
    //       number of partitions added to out_to_compact below.
    auto min_cutoff_timestamp =
        RecordTimestamp(now - config->getMaxBacklogDuration());
    auto min_dirty_partition = min_under_replicated_partition_.load();
    auto max_dirty_partition = max_under_replicated_partition_.load();
    while (min_dirty_partition < oldest_to_keep) {
      if (min_dirty_partition > max_dirty_partition) {
        // All dirty partitions have been trimmed away due to retention.
        break;
      }
      auto partition = partitions->get(min_dirty_partition);
      RecordTimestamp max_timestamp = partition->max_timestamp;
      if (max_timestamp == RecordTimestamp::min()) {
        // Empty partition. It covers up to the start of the next
        // partition. Accessing the next partition is safe here because
        // min_dirty_partition is less than oldest_to_keep.
        max_timestamp =
            partitions->get(min_dirty_partition + 1)->starting_timestamp;
      }
      if (partition->isUnderReplicated() &&
          max_timestamp >= min_cutoff_timestamp) {
        oldest_to_keep = min_dirty_partition;
        break;
      }
      ++min_dirty_partition;
    }
  }

  if (out_to_compact != nullptr) {
    for (auto& it : backlog_durations) {
      // Advise compacting those partitions for which all records for logs with
      // this backlog duration have been marked as trimmed.
      for (partition_id_t partition :
           it.second.partitions_with_trimmed_records) {
        ld_check_lt(partition, it.second.oldest_non_trimmed);
        if (partition >= oldest_to_keep &&
            partitions->get(partition)->compacted_retention.load() < it.first) {
          out_to_compact->emplace_back(partitions->get(partition), it.first);
        }
      }
    }
  }

  if (out_oldest_to_keep != nullptr) {
    *out_oldest_to_keep = oldest_to_keep;
  }

  return LocalLogStoreUtils::updateTrimPoints(new_trim_points,
                                              processor,
                                              *this,
                                              /* sync */ true,
                                              stats_);
}

partition_id_t PartitionedRocksDBStore::findObsoletePartitions() {
  ServerProcessor* processor = processor_.load();

  if (!processor) {
    // We're not fully initialized yet.
    return PARTITION_INVALID;
  }

  LogStorageStateMap& state_map = processor->getLogStorageStateMap();

  partition_id_t oldest_to_keep = latest_.get()->id_;

  PartitionDirectoryIterator iterator(*this);
  while (iterator.nextLog()) {
    logid_t log_id = iterator.getLogID();

    LogStorageState* log_state = state_map.find(log_id, getShardIdx());
    if (!log_state) {
      // trimLogs* will initialise log state.
      return PARTITION_INVALID;
    }
    auto trim_point = log_state->getTrimPoint();
    if (!trim_point.hasValue()) {
      // trimLogs* will initialise trim point.
      return PARTITION_INVALID;
    }

    while (iterator.nextPartition()) {
      partition_id_t partition = iterator.getPartitionID();
      if (iterator.getLastLSN() > trim_point.value()) {
        // This is first partition that has non-trimmed records for this log.
        oldest_to_keep = std::min(oldest_to_keep, partition);
        break;
      }
    }
  }

  if (iterator.error()) {
    return PARTITION_INVALID;
  }

  return oldest_to_keep;
}

void PartitionedRocksDBStore::performCompactionInternal(
    PartitionToCompact to_compact) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  PartitionPtr partition = to_compact.partition;
  partition_id_t partition_id = partition->id_;
  if (to_compact.reason != PartitionToCompact::Reason::PARTIAL &&
      partition_id == latest_.get()->id_) {
    ld_warning("Tried to compact latest partition %lu", partition_id);
    return;
  }

  {
    bool partial = to_compact.reason == PartitionToCompact::Reason::PARTIAL;
    std::string last_compacted;
    if (partition->last_compaction_time == RecordTimestamp::min()) {
      last_compacted = "never";
    } else {
      auto ago = currentTime() - partition->last_compaction_time;
      auto ago_seconds = RecordTimestamp(ago).toSeconds();
      last_compacted = std::to_string(ago_seconds.count()) + " seconds ago";
    }

    std::string partial_info;
    if (partial) {
      partial_info += " (files [";
      for (size_t i = 0; i < to_compact.partial_compaction_filenames.size();
           ++i) {
        if (i) {
          partial_info += ", ";
        }
        const std::string& name = to_compact.partial_compaction_filenames[i];
        // Turn "/129171.sst" into "129171".
        std::string short_name;
        {
          size_t suf = 0; // how much to cut at end
          if (name.size() >= 4 &&
              name.compare(name.size() - 4, 4, ".sst") == 0) {
            suf = 4;
          }
          size_t pref = name.find_last_of('/'); // how much to cut at beginning
          if (pref == std::string::npos) {
            pref = 0;
          } else {
            ++pref;
          }
          short_name = name.substr(pref, name.size() - pref - suf);
        }
        folly::format(&partial_info,
                      "{}: {:.3f} MB",
                      short_name.c_str(),
                      to_compact.partial_compaction_file_sizes[i] / 1e6);
      }
      partial_info += "])";
    }

    ld_log((partial && partition_id < latest_.get()->id_ &&
            !getSettings()->print_details)
               ? dbg::Level::DEBUG
               : dbg::Level::INFO,
           "Starting %spartial compaction%s of partition %lu (%.3f MB), "
           "reason: %s, "
           "last compacted: %s, shard %u",
           partial ? "" : "non-",
           partial_info.c_str(),
           partition_id,
           getApproximatePartitionSize(to_compact.partition->cf_->get()) / 1e6,
           PartitionToCompact::reasonNames()[to_compact.reason].c_str(),
           last_compacted.c_str(),
           shard_idx_);
  }

  STAT_INCR(stats_, partition_compactions_in_progress);
  SCOPE_EXIT {
    STAT_DECR(stats_, partition_compactions_in_progress);
  };

  auto start_time = currentSteadyTime();

  if (to_compact.reason == PartitionToCompact::Reason::RETENTION &&
      !getSettings()->force_no_compaction_optimizations_) {
    bool ok = performStronglyFilteredCompactionInternal(partition);
    if (!ok) {
      return;
    }
  } else {
    auto factory = checked_downcast<RocksDBCompactionFilterFactory*>(
        rocksdb_config_.options_.compaction_filter_factory.get());

    CompactionContext context;
    context.reason = to_compact.reason;
    rocksdb::Status status;

    {
      // This will wait for other compactions to finish.
      auto compaction_lock = factory->startUsingContext(&context);

      if (shutdown_event_.signaled()) {
        return;
      }

      if (to_compact.reason == PartitionToCompact::Reason::PARTIAL) {
        SCOPED_IO_TRACING_CONTEXT(
            getIOTracing(), "part-compact|cf:{}", partition->id_);
        rocksdb::CompactionOptions options;
        options.compression = rocksdb_config_.options_.compression;

        status = db_->CompactFiles(options,
                                   partition->cf_->get(),
                                   to_compact.partial_compaction_filenames,
                                   0 /* L0 */);
      } else {
        // This context currently doesn't do anything because full compactions
        // run on background threads. But let's keep it in case this changes.
        SCOPED_IO_TRACING_CONTEXT(
            getIOTracing(), "full-compact|cf:{}", partition->id_);

        status = db_->CompactRange(rocksdb::CompactRangeOptions(),
                                   partition->cf_->get(),
                                   nullptr,
                                   nullptr);
      }
    }

    if (!status.ok()) {
      enterFailSafeIfFailed(status, "CompactRange()/CompactFiles()");
      return;
    }
  }

  if (to_compact.reason != PartitionToCompact::Reason::PARTIAL) {
    auto cleanup_start_time = currentSteadyTime();

    std::lock_guard<std::mutex> lock(oldest_partition_mutex_);
    cleanUpDirectory({partition_id});

    auto cleanup_end_time = currentSteadyTime();
    auto msec_taken = std::chrono::duration_cast<std::chrono::milliseconds>(
        cleanup_end_time - cleanup_start_time);
    STAT_ADD(stats_, partitions_compaction_cleanup_time, msec_taken.count());
  }

  auto end_time = currentSteadyTime();
  auto msec_taken = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  STAT_INCR(stats_, partitions_compacted);
  STAT_ADD(stats_, partitions_compaction_time, msec_taken.count());

  ld_debug("compacted partition %lu", partition_id);

  // Update partition metadata.

  LocalLogStore::WriteOptions write_options;

  if (to_compact.reason == PartitionToCompact::Reason::PROACTIVE) {
    STAT_INCR(stats_, partition_proactive_compactions);
  } else if (to_compact.reason == PartitionToCompact::Reason::MANUAL) {
    STAT_INCR(stats_, partition_manual_compactions);
  } else if (to_compact.reason == PartitionToCompact::Reason::PARTIAL) {
    STAT_INCR(stats_, partition_partial_compactions);
    STAT_ADD(stats_, partitions_partial_compaction_time, msec_taken.count());
    STAT_ADD(stats_,
             partitions_partial_compaction_files,
             to_compact.partial_compaction_filenames.size());
  }

  if (to_compact.reason == PartitionToCompact::Reason::PARTIAL) {
    // Not updating last compaction time for partial compactions
    return;
  }

  if (to_compact.reason == PartitionToCompact::Reason::RETENTION &&
      atomic_fetch_max(partition->compacted_retention, to_compact.retention) !=
          to_compact.retention) {
    // This is racy: other thread can concurrently write a smaller value and
    // win the race. No big deal, worst that can happen is we compact partition
    // one more time.
    int rv = writer_->writeMetadata(
        PartitionMetaKey(
            PartitionMetadataType::COMPACTED_RETENTION, partition_id),
        PartitionCompactedRetentionMetadata(to_compact.retention),
        write_options,
        metadata_cf_->get());
    if (rv != 0) {
      ld_warning("Failed to store last compacted retention for partition %lu; "
                 "ignoring",
                 partition_id);
    }
  }

  partition->last_compaction_time = RecordTimestamp(currentTime());
  int rv = writer_->writeMetadata(
      PartitionMetaKey(PartitionMetadataType::LAST_COMPACTION, partition_id),
      PartitionTimestampMetadata(PartitionMetadataType::LAST_COMPACTION,
                                 partition->last_compaction_time),
      write_options,
      metadata_cf_->get());
  if (rv != 0) {
    ld_warning("Failed to store last compaction time of partition %lu; "
               "ignoring",
               partition_id);
  }
}

bool PartitionedRocksDBStore::performStronglyFilteredCompactionInternal(
    PartitionPtr partition) {
  ServerProcessor* processor = processor_.load();
  if (!processor) {
    // Can't compact in this mode without trim points.
    return false;
  }
  LogStorageStateMap& log_state_map = processor->getLogStorageStateMap();

  // We expect to throw away a big fraction of the logs in this partition.
  // Instead of reading everything and throwing away trimmed records,
  // use directory to find a list of logs that still have non-trimmed records
  // in this partition, and make compaction filter only read these logs.
  // To avoid race with writing new records, do it in this order:
  //  1. grab the list of all sst files in the partition,
  //  2. read directory to build list of logs that have non-trimmed records in
  //     this partition,
  //  3. compact only the files from 1.; this ensures that the list of logs is
  //     at least as up-to-date as the data we're compacting.

  // But first flush the memtable if there is one. Not really necessary but
  // convenient for tests and consistent with what rocksdb::DB::CompactRange()
  // does.
  if (!flushMemtable(partition->cf_)) {
    ld_error(
        "Won't compact partition %lu because flush failed", partition->id_);
    return false;
  }

  // 1.
  std::vector<std::string> files_to_compact;
  rocksdb::ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(partition->cf_->get(), &cf_meta);
  ld_check_gt(cf_meta.levels.size(), 0);
  for (const auto& f : cf_meta.levels[0].files) {
    files_to_compact.push_back(f.name);
  }

  if (files_to_compact.empty()) {
    ld_info("Won't compact partition %lu because it's empty", partition->id_);
    return true; // count it as a success
  }

  // 2.
  CompactionContext compaction_context;
  compaction_context.reason = PartitionToCompact::Reason::RETENTION;
  compaction_context.logs_to_keep.emplace();
  size_t logs_seen = 0;
  PartitionDirectoryIterator iterator(*this);
  while (iterator.nextLog()) {
    logid_t log_id = iterator.getLogID();

    LogStorageState* log_state = log_state_map.find(log_id, getShardIdx());
    if (!log_state) {
      // Should almost never happen. Assume not trimmed.
      compaction_context.logs_to_keep->push_back(log_id);
      continue;
    }
    auto trim_point = log_state->getTrimPoint();
    if (!trim_point.hasValue()) {
      // Ditto.
      compaction_context.logs_to_keep->push_back(log_id);
      continue;
    }

    while (iterator.nextPartition()) {
      partition_id_t id = iterator.getPartitionID();
      if (id == partition->id_) {
        ++logs_seen;
        if (iterator.getLastLSN() > trim_point.value()) {
          // This log has records above trim point in this partition.
          compaction_context.logs_to_keep->push_back(log_id);
        }
      }
      if (id >= partition->id_) {
        break;
      }
    }
  }

  if (iterator.error()) {
    ld_warning("Won't compact partition %lu because reading directory failed",
               partition->id_);
    return false;
  }

  ld_info("Compaction will keep %lu/%lu logs",
          compaction_context.logs_to_keep->size(),
          logs_seen);

  auto filter_factory = dynamic_cast<RocksDBCompactionFilterFactory*>(
      rocksdb_config_.options_.compaction_filter_factory.get());
  ld_check(filter_factory);
  rocksdb::Status status;

  {
    // This will wait for other compactions to finish.
    auto compaction_lock =
        filter_factory->startUsingContext(&compaction_context);

    if (shutdown_event_.signaled()) {
      return false;
    }

    rocksdb::CompactionOptions options;
    options.compression = rocksdb_config_.options_.compression;

    SCOPED_IO_TRACING_CONTEXT(
        getIOTracing(), "filter-compact|cf:{}", partition->id_);
    status = db_->CompactFiles(
        options, partition->cf_->get(), files_to_compact, 0 /* L0 */);
  }

  if (!status.ok()) {
    enterFailSafeIfFailed(status, "CompactFiles()");
    return false;
  }

  return true;
}

void PartitionedRocksDBStore::cleanUpDirectory(
    const std::set<partition_id_t>& compacted_partitions) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  bool check_full_directory = false;
  if (compacted_partitions.empty()) {
    SteadyTimestamp last_directory_consistency_check_time =
        last_directory_consistency_check_time_;
    auto period = getSettings()->directory_consistency_check_period;
    auto now = currentSteadyTime();

    if (period.count() > 0 &&
        last_directory_consistency_check_time < (now - period)) {
      auto prev_val = last_directory_consistency_check_time_.storeConditional(
          now, last_directory_consistency_check_time, std::equal_to<>());
      check_full_directory = prev_val == last_directory_consistency_check_time;
      if (check_full_directory) {
        ld_debug("Verifying that all on-disk directory entries are consistent "
                 "with in-memory directory for shard %d",
                 getShardIdx());
      }
    }
  }

  partition_id_t oldest_partition_id = oldest_partition_id_.load();
  LogStorageStateMap* log_state_map = nullptr;
  ServerProcessor* processor = processor_.load();
  if (processor) {
    log_state_map = &processor->getLogStorageStateMap();
  }

  rocksdb::WriteBatch batch;
  auto flush_batch = [this, &batch]() {
    if (batch.Count() == 0) {
      return true;
    }
    rocksdb::WriteOptions options;
    auto status = writeBatch(options, &batch);
    if (!status.ok()) {
      ld_error(
          "Failed to delete directory entries: %s", status.ToString().c_str());
      ld_check_eq(acceptingWrites(), E::DISABLED);
      return false;
    }
    batch.Clear();
    return true;
  };

  RocksDBIterator it = createMetadataIterator();
  std::map<lsn_t, DirectoryEntry>::const_iterator in_memory_directory_it;
  std::unordered_set<logid_t> seen_logids;

  auto it_error = [&] {
    if (it.status().ok()) {
      return false;
    }
    ld_check(!it.status().IsIncomplete());
    // If we don't clean up directory after dropping partition, writes may get
    // stuck waiting for the cleanup. So we should stop taking writes if
    // directory cleanup fails.
    enterFailSafeIfFailed(it.status(), "cleanupDirectory()");
    return true;
  };

  enum class Decision {
    KEEP,
    KEEP_AND_SKIP_TO_NEXT_LOG,
    DELETE,
    ERROR,
  };

  auto should_delete_current_entry = [&](logid_t log_id,
                                         lsn_t trim_point,
                                         partition_id_t* out_partition =
                                             nullptr) {
    DirectoryEntry entry;
    if (entry.fromIterator(&it, log_id) != 0) {
      enterFailSafeMode("cleanupDirectory()", "invalid value in directory");
      return Decision::ERROR;
    }

    ld_check_ne(entry.id, PARTITION_INVALID);
    if (check_full_directory && out_partition != nullptr) {
      // Verify consistency between on-disk & in-memory directory
      ld_check_eq(entry.id, in_memory_directory_it->second.id);
      ld_check_eq(entry.first_lsn, in_memory_directory_it->second.first_lsn);
      ld_check_eq(entry.max_lsn, in_memory_directory_it->second.max_lsn);
      ld_check_eq(entry.flags, in_memory_directory_it->second.flags);
    }

    if (out_partition) {
      *out_partition = entry.id;
    }
    if (entry.id < oldest_partition_id) {
      return Decision::DELETE;
    }
    if (entry.max_lsn > trim_point || compacted_partitions.empty() ||
        entry.id > *compacted_partitions.rbegin()) {
      return Decision::KEEP_AND_SKIP_TO_NEXT_LOG;
    }
    return compacted_partitions.count(entry.id) ? Decision::DELETE
                                                : Decision::KEEP;
  };

  for (logid_t log_id(1);; ++log_id.val_) {
    PartitionDirectoryKey key(log_id, 0, 0);
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    ld_check(!it.status().IsIncomplete());
    if (it_error()) {
      return;
    }
    if (!it.Valid() ||
        !PartitionDirectoryKey::valid(it.key().data(), it.key().size())) {
      // End of directory.
      // Go through logs_ and check that we did not miss anything there.
      for (auto logs_it = logs_.cbegin(); logs_it != logs_.cend(); ++logs_it) {
        log_id = logid_t(logs_it->first);
        if (seen_logids.count(log_id) == 1) {
          continue;
        }

        LogState* log_state = logs_it->second.get();
        std::unique_lock<std::mutex> log_lock(log_state->mutex);
        // If we missed a non-empty log, we must now be able to find its data
        if (!log_state->directory.empty()) {
          it = createMetadataIterator();
          key = PartitionDirectoryKey(log_id, 0, 0);
          it.Seek(
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
          ld_check(!it.status().IsIncomplete());
          if (it_error()) {
            return;
          }
          ld_check(it.Valid());
          ld_check(
              PartitionDirectoryKey::valid(it.key().data(), it.key().size()));
          ld_check_eq(log_id, PartitionDirectoryKey::getLogID(it.key().data()));
        }
      }

      break;
    }

    log_id = PartitionDirectoryKey::getLogID(it.key().data());
    seen_logids.insert(log_id);
    key = PartitionDirectoryKey(log_id, 0, 0);
    lsn_t trim_point = LSN_INVALID;
    LogStorageState* log_storage_state =
        log_state_map ? log_state_map->find(log_id, getShardIdx()) : nullptr;
    if (log_storage_state) {
      trim_point = log_storage_state->getTrimPoint().value_or(LSN_INVALID);
    }
    auto res = should_delete_current_entry(log_id, trim_point);
    if (res == Decision::ERROR) {
      return;
    }
    if (res == Decision::KEEP_AND_SKIP_TO_NEXT_LOG && !check_full_directory) {
      // The first directory entry for this log is not obsolete,
      // skip to next log.
      continue;
    }

    // We most likely need to delete some directory entries for this log.
    // This needs to be done with locked LogState::mutex.
    // A possible optimization: if all the obsolete entries are below
    // oldest_partition_id, it's probably not necessary to lock LogState::mutex.
    auto logs_it = logs_.find(log_id.val_);
    ld_check(logs_it != logs_.cend());
    LogState* log_state = logs_it->second.get();
    std::unique_lock<std::mutex> log_lock(log_state->mutex);

    ld_check(log_state->directory.empty() ||
             ((--log_state->directory.cend())->second.id ==
              log_state->latest_partition.latest_partition.load()));

    // Need a new iterator, to make sure we see any changes made by
    // other threads while weren't holding the mutex. In particular a writer
    // could increase max_lsn of our current directory entry, making it
    // non-obsolete.
    it = createMetadataIterator();
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    in_memory_directory_it = log_state->directory.cbegin();
    bool empty = true;
    while (true) {
      if (it_error()) {
        return;
      }
      if (!it.Valid() ||
          !PartitionDirectoryKey::valid(it.key().data(), it.key().size()) ||
          PartitionDirectoryKey::getLogID(it.key().data()) != log_id) {
        ld_check(in_memory_directory_it == log_state->directory.cend());
        break;
      }
      partition_id_t partition_id;
      res = should_delete_current_entry(log_id, trim_point, &partition_id);
      if (res == Decision::ERROR) {
        return;
      }

      ld_check(in_memory_directory_it != log_state->directory.cend());
      ld_check_eq(in_memory_directory_it->second.id, partition_id);

      if (res == Decision::DELETE) {
        // Delete the directory entry.
        batch.Delete(metadata_cf_->get(), it.key());
        // From in-memory directory as well
        in_memory_directory_it =
            log_state->directory.erase(in_memory_directory_it);

        // Delete the coresponding index entry (if any). The index entry would
        // only exist if the user uses custom keys, which is rare. So most of
        // the time this delete will be a no-op, but it's cheap enough to
        // not worry about it.
        for (char index_type :
             CustomIndexDirectoryKey::allEligibleIndexTypes()) {
          CustomIndexDirectoryKey ikey(log_id, index_type, partition_id);
          batch.Delete(metadata_cf_->get(),
                       rocksdb::Slice(
                           reinterpret_cast<const char*>(&ikey), sizeof(ikey)));
        }
      } else {
        empty = false;
        if (res == Decision::KEEP_AND_SKIP_TO_NEXT_LOG) {
          // Periodically keep going to verify consistency of all directory
          // entries on disk with in-memory directory.
          if (!check_full_directory) {
            break;
          }
        } else {
          ld_check(res == Decision::KEEP);
        }
      }

      it.Next();
      if (res != Decision::DELETE) {
        ++in_memory_directory_it;
      }
    }
    if (!flush_batch()) { // done with locked log_lock
      return;
    }

    ld_check_eq(empty, log_state->directory.empty());

    if (log_state->directory.empty()) {
      // Update LogState to reflect that the log is now empty.
      log_state->latest_partition.store(
          PARTITION_INVALID, LSN_INVALID, LSN_INVALID);
      ld_check(in_memory_directory_it == log_state->directory.cend());
    } else {
      const auto& new_latest = (--log_state->directory.cend())->second;
      if (new_latest.id !=
          log_state->latest_partition.latest_partition.load()) {
        ld_assert_lt(
            new_latest.id, log_state->latest_partition.latest_partition.load());
        // Update LogState to reflect the current latest partition. We might
        // compact away the latest directory while an older entry remains,
        // waiting for compaction.
        log_state->latest_partition.store(
            new_latest.id, new_latest.first_lsn, new_latest.max_lsn);
      }
    }
  }

  if (check_full_directory) {
    ld_debug("Finished verifying that all on-disk directory entries are "
             "consistent with in-memory directory for shard %d",
             getShardIdx());
  }

  // Normally every index entry has a corresponding directory entry, so
  // the index is cleaned up by the above loop. The below loop is a paranoid
  // extra cleanup in case something's broken and we somehow end up with index
  // entries not backed by directory.
  //
  // This loop is just like the loop above, but instead of log_id it uses pair
  // (log_id, index_type), increasing lexicographically.
  logid_t log_id(1);
  unsigned char index_type = '\0';
  for (;; index_type == std::numeric_limits<unsigned char>::max()
           ? ++log_id.val_
           : ++index_type) {
    CustomIndexDirectoryKey key(log_id, index_type, 0);
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    ld_check(!it.status().IsIncomplete());
    if (it_error()) {
      return;
    }
    if (!it.Valid() ||
        !CustomIndexDirectoryKey::valid(it.key().data(), it.key().size())) {
      // End of index.
      break;
    }

    log_id = CustomIndexDirectoryKey::getLogID(it.key().data());
    index_type = CustomIndexDirectoryKey::getIndexType(it.key().data());

    while (true) {
      partition_id_t partition_id =
          CustomIndexDirectoryKey::getPartition(it.key().data());
      if (partition_id >= oldest_partition_id) {
        break;
      }
      // It's valid to do deletes below oldest_partition_id without holding
      // LogState::mutex. The creation of partitions with id's less than
      // oldest_partition_id is prevented because oldest_partition_mutex_ is
      // held on entry to this method.
      batch.Delete(metadata_cf_->get(), it.key());
      it.Next();
      if (it_error()) {
        return;
      }
      if (!it.Valid() ||
          !CustomIndexDirectoryKey::valid(it.key().data(), it.key().size()) ||
          CustomIndexDirectoryKey::getLogID(it.key().data()) != log_id ||
          CustomIndexDirectoryKey::getIndexType(it.key().data()) !=
              index_type) {
        break;
      }
    }
  }

  flush_batch();
}

void PartitionedRocksDBStore::cleanUpPartitionMetadataAfterDrop(
    partition_id_t oldest_to_keep) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  RocksDBIterator it = createMetadataIterator();
  for (auto type : {PartitionMetadataType::STARTING_TIMESTAMP,
                    PartitionMetadataType::MIN_TIMESTAMP,
                    PartitionMetadataType::MAX_TIMESTAMP,
                    PartitionMetadataType::LAST_COMPACTION,
                    PartitionMetadataType::DIRTY}) {
    PartitionMetaKey key(type, PARTITION_INVALID);
    it.Seek(rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)));
    while (it.status().ok() && it.Valid() &&
           PartitionMetaKey::valid(it.key().data(), it.key().size(), type)) {
      partition_id_t partition_id =
          PartitionMetaKey::getPartition(it.key().data());
      if (partition_id >= oldest_to_keep) {
        break;
      }

      rocksdb::WriteOptions options;
      rocksdb::WriteBatch batch;
      batch.Delete(metadata_cf_->get(), it.key());
      writeBatch(options, &batch);

      it.Next();
    }
  }
}

int PartitionedRocksDBStore::trimRebuildingRangesMetadata() {
  RebuildingRangesMetadata range_meta;
  if (getRebuildingRanges(range_meta) != 0) {
    return -1;
  }
  if (range_meta.empty()) {
    return 0;
  }
  partition_id_t new_min_ur_partition{PARTITION_MAX};
  partition_id_t new_max_ur_partition{PARTITION_INVALID};
  auto partitions = getPartitionList();
  RecordTimeIntervals time_interval_mask;
  for (PartitionPtr partition : *partitions) {
    if (partition->isUnderReplicated()) {
      new_min_ur_partition = std::min(new_min_ur_partition, partition->id_);
      new_max_ur_partition = std::max(new_max_ur_partition, partition->id_);
      time_interval_mask.insert(RecordTimeInterval(
          partition->min_timestamp, partition->max_timestamp));
    }
  }
  min_under_replicated_partition_.store(new_min_ur_partition);
  max_under_replicated_partition_.store(new_max_ur_partition);

  auto new_range_meta = range_meta;
  new_range_meta &= time_interval_mask;
  if (new_range_meta == range_meta) {
    return 0;
  }

  if (writeRebuildingRanges(new_range_meta) != 0) {
    return -1;
  }

  ServerProcessor* processor = processor_.load();
  if (new_range_meta.empty() && processor && processor->isInitialized()) {
    // Notify RebuildingCoordinator now that the shard is no longer
    // dirty.
    run_on_all_workers(processor_, [&]() {
      if (ServerWorker::onThisThread()->rebuilding_coordinator_) {
        ServerWorker::onThisThread()
            ->rebuilding_coordinator_->onDirtyStateChanged();
      }
      return 0;
    });
  }
  return 0;
}

int PartitionedRocksDBStore::dropPartitionsUpTo(partition_id_t oldest_to_keep) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  partition_id_t latest_id = latest_.get()->id_;
  if (oldest_to_keep > latest_id) {
    ld_error("Tried to drop partitions up to %lu, which is greater than "
             "latest partition id %lu",
             oldest_to_keep,
             latest_id);
    err = E::INVALID_PARAM;
    return -1;
  }

  // To make sure everything we're dropping is logically trimmed,
  // let's trim it when partitions are locked.
  int trim_rv = 0;
  int rv = dropPartitions(oldest_to_keep, [&]() {
    trim_rv = trimLogsToExcludePartitions(oldest_to_keep);
    if (trim_rv != 0) {
      return PARTITION_INVALID;
    }
    return oldest_to_keep;
  });

  if (trim_rv != 0) {
    return trim_rv;
  }

  return rv;
}

void PartitionedRocksDBStore::performMetadataCompaction() {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  auto factory = checked_downcast<RocksDBCompactionFilterFactory*>(
      rocksdb_config_.options_.compaction_filter_factory.get());

  rocksdb::Status status;
  {
    // This will wait for other compactions to finish. Needed even
    // for metadata cf to prevent any invalid context getting leaked to the
    // filter.
    auto compaction_lock = factory->startUsingContext(nullptr);
    if (shutdown_event_.signaled()) {
      return;
    }
    SCOPED_IO_TRACING_CONTEXT(getIOTracing(), "meta-compact");
    status = db_->CompactRange(rocksdb::CompactRangeOptions(),
                               getMetadataCFHandle(),
                               nullptr,
                               nullptr);
  }

  if (!status.ok()) {
    ld_warning(
        "CompactRange() for metadata CF failed: %s", status.ToString().c_str());
  } else {
    ld_debug("metadata CF compacted.");
  }

  last_metadata_manual_compaction_time_ = currentSteadyTime();
}

void PartitionedRocksDBStore::compactMetadataCFIfNeeded() {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  auto period = getSettings()->metadata_compaction_period;
  auto now = currentSteadyTime();
  if (period.count() > 0 &&
      last_metadata_manual_compaction_time_ < (now - period) &&
      getNumL0Files(metadata_cf_->get()) > 1) {
    performMetadataCompaction();
  }
}

void PartitionedRocksDBStore::performCompaction(partition_id_t partition) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  partition_id_t latest_id = latest_.get()->id_;
  if (partition > latest_id) {
    ld_error(
        "partition is %lu is too high: latest is %lu", partition, latest_id);
    return;
  }
  PartitionPtr p;
  if (!getPartition(partition, &p)) {
    ld_error("partition %lu is already dropped", partition);
    return;
  }
  trimLogsBasedOnTime();
  performCompactionInternal(
      PartitionToCompact(p, PartitionToCompact::Reason::MANUAL));
}

uint64_t PartitionedRocksDBStore::getApproximatePartitionSize(
    rocksdb::ColumnFamilyHandle* cf) const {
  // No keys start with a 255 byte.
  rocksdb::Range key_range(rocksdb::Slice("", 0), rocksdb::Slice("\xff", 1));
  uint64_t size;

  // rocksdb::DB::GetApproximateSizes() is not const. I'm not sure whether or
  // not there's a good reason for that. Let's const_cast and keep
  // getApproximatePartitionSize() const. Otherwise we would have to make many
  // PartitionedRocksDBStore pointers non-const just because of this method.
  const_cast<rocksdb::DB*>(db_.get())->GetApproximateSizes(
      cf, &key_range, 1, &size);

  return size;
}

int PartitionedRocksDBStore::getNumL0Files(rocksdb::ColumnFamilyHandle* cf) {
  static const std::string property = "rocksdb.num-files-at-level0";
  std::string val;
  if (!db_->GetProperty(cf, property, &val)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Cannot get the number of L0 files for column family");
    return -1;
  }

  return folly::to<int>(val);
}

uint64_t PartitionedRocksDBStore::getApproximateObsoleteBytes(
    partition_id_t partition_id) {
  PartitionPtr partition, next_partition;
  if (partition_id + 1 > latest_.get()->id_ ||
      !getPartition(partition_id, &partition) ||
      !getPartition(partition_id + 1, &next_partition)) {
    // Dropped.
    return 0;
  }

  // RocksDBTablePropertiesCollector collects total size of data for each
  // backlog duration for each table file flushed/compacted. Here we get
  // properties of all table files of this partition and see how much data
  // is older than its retention.

  rocksdb::TablePropertiesCollection props;
  rocksdb::Status status =
      db_->GetPropertiesOfAllTables(partition->cf_->get(), &props);
  if (!status.ok()) {
    ld_warning("Failed to get properties of sst files in partition %lu: %s",
               partition_id,
               status.ToString().c_str());
    enterFailSafeIfFailed(status, "GetPropertiesOfAllTables()");
    return 0;
  }
  RocksDBTablePropertiesCollector::RetentionSizeMap retention_sizes;
  for (const auto& it : props) {
    RocksDBTablePropertiesCollector::extractRetentionSizeMap(
        it.second->user_collected_properties, retention_sizes);
  }

  // Approximate minimum age of records in partition.
  auto age = std::chrono::duration_cast<std::chrono::seconds>(
      currentTime() - next_partition->starting_timestamp);

  uint64_t res = 0;
  for (const auto& it : retention_sizes) {
    const auto& retention = it.first;
    const auto& size = it.second;
    if (retention <= age) {
      res += size;
    }
  }

  return res;
}

int PartitionedRocksDBStore::isEmpty() const {
  int res = isCFEmpty(unpartitioned_cf_->get());
  if (res != 1) {
    return res;
  }

  // Check that metadata CF doesn't contain anything except:
  //  * "schema_version" or ".schema_version"
  //  * partition metadata of type STARTING_TIMESTAMP.
  //  * partition metadata of type DIRTY.
  RocksDBIterator it = createMetadataIterator(true);
  it.Seek(rocksdb::Slice("", 0));
  while (
      it.status().ok() && it.Valid() &&
      (it.key().compare(OLD_SCHEMA_VERSION_KEY) == 0 ||
       it.key().compare(NEW_SCHEMA_VERSION_KEY) == 0 ||
       PartitionMetaKey::valid(it.key().data(),
                               it.key().size(),
                               PartitionMetadataType::STARTING_TIMESTAMP) ||
       PartitionMetaKey::valid(
           it.key().data(), it.key().size(), PartitionMetadataType::DIRTY))) {
    it.Next();
  }
  if (!it.status().ok()) {
    ld_error("Error checking if database is empty: %s",
             it.status().ToString().c_str());
    return -1;
  }
  return !it.Valid();
}

/**
 * Returns best effort overestimation of the timestamp of given lsn
 * based on PartitionDirectoryKey information. This allows to avoid seeks on
 * disk most of the times because PartitionDirectoryKey is usually in cache.
 *
 * LogsDB v2 provide biggest and smallest lsn written in partition for given
 * log and their approximate timestamps.
 *
 * Here is visual representation of the possible scenarios and return values:
 *
 * 1) ________###########______@________##########_______ -> return s
 *                      e               s
 *
 * 2) _________________________@________##########_______ -> return s
 *                                      s
 *
 * 3) ________#################@##______##########_______ -> return e
 *                               e      s
 *
 * 4) ________#################@##_______________________ -> return e
 *                               e
 *
 * 5) ________###########______@_________________________ -> return E::NOTFOUND
 *                      e
 *
 * 6) _________________________@_________________________ -> return E::NOTFOUND
 *
 * where:
 * @ - target lsn; # - data in partitions; s - timestamp of the oldest partition
 * which has start lsn bigger or equal to target lsn; e - timestamp of the end
 * of the latest partition which has start lsn strictly smaller than target lsn;
 */
int PartitionedRocksDBStore::getApproximateTimestamp(
    logid_t log_id,
    lsn_t lsn,
    bool allow_blocking_io,
    std::chrono::milliseconds* timestamp_out) {
  RocksDBIterator it = createMetadataIterator(allow_blocking_io);
  auto partitions = getPartitionList();

  // make sure that `it' first points to a record with a sequencer number
  // that's strictly greater than lsn
  PartitionDirectoryKey data_key(log_id, lsn, 0);

  auto it_error = [&] {
    rocksdb::Status status = it.status();
    if (status.ok()) {
      return false;
    }
    err = status.IsIncomplete() ? E::WOULDBLOCK : E::LOCAL_LOG_STORE_READ;
    return true;
  };

  it.Seek(rocksdb::Slice(
      reinterpret_cast<const char*>(&data_key), sizeof(data_key)));
  if (it_error()) {
    return -1;
  }

  // Timestamp of the beginning of the oldest partition newer than target lsn.
  // This timestamp denoted as 's' in doc block.
  auto next_partition_ts = RecordTimestamp::min();

  // Timestamp of the end of the newest partition older than target lsn.
  // This timestamp denoted as 's' in doc block.
  auto pre_partition_ts = RecordTimestamp::min();
  lsn_t pre_partition_end_lsn = LSN_INVALID;

  while (it.Valid()) {
    rocksdb::Slice key = it.key();
    // Check if not moved past the section containing keys of needed type or
    // moved to different log key space.
    if (PartitionDirectoryKey::valid(key.data(), key.size()) &&
        PartitionDirectoryKey::getLogID(key.data()) == log_id) {
      partition_id_t partition_id =
          PartitionDirectoryKey::getPartition(key.data());
      if (!dd_assert(partition_id != PARTITION_INVALID,
                     "Invalid partition id when seeking to log %ld, lsn %s",
                     log_id.val(),
                     lsn_to_string(lsn).c_str())) {
        err = E::FAILED;
        return -1;
      }
      PartitionPtr partition = partitions->get(partition_id);
      if (!partition) {
        // chasing the drop point here. partition_id may be dropped between
        // createMetadataIterator() and getPartitionList(); we need to move
        // iterator forward until we find a non-dropped partition.
        if (partition_id < partitions->firstID()) {
          it.Next();
          continue;
        }
      } else {
        next_partition_ts = partition->min_timestamp;
        if (next_partition_ts == RecordTimestamp::max()) {
          next_partition_ts = partition->starting_timestamp;
        }
      }
    }
    it.Prev();
    break;
  }

  if (!it.Valid()) {
    it.SeekToLast();
  }

  if (it_error()) {
    return -1;
  }

  // check that we're at the right position after moving one step back
  if (it.Valid() &&
      PartitionDirectoryKey::valid(it.key().data(), it.key().size()) &&
      PartitionDirectoryKey::getLogID(it.key().data()) == log_id) {
    pre_partition_end_lsn = PartitionDirectoryValue::getMaxLSN(
        it.value().data(), it.value().size());
    partition_id_t partition_id =
        PartitionDirectoryKey::getPartition(it.key().data());
    if (!dd_assert(partition_id != PARTITION_INVALID,
                   "Invalid partition id when seeking to log %ld, "
                   "lsn - previous to %s or last for a log.",
                   log_id.val(),
                   lsn_to_string(lsn).c_str())) {
      err = E::FAILED;
      return -1;
    }
    PartitionPtr partition = partitions->get(partition_id);
    if (partition) {
      pre_partition_ts = partition->max_timestamp;
      if (pre_partition_ts == RecordTimestamp::min()) {
        pre_partition_ts = partition->starting_timestamp;
      }
    }
  }

  if (pre_partition_end_lsn >= lsn) {
    *timestamp_out = pre_partition_ts.toMilliseconds();
  } else if (next_partition_ts != RecordTimestamp::min()) {
    *timestamp_out = next_partition_ts.toMilliseconds();
  } else {
    err = E::NOTFOUND;
    return -1;
  }
  return 0;
}

int PartitionedRocksDBStore::getRebuildingRanges(
    RebuildingRangesMetadata& rrm) {
  rrm.clear();
  int rv = readStoreMetadata(&rrm);
  if (rv != 0) {
    if (err != E::NOTFOUND) {
      ld_error("Error reading RebuildingRangesMetadata for shard %u: %s\r\n",
               getShardIdx(),
               error_description(err));
      return -1;
    }
    ld_check(rrm.empty());
  }
  return 0;
}

int PartitionedRocksDBStore::writeRebuildingRanges(
    RebuildingRangesMetadata& rrm) {
  int rv = writeStoreMetadata(rrm, LocalLogStore::WriteOptions());
  if (rv != 0) {
    ld_error("Error writting RebuildingRangesMetadata for shard %u: %s\r\n",
             getShardIdx(),
             error_description(err));
    return -1;
  }
  return 0;
}

void PartitionedRocksDBStore::listLogs(
    std::function<void(logid_t,
                       lsn_t highest_lsn,
                       uint64_t highest_partition_id,
                       RecordTimestamp highest_timestamp_approx)> cb,
    bool only_log_ids) {
  auto partitions = partitions_.getVersion();
  for (auto p = logs_.cbegin(); p != logs_.cend(); ++p) {
    logid_t log(p->first);
    LogState* state = p->second.get();

    partition_id_t highest_partition =
        state->latest_partition.latest_partition.load();
    if (highest_partition == PARTITION_INVALID) {
      continue;
    }

    if (only_log_ids) {
      cb(log, LSN_INVALID, PARTITION_INVALID, RecordTimestamp::zero());
      continue;
    }

    lsn_t max_lsn_in_latest = state->latest_partition.max_lsn_in_latest.load();

    RecordTimestamp highest_timestamp;
    if (highest_partition + 1 >= partitions->nextID()) {
      // There are records of this log in latest partition.
      highest_timestamp = currentTime();
    } else {
      // starting_timestamp of highest_partition + 1.
      highest_timestamp =
          partitions
              ->get(std::max(partitions->firstID(), highest_partition + 1))
              ->starting_timestamp;
    }

    cb(log, max_lsn_in_latest, highest_partition, highest_timestamp);
  }
}

bool PartitionedRocksDBStore::flushMemtablesAtomically(
    const std::vector<rocksdb::ColumnFamilyHandle*>& cfs,
    bool wait) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  auto options = rocksdb::FlushOptions();
  options.wait = wait;
  // Introduce write stalls if there are memtables that are
  // getting flushed, as we don't want active memtable to grow very large
  // and cause OOM.
  options.allow_write_stall = true;
  rocksdb::Status status = db_->Flush(options, cfs);
  enterFailSafeIfFailed(status, "FlushAtomically()");
  return status.ok();
}

bool PartitionedRocksDBStore::flushMemtable(RocksDBCFPtr& cf, bool wait) {
  return flushMemtablesAtomically({cf->cf_.get()}, wait);
}

int PartitionedRocksDBStore::flushUnpartitionedMemtables(bool wait) {
  ld_check(!getSettings()->read_only);
  ld_check(!immutable_.load());
  auto options = rocksdb::FlushOptions();
  // Introduce write stalls if there are memtables that are
  // getting flushed, as we don't want active memtable to grow very large
  // and cause OOM.
  options.allow_write_stall = true;
  options.wait = wait;
  std::lock_guard<std::mutex> snapshots_lock(snapshots_cf_lock_);
  auto snapshots_cf = snapshots_cf_ ? snapshots_cf_->get() : nullptr;
  for (auto* cf :
       {getMetadataCFHandle(), getUnpartitionedCFHandle(), snapshots_cf}) {
    if (cf) {
      rocksdb::Status status = db_->Flush(options, cf);
      if (!status.ok()) {
        enterFailSafeIfFailed(status, "Flush()");
        err = E::LOCAL_LOG_STORE_WRITE;
        return -1;
      }
    }
  }
  return 0;
}

int PartitionedRocksDBStore::flushAllMemtables(bool wait) {
  if (latest_.get()) {
    auto partitions = getPartitionList();
    for (PartitionPtr partition : *partitions) {
      if (!flushMemtable(partition->cf_, wait)) {
        err = E::LOCAL_LOG_STORE_WRITE;
        return -1;
      }
    }
  }

  return flushUnpartitionedMemtables(wait);
}

void PartitionedRocksDBStore::onMemTableWindowUpdated() {
  // The high priority thread polls for window movements so
  // that cleaning of partitions and notification of the window
  // update to third-parties is disconnected from any thread
  // that is flushing MemTables.
}

void PartitionedRocksDBStore::backgroundThreadSleep(BackgroundThreadType type) {
  std::chrono::milliseconds period;
  switch (type) {
    case BackgroundThreadType::HI_PRI:
      period = getSettings()->partition_hi_pri_check_period_;
      break;
    case BackgroundThreadType::LO_PRI:
      period = getSettings()->partition_lo_pri_check_period_;
      break;
    case BackgroundThreadType::FLUSH:
      period = getSettings()->partition_flush_check_period_;
      if (period.count() == 0) {
        ld_warning(
            "System is coming up without flusher thread. No memtable flushes "
            "will be initiated from logdevice.");
        period = std::chrono::milliseconds::max();
      }
      break;
    case BackgroundThreadType::COUNT:
      ld_check(false);
  }
  shutdown_event_.waitFor(period);
}

SystemTimestamp PartitionedRocksDBStore::currentTime() {
  return SystemTimestamp::now();
}

SteadyTimestamp PartitionedRocksDBStore::currentSteadyTime() {
  return SteadyTimestamp::now();
}

static void setBGThreadName(const char* pri, shard_index_t shard_idx) {
  // Make sure we'll squeeze into 15-character limit.
  ld_check_le(strlen(pri), 2);
  shard_idx =
      std::max(shard_index_t(-9), std::min(shard_index_t(99), shard_idx));

  std::string name = "ld:s" + std::to_string(shard_idx) + ":db-bg-" + pri;
  ld_check_le(name.size(), 15);
  ThreadID::set(ThreadID::Type::LOGSDB, name);
}

LocalLogStore::WriteThrottleState
PartitionedRocksDBStore::subclassSuggestedThrottleState() {
  return too_many_partial_compactions_.load()
      ? WriteThrottleState::STALL_LOW_PRI_WRITE
      : WriteThrottleState::NONE;
}

void PartitionedRocksDBStore::hiPriBackgroundThreadRun() {
  ld_check(!getSettings()->read_only);
  setBGThreadName("hi", shard_idx_);
  while (true) {
    backgroundThreadSleep(BackgroundThreadType::HI_PRI);
    if (shutdown_event_.signaled()) {
      break;
    }
    if (inFailSafeMode()) {
      break;
    }

    if (shouldCreatePartition()) {
      createPartition();
    }

    if (last_broadcast_flush_ < flushedUpThrough()) {
      ld_debug("Shard %d: Flushed up through now %ju.",
               getShardIdx(),
               (uintmax_t)flushedUpThrough());
      last_broadcast_flush_ = flushedUpThrough();
      broadcastFlushEvent(last_broadcast_flush_);
      cleaner_pass_requested_.store(true);
    }

    if (cleaner_pass_requested_.load()) {
      cleaner_pass_requested_.store(false);
      cleaner_work_queue_.emplace_back(
          currentSteadyTime(), last_broadcast_flush_);
    }

    if (!cleaner_work_queue_.empty() &&
        (currentSteadyTime() - cleaner_work_queue_.front().first) >
            getSettings()->partition_redirty_grace_period) {
      ld_debug("Shard %d: Cleaning %ju from time %s.",
               getShardIdx(),
               (uintmax_t)cleaner_work_queue_.front().second,
               toString(cleaner_work_queue_.front().first).c_str());
      updateDirtyState(cleaner_work_queue_.front().second);
      cleaner_work_queue_.pop_front();
    }
  }
  ld_info("Shard %d hi-pri background thread finished", getShardIdx());
}

void PartitionedRocksDBStore::loPriBackgroundThreadRun() {
  ld_check(!getSettings()->read_only);
  setBGThreadName("lo", shard_idx_);
  if (getSettings()->low_ioprio.hasValue()) {
    // Partial compactions can do significant amount of disk IO directly from
    // this thread. Set the same IO priority as RocksDBEnv sets for rocksdb
    // compaction threads.
    set_io_priority_of_this_thread(getSettings()->low_ioprio.value());
  }

  // Don't sleep if we know there's work to do.
  bool skip_sleep = false;

  while (true) {
    if (!skip_sleep) {
      backgroundThreadSleep(BackgroundThreadType::LO_PRI);
    }
    skip_sleep = false;
    if (shutdown_event_.signaled() || inFailSafeMode()) {
      break;
    }

    // The order in which partitions are added to `to_compact` defines the
    // priorities of the compactions. From highest to lowest:
    //  1. Retention-based compactions.
    //  2. Hi-pri manual compactions.
    //  3. Partial compactions.
    //  4. Proactive compactions.
    //  5. Lo-pri manual compactions.
    std::vector<PartitionToCompact> to_compact;

    // Apply any partition drops initiated by per-disk space-based trimming
    partition_id_t oldest_to_keep = space_based_trim_limit.load();
    if (oldest_to_keep != PARTITION_INVALID) {
      // Unlike the dropPartitions() below, this drop will trim logs.
      dropPartitionsUpTo(oldest_to_keep);
    }

    size_t logs_in_grace_period = 0;
    int rv = trimLogsBasedOnTime(
        &oldest_to_keep, &to_compact, &logs_in_grace_period);
    PER_SHARD_STAT_SET(stats_,
                       logs_waiting_for_grace_period,
                       shard_idx_,
                       logs_in_grace_period);
    if (rv != 0) {
      continue;
    }

    PartitionPtr ptr;
    if (getPartition(oldest_to_keep, &ptr)) {
      std::chrono::seconds start = ptr->starting_timestamp.toSeconds();
      ld_debug("oldest partition(%lu) timestamp: %lu s",
               oldest_to_keep,
               (RecordTimestamp::now().toSeconds() - start).count());
      PER_SHARD_STAT_SET(stats_,
                         sbt_effective_retention_seconds,
                         shard_idx_,
                         (RecordTimestamp::now().toSeconds() - start).count());
    }

    if (!getSettings()->partition_compaction_schedule.hasValue()) {
      // no retention-based compactions
      to_compact.clear();
    }

    SteadyTimestamp now = currentSteadyTime();
    SteadyTimestamp avoid_drops_until = avoid_drops_until_;
    bool avoid_drops = avoid_drops_until > now;
    if (!avoid_drops) {
      dropPartitions(
          oldest_to_keep, [&]() { return findObsoletePartitions(); });
    }

    // Fetching hi-pri manual compactions
    size_t hi_pri_manual_compaction_max_num_per_loop = 1;
    if (getPartitionsForManualCompaction(
            &to_compact,
            hi_pri_manual_compaction_max_num_per_loop,
            true /* hi_pri */)) {
      // there are more hi-pri manual compactions to process
      skip_sleep = true;
    }

    size_t partition_partial_compaction_max_num_per_loop{
        getSettings()->partition_partial_compaction_max_num_per_loop_};
    size_t max_pending_partial_compactions =
        getSettings()->partition_partial_compaction_stall_trigger_;

    std::vector<PartitionToCompact> partial_compactions;
    getPartitionsForPartialCompaction(
        &partial_compactions,
        std::max(partition_partial_compaction_max_num_per_loop,
                 max_pending_partial_compactions));

    // If the number of planned partial compactions is above threshold,
    // stall rebuilding.
    bool need_stall = max_pending_partial_compactions != 0 &&
        partial_compactions.size() >= max_pending_partial_compactions;
    bool needed_stall = too_many_partial_compactions_.load();
    if (need_stall != needed_stall) {
      too_many_partial_compactions_.store(need_stall);
      if (!need_stall) {
        ld_info("Shard %u: unstalling rebuilding writes because the number of "
                "pending partial compactions got below threshold (%lu < %lu).",
                shard_idx_,
                partial_compactions.size(),
                max_pending_partial_compactions);
        // Note: would be nice to call throttleIOIfNeeded() here.
      } else {
        ld_info("Shard %u: stalling rebuilding writes because the number of "
                "pending partial compactions is >= %lu.",
                shard_idx_,
                max_pending_partial_compactions);
        // The next iteration will either do more compactions or unstall
        // writers. In either case we don't want to sleep.
        skip_sleep = true;
      }
    }

    size_t num_partial_compactions_postponed = 0;
    if (partial_compactions.size() >=
        partition_partial_compaction_max_num_per_loop) {
      // there are likely more partial compactions to process
      skip_sleep = true;
      num_partial_compactions_postponed = partial_compactions.size() -
          partition_partial_compaction_max_num_per_loop;
      partial_compactions.erase(
          partial_compactions.begin() +
              partition_partial_compaction_max_num_per_loop,
          partial_compactions.end());
    }
    std::move(partial_compactions.begin(),
              partial_compactions.end(),
              std::back_inserter(to_compact));

    getPartitionsForProactiveCompaction(&to_compact);

    // We only get lo-pri manual compactions if there are no other compactions
    // pending. But, we skip the delay if there are pending lo-pri manual
    // compactions even if we aren't scheduling them immediately
    size_t lo_pri_manual_compaction_max_num_per_loop =
        to_compact.empty() ? hi_pri_manual_compaction_max_num_per_loop : 0;
    if (getPartitionsForManualCompaction(
            &to_compact,
            lo_pri_manual_compaction_max_num_per_loop,
            false /* hi_pri*/)) {
      skip_sleep = true;
    }

    PartitionToCompact::removeDuplicates(&to_compact);
    PartitionToCompact::interleavePartialAndNormalCompactions(&to_compact);

    size_t num_partial_compactions = 0;
    for (const auto& p : to_compact) {
      if (p.reason == PartitionToCompact::Reason::PARTIAL) {
        num_partial_compactions += 1;
      }
    }

    PER_SHARD_STAT_SET(stats_,
                       pending_retention_compactions,
                       shard_idx_,
                       to_compact.size() - num_partial_compactions);

    PER_SHARD_STAT_SET(
        stats_,
        pending_partial_compactions,
        shard_idx_,
        num_partial_compactions + num_partial_compactions_postponed);

    auto compactions_start_time = currentTime();

    compactMetadataCFIfNeeded();

    bool first = true;
    for (auto p : to_compact) {
      if (!getSettings()->partition_compactions_enabled ||
          shutdown_event_.signaled() || inFailSafeMode()) {
        break;
      }
      // Don't starve trimming and dropping.
      if (!first &&
          currentTime() - compactions_start_time >
              getSettings()->partition_lo_pri_check_period_) {
        skip_sleep = true;
        break;
      }
      first = false;
      performCompactionInternal(p);
      // The list of partitions to compact can be merged above, but partition
      // compactions with type MANUAL should always take precedence over all
      // other types when de-duping, thus the type for compactions merged with
      // manual ones should be `MANUAL` anyway.
      if (p.reason == PartitionToCompact::Reason::MANUAL) {
        // Drop the partition from the list of manual compactions
        cancelManualCompaction(p.partition->id_);
      }
    }

    // Update stats for total trash size and the rate limit on its deletion
    PER_SHARD_STAT_SET(stats_, trash_size, shard_idx_, getTotalTrashSize());
    PER_SHARD_STAT_SET(stats_,
                       trash_deletion_ratelimit,
                       shard_idx_,
                       getSettings()->sst_delete_bytes_per_sec);
  }

  ld_info("Shard %d lo-pri background thread finished", getShardIdx());
}

static std::string
toString(const PartitionedRocksDBStore::FlushEvaluator::CFData& d) {
  std::string r = d.cf->cf_->GetName();
#define FIELD(f, ch)                                        \
  if (d.stats.f != 0) {                                     \
    folly::format(&r, " " ch ":{:.3f}MB", d.stats.f / 1e6); \
  }
  FIELD(active_memtable_size, "A");
  FIELD(immutable_memtable_size, "F");
  FIELD(pinned_memtable_size, "P");
#undef FIELD
  return r;
}

static std::string toString(const RocksDBLogStoreBase::WriteBufStats& s) {
  return folly::sformat(
      "active: {:.3f}MB in {} memtables, flushing: {:.3f}MB, pinned: {:.3f}MB",
      s.active_memory_usage / 1e6,
      s.num_active_memtables,
      s.memory_being_flushed / 1e6,
      s.pinned_buffer_usage / 1e6);
}

void PartitionedRocksDBStore::flushBackgroundThreadRun() {
  ld_check(!getSettings()->read_only);
  setBGThreadName("fl", shard_idx_);

  auto update_stats = [&](const WriteBufStats& buf_stats) {
    PER_SHARD_STAT_SET(stats_,
                       memtable_size_active,
                       getShardIdx(),
                       buf_stats.active_memory_usage);
    PER_SHARD_STAT_SET(stats_,
                       memtable_size_flushing,
                       getShardIdx(),
                       buf_stats.memory_being_flushed);
    PER_SHARD_STAT_SET(stats_,
                       memtable_size_pinned,
                       getShardIdx(),
                       buf_stats.pinned_buffer_usage);
    PER_SHARD_STAT_SET(stats_,
                       memtable_count_active,
                       getShardIdx(),
                       buf_stats.num_active_memtables);
  };

  while (true) {
    backgroundThreadSleep(BackgroundThreadType::FLUSH);
    if (shutdown_event_.signaled() || inFailSafeMode()) {
      break;
    }

    // Grab settings.
    uint64_t memory_limit = getSettings()->memtable_size_per_node / num_shards_;
    uint64_t flush_trigger = memory_limit / 2;
    uint64_t low_watermark = static_cast<uint64_t>(
        getSettings()->memtable_size_low_watermark_percent / 100.0 *
        flush_trigger);
    flush_trigger = std::max(flush_trigger, 1ul);
    low_watermark = std::min(low_watermark, flush_trigger - 1);
    uint64_t max_individual_memtable_size = getSettings()->write_buffer_size;

    // Check how much was written since last evaluation.
    size_t bytes_since_prev_eval = bytes_written_since_flush_eval_.load();
    if (bytes_since_prev_eval > static_cast<size_t>(1e8)) {
      ld_info("%.3fMB written to shard %u since last flush evaluation.",
              bytes_since_prev_eval / 1e6,
              getShardIdx());
    }

    // Collect information about memtables in each column family.

    folly::stop_watch<std::chrono::milliseconds> watch;
    SteadyTimestamp now = currentSteadyTime();
    auto partitions = partitions_.getVersion();
    FlushEvaluator evaluator(getShardIdx(),
                             flush_trigger,
                             max_individual_memtable_size,
                             low_watermark,
                             getRocksDBLogStoreConfig());

    std::vector<FlushEvaluator::CFData> non_zero_size_cf;
    FlushEvaluator::CFData metadata_cf_data{
        metadata_cf_,
        getMemTableStats(metadata_cf_->get()),
        SteadyTimestamp::now()};

    if (unpartitioned_cf_->first_dirtied_time_ != SteadyTimestamp::min()) {
      FlushEvaluator::CFData unpartitioned_cf_data = {
          unpartitioned_cf_,
          getMemTableStats(unpartitioned_cf_->get()),
          SteadyTimestamp::now()};
      if (unpartitioned_cf_data.stats.active_memtable_size > 0) {
        non_zero_size_cf.push_back(std::move(unpartitioned_cf_data));
      }
    }

    for (auto& partition : *partitions) {
      auto& ds = partition->dirty_state_;
      auto& cf = partition->cf_;
      auto memtable_stats = getMemTableStats(partition->cf_->get());
      if (cf->first_dirtied_time_ == SteadyTimestamp::min()) {
        // RocksDB reports nonzero size of active memtable even if it's empty.
        // If we know it's empty, override its size to zero.
        memtable_stats.active_memtable_size = 0;
      }
      if (memtable_stats.active_memtable_size != 0 ||
          memtable_stats.immutable_memtable_size != 0 ||
          memtable_stats.pinned_memtable_size != 0) {
        FlushEvaluator::CFData cf_data = {
            partition->cf_, memtable_stats, ds.latest_dirty_time};
        if (cf_data.stats.active_memtable_size > 0) {
          non_zero_size_cf.push_back(std::move(cf_data));
        }
      }
    }

    // Decide what to flush.
    auto to_flush =
        evaluator.pickCFsToFlush(now, metadata_cf_data, non_zero_size_cf);
    auto evaluate_time = watch.lap();

    std::vector<rocksdb::ColumnFamilyHandle*> handles_to_flush;
    handles_to_flush.reserve(to_flush.size());
    for (auto& cf_data : to_flush) {
      handles_to_flush.push_back(cf_data.cf->get());
    }
    flushMemtablesAtomically(handles_to_flush, false /* wait */);

    auto flush_time = watch.lap();

    // Throttle writes if memtables are using too much memory.

    auto buf_stats = evaluator.getBufStats();

    {
      std::lock_guard<std::mutex> lock(throttle_eval_mutex_);

      // Reset bytes_written_since_flush_eval_. We could just set it to zero,
      // but instead let's be conservative and set it to what it would have been
      // if we were to set it to zero *before* evaluating flushes.
      bytes_written_since_flush_eval_.fetch_sub(bytes_since_prev_eval);
      last_flush_eval_stats_ = buf_stats;
      throttleIOIfNeeded(buf_stats, memory_limit);
    }

    update_stats(buf_stats);

    auto throttle_time = watch.lap();

    // Update stats and log some info.

    STAT_ADD(stats_, triggered_manual_memtable_flush, !to_flush.empty());
    auto total_time = flush_time + evaluate_time + throttle_time;

    auto describe = [&] {
      // Put non_zero_size_cf at the end of the message because it can be long
      // and get truncated.
      return folly::sformat("Flushing: {}. Total stats: {}. Eval time: {}ms, "
                            "flush init: {}ms, throttle eval: {}ms, "
                            "npartitions: {}. Existing memtables: {}, {}",
                            toString(to_flush),
                            toString(buf_stats),
                            evaluate_time.count(),
                            flush_time.count(),
                            throttle_time.count(),
                            partitions->size(),
                            toString(metadata_cf_data),
                            toString(non_zero_size_cf));
    };

    bool noteworthy = false;
    noteworthy |= getSettings()->print_details &&
        (!to_flush.empty() || total_time.count() > 20);
    noteworthy |= total_time.count() > 500;
    if (noteworthy) {
      ld_info("%s", describe().c_str());
    } else {
      RATELIMIT_INFO(std::chrono::seconds(10), 1, "%s", describe().c_str());
    }
  }

  update_stats(WriteBufStats{});
  ld_info("Shard %d flush background thread finished", getShardIdx());
}

void PartitionedRocksDBStore::LogState::LatestPartitionInfo::load(
    partition_id_t* out_partition,
    lsn_t* out_first_lsn,
    lsn_t* out_max_lsn) const {
  uint64_t v = version.load();
  for (int64_t attempt = 0;; ++attempt) {
    partition_id_t p = latest_partition.load();
    if (p == PARTITION_INVALID) {
      *out_partition = PARTITION_INVALID;
      *out_first_lsn = LSN_INVALID;
      *out_max_lsn = LSN_INVALID;
      return;
    }

    lsn_t min_lsn = first_lsn_in_latest.load();
    lsn_t max_lsn = max_lsn_in_latest.load();
    uint64_t v2 = version.load();

    if (v % 2 == 0 && v == v2) {
      // We've seen an even version, then read `p` and `l`, then saw the same
      // version again. This guarantees that the values `p` and `l` are
      // consistent with each other, i.e. come from the same store() call.
      *out_partition = p;
      *out_first_lsn = min_lsn;
      *out_max_lsn = max_lsn;
      return;
    } else {
      // Another thread is in the middle of store(). This should be rare
      // because the middle of store() is short, and it's not called often.
    }

    v = v2;

    if (attempt > 100000000 && (attempt - 100000000) % 1000000000 == 0) {
      ld_error(
          "Busy waited for %ld iterations. Most likely something's broken.",
          attempt);
    }
    if (attempt > 1000) {
      std::this_thread::yield();
    }
  }
}

void PartitionedRocksDBStore::LogState::LatestPartitionInfo::store(
    partition_id_t partition,
    lsn_t first_lsn,
    lsn_t max_lsn) {
  ld_check_eq(partition == PARTITION_INVALID, first_lsn == LSN_INVALID);
  ++version;
  latest_partition.store(partition);
  first_lsn_in_latest.store(first_lsn);
  max_lsn_in_latest.store(max_lsn);
  ++version;
  ld_check(version.load() % 2 == 0);
}

void PartitionedRocksDBStore::LogState::LatestPartitionInfo::updateMaxLSN(
    lsn_t max_lsn) {
  // Not necessary to bump version when updating only one field.
  max_lsn_in_latest.store(max_lsn);
}

template <>
const std::string& EnumMap<PartitionedRocksDBStore::PartitionToCompact::Reason,
                           std::string>::invalidValue() {
  static const std::string invalidName("");
  return invalidName;
}

template <>
void EnumMap<PartitionedRocksDBStore::PartitionToCompact::Reason,
             std::string>::setValues() {
  using Reason = PartitionedRocksDBStore::PartitionToCompact::Reason;
  set(Reason::INVALID, "INVALID");
  set(Reason::PARTIAL, "PARTIAL");
  set(Reason::RETENTION, "RETENTION");
  set(Reason::PROACTIVE, "PROACTIVE");
  set(Reason::MANUAL, "MANUAL");
  static_assert(
      (size_t)Reason::MAX == 5,
      "Added more values to the enum? Add them above and update this assert.");
}

EnumMap<PartitionedRocksDBStore::PartitionToCompact::Reason, std::string>&
PartitionedRocksDBStore::PartitionToCompact::reasonNames() {
  // Leak it to avoid static destruction order fiasco.
  static auto map = new EnumMap<Reason, std::string>();
  return *map;
}

void PartitionedRocksDBStore::MemtableFlushCallback::
operator()(LocalLogStore* store, FlushToken token) const {
  // Post a request to a worker thread.
  auto s = static_cast<PartitionedRocksDBStore*>(store);

  ld_check(!s->getSettings()->read_only);
  ServerProcessor* processor = s->processor_.load();

  if (!processor) {
    // We're not fully initialized yet.
    return;
  }

  auto nodeIndex = processor->getMyNodeID().index();
  auto serverInstanceId = processor->getServerInstanceId();
  uint32_t shardIdx = store->getShardIdx();

  // Don't send flush notifications if donors do not rely on them.
  if (processor->settings()->rebuilding_dont_wait_for_flush_callbacks) {
    return;
  }

  // Do not send memtable flush notification if the rebuild store durability
  // is anything greater than MEMORY. This is a temporary solution to disable
  // sending a MEMTABLE_FLUSHED update when not using REBUILDING without WAL.
  // TODO: T22931059
  if (processor->settings()->rebuild_store_durability > Durability::MEMORY) {
    return;
  }

  for (int idx = 0; idx < processor->getWorkerCount(WorkerType::GENERAL);
       ++idx) {
    std::unique_ptr<Request> req = std::make_unique<MemtableFlushedRequest>(
        worker_id_t(idx), nodeIndex, serverInstanceId, shardIdx, token);
    if (processor->postWithRetrying(req) != 0) {
      ld_debug("Failed to post MemtableFlushedRequest with {shard:%d, "
               "FlushedUpto:%lu} to worker #%d",
               shardIdx,
               token,
               idx);
    }
  }
}

void PartitionedRocksDBStore::onSettingsUpdated(
    const std::shared_ptr<const RocksDBSettings> rocksdb_settings) {
  if (!db_) {
    return;
  }

  // NOTE: Getting the options ptr here and updating it. Currently updating
  // only those fields that don't need mutex for the update but if any field
  // that needs atomic update is changed here, add protection mechanism for
  // config fields in RocksDBLogStoreConfig .
  auto options = rocksdb_config_.getRocksDBOptions();

  // Change updateable fields within rocksdb below.
  // Here, we're only modifying sst_file_manager, which is shared among all
  // column families.
  auto settingsUpdated = false;
  std::stringstream ss;
  if (options->sst_file_manager != nullptr &&
      options->sst_file_manager->GetDeleteRateBytesPerSecond() !=
          rocksdb_settings->sst_delete_bytes_per_sec) {
    ss << folly::sformat(
        "RocksDB delete rate bytes per second changed from {}"
        " to {}",
        options->sst_file_manager->GetDeleteRateBytesPerSecond(),
        rocksdb_settings->sst_delete_bytes_per_sec);
    options->sst_file_manager->SetDeleteRateBytesPerSecond(
        rocksdb_settings->sst_delete_bytes_per_sec);
    settingsUpdated = true;
  }

  if (settingsUpdated) {
    ld_info("Changes: %s", ss.str().c_str());
  }
}

uint64_t PartitionedRocksDBStore::getTotalTrashSize() {
  auto options = rocksdb_config_.getRocksDBOptions();
  return options->sst_file_manager != nullptr
      ? options->sst_file_manager->GetTotalTrashSize()
      : 0;
}

std::vector<CFData>
FlushEvaluator::pickCFsToFlush(SteadyTimestamp now,
                               CFData& metadata_cf_data,
                               const std::vector<CFData>& input) {
  std::shared_ptr<const RocksDBSettings> settings =
      rocksdb_config_.getRocksDBSettings();
  bool ld_managed_flushes = rocksdb_config_.use_ld_managed_flushes_;
  const auto use_age_size_flush_heuristic =
      settings->use_age_size_flush_heuristic;
  std::vector<CFData> out;

  // Total memory picked for flushing.
  uint64_t amount_picked = 0;
  uint64_t cur_active_memory_usage = 0;

  // Amount of memory to flush to reach target memory consumption.
  uint64_t target = 0;

  // This indicates if there is need to flush metadata memtable.
  auto metadata_memtable_dependency = FlushToken_INVALID;
  // There is need to flush metadata memtable if :
  // 1. memtable size is above max_memtable_size threshold.
  // 2. data cf memtable which depends on metadata memtable is getting flushed.
  // 3. memtable needs to be flushed to reach target memory consumption.
  bool metadata_cf_picked = false;

  auto pick_cf = [&](const CFData& cf_data) {
    auto& cf = cf_data.cf;
    auto& mem_stats = cf_data.stats;
    metadata_memtable_dependency = !metadata_cf_picked
        ? std::max(
              metadata_memtable_dependency, cf->dependentMemtableFlushToken())
        : FlushToken_INVALID;
    amount_picked += mem_stats.active_memtable_size;
    target = target > mem_stats.active_memtable_size
        ? target - mem_stats.active_memtable_size
        : 0;
    out.push_back(cf_data);
  };

  auto pick_metadata_cf = [&] {
    ld_check(!metadata_cf_picked);
    metadata_cf_picked = true;
    auto& mem_stats = metadata_cf_data.stats;
    amount_picked += mem_stats.active_memtable_size;
    target = target > mem_stats.active_memtable_size
        ? target - mem_stats.active_memtable_size
        : 0;
    // Avoid move for metadata cf as it is used everywhere.
    out.push_back(metadata_cf_data);
  };

  auto memLimitThresholdTriggered = [&ld_managed_flushes,
                                     this](uint64_t usage) {
    return ld_managed_flushes && usage >= max_memtable_size_trigger_;
  };

  auto update_buf_stats = [&cur_active_memory_usage,
                           this](const RocksDBMemTableStats& stats) {
    cur_active_memory_usage += stats.active_memtable_size;
    buf_stats_.memory_being_flushed += stats.immutable_memtable_size;
    buf_stats_.pinned_buffer_usage += stats.pinned_memtable_size;
    buf_stats_.num_active_memtables += stats.active_memtable_size > 0;
  };

  update_buf_stats(metadata_cf_data.stats);

  if (memLimitThresholdTriggered(metadata_cf_data.stats.active_memtable_size)) {
    pick_metadata_cf();
  }

  std::vector<CFData> unpicked_cf;
  // Select all oversized memtables, idle memtable and memtables with old data
  // first so that other memtables are allowed to stay in memory for some more
  // time.
  // Update aggregate memory usage stats.
  for (auto& cf_data : input) {
    auto& stats = cf_data.stats;
    update_buf_stats(stats);

    if (stats.active_memtable_size == 0) {
      continue;
    }

    auto oldDataThresholdTriggered = [&] {
      auto& cf = cf_data.cf;
      return settings->partition_data_age_flush_trigger.count() > 0 &&
          (now - cf->first_dirtied_time_) >
          settings->partition_data_age_flush_trigger;
    };

    auto idleThresholdTriggered = [&] {
      auto& latest_dirty_time = cf_data.latest_dirty_time;
      return (settings->partition_idle_flush_trigger.count() > 0) &&
          (latest_dirty_time != SteadyTimestamp::min()) &&
          (now > latest_dirty_time) &&
          (now - latest_dirty_time) > settings->partition_idle_flush_trigger;
    };

    // We try to pick to flush those CFs that we cannot ignore:
    //   * hit memory limit
    //   * too old
    //   * memtable was not touched for a long time
    // The rest of the CFs we will process later and maybe add them to the flush
    // list as well.
    if (memLimitThresholdTriggered(stats.active_memtable_size) ||
        oldDataThresholdTriggered() || idleThresholdTriggered()) {
      pick_cf(cf_data);
    } else {
      unpicked_cf.push_back(std::move(cf_data));
    }
  }

  // Recalculate target to reach using unpicked column families.
  target = 0;
  if (ld_managed_flushes &&
      cur_active_memory_usage > total_active_memory_trigger_) {
    auto diff = cur_active_memory_usage - amount_picked;
    if (diff > total_active_low_watermark_) {
      target = diff - total_active_low_watermark_;
    }
  }

  if (target > 0) {
    if (use_age_size_flush_heuristic) {
      const auto time_now = SteadyTimestamp::now();
      std::sort(unpicked_cf.begin(),
                unpicked_cf.end(),
                [time_now](const CFData& a, const CFData& b) {
                  const auto age_a =
                      (double)(time_now - a.cf->first_dirtied_time_).count();
                  const auto age_b =
                      (double)(time_now - b.cf->first_dirtied_time_).count();
                  return age_a * a.stats.active_memtable_size >
                      age_b * b.stats.active_memtable_size;
                });
    } else {
      // Recent two cfs with non-zero active memory usage are considered newer.
      // Consider all the others older and sort input in ascending order of data
      // age in cf.
      if (unpicked_cf.size() > 3) {
        std::sort(unpicked_cf.begin(),
                  unpicked_cf.end() - 2,
                  [](const CFData& a, const CFData& b) {
                    return a.cf->first_dirtied_time_ <
                        b.cf->first_dirtied_time_;
                  });
      };

      // Arrange the last two cf in descending order according to their
      // memory usage.
      if (unpicked_cf.size() >= 2) {
        auto& latest = unpicked_cf.back();
        auto& penultimate = *(unpicked_cf.rbegin() + 1);
        if (latest.stats.active_memtable_size >
            penultimate.stats.active_memtable_size) {
          std::swap(latest, penultimate);
        }
      }
    }
    // Selects all cf to reach the target memory budget.
    // CFs are already sorted in the correct order.
    for (auto i = 0; i < unpicked_cf.size() && target > 0; ++i) {
      pick_cf(unpicked_cf[i]);
    }
  }

  // Check if there is need to flush metadata cf.
  // If metadata cf is getting flushed activeMemtableFlushToken will return
  // FlushToken_INVALID or return a token greater than
  // metadata_memtable_dependency, this allows picked
  // data cf's to be flushed without metadata cf. This flush req can
  // complete writing out sst files before the dependent inflight metadata cf
  // completes the flush. RocksDB still makes sure that metadata cf flush
  // information gets committed in manifest before data cf flush which initiated
  // later. Each flush request gets its own sequencer number and the data is
  // committed in manifest in that sequencer number order.
  auto active_metadata_token = metadata_cf_data.cf->activeMemtableFlushToken();
  ld_check(active_metadata_token == FlushToken_INVALID ||
           active_metadata_token >= metadata_memtable_dependency ||
           metadata_memtable_dependency == FlushToken_MAX);
  auto isMetadataCFFlushNeeded = [&] {
    auto valid_token =
        !metadata_cf_picked && active_metadata_token != FlushToken_INVALID;
    return valid_token &&
        (metadata_memtable_dependency == FlushToken_MAX ||
         active_metadata_token == metadata_memtable_dependency || target > 0);
  };

  if (isMetadataCFFlushNeeded()) {
    pick_metadata_cf();
    // Make sure metadata cf is first in the list of column families to flush.
    ld_check(out.size() >= 1);
    std::swap(out.front(), out.back());
    ld_check(target == 0);
  }

  ld_check(cur_active_memory_usage >= amount_picked);
  buf_stats_.active_memory_usage = cur_active_memory_usage - amount_picked;

  ld_check_eq(out.size() == 0, amount_picked == 0);

  buf_stats_.memory_being_flushed += amount_picked;
  return out;
}

}} // namespace facebook::logdevice
