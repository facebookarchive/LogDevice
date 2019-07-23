/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/RebuildingReadStorageTaskV2.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

RebuildingReadStorageTaskV2::RebuildingReadStorageTaskV2(
    std::weak_ptr<Context> context)
    : StorageTask(StorageTask::Type::REBUILDING_READ_V2), context_(context) {}

void RebuildingReadStorageTaskV2::execute() {
  std::shared_ptr<Context> context = context_.lock();
  if (context == nullptr) {
    // The ShardRebuilding was aborted. Nothing to do.
    return;
  }

  auto start_time = SteadyTimestamp::now();

  // Let's just lock the mutex for the whole duration of the storage task.
  // We could make it more fine-grained and lock it only for accesses to
  // context's fields, avoiding blocking IO with locked mutex, but it doesn't
  // really matter because ShardRebuilding never accesses Context when a storage
  // task is in flight.
  std::lock_guard<std::mutex> context_lock(context->logsMutex);

  ld_check(!context->reachedEnd);
  ld_check(!context->persistentError);
  ld_check(result_.empty());

  StatsHolder* stats = getStats();

  STAT_INCR(stats, read_streams_num_ops_rebuilding);

  if (context->iterator == nullptr) {
    if (!fetchTrimPoints(context.get())) {
      context->persistentError = true;
      return;
    }

    LocalLogStore::ReadOptions opts(
        "RebuildingReadStorageTaskV2", /* rebuilding */ true);
    opts.fill_cache = context->rebuildingSettings->use_rocksdb_cache;
    opts.allow_copyset_index = true;

    std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>> logs;
    for (const auto& p : context->logs) {
      logs[p.first] =
          std::make_pair(p.second.trimPoint + 1, p.second.plan.untilLSN);
    }

    context->iterator = createIterator(opts, logs);
    context->nextLocation =
        std::shared_ptr<LocalLogStore::AllLogsIterator::Location>(
            context->iterator->minLocation());

    context->filter = std::make_unique<Filter>(context.get());
  }

  LocalLogStore::AllLogsIterator* iterator = context->iterator.get();

  context->filter->task = this;
  context->filter->clearStats();

  LocalLogStore::ReadStats read_stats;
  read_stats.max_bytes_to_read = context->rebuildingSettings->max_batch_bytes;
  read_stats.max_execution_time = context->rebuildingSettings->max_batch_time;
  // Count iterator initialization and trim point fetching towards the
  // execution time limit.
  read_stats.read_start_time = start_time;

  size_t records_in_result = 0;
  size_t bytes_in_result = 0;

  SCOPE_EXIT {
    context->bytesRead =
        read_stats.read_record_bytes + read_stats.read_csi_bytes;

    if (context->persistentError || context->reachedEnd) {
      context->iterator.reset();
    }

    if (context->persistentError) {
      // No point delivering records if rebuilding is going to stall now.
      result_.clear();
    } else {
      auto shard = context->myShardID.shard();
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_records_read_rebuilding,
                         shard,
                         read_stats.read_records);
      PER_SHARD_STAT_ADD(
          stats,
          read_streams_num_bytes_read_rebuilding,
          shard,
          read_stats.read_record_bytes + read_stats.read_csi_bytes);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_record_bytes_read_rebuilding,
                         shard,
                         read_stats.read_record_bytes);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_csi_entries_read_rebuilding,
                         shard,
                         read_stats.read_csi_entries);
      PER_SHARD_STAT_ADD(stats,
                         read_streams_num_csi_bytes_read_rebuilding,
                         shard,
                         read_stats.read_csi_bytes);
      STAT_ADD(stats,
               read_streams_num_records_filtered_rebuilding,
               read_stats.filtered_records);
      STAT_ADD(stats,
               read_streams_num_bytes_filtered_rebuilding,
               read_stats.filtered_record_bytes);
      STAT_ADD(stats,
               read_streams_num_records_late_filtered_rebuilding,
               context->filter->nRecordsLateFiltered);

      size_t tot_skipped = context->filter->nRecordsSCDFiltered +
          context->filter->nRecordsNotDirtyFiltered +
          context->filter->nRecordsTimestampFiltered +
          context->filter->nRecordsDrainedFiltered +
          context->filter->nRecordsEpochRangeFiltered;

      auto end_time = SteadyTimestamp::now();

      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Rebuilding has read a batch of records in %.3fs. Got %lu records "
          "(%lu bytes) in %lu chunks. Skipped %lu records (SCD: %lu, ND: %lu, "
          "DRAINED: %lu, TS: %lu, EPOCH: %lu; LATE: %lu).",
          std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                    start_time)
              .count(),
          records_in_result,
          bytes_in_result,
          result_.size(),
          tot_skipped,
          context->filter->nRecordsSCDFiltered,
          context->filter->nRecordsNotDirtyFiltered,
          context->filter->nRecordsDrainedFiltered,
          context->filter->nRecordsTimestampFiltered,
          context->filter->nRecordsEpochRangeFiltered,
          context->filter->nRecordsLateFiltered);
    }
  };

  std::vector<ShardID> temp_copyset;

  switch (iterator->state()) {
    case IteratorState::AT_RECORD:
      // Previous storage task stopped at a record.
      break;
    case IteratorState::LIMIT_REACHED:
      // Previous storage task reached a limit. Tell it to resume where it
      // left off, but with the refreshed limit.
      iterator->next(context->filter.get(), &read_stats);
      break;
    case IteratorState::AT_END:
      // Newly created or invalidated iterator.
      iterator->seek(
          *context->nextLocation, context->filter.get(), &read_stats);
      break;
    case IteratorState::ERROR:
      // If previous storage task got the iterator into ERROR state, it would
      // have reported it, and ShardRebuilding wouldn't have scheduled another
      // storage task.
    case IteratorState::WOULDBLOCK:
    case IteratorState::MAX:
      ld_critical(
          "Unexpected iterator state: %s", toString(iterator->state()).c_str());
      ld_check(false);
      context->persistentError = true;
      return;
  }

  ld_check(result_.empty());
  ChunkData* chunk = nullptr;
  Context::LogState* log_state = nullptr;
  for (; iterator->state() == IteratorState::AT_RECORD;
       iterator->next(context->filter.get(), &read_stats)) {
    if (!result_.empty() && read_stats.readLimitReached()) {
      // Current record took us over the limit. If it's not the first record,
      // stop here without delivering it.
      break;
    }

    logid_t log = iterator->getLogID();
    lsn_t lsn = iterator->getLSN();
    Slice record = iterator->getRecord();

    bool start_new_chunk = false;

    // Make sure log_state points to current log.
    if (chunk == nullptr || log != chunk->address.log) {
      auto it = context->logs.find(log);
      ld_check(it != context->logs.end()); // Filter should reject such records
      log_state = &it->second;
      start_new_chunk = true;
    }

    if (lsn <= log_state->lastSeenLSN) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "AllLogsIterator's LSN went backwards. This should be rare. Log: "
          "%lu, last seen lsn: %s, current lsn: %s, location: %s",
          log.val(),
          lsn_to_string(log_state->lastSeenLSN).c_str(),
          lsn_to_string(lsn).c_str(),
          iterator->getLocation()->toString().c_str());
      continue;
    }

    // Bump currentBlockID if needed.
    RecordTimestamp timestamp;
    int rv = checkRecordForBlockChange(
        log, lsn, record, context.get(), log_state, &temp_copyset, &timestamp);
    if (rv != 0) {
      ld_check_eq(err, E::MALFORMED_RECORD);
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Malformed record at %s",
                      iterator->getLocation()->toString().c_str());
      STAT_INCR(stats, rebuilding_malformed_records);
      ++context->numMalformedRecordsSeen;
      if (context->numMalformedRecordsSeen >=
          context->rebuildingSettings->max_malformed_records_to_tolerate) {
        // Suspiciously many records are malformed. Escalate and stall.
        ld_critical("Rebuilding saw too many (%lu) malformed records. "
                    "Stopping rebuilding just in case. Please investigate.",
                    context->numMalformedRecordsSeen);
        context->persistentError = true;
        return;
      }

      continue;
    }

    if (!start_new_chunk) {
      // Chunk needs to contain records for the same log with consecutive LSNs,
      // same copyset and same block ID.
      // The consecutive LSN requirement is not necessary currently,
      // but may be useful in future for donor-driven rebuilding without WAL.
      start_new_chunk |= lsn > log_state->lastSeenLSN + 1 ||
          log_state->currentBlockID != chunk->blockID;
    }
    log_state->lastSeenLSN = lsn;

    // Create new chunk if needed.
    if (start_new_chunk) {
      result_.push_back(std::make_unique<ChunkData>());
      chunk = result_.back().get();
      chunk->address.log = log;
      chunk->address.min_lsn = lsn;
      chunk->blockID = log_state->currentBlockID;
      chunk->oldestTimestamp = timestamp;
      ++log_state->chunksDelivered;

      bool found = lookUpEpochMetadata(log,
                                       lsn,
                                       context.get(),
                                       log_state,
                                       /* create_replication_scheme */ true);
      // If the record is not covered by RebuildingPlan, ReadFilter would have
      // discarded it.
      ld_check(found);
      ld_check_le(log_state->currentEpochRange.first, lsn_to_epoch(lsn));
      ld_check_gt(log_state->currentEpochRange.second, lsn_to_epoch(lsn));
      ld_check(log_state->currentReplication != nullptr);
      chunk->replication = log_state->currentReplication;
    } else {
      // All records of a chunk must share the same EpochMetadata.
      // To ensure that let's ensure that they share the same epoch.
      ld_check_eq(lsn_to_epoch(lsn), lsn_to_epoch(chunk->address.min_lsn));
    }

    chunk->address.max_lsn = lsn;
    chunk->addRecord(lsn, record);
    ++log_state->recordsDelivered;
    log_state->bytesDelivered += record.size;
    ++records_in_result;
    bytes_in_result += record.size;
  }

  switch (iterator->state()) {
    case IteratorState::AT_RECORD:
    case IteratorState::LIMIT_REACHED:
      context->nextLocation =
          std::shared_ptr<LocalLogStore::AllLogsIterator::Location>(
              iterator->getLocation());
      context->progress = iterator->getProgress();
      break;
    case IteratorState::AT_END:
      context->reachedEnd = true;
      context->iterator.reset();
      context->nextLocation.reset();
      context->progress = 1;
      break;
    case IteratorState::WOULDBLOCK:
    case IteratorState::MAX:
      ld_critical(
          "Unexpected iterator state: %s", toString(iterator->state()).c_str());
      ld_check(false);
      FOLLY_FALLTHROUGH;
    case IteratorState::ERROR:
      ld_info("Stopping rebuilding after encountering an iterator error.");
      context->persistentError = true;
      break;
  }
}

int RebuildingReadStorageTaskV2::checkRecordForBlockChange(
    logid_t log,
    lsn_t lsn,
    Slice record,
    Context* context,
    Context::LogState* log_state,
    std::vector<ShardID>* temp_copyset,
    RecordTimestamp* out_timestamp) {
  copyset_size_t new_copyset_size;
  Payload payload;
  temp_copyset->resize(COPYSET_SIZE_MAX);
  std::chrono::milliseconds timestamp;
  int rv = LocalLogStoreRecordFormat::parse(record,
                                            &timestamp,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            &new_copyset_size,
                                            temp_copyset->data(),
                                            COPYSET_SIZE_MAX,
                                            nullptr,
                                            nullptr,
                                            &payload,
                                            context->myShardID.shard());
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Cannot parse record at lsn %s of log %lu.",
                    lsn_to_string(lsn).c_str(),
                    log.val());
    ld_check(err == E::MALFORMED_RECORD);
    return -1;
  }
  *out_timestamp = RecordTimestamp(timestamp);
  temp_copyset->resize(new_copyset_size);
  bool copyset_changed = (log_state->lastSeenCopyset != *temp_copyset);
  bool epoch_changed =
      lsn_to_epoch(log_state->lastSeenLSN) != lsn_to_epoch(lsn);
  // Factor 2 is arbitrary.
  size_t max_block_size = getSettings().get()->sticky_copysets_block_size * 2;
  bool byte_limit_exceeded = log_state->bytesInCurrentBlock > max_block_size;
  if (copyset_changed || epoch_changed || byte_limit_exceeded) {
    // end of the block reached, bump block counter and save the new copyset
    ++log_state->currentBlockID;
    log_state->bytesInCurrentBlock = 0;
    std::swap(log_state->lastSeenCopyset, *temp_copyset);
  }
  log_state->bytesInCurrentBlock += payload.size();
  return 0;
}

bool RebuildingReadStorageTaskV2::lookUpEpochMetadata(
    logid_t log,
    lsn_t lsn,
    Context* context,
    Context::LogState* log_state,
    bool create_replication_scheme) {
  epoch_t epoch = lsn_to_epoch(lsn);
  if (epoch < log_state->currentEpochRange.first ||
      epoch >= log_state->currentEpochRange.second) {
    // This record is not in the same epoch range as the last one.
    // Look up the new epoch range.
    log_state->currentEpochMetadata =
        log_state->plan.lookUpEpoch(epoch, &log_state->currentEpochRange);
    log_state->currentReplication = nullptr;
  }

  if (log_state->currentEpochMetadata == nullptr) {
    // Rebuilding is not interested in this epoch.
    return false;
  }

  if (create_replication_scheme && log_state->currentReplication == nullptr) {
    auto ucfg = getConfig();
    auto cfg = ucfg->get();
    auto log_group = cfg->getLogGroupByIDShared(log);
    auto& rebuilding_shards = context->rebuildingSet->shards;
    auto it = rebuilding_shards.find(context->myShardID);
    bool relocate_local_records = it != rebuilding_shards.end() &&
        it->second.mode == RebuildingMode::RELOCATE;
    log_state->currentReplication = std::make_shared<ReplicationScheme>(
        log,
        *log_state->currentEpochMetadata,
        ucfg->getNodesConfiguration(),
        getMyNodeID(),
        log_group ? &log_group->attrs() : nullptr,
        *getSettings().get(),
        relocate_local_records,
        log_state->plan.sequencerNodeID);
    markNodesInRebuildingSetNotAvailable(
        log_state->currentReplication->nodeset_state.get(), context);
  }

  return true;
}

void RebuildingReadStorageTaskV2::markNodesInRebuildingSetNotAvailable(
    NodeSetState* nodeset_state,
    Context* context) {
  for (auto it : context->rebuildingSet->shards) {
    if (!nodeset_state->containsShard(it.first)) {
      // This node is no longer in the config.
      continue;
    }
    if (!it.second.dc_dirty_ranges.empty()) {
      // Shard is only missing some time-ranged records. It should be
      // up and able to take stores.
      continue;
    }
    nodeset_state->setNotAvailableUntil(
        it.first,
        std::chrono::steady_clock::time_point::max(),
        NodeSetState::NodeSetState::NotAvailableReason::STORE_DISABLED);
  }
}

void RebuildingReadStorageTaskV2::onDone() {
  std::shared_ptr<Context> context = context_.lock();
  if (context == nullptr) {
    // The ShardRebuilding was aborted. Nothing to do.
    return;
  }
  context->onDone(std::move(result_));
}
void RebuildingReadStorageTaskV2::onDropped() {
  ld_check(false);
}

void RebuildingReadStorageTaskV2::getDebugInfoDetailed(
    StorageTaskDebugInfo& info) const {
  std::shared_ptr<Context> context = context_.lock();
  if (context == nullptr) {
    info.extra_info = "ShardRebuilding went away";
    return;
  }
  info.extra_info =
      folly::sformat("{} logs, location: {}",
                     context->logs.size(),
                     context->nextLocation ? context->nextLocation->toString()
                                           : std::string("none"));
}

UpdateableSettings<Settings> RebuildingReadStorageTaskV2::getSettings() {
  return storageThreadPool_->getSettings();
}
std::shared_ptr<UpdateableConfig> RebuildingReadStorageTaskV2::getConfig() {
  return storageThreadPool_->getProcessor().config_;
}

folly::Optional<NodeID> RebuildingReadStorageTaskV2::getMyNodeID() {
  return storageThreadPool_->getProcessor().getOptionalMyNodeID();
}
StatsHolder* RebuildingReadStorageTaskV2::getStats() {
  return storageThreadPool_->stats();
}

std::unique_ptr<LocalLogStore::AllLogsIterator>
RebuildingReadStorageTaskV2::createIterator(
    const LocalLogStore::ReadOptions& opts,
    const std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>& logs) {
  return storageThreadPool_->getLocalLogStore().readAllLogs(opts, logs);
}

bool RebuildingReadStorageTaskV2::fetchTrimPoints(Context* context) {
  LogStorageStateMap& log_state_map =
      storageThreadPool_->getProcessor().getLogStorageStateMap();
  for (auto& p : context->logs) {
    LogStorageState* s =
        log_state_map.insertOrGet(p.first, context->myShardID.shard());
    ld_check(s != nullptr);
    folly::Optional<lsn_t> t = s->getTrimPoint();
    if (t.hasValue()) {
      // Got trim point in memory.
      p.second.trimPoint = t.value();
      continue;
    }

    // Need to load trim point from rocksdb.
    TrimMetadata meta{LSN_INVALID};
    int rv =
        storageThreadPool_->getLocalLogStore().readLogMetadata(p.first, &meta);
    if (rv == 0 || err == E::NOTFOUND) {
      s->updateTrimPoint(meta.trim_point_);
      p.second.trimPoint = meta.trim_point_;
    } else {
      storageThreadPool_->getLocalLogStore().enterFailSafeMode(
          "RebuildingReadStorageTaskV2::fetchTrimPoints()",
          "Failed to read TrimMetadata");
      s->notePermanentError(
          "Reading trim point (in RebuildingReadStorageTaskV2)");
      return false;
    }
  }
  return true;
}
void RebuildingReadStorageTaskV2::updateTrimPoint(
    logid_t log,
    Context* context,
    Context::LogState* log_state) {
  LogStorageStateMap& log_state_map =
      storageThreadPool_->getProcessor().getLogStorageStateMap();
  LogStorageState* s = log_state_map.find(log, context->myShardID.shard());
  ld_check(s != nullptr); // fetchTrimPoints() should have created it
  folly::Optional<lsn_t> t = s->getTrimPoint();
  ld_check(t.hasValue()); // fetchTrimPoints() should have set it
  log_state->trimPoint = t.value();
}

RebuildingReadStorageTaskV2::Filter::Filter(Context* context)
    : context(context) {
  scd_my_shard_id_ = context->myShardID;
}

void RebuildingReadStorageTaskV2::Filter::clearStats() {
  nRecordsLateFiltered = 0;
  nRecordsSCDFiltered = 0;
  nRecordsNotDirtyFiltered = 0;
  nRecordsDrainedFiltered = 0;
  nRecordsTimestampFiltered = 0;
  nRecordsEpochRangeFiltered = 0;
}

bool RebuildingReadStorageTaskV2::Filter::shouldProcessTimeRange(
    RecordTimestamp min,
    RecordTimestamp max) {
  auto& cache = timeRangeCache;
  cache.clear();

  if (min > max) {
    // [+inf, -inf] is expected for empty partitions. Any other inverted
    // ranges are not expected.
    if (min != RecordTimestamp::max() || max != RecordTimestamp::min()) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "shouldProcessTimeRange() called with min > max: %s > %s",
                     min.toString().c_str(),
                     max.toString().c_str());
    }
    // Be conservative.
    return true;
  }

  if (min != RecordTimestamp::min()) {
    context->progressTimestamp = min;
  }

  cache.minTs = min;
  cache.maxTs = max;
  bool have_shards_intersecting_range = false;

  for (const auto& node_kv : context->rebuildingSet->shards) {
    ShardID shard = node_kv.first;
    auto& node_info = node_kv.second;
    if (node_info.dc_dirty_ranges.empty()) {
      // Empty dc_dirty_ranges means that the node is dirty for all time points.
      have_shards_intersecting_range = true;
      continue;
    }
    // Node is only partially dirty (time range data is provided).
    ld_check(node_info.mode == RebuildingMode::RESTORE);
    for (const auto& dc_tr_kv : node_info.dc_dirty_ranges) {
      if (dc_tr_kv.second.empty()) {
        ld_check(false);
        continue;
      }

      auto& time_ranges = dc_tr_kv.second;
      if (boost::icl::intersects(time_ranges, RecordTimeInterval(min, max))) {
        // The shard is dirty for some of the timestamps in [min, max].
        have_shards_intersecting_range = true;
      } else {
        // The shard is clean for all timestamps in [min, max].
        cache.shardsOutsideTimeRange.emplace(shard, dc_tr_kv.first);
      }
    }
  }

  return have_shards_intersecting_range;
}

bool RebuildingReadStorageTaskV2::Filter::shouldProcessRecordRange(
    logid_t log,
    lsn_t min_lsn,
    lsn_t max_lsn,
    RecordTimestamp /* min_ts */,
    RecordTimestamp /* max_ts */) {
  // Note: we're not taking time ranges into account. I.e. if in some partition
  // the effective rebuilding set (rebuilding set minus shardsOutsideTimeRange)
  // doesn't intersect some log's nodeset, we could detect it here and skip
  // this log in this partition. For simplicity we're not doing it currently.
  // We're just skipping epoch ranges not covered by rebuilding plan.

  if (!lookUpLogState(log)) {
    return false;
  }

  task->updateTrimPoint(log, context, currentLogState);
  if (max_lsn <= currentLogState->trimPoint ||
      min_lsn > currentLogState->plan.untilLSN) {
    return false;
  }

  // Find the epoch range covering min_lsn.
  bool first_epoch_good =
      task->lookUpEpochMetadata(log,
                                min_lsn,
                                context,
                                currentLogState,
                                /* create_replication_scheme */ false);
  // If min_lsn is covered by rebuilding plan, process the range.
  if (first_epoch_good) {
    return true;
  }
  // If max_lsn is in the same, non-covered, epoch range as min_lsn, reject the
  // lsn range.
  if (lsn_to_epoch(max_lsn) < currentLogState->currentEpochRange.second) {
    return false;
  }

  // The epoch currentEpochRange.second is covered by plan and
  // intersects [min_lsn, max_lsn]. Process the lsn range.
  return true;
}

bool RebuildingReadStorageTaskV2::Filter::
operator()(logid_t log,
           lsn_t lsn,
           const ShardID* copyset,
           copyset_size_t copyset_size,
           LocalLogStoreRecordFormat::csi_flags_t flags,
           RecordTimestamp min_ts,
           RecordTimestamp max_ts) {
  required_in_copyset_.clear();
  scd_known_down_.clear();

  // Assume that iterator passes an exact timestamp iff it had to read the
  // full record. If we end up filtering out such records a lot, that means
  // there's room for improvement: we'd rather filter them out at CSI stage
  // without reading the full records. So count such situations separately in
  // stats.
  bool late = min_ts == max_ts;

  if (flags & LocalLogStoreRecordFormat::CSI_FLAG_DRAINED) {
    noteRecordFiltered(FilteredReason::DRAINED, late);
    return false;
  }

  if (!lookUpLogState(log)) {
    noteRecordFiltered(FilteredReason::EPOCH_RANGE, late);
    return false;
  }

  if (lsn <= currentLogState->trimPoint ||
      lsn > currentLogState->plan.untilLSN) {
    return false;
  }

  // Look up the EpochMetaData to find replication factor.
  if (!task->lookUpEpochMetadata(log,
                                 lsn,
                                 context,
                                 currentLogState,
                                 /* create_replication_scheme */ false)) {
    noteRecordFiltered(FilteredReason::EPOCH_RANGE, late);
    return false;
  }

  // Tell base LocalLogStoreReadFilter what the "normal" replication factor is.
  scd_replication_ =
      currentLogState->currentEpochMetadata->replication.getReplicationFactor();

  auto filtered_reason = FilteredReason::NOT_DIRTY;
  // TODO(T43708398): in order to work around T43708398, we always look for
  // append dirty ranges.
  auto dc = DataClass::APPEND;

  for (copyset_off_t i = 0; i < copyset_size; ++i) {
    ShardID shard = copyset[i];
    auto node_kv = context->rebuildingSet->shards.find(shard);
    if (node_kv != context->rebuildingSet->shards.end()) {
      auto& node_info = node_kv->second;
      if (!node_info.dc_dirty_ranges.empty()) {
        // Node is only partially dirty (time range data is provided).
        ld_check(node_info.mode == RebuildingMode::RESTORE);

        // Exclude if DataClass/Timestamp do not match.
        auto dc_tr_kv = node_info.dc_dirty_ranges.find(dc);
        if (dc_tr_kv == node_info.dc_dirty_ranges.end() ||
            dc_tr_kv->second.empty()) {
          // DataClass isn't dirty.
          // We should never serialize an empty DataClass since it is
          // not dirty, but we tolerate it in production builds.
          ld_check(dc_tr_kv == node_info.dc_dirty_ranges.end());
          continue;
        }

        // Check if the record's timestamp intersects some of the
        // time ranges of this shard in the rebuilding set.
        const auto& time_ranges = dc_tr_kv->second;
        bool intersects;
        if (timeRangeCache.valid(min_ts, max_ts)) {
          // (a) Just like (d), but we already have a cached result for this
          //     time range.
          intersects = !timeRangeCache.shardsOutsideTimeRange.count(
              std::make_pair(shard, dc));
        } else if (min_ts == max_ts) {
          // (b) We know the exact timestamp of the record.
          intersects = time_ranges.find(min_ts) != time_ranges.end();
        } else if (min_ts > max_ts) {
          // (c) Invalid range. Be paranoid and assume that it intersects the
          //     rebuilding range.
          RATELIMIT_INFO(std::chrono::seconds(10),
                         2,
                         "operator() called with min_ts > max_ts: %s > %s",
                         min_ts.toString().c_str(),
                         max_ts.toString().c_str());
          intersects = true;
        } else {
          // (d) We don't know the exact timestamp, but we know that it's
          //     somewhere in [min_ts, max_ts] range. Check if this range
          //     intersects any of the rebuilding time ranges for this shard.
          intersects = boost::icl::intersects(
              time_ranges, RecordTimeInterval(min_ts, max_ts));
          // At the time of writing, this should be unreachable with all
          // existing LocalLogStore::ReadIterator implementations.
          // If you see this message, it's likely that there's a bug.
          RATELIMIT_INFO(
              std::chrono::seconds(10),
              1,
              "Time range in operator() (log %lu lsn %s ts [%s, %s]) doesn't "
              "match time range in shouldProcessTimeRange() ([%s, %s]). "
              "Suspicious. Please check the code.",
              log.val(),
              lsn_to_string(lsn).c_str(),
              min_ts.toString().c_str(),
              max_ts.toString().c_str(),
              timeRangeCache.minTs.toString().c_str(),
              timeRangeCache.maxTs.toString().c_str());
        }

        if (!intersects) {
          // Record falls outside a dirty time range.
          filtered_reason = FilteredReason::TIMESTAMP;
          continue;
        }
      }

      // Records inside a dirty region may be lost, but some/all may
      // have been durably stored before we crashed. We only serve as a
      // donor for records we happen to find in a dirty region if some
      // other node's failure also impacts the record (i.e. if we get past
      // this point during a different iteration of this loop).
      ld_check(scd_my_shard_id_.isValid());
      if (shard == scd_my_shard_id_ &&
          node_kv->second.mode == RebuildingMode::RESTORE) {
        continue;
      }

      // Node's shard either needs to be fully rebuilt or is dirty in this
      // region and we can serve as a donor.
      required_in_copyset_.push_back(shard);

      // If the rebuilding node is rebuilding in RESTORE mode, it should not
      // participate as a donor. Add it to the known down list so that other
      // nodes will send the records for which it was the leader.
      //
      // Note: If this node is in the rebuilding set for a time-ranged
      //       rebuild, and that range overlaps with under-replication
      //       on another node, it is possible for our node id to be added
      //       here. However, the SCD filtering logic ignores known_down
      //       for the local node id and we will be considered a donor for
      //       the record. This can lead to overreplication, but also
      //       ensures that data that can be rebuilt isn't skipped.
      if (node_kv->second.mode == RebuildingMode::RESTORE) {
        scd_known_down_.push_back(shard);
      }
    }
  }

  // Perform SCD copyset filtering.
  bool result = !required_in_copyset_.empty();
  if (result) {
    filtered_reason = FilteredReason::SCD;
    result = LocalLogStoreReadFilter::operator()(
        log, lsn, copyset, copyset_size, flags, min_ts, max_ts);
  }
  if (!result) {
    noteRecordFiltered(filtered_reason, late);
  }
  return result;
}

bool RebuildingReadStorageTaskV2::Filter::lookUpLogState(logid_t log) {
  if (log == currentLog) {
    return currentLogState != nullptr;
  }
  currentLog = log;
  auto it = context->logs.find(log);
  if (it == context->logs.end()) {
    currentLogState = nullptr;
    return false;
  }
  currentLogState = &it->second;
  return true;
}

/**
 * Update stats regarding skipped records.
 */
void RebuildingReadStorageTaskV2::Filter::noteRecordFiltered(
    FilteredReason reason,
    bool late) {
  switch (reason) {
    case FilteredReason::SCD:
      ++nRecordsSCDFiltered;
      break;
    case FilteredReason::NOT_DIRTY:
      ++nRecordsNotDirtyFiltered;
      break;
    case FilteredReason::DRAINED:
      ++nRecordsDrainedFiltered;
      break;
    case FilteredReason::TIMESTAMP:
      ++nRecordsTimestampFiltered;
      break;
    case FilteredReason::EPOCH_RANGE:
      ++nRecordsEpochRangeFiltered;
      break;
  }
  if (late) {
    ++nRecordsLateFiltered;
  }
}

void RebuildingReadStorageTaskV2::Context::getLogsDebugInfo(
    InfoRebuildingLogsTable& table) const {
  std::lock_guard<std::mutex> lock(logsMutex);
  for (const auto& p : logs) {
    const LogState& s = p.second;
    table.next()
        .set<0>(p.first)
        .set<1>(myShardID.shard())
        .set<5>(s.plan.untilLSN)
        .set<7>(s.lastSeenLSN)
        .set<8>(s.recordsDelivered)
        .set<9>(s.bytesDelivered);
  }
}

}} // namespace facebook::logdevice
