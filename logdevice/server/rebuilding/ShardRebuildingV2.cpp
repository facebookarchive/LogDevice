/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

// Hard coded because it's very unlikely that anyone would want to change it.
static constexpr std::chrono::milliseconds PROFILING_TIMER_PERIOD =
    std::chrono::minutes(1);

std::atomic<log_rebuilding_id_t::raw_type> ShardRebuildingV2::nextChunkID_{0};

ShardRebuildingV2::ShardRebuildingV2(
    shard_index_t shard,
    lsn_t rebuilding_version,
    lsn_t restart_version,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    NodeID my_node_id,
    ShardRebuildingInterface::Listener* listener)
    : rebuildingVersion_(rebuilding_version),
      restartVersion_(restart_version),
      shard_(shard),
      rebuildingSet_(rebuilding_set),
      rebuildingSettings_(rebuilding_settings),
      my_node_id_(my_node_id),
      listener_(listener),
      callbackHelper_(this) {}

ShardRebuildingV2::~ShardRebuildingV2() {
  PER_SHARD_STAT_SET(
      getStats(), rebuilding_global_window_waiting_flag, shard_, 0);
  abortChunkRebuildings();
}

void ShardRebuildingV2::abortChunkRebuildings() {
  for (const auto& p : chunkRebuildings_) {
    std::unique_ptr<Request> rq = std::make_unique<AbortChunkRebuildingRequest>(
        p.second.workerID, p.first.chunkID);
    int rv = Worker::onThisThread()->processor_->postImportant(rq);
    if (rv != 0) {
      // If we're shutting down, ServerWorker itself will clean up all chunk
      // rebuildings.
      ld_check(err == E::SHUTDOWN);
    }
  }
}

void ShardRebuildingV2::start(
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan) {
  numLogs_ = plan.size();
  startTime_ = SteadyTimestamp::now();
  readRateLimiter_ = RateLimiter(rebuildingSettings_->rate_limit);
  readContext_ = std::make_shared<RebuildingReadStorageTaskV2::Context>();
  readContext_->onDone = [this, this_ref = callbackHelper_.getHolder().ref()](
                             std::vector<std::unique_ptr<ChunkData>> chunks) {
    if (this_ref.get() != nullptr) {
      onReadTaskDone(std::move(chunks));
    }
  };
  readContext_->rebuildingSet = rebuildingSet_;
  readContext_->rebuildingSettings = rebuildingSettings_;
  readContext_->myShardID = ShardID(getMyNodeIndex(), shard_);

  for (const auto& log_plan : plan) {
    auto ins = readContext_->logs.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(log_plan.first),
        std::forward_as_tuple(std::move(*log_plan.second)));
    ld_check(ins.second);
  }

  delayedReadTimer_ = createTimer([this] { tryMakeProgress(); });
  iteratorInvalidationTimer_ = createTimer([this] { invalidateIterator(); });
  profilingTimer_ = createTimer([this] {
    profilingTimer_->activate(PROFILING_TIMER_PERIOD);
    flushCurrentStateTime();
  });

  profilingTimer_->activate(PROFILING_TIMER_PERIOD);

  tryMakeProgress();
}

void ShardRebuildingV2::advanceGlobalWindow(RecordTimestamp new_window_end) {
  globalWindowEnd_ = new_window_end;
  if (readContext_ != nullptr) {
    tryMakeProgress();
  } else {
    // start() hasn't been called yet.
  }
}

void ShardRebuildingV2::sendStorageTaskIfNeeded() {
  const size_t read_batch_size = rebuildingSettings_->max_batch_bytes;
  // Hard code max size of readBuffer_ as 3x max read batch size.
  // This could be a separate setting, but that doesn't seem very useful.
  const size_t max_read_buffer_size = read_batch_size * 3;

  // Note that reading is not affected by global window or
  // max_record_bytes_in_flight. Reading just tries to keep readBuffer_
  // reasonably full.
  if (completed_ || storageTaskInFlight_ || readContext_->reachedEnd ||
      readContext_->persistentError ||
      bytesInReadBuffer_ + read_batch_size > max_read_buffer_size ||
      rebuildingSettings_->test_stall_rebuilding) {
    return;
  }

  // Consult rate limiter. Use zero cost for now. We'll tell rate limiter the
  // actual cost in bytes once the storage task is done.
  std::chrono::steady_clock::duration to_wait;
  bool allowed = readRateLimiter_.isAllowed(
      0, &to_wait, std::chrono::steady_clock::duration::zero());
  if (!allowed) {
    if (to_wait != std::chrono::steady_clock::duration::max()) {
      delayedReadTimer_->activate(
          std::chrono::duration_cast<std::chrono::microseconds>(to_wait));
    }
    return;
  }

  if (readContext_->iterator != nullptr) {
    iteratorInvalidationTimer_->cancel();
  }
  storageTaskInFlight_ = true;
  putStorageTask();
}

void ShardRebuildingV2::putStorageTask() {
  auto task = std::make_unique<RebuildingReadStorageTaskV2>(readContext_);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_);
  task_queue->putTask(std::move(task));
}

std::chrono::milliseconds ShardRebuildingV2::getIteratorTTL() {
  return Worker::settings().iterator_cache_ttl;
}

std::unique_ptr<TimerInterface>
ShardRebuildingV2::createTimer(std::function<void()> cb) {
  return std::make_unique<Timer>(cb);
}

void ShardRebuildingV2::invalidateIterator() {
  ld_info("Invalidating rebuilding iterator in shard %u", shard_);
  ld_check(!storageTaskInFlight_);
  ld_check(readContext_->iterator != nullptr);
  readContext_->iterator->invalidate();
}

void ShardRebuildingV2::onReadTaskDone(
    std::vector<std::unique_ptr<ChunkData>> chunks) {
  // Report the cost of this read task to rate limiter.
  std::chrono::steady_clock::duration unused;
  readRateLimiter_.isAllowed(readContext_->bytesRead, &unused);

  for (auto& c : chunks) {
    bytesInReadBuffer_ += c->totalBytes();
  }
  readBuffer_.insert(readBuffer_.end(),
                     std::make_move_iterator(chunks.begin()),
                     std::make_move_iterator(chunks.end()));
  storageTaskInFlight_ = false;
  ++readTasksDone_;
  nextLocation_ = readContext_->nextLocation;
  readingProgressTimestamp_ = readContext_->progressTimestamp;
  readingProgress_ = readContext_->progress;
  if (readContext_->iterator != nullptr) {
    iteratorInvalidationTimer_->activate(getIteratorTTL());
  }
  tryMakeProgress();
}

void ShardRebuildingV2::startSomeChunkRebuildingsIfNeeded() {
  const size_t max_records_in_flight =
      rebuildingSettings_->max_records_in_flight;
  const size_t max_bytes_in_flight =
      rebuildingSettings_->max_record_bytes_in_flight;

  auto record_rebuildings_are_too_spread_out = [&] {
    const std::chrono::milliseconds local_window =
        rebuildingSettings_->local_window;
    return !chunkRebuildings_.empty() &&
        readBuffer_.front()->oldestTimestamp -
            chunkRebuildings_.begin()->first.oldestTimestamp >
        local_window;
  };

  while (!readBuffer_.empty() &&
         chunkRebuildingRecordsInFlight_ < max_records_in_flight &&
         chunkRebuildingBytesInFlight_ < max_bytes_in_flight &&
         !record_rebuildings_are_too_spread_out()) {
    if (readBuffer_.front()->oldestTimestamp > globalWindowEnd_) {
      break;
    }

    log_rebuilding_id_t chunk_id{++nextChunkID_};
    std::unique_ptr<ChunkData> chunk = std::move(readBuffer_.front());
    readBuffer_.pop_front();
    ld_check_ge(bytesInReadBuffer_, chunk->totalBytes());
    bytesInReadBuffer_ -= chunk->totalBytes();

    ChunkRebuildingKey key;
    key.oldestTimestamp = chunk->oldestTimestamp;
    key.chunkID = chunk_id;
    ChunkRebuildingInfo info;
    info.address = chunk->address;
    info.numRecords = chunk->numRecords();
    info.totalBytes = chunk->totalBytes();

    worker_id_t worker_id = startChunkRebuilding(std::move(chunk), chunk_id);
    if (worker_id == WORKER_ID_INVALID) {
      // The server is shutting down.
      return;
    }

    info.workerID = worker_id;

    chunkRebuildingRecordsInFlight_ += info.numRecords;
    chunkRebuildingBytesInFlight_ += info.totalBytes;
    chunkRebuildings_.emplace(key, info);
  }

  // Find the oldest timestamp of a chunk that we're going to rebuild but
  // haven't rebuilt yet, and publish this timestamp for other donors to slide
  // global window based on it.
  RecordTimestamp oldest_timestamp;
  if (!chunkRebuildings_.empty()) {
    // If there are some records in flight, use the oldest one.
    oldest_timestamp = chunkRebuildings_.begin()->first.oldestTimestamp;
  } else if (!readBuffer_.empty()) {
    // If there are some records in buffer, use the first one.
    oldest_timestamp = readBuffer_.front()->oldestTimestamp;
  } else {
    // Otherwise, either we're just starting, or we're filtering everything we
    // read. In the latter case it's useful to report the approximate progress
    // anyway, because reading can be slow even if we're filtering everything.
    // Reporting the progress allows other donors to make progress (if global
    // window is enabled), and keeps progress stat up to date.
    oldest_timestamp = readingProgressTimestamp_;
  }
  listener_->notifyShardDonorProgress(
      shard_, oldest_timestamp, rebuildingVersion_, readingProgress_);
}

worker_id_t
ShardRebuildingV2::startChunkRebuilding(std::unique_ptr<ChunkData> chunk,
                                        log_rebuilding_id_t chunk_id) {
  Processor* processor = Worker::onThisThread()->processor_;
  worker_id_t worker_id = processor->selectWorkerRandomly(
      chunk_id.val(), StartChunkRebuildingRequest::workerType);

  auto chunk_rebuilding =
      std::make_unique<ChunkRebuilding>(std::move(chunk),
                                        chunk_id,
                                        rebuildingSet_,
                                        rebuildingSettings_,
                                        rebuildingVersion_,
                                        restartVersion_,
                                        shard_,
                                        callbackHelper_.ticket());
  std::unique_ptr<Request> rq = std::make_unique<StartChunkRebuildingRequest>(
      worker_id, std::move(chunk_rebuilding));
  int rv = processor->postImportant(rq);
  if (rv != 0) {
    ld_check_eq(err, E::SHUTDOWN);
    return WORKER_ID_INVALID;
  }
  return worker_id;
}

void ShardRebuildingV2::onChunkRebuildingDone(
    log_rebuilding_id_t chunk_id,
    RecordTimestamp oldest_timestamp) {
  ChunkRebuildingKey key(oldest_timestamp, chunk_id);
  auto it = chunkRebuildings_.find(key);
  ld_check(it != chunkRebuildings_.end());
  ChunkRebuildingInfo& info = it->second;
  ld_check_ge(chunkRebuildingRecordsInFlight_, info.numRecords);
  ld_check_ge(chunkRebuildingBytesInFlight_, info.totalBytes);
  chunkRebuildingRecordsInFlight_ -= info.numRecords;
  chunkRebuildingBytesInFlight_ -= info.totalBytes;

  chunksRebuilt_ += 1;
  recordsRebuilt_ += info.numRecords;
  bytesRebuilt_ += info.totalBytes;
  PER_SHARD_STAT_ADD(getStats(), chunks_rebuilt, shard_, 1);
  PER_SHARD_STAT_ADD(getStats(), records_rebuilt, shard_, info.numRecords);
  PER_SHARD_STAT_ADD(getStats(), bytes_rebuilt, shard_, info.totalBytes);

  chunkRebuildings_.erase(it);

  tryMakeProgress();
}

void ShardRebuildingV2::finalizeIfNeeded() {
  // We're done if reading has reached the end, read buffer was drained,
  // and all chunk rebuildings have completed.
  // Additionally, if we got a persistent read error, we just stall the
  // ShardRebuilding indefinitely; usually this happens if our own disk is
  // broken, in which case self-initiated rebuilding will soon request a
  // rebuilding, and this ShardRebuilding will be aborted.
  if (completed_ || storageTaskInFlight_ || !readContext_->reachedEnd ||
      readContext_->persistentError || !readBuffer_.empty() ||
      !chunkRebuildings_.empty()) {
    return;
  }
  completed_ = true;
  ld_info("Rebuilt shard %u in %.3fs (%s). Rebuilt %lu chunks, %lu records, "
          "%lu bytes. Executed %lu read storage tasks.",
          shard_,
          std::chrono::duration_cast<std::chrono::duration<double>>(
              SteadyTimestamp::now() - startTime_)
              .count(),
          describeTimeByState().c_str(),
          chunksRebuilt_,
          recordsRebuilt_,
          bytesRebuilt_,
          readTasksDone_);
  listener_->onShardRebuildingComplete(shard_);
}

void ShardRebuildingV2::tryMakeProgress() {
  startSomeChunkRebuildingsIfNeeded();
  sendStorageTaskIfNeeded();
  updateProfilingState();
  finalizeIfNeeded();
}

void ShardRebuildingV2::noteConfigurationChanged() {
  // No need to do anything even when logs are removed from config.
  // Neither RebuildingReadStorageTaskV2 nor ChunkRebuilding require the log to
  // be in config.
}

void ShardRebuildingV2::noteRebuildingSettingsChanged() {
  readRateLimiter_.update(rebuildingSettings_->rate_limit);
  tryMakeProgress();
}

void ShardRebuildingV2::getDebugInfo(InfoRebuildingShardsTable& table) const {
  // Some measure of how far we have progressed, in terms of record timestamps.
  // TODO (#24665001):
  //   This doesn't match the current column name is "local_window_end".
  //   When rebuilding v2 becomes the default, rename the column.
  if (!chunkRebuildings_.empty()) {
    table.set<4>(
        chunkRebuildings_.begin()->first.oldestTimestamp.toMilliseconds());
  } else if (!readBuffer_.empty()) {
    table.set<4>(readBuffer_.front()->oldestTimestamp.toMilliseconds());
  } else {
    table.set<4>(readingProgressTimestamp_.toMilliseconds());
  }
  // Total memory used.
  table.set<9>(bytesInReadBuffer_ + chunkRebuildingBytesInFlight_);
  table.set<12>(numLogs_);
  table.set<14>(describeTimeByState());
  table.set<15>(storageTaskInFlight_);
  if (!storageTaskInFlight_) {
    table.set<16>(readContext_->persistentError);
  }
  table.set<17>(bytesInReadBuffer_);
  // TODO (#24665001):
  //   When ChunkRebuilding gets reimplemented to process all records at once,
  //   change this into number of chunks in flight.
  table.set<18>(chunkRebuildingRecordsInFlight_);
  if (nextLocation_ != nullptr) {
    table.set<19>(nextLocation_->toString());
  }
  table.set<20>(readingProgress_);
}

std::function<void(InfoRebuildingLogsTable&)>
ShardRebuildingV2::beginGetLogsDebugInfo() const {
  ld_check(readContext_ != nullptr);
  return [context = readContext_](InfoRebuildingLogsTable& table) {
    context->getLogsDebugInfo(table);
  };
}

StatsHolder* ShardRebuildingV2::getStats() {
  return Worker::stats();
}

node_index_t ShardRebuildingV2::getMyNodeIndex() {
  return my_node_id_.index();
}

const SimpleEnumMap<ShardRebuildingV2::ProfilingState, std::string>&
ShardRebuildingV2::profilingStateNames() {
  static SimpleEnumMap<ProfilingState, std::string> s_names(
      {{ProfilingState::FULLY_OCCUPIED, "fully_occupied"},
       {ProfilingState::WAITING_FOR_READ, "waiting_for_read"},
       {ProfilingState::RATE_LIMITED, "rate_limited"},
       {ProfilingState::WAITING_FOR_REREPLICATION, "waiting_for_rereplication"},
       {ProfilingState::STALLED, "stalled"}});
  return s_names;
}

void ShardRebuildingV2::flushCurrentStateTime() {
  auto now = SteadyTimestamp::now();
  if (profilingState_ == ProfilingState::MAX) {
    currentStateStartTime_ = now;
    return;
  }
  ld_check(currentStateStartTime_ != SteadyTimestamp::min());
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - currentStateStartTime_);
  currentStateStartTime_ = now;

  totalTimeByState_[(int)profilingState_] += elapsed_ms;

  switch (profilingState_) {
#define S(upper, lower)                                                 \
  case ProfilingState::upper:                                           \
    PER_SHARD_STAT_ADD(                                                 \
        getStats(), rebuilding_ms_##lower, shard_, elapsed_ms.count()); \
    break;
    S(FULLY_OCCUPIED, fully_occupied)
    S(WAITING_FOR_READ, waiting_for_read)
    S(RATE_LIMITED, rate_limited)
    S(WAITING_FOR_REREPLICATION, waiting_for_rereplication)
    S(STALLED, stalled)
#undef S
    case ProfilingState::MAX:
      ld_check(false);
  }
}
void ShardRebuildingV2::updateProfilingState() {
  ProfilingState new_state;
  if (chunkRebuildings_.empty()) {
    if (storageTaskInFlight_) {
      new_state = ProfilingState::WAITING_FOR_READ;
    } else if (readBuffer_.empty()) {
      new_state = ProfilingState::RATE_LIMITED;
    } else {
      new_state = ProfilingState::STALLED;
    }
  } else {
    new_state = storageTaskInFlight_
        ? ProfilingState::FULLY_OCCUPIED
        : ProfilingState::WAITING_FOR_REREPLICATION;
  }
  if (new_state != profilingState_) {
    // Log a message if we started or stopped waiting on global window.
    if (storageTaskInFlight_ || !readContext_->persistentError) {
      if (new_state == ProfilingState::STALLED) {
        PER_SHARD_STAT_SET(
            getStats(), rebuilding_global_window_waiting_flag, shard_, 1);
        ld_info("Rebuilding of shard %u is now waiting for global window to "
                "slide. Next timestamp to rebuild: %s, global window end: %s, "
                "total wait time so far: %.3fs",
                shard_,
                readBuffer_.empty()
                    ? "none"
                    : readBuffer_.front()->oldestTimestamp.toString().c_str(),
                globalWindowEnd_.toString().c_str(),
                totalTimeByState_[(int)ProfilingState::STALLED].count() / 1e3);
      } else if (profilingState_ == ProfilingState::STALLED) {
        PER_SHARD_STAT_SET(
            getStats(), rebuilding_global_window_waiting_flag, shard_, 0);
        ld_info(
            "Rebuilding of shard %u has finished waiting for global window to "
            "slide. Global window end: %s, total wait time so far: %.3fs",
            shard_,
            globalWindowEnd_.toString().c_str(),
            totalTimeByState_[(int)ProfilingState::STALLED].count() / 1e3);
      }
    }

    flushCurrentStateTime();
    profilingState_ = new_state;
  }
}

std::string ShardRebuildingV2::describeTimeByState() const {
  std::stringstream ss;
  ss.precision(3);
  ss.setf(std::ios::fixed, std::ios::floatfield);
  for (int i = 0; i < (int)ProfilingState::MAX; ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << profilingStateNames()[(ProfilingState)i] << ": "
       << totalTimeByState_[i].count() / 1e3 << "s";
  }
  return ss.str();
}

bool ShardRebuildingV2::ChunkRebuildingKey::
operator<(const ChunkRebuildingKey& rhs) const {
  return std::tie(oldestTimestamp, chunkID) <
      std::tie(rhs.oldestTimestamp, rhs.chunkID);
}
}} // namespace facebook::logdevice
