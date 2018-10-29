/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"

#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

std::atomic<log_rebuilding_id_t::raw_type> ShardRebuildingV2::nextChunkID_{0};

ShardRebuildingV2::ShardRebuildingV2(
    shard_index_t shard,
    lsn_t rebuilding_version,
    lsn_t restart_version,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    std::shared_ptr<UpdateableConfig> config,
    ShardRebuildingInterface::Listener* listener)
    : rebuildingVersion_(rebuilding_version),
      restartVersion_(restart_version),
      shard_(shard),
      rebuildingSet_(rebuilding_set),
      rebuildingSettings_(rebuilding_settings),
      config_(config),
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
  readContext_ = std::make_shared<RebuildingReadStorageTaskV2::Context>();
  readContext_->onDone = [this, this_ref = callbackHelper_.getHolder().ref()](
                             std::vector<std::unique_ptr<ChunkData>> chunks) {
    // onDone is only called if Context is still alive, which means
    // ShardRebuildingV2 must be alive as well.
    assert(this_ref.get() != nullptr);
    onReadTaskDone(std::move(chunks));
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
  if (rebuildingSettings_->test_stall_rebuilding) {
    return;
  }

  const size_t read_batch_size = rebuildingSettings_->max_batch_bytes;
  // Hard code max size of readBuffer_ as 3x max read batch size.
  // This could be a separate setting, but that doesn't seem very useful.
  const size_t max_read_buffer_size = read_batch_size * 3;

  // Note that reading is not affected by global window or
  // max_record_bytes_in_flight. Reading just tries to keep readBuffer_
  // reasonably full.
  if (completed_ || storageTaskInFlight_ || readContext_->reachedEnd ||
      readContext_->persistentError ||
      bytesInReadBuffer_ + read_batch_size > max_read_buffer_size) {
    return;
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

void ShardRebuildingV2::onReadTaskDone(
    std::vector<std::unique_ptr<ChunkData>> chunks) {
  for (auto& c : chunks) {
    bytesInReadBuffer_ += c->totalBytes();
  }
  readBuffer_.insert(readBuffer_.end(),
                     std::make_move_iterator(chunks.begin()),
                     std::make_move_iterator(chunks.end()));
  storageTaskInFlight_ = false;
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

  bool waiting_for_global_window = false;

  while (!readBuffer_.empty() &&
         chunkRebuildingRecordsInFlight_ < max_records_in_flight &&
         chunkRebuildingBytesInFlight_ < max_bytes_in_flight &&
         !record_rebuildings_are_too_spread_out()) {
    if (readBuffer_.front()->oldestTimestamp > globalWindowEnd_) {
      waiting_for_global_window = true;
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
  if (!chunkRebuildings_.empty()) {
    listener_->notifyShardDonorProgress(
        shard_,
        chunkRebuildings_.begin()->first.oldestTimestamp,
        rebuildingVersion_);
  } else if (!readBuffer_.empty()) {
    listener_->notifyShardDonorProgress(
        shard_, readBuffer_.front()->oldestTimestamp, rebuildingVersion_);
  }

  // Update stats about how much time we're spending waiting on global window.
  waiting_for_global_window &= chunkRebuildings_.empty();
  if (waiting_for_global_window !=
      (waitingOnGlobalWindowSince_ != SteadyTimestamp::max())) {
    if (waiting_for_global_window) {
      waitingOnGlobalWindowSince_ = SteadyTimestamp::now();
      PER_SHARD_STAT_SET(
          getStats(), rebuilding_global_window_waiting_flag, shard_, 1);

      ld_info("Rebuilding of shard %u is now waiting for global window to "
              "slide. Next timestamp to rebuild: %s, global window end: %s",
              shard_,
              readBuffer_.front()->oldestTimestamp.toString().c_str(),
              globalWindowEnd_.toString().c_str());
    } else {
      auto waited_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          SteadyTimestamp::now() - waitingOnGlobalWindowSince_);
      waitingOnGlobalWindowSince_ = SteadyTimestamp::max();
      PER_SHARD_STAT_ADD(getStats(),
                         rebuilding_global_window_waiting_total,
                         shard_,
                         waited_ms.count());
      PER_SHARD_STAT_SET(
          getStats(), rebuilding_global_window_waiting_flag, shard_, 0);

      ld_info(
          "Rebuilding of shard %u has finished waiting for global window to "
          "slide. Waited for %.3fs. Global window end: %s",
          shard_,
          waited_ms.count() / 1e3,
          globalWindowEnd_.toString().c_str());
    }
  }
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
  listener_->onShardRebuildingComplete(shard_);
}

void ShardRebuildingV2::noteConfigurationChanged() {
  // TODO (#T24665001): Do we need to handle logs being removed from config, or
  // do RecordRebuildings already handle it sufficiently?
}

void ShardRebuildingV2::getDebugInfo(InfoShardsRebuildingTable& table) const {
  // TODO (#T24665001): implement.
}

StatsHolder* ShardRebuildingV2::getStats() {
  return Worker::stats();
}

node_index_t ShardRebuildingV2::getMyNodeIndex() {
  return config_->getServerConfig()->getMyNodeID().index();
}

bool ShardRebuildingV2::ChunkRebuildingKey::
operator<(const ChunkRebuildingKey& rhs) const {
  return std::tie(oldestTimestamp, chunkID) <
      std::tie(rhs.oldestTimestamp, rhs.chunkID);
}

}} // namespace facebook::logdevice
