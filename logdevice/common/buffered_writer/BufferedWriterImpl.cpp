/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"

#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/buffered_writer/BufferedWriterShard.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

int ProcessorProxy::postWithRetrying(std::unique_ptr<Request>& rq) {
  return processor_->postWithRetrying(rq);
}

namespace {

// Instructs the Worker to create a BufferedWriterShard instance with the
// specified ID.
class CreateShardRequest : public Request {
 public:
  CreateShardRequest(worker_id_t worker,
                     std::function<BufferedWriterShard*()> factory,
                     Semaphore* sem)
      : Request(RequestType::BUFFERED_WRITER_CREATE_SHARD),
        worker_(worker),
        factory_(std::move(factory)),
        sem_(sem) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  Execution execute() override {
    BufferedWriterShard* instance = factory_();
    Worker::onThisThread()->active_buffered_writers_.insert(
        std::make_pair(instance->id_, instance));
    sem_->post();
    return Execution::COMPLETE;
  }

 private:
  worker_id_t worker_;
  std::function<BufferedWriterShard*()> factory_;
  Semaphore* sem_;
};
} // namespace

WaitableCounter::IncrWhileAlive::IncrWhileAlive(
    WaitableCounter& waitable_counter)
    : waitable_counter_(waitable_counter), valid_(true) {
  std::lock_guard<std::mutex> guard(waitable_counter_.mutex_);
  if (!waitable_counter.allow_more_) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Incrementing WaitableCounter when no more allowed!");
    ld_check(false);
  }
  waitable_counter.counter_++;
}

WaitableCounter::IncrWhileAlive::~IncrWhileAlive() {
  if (!valid_) {
    return;
  }

  std::lock_guard<std::mutex> guard(waitable_counter_.mutex_);

  ld_check(waitable_counter_.counter_ > 0);
  if (waitable_counter_.counter_ > 0) {
    waitable_counter_.counter_--;
  } else {
    ld_error("Waitable counter was already zero when we tried to decrement!");
  }

  if (waitable_counter_.counter_ == 0) {
    waitable_counter_.cv_.notify_all();
  }
}

void WaitableCounter::waitForZeroAndDisallowMore() {
  std::unique_lock<std::mutex> mylock{mutex_};
  allow_more_ = false;
  cv_.wait(mylock, [this] { return counter_ == 0; });
  ld_check(counter_ == 0);
  if (counter_ != 0) {
    ld_error("Waitable counter not zero after waiting!");
  }
}

using GetLogOptionsFunc = std::function<BufferedWriter::LogOptions(logid_t)>;

BufferedWriterImpl::BufferedWriterImpl(ProcessorProxy* processor_proxy,
                                       AppendCallback* client_cb,
                                       GetLogOptionsFunc get_log_options,
                                       int32_t memory_limit_mb,
                                       BufferedWriterAppendSink* append_sink)
    : processor_proxy_(processor_proxy),
      client_callback_wrapped_(client_cb),
      callback_(&client_callback_wrapped_),
      memory_limit_mb_(memory_limit_mb),
      append_sink_(append_sink) {
  // Calculate a salt for hashing.  The first portion is a random constant.
  // The second is a Processor-wide counter; different BufferedWriter
  // instances attached to the same Processor will map logs to Workers
  // differently, which can be used to scale throughput to a log (although the
  // usefulness of this is unclear).
  hash_salt_ = 0xb390c4e7a94009f2L ^ processor()->issueBufferedWriterID().val_;

  // Initialize memory budgeting
  if (memory_limit_mb >= 0) {
    memory_available_.store(int64_t(memory_limit_mb) << 20);
  }

  const int nworkers = processor()->getWorkerCount(WorkerType::GENERAL);
  Semaphore sem;
  for (worker_id_t widx(0); widx.val_ < nworkers; ++widx.val_) {
    // We generate the ID here but let the Worker actually create the
    // BufferedWriterShard instance so that it's collocated with other data
    // belonging to that Worker.
    buffered_writer_id_t id = processor()->issueBufferedWriterID();
    auto factory = [id, &get_log_options, this]() {
      return new BufferedWriterShard(id, get_log_options, this);
    };
    std::unique_ptr<Request> req =
        std::make_unique<CreateShardRequest>(widx, factory, &sem);
    int rv = processor()->postWithRetrying(req);
    ld_check(rv == 0);
    shards_.push_back(id);
  }

  // Wait until all CreateShardRequest's have been processed by Workers.
  // After this it is surely safe to process append() calls and the
  // constructor can return.
  for (int i = 0; i < nworkers; ++i) {
    sem.wait();
  }
}

namespace {

class PerShardRequest : public Request {
 public:
  PerShardRequest(RequestType type,
                  worker_id_t worker,
                  buffered_writer_id_t id,
                  Semaphore* sem)
      : Request(type), worker_(worker), id_(id), sem_(sem) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  Execution execute() override {
    auto& map = Worker::onThisThread()->active_buffered_writers_;
    auto it = map.find(id_);
    ld_check(it != map.end());
    doWork(it);
    sem_->post();
    return Execution::COMPLETE;
  }

  virtual void doWork(Worker::ActiveBufferedWritersMap::iterator it) = 0;

 protected:
  worker_id_t worker_;
  buffered_writer_id_t id_;
  Semaphore* sem_;
};

// Instructs the Shard to quiesce, in particular, not enqueue any more blob
// construction work into the Processor's BackgroundThreads.
class QuiesceShardRequest : public PerShardRequest {
 public:
  QuiesceShardRequest(worker_id_t worker,
                      buffered_writer_id_t id,
                      Semaphore* sem)
      : PerShardRequest(RequestType::BUFFERED_WRITER_QUIESCE_SHARD,
                        worker,
                        id,
                        sem) {}
  void doWork(Worker::ActiveBufferedWritersMap::iterator it) override {
    it->second->quiesce();
  }
};

// Instructs the Worker to destroy a BufferedWriterShard instance.
class DestroyShardRequest : public PerShardRequest {
 public:
  DestroyShardRequest(worker_id_t worker,
                      buffered_writer_id_t id,
                      Semaphore* sem)
      : PerShardRequest(RequestType::BUFFERED_WRITER_DESTROY_SHARD,
                        worker,
                        id,
                        sem) {}
  void doWork(Worker::ActiveBufferedWritersMap::iterator it) override {
    delete it->second;
    auto& map = Worker::onThisThread()->active_buffered_writers_;
    map.erase(it);
  }
};

} // anonymous namespace

BufferedWriterImpl::~BufferedWriterImpl() {
  shutDown();
}

template <typename RequestClass>
void BufferedWriterImpl::postToAllWorkersAndBlockUntilDone() {
  const int nworkers = processor()->getWorkerCount(WorkerType::GENERAL);
  Semaphore sem;
  for (worker_id_t widx(0); widx.val_ < nworkers; ++widx.val_) {
    std::unique_ptr<Request> req =
        std::make_unique<RequestClass>(widx, shards_[widx.val_], &sem);
    int rv = processor()->postWithRetrying(req);
    ld_check(rv == 0);
  }

  for (int i = 0; i < nworkers; ++i) {
    sem.wait();
  }
}

void BufferedWriterImpl::shutDown() {
  // Must not be on a worker or the DestroyShardRequest posting below will
  // deadlock.  (This can be avoided if needed, by executing this worker's
  // request inline.)
  ld_check(!Worker::onThisThread(false));

  if (shutting_down_.exchange(true)) {
    // already called
    return;
  }

  ld_info("Shutting down BufferedWriter.");

  // When shutting down, we can't destory the shards while compression is
  // happening, since destroying the shards frees all Batches, including the
  // memory that stores the raw data being compressed.
  //
  // First, wait for shards to finish all requests in their queues, and notice
  // that we're shutting down.  After this point, they won't enqueue anything
  // else to the background threads.

  postToAllWorkersAndBlockUntilDone<QuiesceShardRequest>();

  // Next wait for existing blob construction to finish before we destroy
  // shards.
  num_background_tasks_.waitForZeroAndDisallowMore();

  // Now that all blob construction tasks are finished, its safe to delete the
  // Batch objects.

  // Send DestroyShardRequests to all workers and wait until they've been
  // processed.  After this, the BufferedWriterShard instances will no longer
  // exist and no Workers will try to use them because they are no longer in the
  // map.
  ld_info("Queuing DestroyShardRequests.");
  postToAllWorkersAndBlockUntilDone<DestroyShardRequest>();
}

namespace {
// Transfers an AppendChunk to a Worker for processing
class BufferedAppendRequest : public Request {
 public:
  BufferedAppendRequest(worker_id_t worker,
                        buffered_writer_id_t id,
                        BufferedWriterShard::AppendChunk appends,
                        bool atomic)
      : Request(RequestType::BUFFERED_WRITER_APPEND),
        worker_(worker),
        id_(id),
        appends_(std::move(appends)),
        atomic_(atomic) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

  Execution execute() override {
    Worker* w = Worker::onThisThread();
    auto it = w->active_buffered_writers_.find(id_);
    if (it == w->active_buffered_writers_.end()) {
      // This is unexpected as long as Workers process their requests in FIFO
      // order and this is not called concurrently with
      // BufferedWriter()::shutDown().
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "BufferedWriterShard with id %lu not found.  Unexpected!",
                      id_.val_);
      return Execution::COMPLETE;
    }

    BufferedWriterShard* shard = it->second;
    shard->append(std::move(appends_), atomic_);
    return Execution::COMPLETE;
  }

  // If the Request failed to get posted, this allows the payloads to be
  // transferred back to the client
  BufferedWriterShard::AppendChunk releaseChunk() {
    return std::move(appends_);
  }

 private:
  worker_id_t worker_;
  buffered_writer_id_t id_;
  BufferedWriterShard::AppendChunk appends_;
  bool atomic_;
};
} // namespace

// Helper function shared by two append()s so that it's a single logging
// callsite
static void log_memory_limit_exceeded(int memory_limit_mb) {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  1,
                  "Rejecting write(s) that would exceed client-configured "
                  "memory limit of %d MB",
                  memory_limit_mb);
}

int BufferedWriterImpl::append(logid_t log_id,
                               std::string&& payload,
                               AppendCallback::Context cb_context,
                               AppendAttributes&& attrs) {
  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }

  if (!append_sink_->checkAppend(log_id, payload.size(), false)) {
    return -1;
  }

  if (acquireMemory(payload.size()) != 0) {
    log_memory_limit_exceeded(memory_limit_mb_);
    err = E::NOBUFS;
    return -1;
  }

  // Post a BufferedAppendRequest to the appropriate Worker.
  int shard_idx = mapLogToShardIndex(log_id);

  Status shard_status = append_sink_->canSendToWorker();
  if (shard_status != E::OK) {
    err = shard_status;
    return -1;
  }
  size_t payload_size = payload.size();

  BufferedWriterShard::AppendChunk chunk;
  chunk.emplace_back(
      log_id, std::move(payload), std::move(cb_context), std::move(attrs));
  std::unique_ptr<Request> req =
      std::make_unique<BufferedAppendRequest>(worker_id_t(shard_idx),
                                              shards_[shard_idx],
                                              std::move(chunk),
                                              /* atomic */ false);
  append_sink_->onBytesSentToWorker(payload_size);
  int rv = processor()->postRequest(req);
  if (rv != 0) {
    // Failed to queue the append.  Return the payload to the caller.
    append_sink_->onBytesSentToWorker(-payload_size);
    BufferedAppendRequest* rawreq =
        static_cast<BufferedAppendRequest*>(req.get());
    chunk = rawreq->releaseChunk();
    ld_check(chunk.size() == 1);
    payload = std::move(chunk.front().payload);
    attrs = std::move(chunk.front().attrs);
  }
  return rv;
}

std::vector<Status>
BufferedWriterImpl::append(std::vector<Append>&& input_appends) {
  return appendImpl(std::move(input_appends), /* atomic */ false);
}

int BufferedWriterImpl::appendAtomic(logid_t log_id,
                                     std::vector<Append>&& input_appends) {
  if (input_appends.empty()) {
    return 0;
  }

  if (shutting_down_.load()) {
    err = E::SHUTDOWN;
    return -1;
  }

  int64_t payload_bytes = 0;
  for (const Append& append : input_appends) {
    payload_bytes += append.payload.size();
    if (log_id != append.log_id) {
      ld_info("Input appends must all be for the same log "
              "but at least two logs passed (%lu and %lu)",
              log_id.val_,
              append.log_id.val_);
      err = E::INVALID_PARAM;
      return -1;
    }
  }

  if (memory_limit_mb_ >= 0) {
    // Check if we have enough memory for these writes
    if (acquireMemory(payload_bytes) != 0) {
      log_memory_limit_exceeded(memory_limit_mb_);
      err = E::NOBUFS;
      return -1;
    }
  }

  auto shard_status = append_sink_->canSendToWorker();
  if (shard_status != Status::OK) {
    err = shard_status;
    return -1;
  }

  BufferedWriterShard::AppendChunk chunks;
  int shard = mapLogToShardIndex(log_id);
  size_t append_sizes = 0;

  for (size_t i = 0; i < input_appends.size(); ++i) {
    auto& append = input_appends[i];
    if (!append_sink_->checkAppend(log_id, append.payload.size(), false)) {
      err = E::TOOBIG;
      return -1;
    }

    append_sizes += append.payload.size();
    chunks.emplace_back(std::move(append));
  }

  std::unique_ptr<Request> req = std::make_unique<BufferedAppendRequest>(
      worker_id_t(shard), shards_[shard], std::move(chunks), true);
  append_sink_->onBytesSentToWorker(append_sizes);
  int rv = processor()->postRequest(req);
  if (rv != 0) {
    // Failed to queue the append.  Return the payload to the caller.
    append_sink_->onBytesSentToWorker(-append_sizes);
    BufferedAppendRequest* rawreq =
        static_cast<BufferedAppendRequest*>(req.get());
    chunks = rawreq->releaseChunk();
    // err set by postRequest
    return -1;
  }

  return 0;
}

std::vector<Status>
BufferedWriterImpl::appendImpl(std::vector<Append>&& input_appends,
                               bool atomic) {
  // This is the multi-write variant of append().  It groups appends into
  // chunks that map to the same target shard/worker.  A single `Request` is
  // used to pass an entire chunk to the right LogDevice worker, instead of
  // one `Request` per append.
  //
  // NOTE: this is O(shards_.size() + input_appends.size()).

  if (shutting_down_.load()) {
    return std::vector<Status>(input_appends.size(), E::SHUTDOWN);
  }

  if (memory_limit_mb_ >= 0) {
    // Check if we have enough memory for these writes
    int64_t payload_bytes = 0;
    for (auto& append : input_appends) {
      payload_bytes += append.payload.size();
    }
    if (acquireMemory(payload_bytes) != 0) {
      log_memory_limit_exceeded(memory_limit_mb_);
      return std::vector<Status>(input_appends.size(), E::NOBUFS);
    }
  }

  std::vector<Status> result(input_appends.size());
  std::vector<BufferedWriterShard::AppendChunk> chunks(shards_.size());
  std::vector<int> shard_idxs;
  shard_idxs.reserve(input_appends.size());
  std::vector<folly::Optional<Status>> shard_status(shards_.size());
  std::vector<size_t> shard_append_sizes(shards_.size());
  for (size_t i = 0; i < input_appends.size(); ++i) {
    auto& append = input_appends[i];
    logid_t log_id = append.log_id;
    if (!append_sink_->checkAppend(log_id, append.payload.size(), false)) {
      shard_idxs.push_back(-1);
      result[i] = E::TOOBIG;
      continue;
    }
    int shard_idx = mapLogToShardIndex(log_id);

    ld_check(shard_idx < shard_status.size());
    if (!shard_status[shard_idx].hasValue()) {
      shard_status[shard_idx].assign(append_sink_->canSendToWorker());
    }
    if (shard_status[shard_idx].value() != E::OK) {
      shard_idxs.push_back(-1);
      result[i] = shard_status[shard_idx].value();
      continue;
    }
    shard_append_sizes[shard_idx] += append.payload.size();

    chunks[shard_idx].emplace_back(std::move(append));
    // Record the shard ID so we can match up return codes
    shard_idxs.push_back(shard_idx);
  }

  std::vector<Status> chunk_status(shards_.size());
  for (size_t i = 0; i < shards_.size(); ++i) {
    if (chunks[i].empty()) {
      chunk_status[i] = E::OK;
      continue;
    }
    // Post a BufferedAppendRequest to process this shard's chunk on the
    // appropriate Worker.
    std::unique_ptr<Request> req = std::make_unique<BufferedAppendRequest>(
        worker_id_t(i), shards_[i], std::move(chunks[i]), atomic);
    append_sink_->onBytesSentToWorker(shard_append_sizes[i]);
    int rv = processor()->postRequest(req);
    if (rv == 0) {
      chunk_status[i] = E::OK;
    } else {
      // Failure!  Take the chunk back so that we can restore payloads in the
      // input vector for all affected appends.
      chunk_status[i] = err;
      BufferedAppendRequest* rawreq =
          static_cast<BufferedAppendRequest*>(req.get());
      chunks[i] = rawreq->releaseChunk();
      // rewinding the counter
      append_sink_->onBytesSentToWorker(-shard_append_sizes[i]);
    }
  }

  // Iterating backwards so that we can repopulate the input vector for any
  // failed chunks by popping from `chunks'
  for (ssize_t i = ssize_t(input_appends.size()) - 1; i >= 0; --i) {
    int shard_idx = shard_idxs[i];
    if (shard_idx == -1) {
      continue;
    }
    result[i] = chunk_status[shard_idx];
    if (result[i] != E::OK) {
      input_appends[i] = std::move(chunks[shard_idx].back());
      ld_assert(mapLogToShardIndex(input_appends[i].log_id) == shard_idx);
      chunks[shard_idx].pop_back();
    }
  }
  return result;
}

// Instructs the Worker to call BufferedWriterShard::flushAll().
class FlushShardRequest : public Request {
 public:
  FlushShardRequest(worker_id_t worker, buffered_writer_id_t writer_id)
      : Request(RequestType::BUFFERED_WRITER_FLUSH_SHARD),
        worker_(worker),
        writer_id_(writer_id) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_.val_;
  }

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

  Execution execute() override {
    Worker* w = Worker::onThisThread();
    auto it = w->active_buffered_writers_.find(writer_id_);
    if (it != w->active_buffered_writers_.end()) {
      it->second->flushAll();
    }
    return Execution::COMPLETE;
  }

 private:
  worker_id_t worker_;
  buffered_writer_id_t writer_id_;
};

int BufferedWriterImpl::flushAll() {
  int rv = 0;
  for (size_t i = 0; i < shards_.size(); ++i) {
    std::unique_ptr<Request> req =
        std::make_unique<FlushShardRequest>(worker_id_t(i), shards_[i]);
    if (processor()->postRequest(req) != 0) {
      rv = -1;
    }
  }
  return rv;
}

int BufferedWriterImpl::mapLogToShardIndex(logid_t log_id) const {
  return folly::hash::hash_128_to_64(hash_salt_, log_id.val_) % shards_.size();
}

}} // namespace facebook::logdevice
