/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <condition_variable>

#include <folly/Preprocessor.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/BufferedWriter.h"
#include "logdevice/include/types.h"

class BufferedWriterTest;
class StreamWriterTest;

namespace facebook { namespace logdevice {

class Client;
class Processor;
class Request;

// Abstract interface for BufferedWriter to sink appends when batches are
// formed.  It needs to outlive the BufferedWriter instance.
// In practice this will be one of:
// 1) A proxy for ClientImpl::appendBuffered()
// 2) SequencerBatching for batching on the sequencer
// 3) Overridden in tests to inject failures
class BufferedWriterAppendSink {
 public:
  using AppendRequestCallback =
      std::function<void(Status, const DataRecord&, NodeID)>;

  virtual bool checkAppend(logid_t logid,
                           size_t payload_size,
                           bool allow_extra) = 0;

  /**
   * If this returns anything except for E::OK, we fail sending writes to the
   * corresponding worker with the specified error code.
   * Overridden in SequencerBatching
   */
  virtual Status canSendToWorker() {
    return E::OK;
  }

  /**
   * This is called before bytes are sent to a worker for processing. Note that
   * it can be called with a negative value afterwards if posting a request does
   * not succeed.
   * Overridden in SequencerBatching
   */
  virtual void onBytesSentToWorker(ssize_t /*bytes*/) {}

  /**
   * This is called when payloads are freed if options->destroy_payloads ==
   * true.
   * Overridden in SequencerBatching
   */
  virtual void onBytesFreedByWorker(size_t /*bytes*/) {}

  /**
   * `checksum_bits' says how many of the first bits are the checksum,
   * prepended to the payload because BufferedWriterImpl::prependChecksums()
   * was called.  This call should make sure to set the right checksum flags
   * when forming the append, for the bytes to be correctly processed
   * downstream.
   *
   * @return E::OK if the buffered append was accepted for processing,
   * otherwise the error code and an optional NodeID redirect (used in a
   * server context)
   */
  virtual std::pair<Status, NodeID>
  appendBuffered(logid_t logid,
                 const BufferedWriter::AppendCallback::ContextSet& contexts,
                 AppendAttributes attrs,
                 const Payload& payload,
                 AppendRequestCallback callback,
                 worker_id_t target_worker,
                 int checksum_bits) = 0;
  virtual ~BufferedWriterAppendSink() {}
};

/**
 * Wraps a Processor, to allow sub-classes in tests to re-order, inject faults,
 * etc..  This version just forwards, and is used in production.
 */
class ProcessorProxy {
 protected:
  Processor* const processor_;

 public:
  explicit ProcessorProxy(Processor* processor) : processor_(processor) {}

  Processor* processor() const {
    return processor_;
  }

  virtual ~ProcessorProxy() {}

  virtual int postWithRetrying(std::unique_ptr<Request>& rq);
};

/**
 * A counter used for interthread communication.  Used during shutdown to wait
 * for the number of outstanding tasks on the background thread to reach zero.
 */
class WaitableCounter {
 public:
  /**
   * A small RAII class to increment/decrement the counter.
   */
  class IncrWhileAlive {
   public:
    explicit IncrWhileAlive(WaitableCounter& waitable_counter);

    IncrWhileAlive(const IncrWhileAlive&) = delete;
    IncrWhileAlive& operator=(const IncrWhileAlive&) = delete;

    IncrWhileAlive(IncrWhileAlive&& other) noexcept
        : waitable_counter_(other.waitable_counter_), valid_(other.valid_) {
      other.valid_ = false;
    }
    IncrWhileAlive& operator=(IncrWhileAlive&& other) = delete;

    ~IncrWhileAlive();

   private:
    WaitableCounter& waitable_counter_;
    bool valid_;
  };

  void waitForZeroAndDisallowMore();

  /* WARNING: FOR DEBUGGING ONLY.
   *
   * By the time you do anything with this value, the underlying value will most
   * likely have changed.  So don't have the code make any important decisions
   * based on it.  This is exposed for stats, logging, etc.
   */
  uint64_t recentValue() const {
    return counter_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<uint64_t> counter_{0};
  bool allow_more_{true};
};

class BufferedWriterImpl : public BufferedWriter {
 public:
  // BufferedWriter interface
  int append(logid_t log_id,
             std::string&& payload,
             AppendCallback::Context,
             AppendAttributes&& attrs = AppendAttributes());
  std::vector<Status> append(std::vector<Append>&& appends);
  int flushAll();

  // Internal version of BufferedWriter::AppendCallback that takes more
  // information on failure.  In a client context it just discards the extra
  // information and forwards to the client-provided AppendCallback.  In a
  // server context, SequencerBatching overrides the method and uses the extra
  // information.
  class AppendCallbackInternal {
   public:
    using ContextSet = BufferedWriter::AppendCallback::ContextSet;
    using RetryDecision = BufferedWriter::AppendCallback::RetryDecision;

    AppendCallbackInternal() : client_cb_(nullptr) {}
    explicit AppendCallbackInternal(AppendCallback* client_cb)
        : client_cb_(client_cb) {}
    virtual ~AppendCallbackInternal() {}

    virtual void onSuccess(logid_t log_id,
                           ContextSet contexts,
                           const DataRecordAttributes& attrs) {
      ld_check(client_cb_);
      client_cb_->onSuccess(log_id, std::move(contexts), attrs);
    }
    virtual void onFailureInternal(logid_t log_id,
                                   ContextSet contexts,
                                   Status status,
                                   NodeID /*redirect*/) {
      ld_check(client_cb_);
      client_cb_->onFailure(log_id, std::move(contexts), status);
    }
    virtual RetryDecision onRetry(logid_t log_id,
                                  const ContextSet& contexts,
                                  Status status) {
      ld_check(client_cb_);
      return client_cb_->onRetry(log_id, contexts, status);
    }

   private:
    AppendCallback* client_cb_;
  };

  BufferedWriterImpl(ProcessorProxy*,
                     AppendCallback*,
                     std::function<BufferedWriter::LogOptions(logid_t)>,
                     int32_t,
                     BufferedWriterAppendSink*);
  ~BufferedWriterImpl() override;

  void setCallbackInternal(AppendCallbackInternal* cb) {
    callback_ = cb;
  }

  // Variant of append() that ensures that all appends go into the same batch.
  // They must all belong to the same log specified by @param log_id.
  int appendAtomic(logid_t log_id, std::vector<Append>&& appends);

  // Thread-safe memory budgeting functions.  If a memory limit was configured
  // by the client via Options::memory_limit_mb, append() calls acquire memory
  // which gets released when the writes finish.
  //
  // An important property of the memory usage heuristic in
  // memoryForPayloadBytes() is linearity.  It makes it fine to combine or
  // split payloads when calling acquireMemory() and releaseMemory().  For
  // example, calling acquireMemory(a) and acquireMemory(b + c), then later
  // releaseMemory(a + b) and releaseMemory(c) returns to the initial state.
  // This allows calls to be batched, which reduces contention on the
  // std::atomic.
  int acquireMemory(int64_t payload_bytes) {
    if (memory_limit_mb_ >= 0) {
      const int64_t nbytes = memoryForPayloadBytes(payload_bytes);
      auto prev = memory_available_.load();
      do {
        if (prev < nbytes) {
          return -1;
        }
      } while (!memory_available_.compare_exchange_weak(prev, prev - nbytes));
    }
    return 0;
  }
  void releaseMemory(int64_t payload_bytes) {
    if (memory_limit_mb_ >= 0) {
      memory_available_ += memoryForPayloadBytes(payload_bytes);
    }
  }

  /**
   * Requests shutdown.  Must not be called on a worker thread because it
   * requires communication with workers.
   *
   * The class will reject further work; append() will fail with E::SHUTDOWN.
   *
   * After this returns, it is guaranteed that the callback will no longer be
   * invoked.
   */
  void shutDown();

  AppendCallbackInternal* getCallback() const {
    return callback_;
  }

  BufferedWriterAppendSink* appendSink() const {
    return append_sink_;
  }

  Processor* processor() const {
    return processor_proxy_->processor();
  }

  ProcessorProxy* processorProxy() const {
    return processor_proxy_.get();
  }

  void pinClient(std::shared_ptr<Client> client) {
    client_pin_ = std::move(client);
  }

  /**
   * If called, BufferedWriter will prepend a checksum to every payload.  The
   * checksum size is controlled by the `checksum_bits' Processor setting.
   *
   * NOTE: This is used in a server context where the buffered append does not
   * go out as an APPEND message but as STORE, bypassing
   * APPEND_Message::serialize() where the checksum is normally injected
   * on the client.  AppendSink::appendBuffered() still needs to set the right
   * checksum flags for the bytes to be correctly processed downstream.
   */
  void prependChecksums() {
    prepend_checksums_ = true;
  }

  bool shouldPrependChecksum() const {
    return prepend_checksums_;
  }

  bool isShuttingDown() const {
    return shutting_down_.load();
  }

  WaitableCounter::IncrWhileAlive getBackgroundTaskCountHolder() {
    return WaitableCounter::IncrWhileAlive{num_background_tasks_};
  }

  uint64_t recentNumBackground() const {
    return num_background_tasks_.recentValue();
  }

 private:
  int mapLogToShardIndex(logid_t) const;

  int64_t memoryForPayloadBytes(int64_t payload_bytes) const {
    // Budget 2x the payload size; 1x for the original std::string which we
    // keep around, and another 1x in the blob sent to LogDevice (which we
    // keep around for any retries).
    return 2 * payload_bytes;
  }

  // implementation of append(vector)
  std::vector<Status> appendImpl(std::vector<Append>&& appends, bool atomic);

  template <typename RequestClass>
  void postToAllWorkersAndBlockUntilDone();

  // Following members are initialized in the constructor and then accessed
  // from multiple threads but never changed

  std::shared_ptr<Client> client_pin_;
  const std::unique_ptr<ProcessorProxy> processor_proxy_;
  AppendCallbackInternal client_callback_wrapped_;
  AppendCallbackInternal* callback_;
  int32_t memory_limit_mb_;
  BufferedWriterAppendSink* const append_sink_;
  std::atomic<bool> shutting_down_{false};
  WaitableCounter num_background_tasks_;
  uint64_t hash_salt_;
  bool prepend_checksums_ = false;
  // This will have exactly one entry for each Worker in the Processor's
  // thread pool.
  std::vector<buffered_writer_id_t> shards_;

  // Memory available in bytes, applies if Options::memory_limit_mb is set.
  // This will be modified from multiple threads in a ticket-dispenser
  // fashion, so it gets its own cache line to avoid interference with
  // read-only traffic on other members.
  char FB_ANONYMOUS_VARIABLE(padding)[128];
  alignas(128) std::atomic<int64_t> memory_available_;
  char FB_ANONYMOUS_VARIABLE(padding)[128 - sizeof(std::atomic<int64_t>)];

  friend class ::BufferedWriterTest;
  friend class ::StreamWriterTest;
};
}} // namespace facebook::logdevice
