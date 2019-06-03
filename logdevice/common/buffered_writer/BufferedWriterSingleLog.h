/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>
#include <queue>
#include <string>
#include <vector>

#include <folly/Function.h>
#include <folly/IntrusiveList.h>
#include <folly/MPMCQueue.h>
#include <folly/small_vector.h>

#include "logdevice/common/CompactableContainer.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/BufferedWriter.h"

class ProcessorTestProxy;

namespace facebook { namespace logdevice {

/**
 * @file Part of BufferedWriter that manages appends for a single log.  See
 * doc/buffered-writer.md for an overview of the implementation.
 *
 * Appends sent by the client are grouped into batches.  There is always at
 * most one batch in the BUILDING state, to which new appends from the client
 * can be added.  The batch eventually gets flushed, which involves
 * serializing the appends into a single blob and sending to LogDevice in one
 * write.
 *
 * All methods must be invoked on the same LogDevice worker thread.
 */

class BufferedWriterShard;
class ExponentialBackoffTimer;
class Timer;
class StatsHolder;
class Processor;

using GetLogOptionsFunc = std::function<BufferedWriter::LogOptions(logid_t)>;

class BufferedWriterSingleLog {
 public:
  struct Batch {
    explicit Batch(uint64_t num) : num(num) {}

    // We keep pointers to Batches when moving things between threads, so don't
    // allow moving or copying of batches, since that might invalidate those
    // pointers.  If we have a good reason to, we could enable copying and/or
    // moving, as long as we're careful.
    Batch(const Batch&) = delete;
    Batch& operator=(const Batch&) = delete;

    Batch(Batch&&) = delete;
    Batch& operator=(Batch&&) = delete;

    enum class State {
      // Appends are still being added; the batch has not been sent out yet.
      BUILDING,
      // A batch is being constructed, which may include compression on a
      // separate thread.
      CONSTRUCTING_BLOB,
      // This batch is ready to send, but an earlier one isn't and we send
      // batches out in order, so this one is waiting for the earlier one.
      READY_TO_SEND,
      // A write to LogDevice is in flight.
      INFLIGHT,
      // The batch was sent out but the write failed or timed out.  A retry is
      // pending.
      RETRY_PENDING,
      // The batch was successfully written to LogDevice, or it failed and we
      // exhausted all retries.  The client callback was invoked for all
      // appends and the batch can be reaped.
      FINISHED,
      // Doesn't represent an actual state.  Instead, for counting the number of
      // states.
      MAX
    } state;

    // Batches are numbered in order when created.  We use this to help preserve
    // ordering, so that batches land in the same order that user's append()
    // calls land.
    uint64_t num;
    // Appends in this batch.  NOTE: this is ContextSet so that we can directly
    // pass it to the application callback.
    BufferedWriter::AppendCallback::ContextSet appends;
    // AppendAttributes may contain 2 pieces of information:
    // 1/ Custom keys. We aggregate the custom keys by keeping the minimum
    //     across all records batched together;
    // 2/ Custom counters. We keep the sum.
    AppendAttributes attrs;
    // Sum of payload sizes in `appends'
    size_t payload_bytes_total = 0;
    // Projection of how big the uncompressed blob will be.  Updated while the
    // batch is BUILDING so that we can early-flush when a large append would
    // take us over the max payload size.
    size_t blob_bytes_total = 0;
    // Keeps track of amount of payload destroyed in
    // construct_uncompressed_blob().
    size_t total_size_freed = 0;
    // Blob to send to LogDevice, with the entire batch serialized.
    // Constructed the first time the batch transitions from BUILDING to
    // INFLIGHT.
    Slice blob;
    std::unique_ptr<uint8_t[]> blob_buf;
    size_t blob_header_size = 0;

    // How many times we've retried sending this batch
    int retry_count = 0;
    // Retry timer if in state RETRY_PENDING
    std::unique_ptr<ExponentialBackoffTimer> retry_timer;

    static const SimpleEnumMap<State, std::string>& names();
  };

  BufferedWriterSingleLog(BufferedWriterShard* parent,
                          logid_t log_id,
                          GetLogOptionsFunc get_log_options);

  using AppendChunk = folly::small_vector<BufferedWriter::Append, 4>;
  /**
   * Buffers a chunk of client appends for the log.  The append is "atomic";
   * all writes in the chunk will get flushed in the same batch.
   */
  void append(AppendChunk chunk);

  /**
   * Sends any buffered appends.
   */
  void flush();

  /**
   * Called when an APPEND comes back from LogDevice.
   */
  void onAppendReply(Batch&, Status, const DataRecord&, NodeID redirect);

  void quiesce();

  ~BufferedWriterSingleLog();

  /**
   * Implementation helpers. Exported for tests/benchmarks
   */
  class Impl {
   public:
    // Constructs, compresses (if appropriate), and checksums blob.  Potentially
    // long running, so is typically called on the processor's BackgroundThread.
    static void
    construct_blob_long_running(Batch& batch,
                                BufferedWriteDecoderImpl::flags_t flags,
                                int checksum_bits,
                                bool destroy_payloads,
                                int zstd_level);

    // Possibly long running.  Checks conditions for compression, and if
    // satisfied, compresses.
    static void
    maybe_compress_blob(Batch& batch,
                        BufferedWriter::Options::Compression compression,
                        int checksum_bits,
                        int zstd_level);
    // Constructs a blob from a batch.  Copies the data, so is therefore
    // potentially long running.
    static void
    construct_uncompressed_blob(Batch& batch,
                                BufferedWriteDecoderImpl::flags_t flags,
                                int checksum_bits,
                                bool destroy_payloads);
  };

  // We add ourselves to the BufferedWriterShard's `flushable' list when there
  // is a batch to be flushed
  folly::IntrusiveListHook flushable_list_hook_;

 private:
  friend class ::ProcessorTestProxy;

  class ContinueBlobSendRequest : public Request {
    BufferedWriterSingleLog* buffered_writer_single_log_;
    Batch& batch_;
    const int threadAffinity_;

   public:
    ContinueBlobSendRequest(BufferedWriterSingleLog* buffered_writer_single_log,
                            Batch& batch,
                            int threadAffinity)
        : Request(RequestType::CONTINUE_BLOB_SEND),
          buffered_writer_single_log_(buffered_writer_single_log),
          batch_(batch),
          threadAffinity_(threadAffinity) {}

    Execution execute() override {
      buffered_writer_single_log_->readyToSend(batch_);
      return Execution::COMPLETE;
    }

    int getThreadAffinity(int nthreads) override {
      ld_check(threadAffinity_ < nthreads);
      return threadAffinity_;
    }

    int8_t getExecutorPriority() const override {
      return folly::Executor::HI_PRI;
    }

    const Batch& getBatch() const {
      return batch_;
    }
  };

  // Internal version of append() that may fail, when the append is blocked in
  // ONE_AT_A_TIME mode because there is a batch already inflight.  In that
  // case the public append() then adds the chunk to `blocked_appends_'.
  int appendImpl(AppendChunk& chunk, bool defer_size_trigger);
  // Checks if the batch that is currently building has met any of the
  // criteria for automatic flushing, and flushes if so.
  void flushMeMaybe(bool defer_client_size_trigger);
  // Sends the batch to LogDevice
  void sendBatch(Batch&);
  // Re-orders batches coming back from the helper thread, and sends them to
  // appendBatch() in order.
  void readyToSend(Batch& batch);
  // Appends the batch to the sink.
  void appendBatch(Batch&);
  // Reaps any finished batches at the front of batches_
  void reap();
  // In the ONE_AT_A_TIME mode, unblocks appends that could not be processed
  // earlier because there was a batch already inflight.
  void unblockAppends();
  void dropBlockedAppends(Status status, NodeID redirect);
  // Ensures that time_trigger_timer_ is active if Options::time_trigger was
  // set by client
  void activateTimeTrigger();
  // Called when a batch fails to send.  Attempts to schedule a retry if
  // configured, returns 0 if the retry was successfully scheduled.
  int scheduleRetry(Batch& batch, Status, const DataRecord& dr_batch);
  // Invoke client-supplied callback (single BufferedWriter::AppendCallback*
  // retrieved from BufferedWriter) with client-supplied contexts that form
  // this batch.
  //
  // If status is OK, invokes onSuccess().  Otherwise, invokes onFailure().
  // Hands payloads over back to the application.
  void invokeCallbacks(Batch&,
                       Status,
                       const DataRecord& dr_batch,
                       NodeID redirect);
  // Does a flush() call have anything to do?
  bool isFlushable() const;
  // Is there a batch in the BUILDING state?
  bool haveBuildingBatch() const;
  // Sets a Batch's state.  Changes to Batch states can alter the result of
  // isFlushable(), need to inform the parent.
  void setBatchState(Batch&, Batch::State);

  // How many checksum bits to prepend to every payload, if
  // BufferedWriterImpl::prependChecksums() was called
  int checksumBits() const;

  void finishBatch(Batch&);

  void construct_blob(Batch& batch,
                      BufferedWriteDecoderImpl::flags_t flags,
                      int checksum_bits,
                      bool destroy_payloads);

  BufferedWriterShard* parent_;
  logid_t log_id_;
  GetLogOptionsFunc get_log_options_;
  CompactableContainer<std::deque<std::unique_ptr<Batch>>> batches_;
  std::unique_ptr<Timer> time_trigger_timer_;

  // get_log_options_() call is not very cheap, so we call it only when
  // starting a new batch and cache the result.
  BufferedWriter::LogOptions options_;

  // In the ONE_AT_A_TIME mode, this is a buffer for appends that came in
  // while a batch was already inflight.  When that batch finishes, we can
  // create a batch for them.
  CompactableContainer<std::deque<AppendChunk>> blocked_appends_;
  // How many entries at the front of `blocked_appends_' were supposed to have
  // been flushed already, due to an explicit flush() call or the time
  // trigger.  unblockAppends() should flush them whenever it gets a chance.
  ssize_t blocked_appends_flush_deferred_count_ = 0;

  // We consecutively number batches, to make it easier to find the first unsent
  // one after they come back from being constructed on the Processor's
  // BackgroundThread.
  uint64_t next_batch_num_ = 0;

  // The number of the next batch to be sent; it may still be being
  // constructed on the Processor's BackgroundThread, while later ones are
  // finished and ready to send.
  uint64_t next_batch_to_send_ = 0;
};

}} // namespace facebook::logdevice
