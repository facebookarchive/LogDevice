/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/buffered_writer/BufferedWriterSingleLog.h"

#include <chrono>
#include <lz4.h>
#include <lz4hc.h>
#include <zstd.h>

#include <folly/IntrusiveList.h>
#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/Varint.h>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/buffered_writer/BufferedWriteDecoderImpl.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/buffered_writer/BufferedWriterShard.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace std::literals::chrono_literals;

const SimpleEnumMap<BufferedWriterSingleLog::Batch::State, std::string>&
BufferedWriterSingleLog::Batch::names() {
  static SimpleEnumMap<BufferedWriterSingleLog::Batch::State, std::string>
      s_names({{State::BUILDING, "BUILDING"},
               {State::CONSTRUCTING_BLOB, "CONSTRUCTING_BLOB"},
               {State::READY_TO_SEND, "READY_TO_SEND"},
               {State::INFLIGHT, "INFLIGHT"},
               {State::RETRY_PENDING, "RETRY_PENDING"},
               {State::FINISHED, "FINISHED"}});

  return s_names;
}

using batch_flags_t = BufferedWriteDecoderImpl::flags_t;
using Compression = BufferedWriter::Options::Compression;
using Flags = BufferedWriteDecoderImpl::Flags;

BufferedWriterSingleLog::BufferedWriterSingleLog(BufferedWriterShard* parent,
                                                 logid_t log_id,
                                                 GetLogOptionsFunc get_options)
    : parent_(parent),
      log_id_(log_id),
      get_log_options_(std::move(get_options)),
      options_(get_log_options_(log_id_)) {}

BufferedWriterSingleLog::~BufferedWriterSingleLog() {
  for (std::unique_ptr<Batch>& batch : *batches_) {
    if (batch->state != Batch::State::FINISHED) {
      invokeCallbacks(*batch, E::SHUTDOWN, DataRecord(), NodeID());
      finishBatch(*batch);
    }
  }

  dropBlockedAppends(E::SHUTDOWN, NodeID());
}

void BufferedWriterSingleLog::append(AppendChunk chunk) {
  int rv = appendImpl(chunk, /* defer_client_size_trigger */ false);
  if (rv != 0) {
    // Buffer this chunk; when the inflight batch finishes we will re-call
    // appendImpl().
    blocked_appends_->push_back(std::move(chunk));
    blocked_appends_.observe();
    // The new append can be flushed; need to inform parent
    parent_->setFlushable(*this, isFlushable());
    // Ensure that the time trigger (if configured) is active.  Even if the
    // currently-inflight batch does not finish by when the timer fires, the
    // flush() call from the timer will mark all blocked appends for flushing
    // ASAP.
    activateTimeTrigger();
  }
}

int BufferedWriterSingleLog::appendImpl(AppendChunk& chunk,
                                        bool defer_client_size_trigger) {
  // Calculate how many bytes these records will take up in the blob
  size_t payload_bytes_added = 0;
  size_t blob_bytes_added = 0;
  for (const BufferedWriter::Append& append : chunk) {
    const std::string& payload = append.payload;
    uint8_t buf[folly::kMaxVarintLength64];
    payload_bytes_added += payload.size();
    blob_bytes_added +=
        folly::encodeVarint(payload.size(), buf) + payload.size();
  }
  const size_t max_payload_size = Worker::settings().max_payload_size;

  if (haveBuildingBatch() &&
      batches_->back()->blob_bytes_total + blob_bytes_added >
          max_payload_size) {
    // These records would take us over the payload size limit.  Flush the
    // already buffered records first, then we will create a new batch for
    // these records.
    StatsHolder* stats{parent_->parent_->processor()->stats_};
    STAT_INCR(stats, buffered_writer_max_payload_flush);
    flush();
    ld_check(!haveBuildingBatch());
  }

  // If there is no batch in the BUILDING state, create one now.
  if (!haveBuildingBatch()) {
    if (options_.mode == BufferedWriter::Options::Mode::ONE_AT_A_TIME &&
        !batches_->empty()) {
      // In the one-at-a-time mode, if there is already a batch inflight, we
      // must wait for it to finish before creating a new batch.
      return -1;
    }

    // Refresh log options once per batch.
    options_ = get_log_options_(log_id_);

    auto batch = std::make_unique<Batch>(next_batch_num_++);
    batch->blob_bytes_total =
        // Any bytes for the checksum.  This goes first since it gets stripped
        // first on the read path.
        (checksumBits() / 8) +
        // 2 bytes for header (magic marker and header)
        2 +
        // The batch size.  The number of bytes for this is an overestimate
        // since the batch size is unknown until we flush the batch.
        folly::kMaxVarintLength64;
    batches_->push_back(std::move(batch));
    // Intentionally setting state after pushing to make sure isFlushable()
    // becomes true *during* the setBatchState() call
    setBatchState(*batches_->back(), Batch::State::BUILDING);
    ld_check(isFlushable());

    batches_.observe();
    activateTimeTrigger();
  }
  Batch& batch = *batches_->back();
  // Add these appends to the BUILDING batch
  batch.payload_bytes_total += payload_bytes_added;
  batch.blob_bytes_total += blob_bytes_added;
  ld_check(batch.blob_bytes_total <= MAX_PAYLOAD_SIZE_INTERNAL);
  for (auto& append : chunk) {
    std::string& payload = append.payload;
    BufferedWriter::AppendCallback::Context& context = append.context;
    batch.appends.emplace_back(std::move(context), std::move(payload));

    if (append.attrs.optional_keys.find(KeyType::FINDKEY) !=
        append.attrs.optional_keys.end()) {
      const std::string& key = append.attrs.optional_keys[KeyType::FINDKEY];
      // The batch of records will have the smallest custom key from the ones
      // provided by the client for each record.
      if (batch.attrs.optional_keys.find(KeyType::FINDKEY) ==
              batch.attrs.optional_keys.end() ||
          key < batch.attrs.optional_keys[KeyType::FINDKEY]) {
        batch.attrs.optional_keys[KeyType::FINDKEY] = key;
      }
    }
    if (append.attrs.counters.hasValue()) {
      const auto& new_counters = append.attrs.counters.value();
      if (!batch.attrs.counters.hasValue()) {
        batch.attrs.counters.emplace();
      }
      auto& curr_counters = batch.attrs.counters.value();
      for (auto counter : new_counters) {
        auto it = curr_counters.find(counter.first);
        if (it == curr_counters.end()) {
          curr_counters.emplace(counter);
          continue;
        }
        it->second += counter.second;
      }
    }
  }

  // Check if we hit the size trigger and should flush
  flushMeMaybe(defer_client_size_trigger);
  return 0;
}

void BufferedWriterSingleLog::flushMeMaybe(bool defer_client_size_trigger) {
  ld_check(haveBuildingBatch());
  const Batch& batch = *batches_->back();
  Worker* w = Worker::onThisThread();
  const size_t max_payload_size = w->immutable_settings_->max_payload_size;

  // If we're at the LogDevice hard limit on payload size, flush
  if (batch.blob_bytes_total >= max_payload_size) {
    ld_check(batch.blob_bytes_total <= MAX_PAYLOAD_SIZE_INTERNAL);
    STAT_INCR(w->getStats(), buffered_writer_max_payload_flush);
    flush();
    return;
  }

  // If client set `Options::size_trigger', check if the sum of payload bytes
  // buffered exceeds it
  if (!defer_client_size_trigger && options_.size_trigger >= 0 &&
      batch.payload_bytes_total >= options_.size_trigger) {
    STAT_INCR(w->getStats(), buffered_writer_size_trigger_flush);
    flush();
    return;
  }
}

void BufferedWriterSingleLog::flush() {
  // Parent shouldn't have called us from flushAll() if we're not flushable,
  // internal methods ensure we're flushable
  ld_check(isFlushable());
  // Make sure to let parent know whether this log is flushable or not. It could
  // still be flushable if we flushed a batch (e.g. because of reaching the size
  // threshold) and have blocked appenders that are not deferred.
  SCOPE_EXIT {
    parent_->setFlushable(*this, isFlushable());
  };

  if (!haveBuildingBatch()) {
    // No batch in BUILDING state, nothing to send to servers

    // In the ONE_AT_A_TIME mode, there may be appends blocked because there
    // is a batch currently inflight.  But this flush() call is supposed to
    // flush them.  Defer the flush; record the count so that these appends
    // get flushed as soon as the currently inflight batch comes back.
    blocked_appends_flush_deferred_count_ = blocked_appends_->size();

    return;
  }

  if (time_trigger_timer_) {
    time_trigger_timer_->cancel();
  }
  sendBatch(*batches_->back());
}

bool BufferedWriterSingleLog::isFlushable() const {
  return haveBuildingBatch() ||
      blocked_appends_flush_deferred_count_ < blocked_appends_->size();
}

bool BufferedWriterSingleLog::haveBuildingBatch() const {
  return !batches_->empty() &&
      batches_->back()->state == Batch::State::BUILDING;
}

struct AppendRequestCallbackImpl {
  void operator()(Status st, const DataRecord& record, NodeID redirect) {
    // Check that the BufferedWriter still exists; might have been destroyed
    // since the append went out
    Worker* w = Worker::onThisThread();
    auto it = w->active_buffered_writers_.find(writer_id_);
    if (it == w->active_buffered_writers_.end()) {
      return;
    }
    parent_->onAppendReply(batch_, st, record, redirect);
  }

  buffered_writer_id_t writer_id_;
  BufferedWriterSingleLog* parent_;
  BufferedWriterSingleLog::Batch& batch_;
};

void BufferedWriterSingleLog::sendBatch(Batch& batch) {
  if (batch.state == Batch::State::BUILDING) {
    ld_check(batch.blob.data == nullptr);

    batch_flags_t flags = Flags::SIZE_INCLUDED |
        (batch_flags_t(options_.compression) & Flags::COMPRESSION_MASK);

    setBatchState(batch, Batch::State::CONSTRUCTING_BLOB);
    construct_blob(batch, flags, checksumBits(), options_.destroy_payloads);
  } else {
    // This is a retry, so we must have already sent it, so we can skip the
    // purgatory of READY_TO_SEND.
    ld_check(batch.state == Batch::State::RETRY_PENDING);
    appendBatch(batch);
  }
}

void BufferedWriterSingleLog::readyToSend(Batch& batch) {
  ld_check(batch.state == Batch::State::CONSTRUCTING_BLOB);

  StatsHolder* stats{parent_->parent_->processor()->stats_};

  // Collect before&after byte counters to give clients an
  // idea of the compression ratio
  STAT_ADD(stats, buffered_writer_bytes_in, batch.payload_bytes_total);
  STAT_ADD(stats, buffered_writer_bytes_batched, batch.blob.size);

  setBatchState(batch, Batch::State::READY_TO_SEND);

  bool sent_at_least_one{false};
  while (!batches_->empty()) {
    size_t index = next_batch_to_send_ - batches_->front()->num;
    if (index >= batches_->size()) {
      break;
    }

    if ((*batches_)[index]->state != Batch::State::READY_TO_SEND) {
      if (!sent_at_least_one) {
        RATELIMIT_WARNING(
            1s,
            1,
            "On log %s, %lu batches are behind next_batch_to_send_ "
            "(%lu), which is in state %s",
            toString(log_id_).c_str(),
            batches_->size() - index,
            next_batch_to_send_,
            Batch::names()[(*batches_)[index]->state].c_str());
      }

      break;
    }

    next_batch_to_send_++;
    appendBatch(*(*batches_)[index]);
    sent_at_least_one = true;
  }
}

void BufferedWriterSingleLog::appendBatch(Batch& batch) {
  ld_check(batch.state == Batch::State::READY_TO_SEND ||
           batch.state == Batch::State::RETRY_PENDING);
  ld_check(batch.blob.data != nullptr);
  ld_check(!batch.retry_timer || !batch.retry_timer->isActive());

  if (batch.total_size_freed > 0) {
    parent_->parent_->appendSink()->onBytesFreedByWorker(
        batch.total_size_freed);
    // Now that we've recorded that they're freed, reset the counter so we don't
    // double count by double calling onBytesFreedByWorker(), e.g. if we retry.
    batch.total_size_freed = 0;
  }

  setBatchState(batch, Batch::State::INFLIGHT);

  // Call into BufferedWriter::appendBuffered() which in production is just a
  // proxy for ClientImpl::appendBuffered() or
  // SequencerBatching::appendBuffered() but in tests may be overridden to fail
  std::pair<Status, NodeID> rv = parent_->parent_->appendSink()->appendBuffered(
      log_id_,
      batch.appends,
      std::move(batch.attrs),
      Payload(batch.blob.data, batch.blob.size),
      AppendRequestCallbackImpl{parent_->id_, this, batch},
      Worker::onThisThread()->idx_, // need the callback on this same Worker
      checksumBits());
  if (rv.first != E::OK) {
    // Simulate failure reply
    onAppendReply(batch, rv.first, DataRecord(log_id_, Payload()), rv.second);
  }
}

void BufferedWriterSingleLog::onAppendReply(Batch& batch,
                                            Status status,
                                            const DataRecord& dr_batch,
                                            NodeID redirect) {
  ld_spew("status = %s", error_name(status));

  ld_check(batch.state == Batch::State::INFLIGHT);

  if (status != E::OK && scheduleRetry(batch, status, dr_batch) == 0) {
    // Scheduled retry, nothing else to do
    return;
  }

  invokeCallbacks(batch, status, dr_batch, redirect);
  finishBatch(batch);
  reap();

  if (options_.mode == BufferedWriter::Options::Mode::ONE_AT_A_TIME) {
    if (status == E::OK) {
      // Now that some batch finished successfully, reissue any appends that
      // were blocked in ONE_AT_A_TIME mode while the batch was inflight.
      unblockAppends();
    } else {
      // Batch failed (exhausted all retries), also fail any blocked appends to
      // preserve ordering.
      dropBlockedAppends(status, redirect);
    }
  }
}

void BufferedWriterSingleLog::quiesce() {
  // Stop all timers.
  if (time_trigger_timer_) {
    time_trigger_timer_->cancel();
  }

  for (std::unique_ptr<Batch>& batch : *batches_) {
    if (batch->retry_timer) {
      batch->retry_timer->cancel();
    }
  }
}

void BufferedWriterSingleLog::reap() {
  while (!batches_->empty() &&
         batches_->front()->state == Batch::State::FINISHED) {
    batches_->pop_front();
  }
  batches_.compact();
}

void BufferedWriterSingleLog::unblockAppends() {
  bool flush_at_end = false;

  while (!blocked_appends_->empty()) {
    // If there are more blocked appends after this one, defer the client size
    // trigger so that we fit as many as possible (still subject to the max
    // payload size limit) in the next batch.
    bool defer_client_size_trigger = blocked_appends_->size() > 1;
    int rv = appendImpl(blocked_appends_->front(), defer_client_size_trigger);
    if (rv != 0) {
      // Blocked.  Must have just flushed a new batch; keep the chunk in
      // `blocked_appends_'.
      break;
    }
    // Chunk contents were moved out of the queue, pop.
    blocked_appends_->pop_front();

    if (blocked_appends_flush_deferred_count_ > 0) {
      // There was a flush() call while this append was blocked.  That flush
      // had to be deferred because there was a batch already inflight and the
      // flush would have created a new one (but we are in ONE_AT_A_TIME
      // mode).
      flush_at_end = true;
      --blocked_appends_flush_deferred_count_;
    }
  }
  blocked_appends_.compact();

  // Despite `flush_at_end == true`, the log may not be flushable if the last
  // call to `appendImpl()` above has flushed the last batch
  if (flush_at_end && isFlushable()) {
    flush();
  }
}

void BufferedWriterSingleLog::dropBlockedAppends(Status status,
                                                 NodeID redirect) {
  BufferedWriterImpl::AppendCallbackInternal* cb =
      parent_->parent_->getCallback();
  for (auto& chunk : *blocked_appends_) {
    std::vector<std::pair<BufferedWriter::AppendCallback::Context, std::string>>
        context_set;
    for (auto& append : chunk) {
      std::string& payload = append.payload;
      BufferedWriter::AppendCallback::Context& context = append.context;
      context_set.emplace_back(std::move(context), std::move(payload));
    }
    cb->onFailureInternal(log_id_, std::move(context_set), status, redirect);
  }
  blocked_appends_->clear();
  blocked_appends_.compact();
  blocked_appends_flush_deferred_count_ = 0;
}

void BufferedWriterSingleLog::activateTimeTrigger() {
  if (options_.time_trigger.count() < 0) {
    return;
  }

  if (!time_trigger_timer_) {
    time_trigger_timer_ = std::make_unique<Timer>([this] {
      StatsHolder* stats{parent_->parent_->processor()->stats_};
      STAT_INCR(stats, buffered_writer_time_trigger_flush);
      flush();
    });
  }
  if (!time_trigger_timer_->isActive()) {
    time_trigger_timer_->activate(options_.time_trigger);
  }
}

int BufferedWriterSingleLog::scheduleRetry(Batch& batch,
                                           Status status,
                                           const DataRecord& /*dr_batch*/) {
  if (options_.retry_count >= 0 && batch.retry_count >= options_.retry_count) {
    return -1;
  }

  // Invoke retry callback to let the upper layer (application typically) know
  // about the failure (if interested) and check if it wants to block the
  // retry.
  BufferedWriterImpl::AppendCallbackInternal* cb =
      parent_->parent_->getCallback();
  if (cb->onRetry(log_id_, batch.appends, status) !=
      BufferedWriter::AppendCallback::RetryDecision::ALLOW) {
    // Client blocked the retry; bail out.  The caller will invoke the failure
    // callback shortly.
    return -1;
  }

  // Initialize `retry_timer' if this is the first retry
  if (!batch.retry_timer) {
    ld_check(options_.retry_initial_delay.count() >= 0);
    std::chrono::milliseconds max_delay = options_.retry_max_delay.count() >= 0
        ? options_.retry_max_delay
        : std::chrono::milliseconds::max();
    max_delay = std::max(max_delay, options_.retry_initial_delay);

    batch.retry_timer = std::make_unique<ExponentialBackoffTimer>(
        [this, &batch]() { sendBatch(batch); },
        options_.retry_initial_delay,
        max_delay);

    batch.retry_timer->randomize();
  }

  setBatchState(batch, Batch::State::RETRY_PENDING);
  ++batch.retry_count;
  ld_spew(
      "scheduling retry in %ld ms", batch.retry_timer->getNextDelay().count());
  batch.retry_timer->activate();
  return 0;
}

void BufferedWriterSingleLog::invokeCallbacks(Batch& batch,
                                              Status status,
                                              const DataRecord& dr_batch,
                                              NodeID redirect) {
  ld_check(batch.state != Batch::State::FINISHED);

  BufferedWriterImpl::AppendCallbackInternal* cb =
      parent_->parent_->getCallback();

  if (status == E::OK) {
    cb->onSuccess(log_id_, std::move(batch.appends), dr_batch.attrs);
  } else {
    cb->onFailureInternal(log_id_, std::move(batch.appends), status, redirect);
  }
}

void BufferedWriterSingleLog::finishBatch(Batch& batch) {
  setBatchState(batch, Batch::State::FINISHED);
  // Make sure payloads are deallocated; invokeCallbacks() ought to have moved
  // them back to the application
  ld_check(batch.appends.empty());
  batch.appends.shrink_to_fit();
  // Reclaim blob memory
  batch.blob = Slice();
  batch.blob_buf.reset();
  // Return the memory budget
  parent_->parent_->releaseMemory(batch.payload_bytes_total);
}

void BufferedWriterSingleLog::setBatchState(Batch& batch, Batch::State state) {
  batch.state = state;
  parent_->setFlushable(*this, isFlushable());
}

int BufferedWriterSingleLog::checksumBits() const {
  return parent_->parent_->shouldPrependChecksum()
      ? Worker::settings().checksum_bits
      : 0;
}

void BufferedWriterSingleLog::Impl::construct_uncompressed_blob(
    BufferedWriterSingleLog::Batch& batch,
    batch_flags_t flags,
    int checksum_bits,
    bool destroy_payloads) {
  ld_check(batch.total_size_freed == 0);
  // Note that due to the variable-sized header, blob_bytes_total is usually
  // slightly larger than the size of the constructed blob.
  std::unique_ptr<uint8_t[]> blob_buf(new uint8_t[batch.blob_bytes_total]);
  uint8_t* out = blob_buf.get();
  uint8_t* const end = out + batch.blob_bytes_total;
  ld_check(out < end);
  // Format of the header:
  // * 0-8 bytes reserved for checksum -- this is not really part of the
  //   BufferedWriter format, see BufferedWriterImpl::prependChecksums()
  // * 1 magic marker byte
  // * 1 flags byte
  // * 0-9 bytes varint batch size (optional, depending on flags)
  batch.blob_header_size = 0;

  if (checksum_bits > 0) {
    // construct_blob() expects the checksum to go at the beginning
    ld_check(out == blob_buf.get());
    size_t nbytes = checksum_bits / 8;
    ld_check(out + nbytes < end);
    std::memset(out, 0, nbytes);
    out += nbytes;
    batch.blob_header_size += nbytes;
  }

  batch.blob_header_size += 2;
  *out++ = 0xb1;
  ld_check(out < end);
  // Adjust flags to indicate no compression for now; maybe_compress_blob() will
  // overwrite it if it decides to compress.
  *out++ = (batch_flags_t)BufferedWriter::Options::Compression::NONE |
      (flags & ~Flags::COMPRESSION_MASK);
  size_t batch_size_varint_len = 0;
  if (flags & Flags::SIZE_INCLUDED) {
    // Append the number of records in this batch.
    batch_size_varint_len = folly::encodeVarint(batch.appends.size(), out);
    out += batch_size_varint_len;
    batch.blob_header_size += batch_size_varint_len;
    ld_check(out <= end);
  }
  for (auto& append : batch.appends) {
    const std::string& client_payload = append.second;
    size_t len = folly::encodeVarint(client_payload.size(), out);
    out += len;
    ld_check(out <= end);
    ld_check((ssize_t)(end - out) >= (ssize_t)client_payload.size());
    memcpy(out, client_payload.data(), client_payload.size());
    out += client_payload.size();
    if (destroy_payloads) {
      batch.total_size_freed += append.second.size();
      // Can't do append.second.clear() because that usually doesn't free
      // memory.
      append.second.clear();
      append.second.shrink_to_fit();
    }
  }
  // Assert that the running count (batch.blob_bytes_total) was accurate, taking
  // into account that we didn't use all the bytes we'd reserved for the varint.
  ld_check(out + (folly::kMaxVarintLength64 - batch_size_varint_len) == end);
  batch.blob = Slice(blob_buf.get(), out - blob_buf.get());
  batch.blob_buf = std::move(blob_buf);
}

void BufferedWriterSingleLog::Impl::maybe_compress_blob(
    BufferedWriterSingleLog::Batch& batch,
    Compression compression,
    int checksum_bits,
    const int zstd_level) {
  if (compression == Compression::NONE) {
    // Nothing to do.
    return;
  }
  ld_check(compression == Compression::ZSTD ||
           compression == Compression::LZ4 ||
           compression == Compression::LZ4_HC);

  // Skip the uncompressed blob header.
  ld_check(batch.blob_header_size > 0);
  ld_check(batch.blob.size > batch.blob_header_size);
  const Slice to_compress((uint8_t*)batch.blob.data + batch.blob_header_size,
                          batch.blob.size - batch.blob_header_size);

  const size_t compressed_data_bound = compression == Compression::ZSTD
      ? ZSTD_compressBound(to_compress.size)
      : LZ4_compressBound(to_compress.size);

  const size_t compressed_buf_size = batch.blob_header_size + // header
      folly::kMaxVarintLength64 + // uncompressed length
      compressed_data_bound       // compressed bytes
      ;
  std::unique_ptr<uint8_t[]> compress_buf(new uint8_t[compressed_buf_size]);
  uint8_t* out = compress_buf.get();
  uint8_t* const end = out + compressed_buf_size;

  // Copy the header from batch.blob and set the compression flag
  memcpy(out, batch.blob.data, batch.blob_header_size);
  out[checksum_bits / 8 + 1] |= (uint8_t)compression & Flags::COMPRESSION_MASK;
  out += batch.blob_header_size;

  // Append uncompressed size so that the decoding path knows how much memory
  // to allocate
  out += folly::encodeVarint(to_compress.size, out);

  size_t compressed_size;
  if (compression == Compression::ZSTD) {
    compressed_size = ZSTD_compress(out,              // dst
                                    end - out,        // dstCapacity
                                    to_compress.data, // src
                                    to_compress.size, // srcSize
                                    zstd_level);      // level
    if (ZSTD_isError(compressed_size)) {
      ld_error(
          "ZSTD_compress() failed: %s", ZSTD_getErrorName(compressed_size));
      ld_check(false);
      return;
    }
  } else {
    // LZ4
    int rv;
    if (compression == Compression::LZ4) {
      rv = LZ4_compress_default(
          (char*)to_compress.data, (char*)out, to_compress.size, end - out);
    } else {
      rv = LZ4_compress_HC(
          (char*)to_compress.data, (char*)out, to_compress.size, end - out, 0);
    }
    ld_spew("LZ4_compress() returned %d", rv);
    ld_check(rv > 0);
    compressed_size = rv;
  }
  out += compressed_size;
  ld_check(out <= end);

  const size_t compressed_len = out - compress_buf.get();
  ld_spew(
      "original size is %zu, compressed %zu", batch.blob.size, compressed_len);
  if (compressed_len < batch.blob.size) {
    // Compression was a win.  Replace the uncompressed blob.
    batch.blob = Slice(compress_buf.get(), compressed_len);
    batch.blob_buf = std::move(compress_buf);
  }
}

void BufferedWriterSingleLog::Impl::construct_blob_long_running(
    BufferedWriterSingleLog::Batch& batch,
    batch_flags_t flags,
    int checksum_bits,
    bool destroy_payloads,
    const int zstd_level) {
  ld_check(batch.state == Batch::State::CONSTRUCTING_BLOB);

  construct_uncompressed_blob(batch, flags, checksum_bits, destroy_payloads);
  maybe_compress_blob(batch,
                      (Compression)(flags & Flags::COMPRESSION_MASK),
                      checksum_bits,
                      zstd_level);

  if (checksum_bits > 0) {
    // construct_uncompressed_blob() left this many bytes at the front to put
    // the checksum into.  It couldn't have done so itself because the
    // checksum needs to cover the compressed blob, if we compressed.
    size_t nbytes = checksum_bits / 8;
    Slice checksummed(
        (uint8_t*)batch.blob.data + nbytes, batch.blob.size - nbytes);
    checksum_bytes(checksummed, checksum_bits, (char*)batch.blob.data);
  }
}

void BufferedWriterSingleLog::construct_blob(
    BufferedWriterSingleLog::Batch& batch,
    batch_flags_t flags,
    int checksum_bits,
    bool destroy_payloads) {
  ld_check(batch.state == Batch::State::CONSTRUCTING_BLOB);

  if (parent_->parent_->isShuttingDown()) {
    // Our destructor will call callbacks with E::SHUTDOWN.
    return;
  }

  const int zstd_level = Worker::settings().buffered_writer_zstd_level;

  // We need to call construct_blob_long_running(), then callback().  If the
  // batch is large, we send it to a background thread so that this thread can
  // process more requests.  However, if it's small, we just do that inline
  // since the queueing & switching overhead would cost more than we save.

  if (batch.blob_bytes_total <
      Worker::settings().buffered_writer_bg_thread_bytes_threshold) {
    Impl::construct_blob_long_running(
        batch, flags, checksum_bits, destroy_payloads, zstd_level);
    readyToSend(batch);
  } else {
    ProcessorProxy* processor_proxy = parent_->parent_->processorProxy();
    ld_spew("Enqueueing batch %lu for log %s to background thread.  Batches "
            "outstanding for this log: %lu Background tasks: %lu",
            batch.num,
            toString(log_id_).c_str(),
            batches_->size(),
            parent_->parent_->recentNumBackground());

    processor_proxy->processor()->enqueueToBackgroundBlocking(
        [&batch,
         flags,
         checksum_bits,
         destroy_payloads,
         processor_proxy,
         trigger = parent_->parent_->getBackgroundTaskCountHolder(),
         thread_affinity = Worker::onThisThread()->idx_.val(),
         zstd_level,
         this]() mutable {
          BufferedWriterSingleLog::Impl::construct_blob_long_running(
              batch, flags, checksum_bits, destroy_payloads, zstd_level);
          std::unique_ptr<Request> request =
              std::make_unique<ContinueBlobSendRequest>(
                  this, batch, thread_affinity);
          // Since this runs on the background thread, be careful not to access
          // non-constant fields of "this", unless you know what you're doing.
          ld_spew("Done constructing batch %lu for log %s, posting back to "
                  "worker.  Background tasks: %lu",
                  batch.num,
                  toString(log_id_).c_str(),
                  parent_->parent_->recentNumBackground());

          int rc = processor_proxy->postWithRetrying(request);
          if (rc != 0) {
            ld_error("Processor::postWithRetrying() failed: %d", rc);
          }
        });
  }
}

}} // namespace facebook::logdevice
