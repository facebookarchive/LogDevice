/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <tuple>
#include <vector>

#include "logdevice/include/Client.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file BufferedWriter.h
 * @brief Utility class for buffering and batching appends on the client.
 *
 * The regular Client::append() immediately sends the record to LogDevice.
 * Because of the per-append cost of processing inside LogDevice, sending many
 * small records can limit throughput.
 *
 * This class allows latency of writes to be traded off for throughput.  It
 * presents a similar API to Client::append() but buffers appends for the same
 * log on the client and sends them to LogDevice in fewer, larger, records.
 * The records are automatically decoded on the read path by Reader.
 *
 * BufferedWriter appends are by necessity async so there is a callback
 * interface to notify the application when an append has completed.  Because
 * BufferedWriter is meant for high-throughput writing, the callback interface
 * does not use std::function but a slightly more complicated setup: the
 * application provides a single subclass of AppendCallback when it creates
 * BufferedWriter.
 *
 * When it calls BufferedWriter::append(), the application may, optionally,
 * provide a pointer to a piece of context. This pointer is included,
 * along with the payload, in the ContextSet vector at callback.
 *
 * Applications are expected to configure the latency tradeoff via
 * Options::time_trigger.  For example, a value of 1 second means that
 * buffered writes for a log will be flushed when the oldest of them has been
 * buffered for 1 second.  With a steady stream of appends to the log, we will
 * essentially flush once every second.
 *
 * See Options for additional features:
 * - automatic retrying of failed writes
 * - compression
 * - overall memory limit
 *
 * All methods in this class are thread-safe.
 *
 * See doc/buffered-writer.md for an overview of the implementation.
 */

class BufferedWriterImpl;
class BufferedWriterAppendSink;

class BufferedWriter {
 public:
  /**
   * Callback interface.  All methods get called on an unspecified thread.
   * Applications should subclass and override desired notification methods.
   */
  class AppendCallback {
   public:
    using Context = void*;
    using ContextSet = std::vector<std::pair<Context, std::string>>;

    /**
     * Called when a batch of records for the same log was successfully
     * appended.
     *
     * Payload strings (in the ContextSet vector) are no longer needed within
     * BufferedWriter so the application is free to steal them.  All of the
     * records share the same LSN and timestamp, available in `attrs'.
     */
    virtual void onSuccess(logid_t /*log_id*/,
                           ContextSet /*contexts_and_payloads*/,
                           const DataRecordAttributes& /*attrs*/) {}
    /**
     * Called when a batch of records for the same log failed to be appended,
     * and BufferedWriter exhausted all retries it was configured to do (if
     * any).
     *
     * Payload strings (in the ContextSet vector) are no longer needed within
     * BufferedWriter so the application is free to steal them.
     */
    virtual void onFailure(logid_t /*log_id*/,
                           ContextSet /*contexts_and_payloads*/,
                           Status /*status*/) {}

    enum class RetryDecision { ALLOW, DENY };

    /**
     * Called when a batch of records for the same log failed to be appended,
     * but BufferedWriter is planning to retry.
     *
     * If ALLOW is returned, BufferedWriter will proceed to schedule the retry
     * for this batch.  If DENY is returned, BufferedWriter will not retry and
     * will instead invoke onFailure() shortly after.
     */
    virtual RetryDecision onRetry(logid_t /*log_id*/,
                                  const ContextSet& /*contexts_and_payloads*/,
                                  Status /*status*/) {
      return RetryDecision::ALLOW;
    }

    virtual ~AppendCallback() {}
  };
  struct Append {
    Append(logid_t log_id,
           std::string payload,
           AppendCallback::Context context,
           AppendAttributes attrs = AppendAttributes())
        : log_id(log_id),
          payload(std::move(payload)),
          context(context),
          attrs(std::move(attrs)) {}

    Append(const Append&) = delete;
    Append(Append&&) = default;
    Append& operator=(Append&&) = default;
    logid_t log_id;
    std::string payload;
    AppendCallback::Context context;
    AppendAttributes attrs;
  };
  struct LogOptions {
    // TODO: Remove it from this struct.
    using Compression = facebook::logdevice::Compression;

    LogOptions() {}

    // Flush buffered writes for a log when the oldest has been buffered this
    // long (negative for no trigger)
    std::chrono::milliseconds time_trigger{-1};

    // Flush buffered writes for a log as soon there are this many payload
    // bytes buffered (negative for no trigger)
    ssize_t size_trigger = -1;

    enum class Mode {
      // Write each batch independently (also applies to retries if
      // configured).
      //
      // This is the default mode which allows for highest throughput, but can
      // cause writes to get reordered.  For example, suppose two batches 1
      // and 2 get sent out, 1 fails and 2 succeeds.  After 1 is retried, the
      // contents of the log would be 21 (or 121 if the very first write
      // actually succeeded but we could not get confirmation).
      INDEPENDENT,

      // Only allow one batch at a time to be inflight to LogDevice servers.
      // This fixes the ordering issue in the INDEPENDENT mode.  It is
      // especially useful for stream processing cases where records in
      // LogDevice need to end up in the same order as in the input stream.
      //
      // The size and time triggers are relaxed (batches can get larger and
      // delayed while one is already in flight).  This mode possibly limits
      // throughput under certain conditions (extremely high throughput on a
      // single log and/or errors writing to LogDevice).
      ONE_AT_A_TIME,
    };
    Mode mode = Mode::INDEPENDENT;
    // Max number of times to retry (0 for no retrying, negative for
    // unlimited).  You may also manually track retries and have onRetry()
    // return DENY to stop retrying a particular batch.
    int retry_count = 0;
    // Initial delay before retrying (negative for a default 2x the append
    // timeout).  Subsequent retries are made after successively larger delays
    // (exponential backoff with a factor of 2) up to retry_max_delay
    std::chrono::milliseconds retry_initial_delay{-1};
    // Max delay when retrying (negative for no limit)
    std::chrono::milliseconds retry_max_delay{60000};

    // Compression codec.
    Compression compression = Compression::LZ4;

    // If set to true, will destroy individual payloads immediately after they
    // are batched together. onSuccess(), onFailure() and onRetry() callbacks
    // will not contain payloads.
    bool destroy_payloads = false;

    // Returns "independent" or "one_at_a_time".
    static std::string modeToString(Mode mode);
    // Returns 0 on success, -1 on error.
    static int parseMode(const char* str, Mode* out_mode);

    // COMPAT. Use `compressionToString` and `parseCompression` from
    // facebook::logdevice namespace.
    static std::string compressionToString(Compression c);
    static int parseCompression(const char* str, Compression* out_c);
  };

  // NOTE: It is possible to have different options for each log but currently
  // this feature is not supported in the interface.
  struct Options : public LogOptions {
    Options() : LogOptions() {}

    // Approximate memory budget for buffered and inflight writes.  If an
    // append() call would exceed this limit, it fails fast with E::NOBUFS.
    //
    // Accounting is not completely accurate for performance reasons.  There
    // is internal overhead per batch and there may be pathological cases
    // where actual memory usage exceeds the limit.  However, in most cases it
    // should stay well under.
    //
    // Negative for no limit.
    int32_t memory_limit_mb = -1;
  };

  /**
   * Constructing and destructing a BufferedWriter involves interthread
   * communication (with LogDevice library threads) and may block if those
   * threads are busy.  BufferedWriter instances are meant to be long-lived
   * (and clients will typically use just one).
   *
   * Uses client as sink for sending buffered appends.
   */
  static std::unique_ptr<BufferedWriter> create(std::shared_ptr<Client> client,
                                                AppendCallback* callback,
                                                Options options = Options());

  /**
   * Creates a BufferedWriter for the 'client' and sends buffered appends to
   * 'sink'. Note that'sink' must outlive BufferedWriter. Not ready for use yet.
   * Incomplete experimental feature.
   */
  static std::unique_ptr<BufferedWriter> create(std::shared_ptr<Client> client,
                                                BufferedWriterAppendSink* sink,
                                                AppendCallback* callback,
                                                Options options = Options());

  /**
   * Same as Client::append() except the append may get buffered.
   *
   * If the call succeeds (returns 0), the class assumes ownership of the
   * payload.  If the call fails, the payload remains in the given
   * std::string.
   * See Client::append for explanation of AppendAttributes
   */
  int append(logid_t logid,
             std::string&& payload,
             AppendCallback::Context callback_context,
             AppendAttributes&& attrs = AppendAttributes());

  /**
   * Multi-write version of append().  Requires less interthread communication
   * than calling append() for each record.
   *
   * @return A vector of Status objects, one for each input append.  The
   *         status is E::OK if the append was successfully queued for
   *         writing, or otherwise one of the `err' codes documented for the
   *         single-write append().  If some of the appends fail, their
   *         payloads remain in the input vector.
   */
  std::vector<Status> append(std::vector<Append>&& appends);

  /**
   * Instructs the class to immediately flush all buffered appends.  Does not
   * block, just passes messages to LogDevice threads.

   * It is not intended for this to be called often in production as it
   * can limit the amount of batching; space- and time-based flushing should
   * be preferred.
   *
   * @return 0 on success, -1 if messages could not be posted to some
   *         LogDevice threads
   */
  int flushAll();

  /**
   * NOTE: involves communication with LogDevice threads, blocks until they
   * acknowledge the destruction.
   */
  virtual ~BufferedWriter() {}

 private:
  BufferedWriter() {} // can be constructed by the factory only
  BufferedWriter(const BufferedWriter&) = delete;
  BufferedWriter& operator=(const BufferedWriter&) = delete;

  friend class BufferedWriterImpl;
  BufferedWriterImpl* impl(); // downcasts (this)
};
}} // namespace facebook::logdevice
