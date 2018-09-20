/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <memory>

#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

struct ReadStreamAttributes;

enum class HealthChangeType {
  LOG_HEALTHY = 0,
  LOG_UNHEALTHY = 1,
};

/**
 * @file AsyncReader objects offer an alternative interface (to the
 * synchronous Reader) for reading logs.  Records are delivered via callbacks.
 *
 * Callbacks are invoked on internal LogDevice threads that belong to a
 * Client.  Callbacks should not do too much work or they might block other
 * communication on the Client.  Callbacks for one log will be called on the
 * same thread, however callbacks for different logs typically use multiple
 * threads.  The thread for one log may change if reading is stopped and
 * restarted for the log.
 *
 * This class is *not* thread-safe - calls should be made from one thread at a
 * time.
 */

class AsyncReader {
 public:
  /**
   * Sets a callback that the LogDevice client library will call when a record
   * is read.
   *
   * The callback should return true if the record was successfully consumed.
   * If the callback returns false, delivery of the same record will be
   * retried after some time. Redelivery can also be requested with a
   * resumeReading() call.
   *
   * NOTE: The callback must not drain the input unique_ptr& if it return false
   * (this is asserted in debug builds).
   *
   * Only affects subsequent startReading() calls; calling startReading()
   * first and setRecordCallback() after has no effect.
   */
  virtual void
  setRecordCallback(std::function<bool(std::unique_ptr<DataRecord>&)>) = 0;

  /**
   * Sets a callback that the LogDevice client library will call when a gap
   * record is delivered for this log. A gap record informs the reader about
   * gaps in the sequence of record numbers. In most cases such gaps are
   * benign and not an indication of data loss. See class GapRecord in
   * Record.h for details.
   *
   * The callback should return true if the gap was successfully consumed.
   * If the callback returns false, delivery of the same gap will be
   * retried after some time. Redelivery can also be requested with a
   * resumeReading() call.
   */
  virtual void setGapCallback(std::function<bool(const GapRecord&)>) = 0;

  /**
   * Sets a callback that the LogDevice client library will call when it has
   * finished reading the requested range of LSNs.
   */
  virtual void setDoneCallback(std::function<void(logid_t)>) = 0;

  /**
   * Sets a callback that the LogDevice client library will call when
   * a health change is detected for any log it is reading from.
   *
   * The callback receives as parameters the log ID for which the
   * change was detected and a status reporting the type of health
   * change.
   */
  virtual void
  setHealthChangeCallback(std::function<void(logid_t, HealthChangeType)>) = 0;

  /**
   * Start reading records from a log in a specified range of LSNs.  The
   * function will return as soon as the request is put on a local queue.
   * Upon successful return, the next record to be delivered to a callback will
   * be as described in @param from below.
   *
   * If the log is already being read by this AsyncReader, this method stops
   * reading and starts again with the new parameters. However, the stopping is
   * asynchronous. For a short time you may see records from both the old
   * and the new stream, interleaved arbitrarily. If you need a clean
   * cutover, call stopReading() (passing a callback), then startReading() after
   * the callback is called.
   *
   * @param log_id log ID to start reading
   *
   * @param from  log sequence number (LSN) to move the read pointer to. If this
   *              LSN identifies a data record currently in the log, that record
   *              will be the next one delivered to a data callback installed
   *              for the log, or to a Reader object for the log.
   *
   *              If the lowest (oldest) LSN in the log is greater than
   *              this value, the read pointer will move to the oldest record
   *              in the log and that record will be the next one delivered.
   *              See LSN_OLDEST in types.h.
   *
   *              If _from_ falls into a gap in the numbering sequence, the
   *              next record delivered to this reader will be the gap record.
   *
   * @param until  the highest LSN the LogDevice cluster will deliver to this
   *               AsyncReader object.  Once this LSN is reached, the LogDevice
   *               client library will call the done callback. The client
   *               must call startReading() again in order to continue
   *               delivery. If the read pointer comes across a sequence gap
   *               that includes this LSN, the delivery stops after the gap
   *               record is delivered. By default (see LSN_MAX in types.h)
   *               records continue to be delivered in sequence until delivery
   *               is explicitly cancelled by a call to stopReading() below,
   *               or altered by another call to startReading().
   *
   * @param attrs    Structure containing parameters that alter
   *                 the behavior of the read stream. In
   *                 particular it is used to pass filters
   *                 for the server-side filtering experimental feature.
   *
   * @return  0 is returned if the request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             NOBUFS        if request could not be enqueued because a buffer
   *                           space limit was reached
   *             INVALID_PARAM if from > until or the record callback was not
   *                           specified.
   *             SHUTDOWN      the logdevice::Client instance was destroyed.
   *             INTERNAL      An internal error has been detected, check logs.
   *
   */
  virtual int startReading(logid_t log_id,
                           lsn_t from,
                           lsn_t until = LSN_MAX,
                           const ReadStreamAttributes* attrs = nullptr) = 0;

  /**
   * Ask LogDevice cluster to stop delivery of this log's records.  The
   * callbacks registered for the log or the Reader object reading records
   * from this log will stop receiving this log's records until one of
   * startReading() methods is called again.
   *
   * The function returns as soon as the request is put on a local queue.
   * However, record/gap callbacks may continue to be called for the log until
   * a Client thread is able to process the stop request.  After the optional
   * callback is called (on the Client thread), it is guaranted that no further
   * records or gaps will be delivered. The callback is called exactly once,
   * but might be called during or after AsyncReader destruction.
   *
   * @param log_id log ID to stop reading
   * @param callback optional callback to invoke when the request has taken
   *                 effect and no more records will be delivered
   *
   * @return  0 is returned if a stop request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             NOTFOUND if reading was not started for specified log
   */
  virtual int stopReading(logid_t log_id,
                          std::function<void()> callback = nullptr) = 0;

  /**
   * Requests delivery for a log to resume after a previous delivery was
   * declined (callback returned false). This can be used to avoid waiting on
   * the redelivery timer when the callback becomes ready to accept new
   * records.
   *
   * NOTE: involves interthread communication which can fail if the queues
   * fill up.  However, no failure handling is generally needed because
   * delivery is retried on a timer.
   *
   * @param log_id log ID to stop reading
   *
   * @return  0 is returned if resume request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *              NOBUFS   if request could not be enqueued because a buffer
   *                       space limit was reached
   *              NOTFOUND if reading was not started for specified log
   */
  virtual int resumeReading(logid_t log_id) = 0;

  /**
   * If called, data records read by this AsyncReader will not include payloads.
   *
   * This makes reading more efficient when payloads are not needed (they won't
   * be transmitted over the network).
   *
   * Only affects subsequent startReading() calls.
   */
  virtual void withoutPayload() = 0;

  /**
   * If called, disable the single copy delivery optimization even if the log is
   * configured to support it. Each data record will be sent by all storage
   * nodes that have a copy instead of exactly one.
   * This greatly increases read availability at the cost of higher network
   * bandwith and cpu usage.
   *
   * Only affects subsequent startReading() calls.
   */
  virtual void forceNoSingleCopyDelivery() = 0;

  /**
   * If called, data records read by this AsyncReader will start including
   * approximate amount of data written to given log up to current record
   * once it become available to AsyncReader.
   *
   * The value itself stored in DataRecord::attrs::byte_offset. Set as
   * BYTE_OFFSET_INVALID if unavailable to AsyncReader yet.
   *
   * Only affects subsequent startReading() calls.
   */
  virtual void includeByteOffset() = 0;

  /**
   * If called, when reading a section of the log that has been partially
   * trimmed, the reader will deliver whatever records are still
   * available, which (because of LogDevice's distributed and nondeterministic
   * nature) results in an interleaved stream of records and TRIM gaps, which
   * is undesirable in some cases.
   *
   * The default behaviour is to deliver a large trim gap for the entire
   * section.
   *
   * See doc/partially-trimmed.md for a detailed explanation.
   */
  virtual void doNotSkipPartiallyTrimmedSections() = 0;

  /**
   * Checks if the connection to the LogDevice cluster for a log appears
   * healthy.  When a read() call times out, this can be used to make an
   * informed guess whether this is because there is no data or because there
   * a service interruption.
   *
   * NOTE: this is not 100% accurate but will expose common issues like losing
   * network connectivity.
   *
   * @return On success, returns 1 if the connection appears healthy or 0 if
   * there are issues talking to the cluster.  On error returns -1 and sets
   * err to NOTFOUND (not reading given log).
   */
  virtual int isConnectionHealthy(logid_t) const = 0;

  /**
   * Instructs the Reader instance to pass through blobs created by
   * BufferedWriter.
   *
   * By default (if this method is not called), AsyncReader automatically
   * decodes blobs written by BufferedWriter and yields original records as
   * passed to BufferedWriter::append(). If this method is called,
   * BufferedWriteDecoder can be used to decode the blobs.
   */
  virtual void doNotDecodeBufferedWrites() = 0;

  /**
   * Get next lowest recommended LSN to read from when servers appear stuck.
   *
   * To force progress when stuck at `stuck_lsn`, call stopReading(), then call
   * startReading() from nextFromLsnWhenStuck(stuck_lsn).
   *
   * NOTE: Forcing progress is likely to miss some records between stuck_lsn
   * and nextFromLsnWhenStuck(stuck_lsn). The records will become available
   * eventually (once server-side issues have been resolved), but the only way
   * to read them is to re-read the section from stuck_lsn to
   * nextFromLsnWhenStuck(stuck_lsn) later.
   *
   * @param stuck_lsn  LSN at which the reader got stuck; that is, the last
   *                   LSN the reader tried to read or managed to read (+-1
   *                   does not affect the output of this function).
   * @param tail_lsn   Tail LSN, if known, as returned by Client::getTailLSN()
   *                   or Client::findTime().
   * @return           Recommended LSN to use as `from_lsn` in startRead() to
   *                   make progress, no less than `stuck_lsn`.
   */
  static lsn_t nextFromLsnWhenStuck(lsn_t stuck_lsn = LSN_INVALID,
                                    lsn_t tail_lsn = LSN_INVALID);

  /**
   * Report the size (in bytes) of the data records that the underlying
   * ClientReadStreamBuffers currently occupy
   */
  virtual void getBytesBuffered(std::function<void(size_t)> callback) = 0;

  virtual ~AsyncReader() {}
};

}} // namespace facebook::logdevice
