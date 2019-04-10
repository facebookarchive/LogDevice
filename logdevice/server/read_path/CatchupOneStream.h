/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/EnumMap.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/read_path/ServerReadStream.h"

namespace facebook { namespace logdevice {

class BWAvailableCallback;
class ReadIoShapingCallback;
class CatchupQueue;
class CatchupQueueDependencies;
class LogStorageState;
class ReadStorageTask;
class ServerReadStream;
enum class GapReason : uint8_t;

/**
 * @file Manages reading and shipping a batch of records for one read stream
 *       that is catching up. For an explanation of "catching up", see doc for
 *       ServerReadStream::isCatchingUp(). This class is responsible for
 *       advancing ServerReadStream::read_ptr and
 *       ServerReadStream::last_delivered_record_ as records are shipped to the
 *       client.
 *
 *       Reading from the local log store can be done on the worker thread if a
 *       non blocking read can be performed, or on a separate pool of storage
 *       threads so that worker threads don't block on I/O. To do so,
 *       CatchupOneStream provides two methods read() and onReadTaskDone().
 *       The former will try to read some records without blocking. If more
 *       records can be read but reading them would block, read() will create a
 *       storage to be executed by a storage thread and return
 *       Action::WAIT_FOR_STORAGE_TASK. The user is then expected to call
 *       onReadTaskDone() with the storage task upon completion.
 */

class CatchupOneStream {
 public:
  /**
   * List actions that CatchupQueue may take after reading a batch of records on
   * a stream.
   */
  enum class Action {
    // We successfully shipped some records (possibly none) but failed early
    // because of a transient error such as a message could not be successfully
    // sent. CatchupQueue should issue a timer to try again after some time.
    TRANSIENT_ERROR,
    // There is an I/O error on shard(s), due to which no further reading is
    // possible.
    PERMANENT_ERROR,
    // We shipped some records (possibly none) but were throttled by traffic
    // shaping. A callback to resume processing has been registered and will
    // fire once bandwidth is available.
    WAIT_FOR_BANDWIDTH,
    // We got ratelimited because of excessive read I/O, a callback will be
    // fired when bandwidth is available
    WAIT_FOR_READ_BANDWIDTH,
    // Can only be returned by read(). Some records may have already been
    // delivered to the client if try_non_blocking_read is true, but some more
    // records are available and a storage task has been created to read them
    // CatchupQueue is expected to call onReadTaskDone() when the task
    // completes.
    WAIT_FOR_STORAGE_TASK,
    // Can only be returned by read(). The last released lsn has been exceeded
    // and a storage task has been created to read the last known good
    // (lng) lsn required to make progress. A callback to resume processing has
    // been registered and will fire once the lng is available.
    WAIT_FOR_LNG,
    // Delivered all (possibly none) records that can be shipped to the clients
    // currently.
    // CatchupQueue should remove the stream from its queue and continue
    // processing the queue.
    // Two possibilities:
    //  - The last released record was delivered and there are no more records
    //    to be sent. The user should read again the next time a record is
    //    released.
    //  - We reached the end of the current window. The user should read again
    //    the next time the window is updated.
    DEQUEUE_AND_CONTINUE,
    // Delivered all records the client was interested in (until_lsn reached),
    // or a fatal error occured preventing from continuing reading on that
    // stream. CatchupQueue should delete the stream and continue processing its
    // queue.
    ERASE_AND_CONTINUE,
    // We may have successfully shipped some records. CatchupQueue should
    // requeue the stream and continue processing its queue.
    REQUEUE_AND_CONTINUE,
    // We successfully shipped some records and there are more to ship but we
    // already reached the byte limit. CatchupQueue should requeue the stream
    // but wait until all the already enqueued messages drain before continuing
    // processing its queue.
    REQUEUE_AND_DRAIN,
    // This read requires blocking, but we were told not to block.
    WOULDBLOCK,
    // When trying to retrieve an item from the RealTiemBuffer, if the item
    // isn't there, we return this so we know to look in LogsDB.
    NOT_IN_REAL_TIME_BUFFER,

    MAX,
    INVALID
  };

  static EnumMap<Action, std::string> action_names;

  /**
   * Read a batch of records.
   *
   * @param deps                    Mocked by unit tests.
   * @param stream                  The stream being read.
   * @param catchup_queue           Weak reference to CatchupQueue that
   *                                initiates the read.
   * @param try_non_blocking_read   If true, try a non-blocking read on the
   *                                worker thread before involving a storage
   *                                thread.
   * @param max_record_bytes_queued Limit on number of record bytes queued in
   *                                output evbuffer.
   * @param first_record_any_size   First record ignores previous limit.
   *                                for records read on a storage thread.
   * @param allow_storage_task      If a non-blocking read isn't tried, either
   *                                because the data isn't in memory or because
   *                                try_non_blocking_read is false, this
   *                                controls whether we issue a StorageTask to
   *                                perform blocking I/O, or just return
   *                                WOULDBLOCK.
   *
   * @return std::pair of Action, and number of RECORD bytes queued, the number
   *                                of bytes that were immediatly enqueued in
   *                                the output evbuffer.
   */
  static std::pair<Action, size_t> read(CatchupQueueDependencies& deps,
                                        ServerReadStream* stream,
                                        WeakRef<CatchupQueue> catchup_queue,
                                        bool try_non_blocking_read,
                                        size_t max_record_bytes_queued,
                                        bool first_record_any_size,
                                        bool allow_storage_task,
                                        CatchupEventTrigger reason);

  /**
   * Called when the storage task that was started following read()
   * returning Action::WAIT_FOR_STORAGE_TASK completes.
   *
   * @param deps                See read().
   * @param stream              See read().
   * @param task                Storage task that completed.
   *
   * @return see Action, and the number of bytes that were enqueued in the
   *                            output evbuffer for RECORD messages.
   */
  static std::pair<Action, size_t>
  onReadTaskDone(CatchupQueueDependencies& deps,
                 ServerReadStream* stream,
                 const ReadStorageTask& task);

 private:
  CatchupOneStream(CatchupQueueDependencies& deps,
                   ServerReadStream* stream,
                   BWAvailableCallback& resume_cb);

  Action startRead(WeakRef<CatchupQueue> catchup_queue,
                   bool try_non_blocking_read,
                   size_t max_record_bytes_queued,
                   bool first_record_any_size,
                   bool allow_storage_task,
                   CatchupEventTrigger reason,
                   ReadIoShapingCallback& read_shaping_cb);

  Action pushReleasedRecords(std::vector<std::shared_ptr<ReleasedRecords>>&,
                             LocalLogStoreReader::ReadContext& read_ctx);

  Action processTask(const ReadStorageTask& task);

  Action processRecords(const std::vector<RawRecord>& records,
                        server_read_stream_version_t version,
                        const LocalLogStoreReader::ReadPointer& read_ptr,
                        bool accessed_under_replicated_region,
                        Status status,
                        CatchupEventTrigger catchup_reason);

  /**
   * Attempts a non-blocking read of a batch of records from the local log
   * store. If no records can be read without blocking, we return right away.
   *
   * @param catchup_queue           Weak reference to CatchupQueue that
   *                                initiates the read.
   * @param last_released_lsn read from the Processor's LogStorageStateMap
   * @param max_record_bytes_queued See read().
   * @param first_record_any_size   See read().
   *
   * @return see Action.
   */
  Action readNonBlocking(WeakRef<CatchupQueue> catchup_queue,
                         LocalLogStoreReader::ReadContext& read_ctx);

  /**
   * Schedules a ReadStorageTask for a storage thread to read a batch of
   * records from the local log store.
   *
   * @param catchup_queue           Weak reference to CatchupQueue that
   *                                initiates the read.
   * @param read_ctx        ReadContext object which defines last_released_lsn
   *                        read from the Processor's LogStorageStateMap,
   *                        max_record_bytes_queued as well as
   *                        first_record_any_size (See read()).
   * @cost_estimate         amount of read shaping credits to debit in order to
   *                        issue this storage task.
   * @param inject_latency  whether to simulate slow reads by injecting latency
   */
  void readOnStorageThread(WeakRef<CatchupQueue> catchup_queue,
                           LocalLogStoreReader::ReadContext& read_ctx,
                           size_t cost_estimate,
                           bool inject_latency = false);

  /**
   * Conditionally send the STARTED message if one is needed for this
   * stream.
   *
   * @return Any error code from Sender::sendMessage().
   */
  int sendStarted(LogStorageState::LastReleasedLSN last_released);

  /**
   * Send a gap to indicate that there are no records between
   * stream_->last_delivered_record_ and no_records_upto.
   *
   * @param lsn up to which there are no more records.
   * @return the return value of sendGAP.
   */
  int sendGapNoRecords(lsn_t no_records_upto);

  /**
   * After processing a batch of records read from the local log store,
   * updates the stream's state depending on the status provided by
   * LocalLogStoreReader::read().
   *
   * @param stream_version_before This value will be compared with the current
   *                              stream's version. If the values don't match
   *                              this function will return
   *                              Action::REQUEUE_AND_CONTINUE instead of
   *                              Action::DEQUEUE_AND_CONTINUE if
   *                              the stream appears to be caught up. See
   *                              ServerReadStream::version_ for more details.
   * @param status                Status returned by the call to
   *                              LocalLogStoreReader::read() that issued
   *                              records for this batch.
   * @param read_ptr              Value provided by the last call to
   *                              LocalLogStoreReader::read(). This is how far
   *                              it was able to go in the db.
   *                              Used to determine the type and boundaries of
   *                              any gap to be sent to the client.
   *                              This is also used to fast-forward the stream's
   *                              read_ptr. Indeed, the stream's read_ptr is
   *                              already increased by processRecord(), but we
   *                              may be able to fast-forward it to take into
   *                              account the records that were filtered by
   *                              LocalLogStoreReder::read() or because it
   *                              determined that there are no records up to
   *                              some lsn.
   * @return see Action.
   */
  Action handleBatchEnd(server_read_stream_version_t stream_version_before,
                        Status status,
                        const LocalLogStoreReader::ReadPointer& read_ptr);

  /**
   * Send a GAP message to notify the client that the next LSN available on this
   * storage node is end_lsn + 1 and is outside of its window. Can specify a
   * provided_start_lsn to override the default start_lsn
   *
   * @return On success, returns 0. On failure, returns -1 with err set
   *         according to Sender::sendMessage()
   */
  int sendGAP(lsn_t end_lsn,
              GapReason reason,
              lsn_t provided_start_lsn = LSN_INVALID);

  /**
   * @return ReadContext to be passed by LocalLogStoreReader::read().
   */
  LocalLogStoreReader::ReadContext
  createReadContext(lsn_t last_released_lsn,
                    size_t max_record_bytes_queued,
                    bool first_record_any_size,
                    CatchupEventTrigger catchup_reason);

  std::tuple<StorageTaskType,
             StorageTaskThreadType,
             StorageTaskPriority,
             StorageTaskPrincipal>
  getPriorityForStorageTasks();

  /**
   * Read last known good.
   *
   * @return Attempts to perform a non-blocking read. Returns 0 on success and
   *         updates stream_->last_known_good_. Otherwise, if allow_storage_task
   *         is true, it creates a storage task and returns -1.  Otherwise,
   *         just returns -1.
   */
  int readLastKnownGood(WeakRef<CatchupQueue> catchup_queue,
                        bool allow_storage_task);

  /**
   * Helper method for sending FILTERED_OUT gap
   * @param  trim_point     an optional trim point
   *
   * @return   -1   when failed
   *            0   when success or there is no FILTERED_OUT gap pending
   */
  int sendGapFilteredOutIfNeeded(folly::Optional<lsn_t> trim_point);

  CatchupQueueDependencies& deps_;
  ServerReadStream* stream_{nullptr};
  BWAvailableCallback& resume_cb_;

  // Current amount of bytes we have enqueued in the output evbuffer so far.
  size_t record_bytes_queued_;

  friend class ReadingCallback;
};

}} // namespace facebook::logdevice
