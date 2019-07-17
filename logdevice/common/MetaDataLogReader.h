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
#include <set>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file A utility class used to read epoch metadata information from a metadata
 * log for the corresponding data log. It supports reading epoch metadata for
 * a single _epoch_, or a range of epochs [start_epoch, end_epoch] for the
 * data log. Epoch metadata is delivered using the MetaDataCallback initially
 * provided by the user. Noted that instead of calling the callback for every
 * epoch requested, MetaDataCallback delivers epoch metadata with an epoch
 * interval [epoch_requested, epoch_until] informing the user the epoch until
 * which the fetched metadata is effective. An example:
 *
 * The metadata log contains three records with epochs of metadata: {10, 22, 30}
 * MetaDataLogReader is started to fetch epoch metadata for epochs in range
 * [1, 35]. It should deliver three epoch metadata:
 * (epoch metadata: 10, requested epoch: 1, effective until: 21)
 * (epoch metadata: 22, requested epoch: 22, effective until: 29)
 * (epoch metadata: 30, requested epoch: 30, effective until: 35)
 *
 * Noted that for requested epochs smaller than the epoch of first available
 * metadata, it delivers the first available epoch metadata to the requester.
 *
 * MetaDataLogReader always reads all records in the metadata log to look for
 * the correct epoch metadata. However, it only guarantees to read all records
 * that were released in the metadata log BEFORE it started reading the log.
 * Therefore, users of this class must ensure that by the time the reader
 * starts, the records already released in the metadata log are sufficient for
 * determine the epoch metadata for all requested epochs (an exception is
 * the WAIT_UNTIL_RELEASED mode, see comments in Mode definition for details).
 */

class ClientReadStream;
class StatsHolder;

/**
 * A Note on reading the metadata log, and why it is safe to use
 * ignoreReleasedStatus() to read the metadata log
 *
 * Invariants for logdevice:
 * I1: A record is released only after it is fully stored.
 * I2: A record can only be read (without using ignoreReleasedStatus()) once it
 *     is released by the sequencer and at lease one storage node.
 * I3: A record, once it is fully stored, will not be deleted during recovery.
 * I4: (nodeset) A data log sequencer won't finish recovery or advance
 *     last released lsn to its current epoch _e_ until it can read the same
 *     metadata it used to activate epoch _e_ from the metadata log.
 * I5: (nodeset) write to metadata log is serialized so that an append request
 *     can be accepted only after the previous append is fully stored. Each
 *     append, once started, is guaranteed to have distinct (and monotonically
 *     increasing) epochs (`epoch' here refers to the epoch of the metadata
 *     record).
 *
 * Invariants for the metadata utility that writes to metdatalog:
 * U1: The metadata utility only appends to metadata log with the same epoch
 *     data it used to update the epoch store.
 * U2: The metadata utility keeps retrying to write a metadata record until
 *     it gets a success status and it can read the same record from the metdata
 *     log.
 * U3: The metadata utility won't attempt to perform new update on epoch
 *     metadata for the log until the previous metadata can be read.
 *
 * Property: Reading metadata log with ignoreReleasedStatus() always
 * yields the correct epoch metadata IF the reader starts reading after at least
 * one lsn is released for epoch _e_.
 *
 * Reasoning: If at least one lsn is released in _e_, according to I4 and I2, we
 * know that at least one record containing epoch metadata for _e_ must be
 * released in the metadata log. According to I1 and I3, the record is
 * guaranteed to persist in the log (except for losing r nodes at the same time)
 * from the time _t_ it is stored, and must be read by all readers starting
 * reading after time _t_.
 *
 * On the other hand, since ignoreReleasedStatus() is used, a reader may also
 * get metadata records that are not fully stored. This includes (1) records
 * not yet deleted by recovery, and (2) records not yet deleted by purging.
 * This is still correct for the following:
 * According to I5, records read must follow the same order that the utilities
 * had attempted to append, and with U1-U3, metadata inside these records must
 * be (1) valid metadata that used to activate the data sequencer, and (2)
 * epochs of metadata must be the same for the same metadata and must be in
 * order for both previous and next metadata in the metadata log.
 *
 * In sum, when the reader starts reading after a lsn is released for _e_,
 * it is ensured to get the record containing epoch metdata for _e_. It may
 * get records with same payloads, but they can never affect determining
 * the epoch metadata for _e_.
 */

class MetaDataLogReader {
 public:
  enum class RecordSource : uint8_t {
    // not the last record in the metdata log
    NOT_LAST = 0,
    // last record in the metadata log at the time of reading
    LAST,
    // record is fetched from a cache and is guaranteed to be consistent
    CACHED_CONSISTENT,
    // record is fetched from a cache and might be inconsistent
    CACHED_SOFT,
  };

  // check if the record source is one of the source from the cache
  static bool isCachedSource(RecordSource source) {
    return source == RecordSource::CACHED_CONSISTENT ||
        source == RecordSource::CACHED_SOFT;
  }

  // contains information about epoch metadata delivered by Callback
  struct Result {
    // data log id whose epoch metadata is fetched
    logid_t log_id;
    // epoch of the data log whose metadata is requested
    epoch_t epoch_req;
    // epoch which the metadata is effective until (inclusive)
    epoch_t epoch_until;
    // from where the record is fectched
    RecordSource source;
    // lsn of the metadata log record, used for metadata log trimming
    lsn_t lsn = LSN_INVALID;
    // timestamp of the metadata log record, used for debugging
    std::chrono::milliseconds timestamp{0};
    // epoch metadata delivered, may be nullptr if there is an error
    // fetching the metadata
    std::unique_ptr<EpochMetaData> metadata;
  };

  /**
   * Callback function used to deliver epoch metadata to the requesting user of
   * the class
   *
   * @param status        status of the request. Could be:
   *                         E::OK         metadata is successfully fetched
   *                         E::NOTFOUND   metadata does not exist
   *                         E::BADMSG     malformed record
   *                         E::ACCESS     permission denied
   *                         E::DATALOSS   metadata loss
   * @param result        Result object containing the result of the request
   */
  using Callback = std::function<void(Status, Result)>;

  /**
   * By default, MetaDataLogReader uses ignoreReleasedStatus() to read metadata
   * logs as described above. However, for some use cases (e.g., log recovery
   * and epoch store utility), it needs to wait until certain epoch metadata
   * record (with specific metadata epoch) is released and can be read from the
   * metadata log. Therefore, MetaDataLogReader supports a second mode:
   * WAIT_UNTIL_RELEASED, in which it reads the metadata log in an ordinary way
   * without using ignoreReleasedStatus(), and unlike the IGNORE_LAST_RELEASED
   * mode, the reading won't stop until it finds a metadata record with epoch
   * equal or larger than the requested epoch.
   */
  enum class Mode { IGNORE_LAST_RELEASED, WAIT_UNTIL_RELEASED };

  /**
   * Creating a MetaDataLogReader object for fetching epoch metadata for epochs
   * within [epoch_start, epoch_end] for data log specified by log_id. Epoch
   * metadata will be delivered using the callback function _cb_ provided.
   *
   * For implementation, MetaDataLogReader creates a ClientReadStream that reads
   * the metadata log. It does not own the ClientReadStream object, but it is
   * guaranteed to outlive the object. However, users of this class should be
   * aware that MetaDataLogReader objects cannot be destroyed inside the
   * metadata callback. To properly dispose the object, users should use
   * Worker::disposeOfMetaReader() to transfer the object to the worker to
   * destroy it later.
   */
  MetaDataLogReader(logid_t log_id,
                    epoch_t epoch_start,
                    epoch_t epoch_end,
                    Callback cb)
      : log_id_(log_id),
        epoch_start_(epoch_start),
        epoch_end_(epoch_end),
        epoch_to_deliver_(epoch_start_),
        mode_(Mode::IGNORE_LAST_RELEASED),
        rsid_(READ_STREAM_ID_INVALID),
        metadata_cb_(std::move(cb)) {
    ld_check(log_id_ != LOGID_INVALID);
    // must fetch epoch metadata for a data log
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
    ld_check(epoch_start <= epoch_end);
    ld_check(epoch_end <= EPOCH_MAX);
    STAT_INCR(getStats(), metadata_log_readers_created);
  }

  // overloaded constructor for requesting epoch metadata for a single epoch
  // Also allows Mode::WAIT_UNTIL_RELEASED to be set for reading
  MetaDataLogReader(logid_t log_id,
                    epoch_t epoch,
                    Callback cb,
                    Mode mode = Mode::IGNORE_LAST_RELEASED)
      : MetaDataLogReader(log_id, epoch, epoch, std::move(cb)) {
    // explicitly set mode_ in constructor body since it is not allowed to
    // have extra mem-initializers with delegating constructor
    mode_ = mode;
  }

  virtual ~MetaDataLogReader();

  void start();

  // conclude MetaDataLogReader. It will stop processing records/gaps, and
  // metadata callback will no longer be called
  virtual void finalize();

  // Don't print a warning and don't bump stat if result is NOTFOUND.
  void dontWarnIfNotFound() {
    warn_if_notfound_ = false;
  }

  // callback functions for the internal ClientReadStream
  void onDataRecord(std::unique_ptr<DataRecord>);
  void onGapRecord(const GapRecord&);

  // get the set of LSNs of metadata log records that make an epoch
  // interval considered as bad or invalid. used for toolings to repair/fix
  // the metadata log
  std::set<lsn_t> getOffendingLSNs() const {
    return offending_lsns_;
  }

  // return the start epoch requested
  epoch_t getStartEpoch() const {
    return epoch_start_;
  }

  // override in tests
 protected:
  virtual void startReading();
  virtual void stopReading();
  virtual StatsHolder* getStats();
  virtual std::shared_ptr<Configuration> getClusterConfig() const;
  virtual std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const;

 private:
  // data log id whose epoch metadata are to be fetched
  const logid_t log_id_;
  const epoch_t epoch_start_;
  epoch_t epoch_end_;

  // next metadata epoch to be delivered
  epoch_t epoch_to_deliver_;

  // see comment in enum class Mode
  Mode mode_{Mode::IGNORE_LAST_RELEASED};

  // if true, indicates that the current epoch interval contains malformed
  // records or dataloss. We can never be certain about metadata within the
  // current interval.
  bool bad_epoch_{false};

  // error reason of the current bad epoch interval
  Status error_reason_{E::NOTFOUND};

  // collect all LSNs that make an epoch to be considered as `bad'
  std::set<lsn_t> offending_lsns_;

  // last epoch metadata read from the metadata log but not yet delivered
  // to the requester through the callback
  std::unique_ptr<EpochMetaData> metadata_;
  lsn_t record_lsn_{LSN_INVALID};
  std::chrono::milliseconds record_timestamp_{0};

  // readstream id of the internal meta readstream
  read_stream_id_t rsid_;

  // callback function that delivers epoch metadata information
  Callback metadata_cb_;

  // Print a warning and bump a stat if result is NOTFOUND.
  bool warn_if_notfound_ = true;

  // if true, the reader is actively running. Otherwise, it is
  // either not started or concluded
  bool started_{false};

  // indicate that the current context is within the user provided metadata
  // callback (also within callback of the internal client readstream). It is
  // unsafe to destroy either this MetaDataLogReader object or the associated
  // client read stream. Used for assertion only.
  bool inside_callback_{false};

  // the first metadata epoch in the metadata log. used in assert only.
  epoch_t first_metadata_epoch_{EPOCH_INVALID};

  // starting point of the current metadata epoch interval
  epoch_t lastReadEpoch() const {
    return metadata_ == nullptr ? EPOCH_INVALID : metadata_->h.epoch;
  }

  /**
   * Conclude the epoch interval with the current metadata
   *
   * called when the reader reads a new epoch metadata with epoch _e_ and
   * confirms that the current epoch metadata is effective until _e_ - 1.
   * The function is also called when the reader receives the special gap
   * upto LSN_MAX -1.
   */
  void concludeEpochInterval(epoch_t interval_until);

  // deliver current epoch metadata to the requester with an effective until
  // epoch
  void deliverMetaData(Status st, epoch_t until, bool last_record);
};

}} // namespace facebook::logdevice
