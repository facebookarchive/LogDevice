/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

/**
 * NodeSetFinder encapsulates the logic to retrieve historical epoch metadata
 * for a given (non-metadata) log. The historical metadata is returned as an
 * EpochMetaDataMap. NodeSetFinder supports getting historical metadata from
 * different sources as follows:
 *    - Sequencer: NodeSetFinder sends GET_SEQ_STATE message to get historical
 *      metadata from the sequencer using SyncSequencerRequest
 *    - Metadata log: NodeSetFinder reads the metadata log and keeps track of
 *      metadata received for each epoch.
 *    - Both: NodeSetFinder first attempts to get historical metadata from
 *      sequencer, if the first attempt failed, it retries reading from the
 *      metadata log.
 */
class NodeSetFinder {
 public:
  // decide where to retrieve the historical metadata
  enum class Source {
    // read from metadata log only
    METADATA_LOG,
    // read from sequencer only
    SEQUENCER,
    // first attempt to read from sequencer, then read from metadata log if the
    // attempt is not successful (failure or timed out)
    BOTH
  };

  // execution state of the NodeSetFinder
  enum class State {
    NOT_STARTED,
    // getting historical metadata through sequencer
    READ_FROM_SEQUENCER,
    // reading historical metadata from metadata logs
    READ_METADATALOG,
    // execution finalized and callback is called or being called
    FINISHED
  };

  // extra information about where metadata is fetched. only applies
  // to metadata gotten from metadata logs
  struct MetaDataExtras {
    // lsn of the metadata log record
    lsn_t lsn;
    // timestamp of the metadata log record
    std::chrono::milliseconds timestamp;
  };

  using MetaDataExtrasMap = std::map<epoch_t, MetaDataExtras>;

  /**
   * Constructs a NodeSetFinder object.
   *
   * Once historical metadata has been found or there was an error,
   * NodeSetFinder executes the callback passed by the caller with the status as
   * argument. The status can have the following values:
   *    OK - the process completed successfully and the caller may access the
   *         gathered data through the accessors.
   *    TIMEDOUT - a timeout occurred while fetching the result
   *    INVALID_PARAM - the log_id is not in the configuration
   *    ACCESS - permission denied
   *    FAILED - failed to read the metadata log
   *    NOTFOUND - see checkMetadataLogEmptyMode
   *    NOTEMPTY - see checkMetadataLogEmptyMode
   *
   * @param source        indicate the source to fetch historical metadata
   * @param timeout       time out duration for the entire operation. For
   *                      source == Source::BOTH, 1/3 of the total time out
   *                      is allocated to reading from sequencer and the rest
   *                      2/3 is used for reading the metadata log.
   */
  NodeSetFinder(logid_t log_id,
                std::chrono::milliseconds timeout,
                std::function<void(Status status)> callback,
                Source source = Source::METADATA_LOG);

  virtual ~NodeSetFinder();

  /**
   * Start the execution of NodeSetFinder.
   */
  virtual void start();

  /**
   * Enter mode where NodeSetFinder will only check if metadata log is empty.
   *
   * Might not finish reading data, so if any data was found, this is indicated
   * by returning NOTEMPTY rather than OK. Empty metadata log is indicated by
   * NOTFOUND; other status codes used per usual.
   */
  void checkMetadataLogEmptyMode() {
    ld_assert(state_ == State::NOT_STARTED);
    metadata_log_empty_mode_ = true;
  }

  /**
   * Get the final result of all historical epoch metadata read. Note that
   * it returns not nullptr iff callback is called with E::OK.
   */
  std::shared_ptr<const EpochMetaDataMap> getResult() const {
    return result_;
  }

  /**
   * Get the MetaDataExtras info of all historical epoch metadata read. Note
   * that it returns an empty map if metadata is not fetched from metadata logs.
   * Undefined behavior if callback is not called with E::OK.
   */
  MetaDataExtrasMap getMetaDataExtras() const {
    return metadata_extras_;
  }

  /**
   * Convenient function that computes the union of the storage sets from the
   * result gotten. Equivalent to
   * result_->getUnionStorageSet(cfg, EPOCH_MIN, result_->getEffectiveUntil()).
   *
   * Note: The caller should only use this method if the status returned via the
   * callback is E::OK. Otherwise the behavior is undefined.
   */
  StorageSet getUnionStorageSet(
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  /**
   * Computes the "minimum" replication property of the result gotten.
   * Use this when you need to access the union of storage set. Equivalent to
   * result_->getNarrowestReplication(EPOCH_MIN, result_->getEffectiveUntil())
   *
   * Note: like getUnionStorageSet(), the caller should only use this method if
   * the status returned via the callback is E::OK. Otherwise the behavior is
   * undefined.
   */
  virtual ReplicationProperty getNarrowestReplication();

  /**
   * Accessor to the map of all storage sets. It is indexed by the
   * effective_since epoch (see EpochMetaDataMap.h).
   * Note: like getUnionStorageSet(), the caller should only use this method if
   * the status returned via the callback is E::OK. Otherwise the behavior is
   * undefined.
   *
   * @return map of epoch => metadata, where epoch is the effective since
   *         epoch of the metadata
   */
  const EpochMetaDataMap::Map& getAllEpochsMetaData() const;

  /**
   * Called when a metadata is received.
   * Public for tests.
   */
  void onMetaDataLogRecord(Status st, MetaDataLogReader::Result result);

  /**
   * Called when historical metadata result is fetched from the sequencer.
   * Public for tests.
   */
  void
  onMetaDataFromSequencer(Status status,
                          NodeID seq_node,
                          std::shared_ptr<const EpochMetaDataMap> metadata);

  /**
   * Called when reading the metadata failed with E::NOTFOUND.
   * This can happen if the log has not been provisioned yet.
   * Schedules a timer to retry reading the metadata log after some time.
   */
  void onMetadataLogNotFound();

  /**
   * Expose the state of the NodeSetFinder.
   */
  State getState() const {
    return state_;
  }

  /**
   * (Only Used for testing).
   * Set the result of the NodeSetFinder and finalize with given status.
   */
  void setResultAndFinalize(Status status,
                            std::shared_ptr<const EpochMetaDataMap> result);

  /**
   * (Only Used for testing).
   * Expose Timers for testing.
   */
  Timer* getMetaDataLogReadTimer() const {
    return metadata_log_read_job_timer_.get();
  }

  BackoffTimer* getMetaDataLogRetryTimer() const {
    return metadata_log_retry_timer_.get();
  }

 protected:
  virtual std::unique_ptr<Timer> createJobTimer(std::function<void()> callback);

  virtual std::unique_ptr<BackoffTimer>
  createMetaDataLogRetryTimer(std::function<void()> callback);

  virtual void readFromSequencer(std::chrono::milliseconds timeout);
  virtual void readFromMetaDataLog();
  virtual void stopReadingMetaDataLog();

 private:
  const logid_t log_id_;
  std::function<void(Status status)> callback_;
  const std::chrono::milliseconds timeout_;
  const Source source_;
  bool metadata_log_empty_mode_{false};

  State state_;

  // stores the end result of metadata reading,
  // only populated when callback is called with E::OK
  std::shared_ptr<const EpochMetaDataMap> result_;

  // stores intermediate metadata received by reading metadata logs
  EpochMetaDataMap::Map metadata_recv_;

  // stores extras information about metadata log records
  MetaDataExtrasMap metadata_extras_;

  // timers used for reading metadata logs
  std::unique_ptr<Timer> metadata_log_read_job_timer_;
  std::unique_ptr<BackoffTimer> metadata_log_retry_timer_;
  std::unique_ptr<MetaDataLogReader> meta_reader_;

  WorkerCallbackHelper<NodeSetFinder> callback_helper_;

  // called when the stage of reading metadata log timed out
  void onMetaDataLogReadTimeout();

  // called when the last metadata log record is delivered
  void onMetaDataLogFullyRead();

  // complete execution and call the callback
  void finalize(Status status);

  // get the time out for the execution stage determined by the given
  // source and target state
  static std::chrono::milliseconds
  getStageTimeout(Source source,
                  State target_state,
                  std::chrono::milliseconds total_timeout);

  // settings that control the backoff behavior of metadata log reading retries
  static constexpr std::chrono::milliseconds
      RETRY_READ_METADATALOG_INITIAL_DELAY = std::chrono::milliseconds(100);

  static constexpr std::chrono::milliseconds RETRY_READ_METADATALOG_MAX_DELAY =
      std::chrono::milliseconds(10000);
};

}} // namespace facebook::logdevice
