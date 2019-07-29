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
#include <queue>
#include <set>

#include <boost/noncopyable.hpp>
#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamSenderState.h"
#include "logdevice/common/client_read_stream/RewindScheduler.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/protocol/WINDOW_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class BackoffTimer;
class ClientGapTracer;
class ClientReadStreamBuffer;
class ClientReadStreamConnectionHealth;
class ClientReadStreamScd;
class ClientReadStreamTracer;
class ClientReadTracer;
class ClientReadersFlowTracer;
class ClientStalledReadTracer;
class EpochMetaDataCache;
class ShardAuthoritativeStatusMap;
class STARTED_Message;
class UpdateableConfig;
enum class AuthoritativeStatus : uint8_t;
enum class ClientReadStreamBufferType : uint8_t;

/**
 * @file State and logic for a read stream for a single log from a client's
 *       perspective.  Manages incoming streams of records from different
 *       storage shards. Buffers records to present a continuous stream to the
 *       application.
 *
 *       The class is not thread-safe; all calls are assumed to happen on a
 *       single thread.
 */

/**
 * ReaderImpl provides an instance of this type through which ClientReadStream
 * transfers records to it.  This is a pure virtual interface so that the
 * implementation can live in lib/.
 */
class ReaderBridge {
 public:
  // @return 0 on success.  -1 if Reader is unable to accept the record;
  // ClientReadStream should retry delivery later.
  virtual int onDataRecord(read_stream_id_t,
                           std::unique_ptr<DataRecordOwnsPayload>&&,
                           bool notify_when_consumed) = 0;
  // @return 0 on success.  -1 if Reader is unable to accept the gap;
  // ClientReadStream should retry delivery later.
  virtual int onGapRecord(read_stream_id_t,
                          GapRecord,
                          bool notify_when_consumed) = 0;
  virtual ~ReaderBridge() {}
};

/**
 * External dependencies of ClientReadStream are isolated into this class.  The
 * main purpose of this isolation is to allow easy testing of the intricate
 * logic in ClientReadStream.
 */
class ClientReadStreamDependencies {
 public:
  ClientReadStreamDependencies();

  using record_cb_t = std::function<bool(std::unique_ptr<DataRecord>&)>;
  using gap_cb_t = std::function<bool(const GapRecord&)>;
  using done_cb_t = std::function<void(logid_t)>;
  using record_copy_cb_t =
      std::function<void(ShardID, const DataRecordOwnsPayload*)>;
  using health_cb_t = std::function<void(bool)>;

  ClientReadStreamDependencies(read_stream_id_t rsid,
                               logid_t log_id,
                               std::string csid,
                               record_cb_t record_cb,
                               gap_cb_t gap_cb,
                               done_cb_t done_cb,
                               EpochMetaDataCache* metadata_cache,
                               health_cb_t health_cb,
                               record_copy_cb_t record_copy_cb = nullptr)
      :

        read_stream_id_(rsid),
        log_id_(log_id),
        client_session_id_(std::move(csid)),
        record_callback_(std::move(record_cb)),
        gap_callback_(std::move(gap_cb)),
        done_callback_(std::move(done_cb)),
        health_callback_(std::move(health_cb)),
        record_copy_callback_(std::move(record_copy_cb)),
        metadata_cache_(metadata_cache) {}

  /**
   * Request the epoch metadata for an epoch of the log. Once the metadata is
   * fetched, it will be delivered through the provided MetaDataCallback.
   *
   * If log_id_ is a metadata log id (i.e., the read stream is used for reading
   * a metadata log), the epoch metadata is generated from the current config
   * with effective until set to EPOCH_MAX. The function calls the callback and
   * returns in a synchronous manner.
   *
   * If log_id_ is a data log, the epoch metadata is read from the
   * corresponding metadata log using a MetaDataLogReader object. In such
   * case, the callback will be passed to the MetaDataLogReader object and will
   * be called asynchronously.
   *
   * The function only allows one active running MetaDataLogReader object at a
   * time. So if the caller calls this function to request metadata while a
   * previous request is still inflight, the previous request will be cancelled
   * and overridden by the new question so that its callback won't get called.
   *
   * @param rsid               The ID of the calling ClientReadStream instance.
   * @param epoch              epoch for which the metadata is requested
   * @param cb                 MetaDataReader::Callback object used to deliver
   *                           epoch metadata fetched
   *
   * @param allow_from_cache   allow using the EpochMetaDataCache to get the
   *                           cached EpochMetaData, if possible
   *
   * @param require_consistent_from_cache    only allow consistent metadata from
   *                                         EpochMetaDataCache, otherwise
   *                                         consider it a cache miss
   *
   * @return  true if the operation completed (and called `cb`) synchronously;
   *          false if the operation will complete asynchronously
   */
  virtual bool getMetaDataForEpoch(read_stream_id_t rsid,
                                   epoch_t epoch,
                                   MetaDataLogReader::Callback cb,
                                   bool allow_from_cache,
                                   bool require_consistent_from_cache);

  /**
   * Sends a message to the storage shard to start sending records.
   *
   * @param shard          Shard to read from.
   * @param onclose        callback if connection closes
   * @param header         message header; log ID and read stream ID will be
   *                       filled in
   * @param filtered_out   list of filtered out shards to be sent to storage
   *                       shards. These may be either shards that are known to
   *                       be down or shards that are very slow (see @file
   *                       ClientReadStreamScd for more detauls)
   *                       Must be empty if the SINGLE_COPY_DELIVERY
   *                       flag is not set in the header's flags.
   * @param attrs          Structure containing parameters that alter
   *                       the behavior of the read stream. In
   *                       particular it is used to pass filters
   *                       for the server-side filtering experimental feature.
   */
  virtual int sendStartMessage(ShardID shard,
                               SocketCallback* onclose,
                               START_Header header,
                               const small_shardset_t& filtered_out,
                               const ReadStreamAttributes* attrs = nullptr);

  /**
   * Sends a message to the storage shard to stop sending records.
   */
  virtual int sendStopMessage(ShardID shard);

  /**
   * Sends a message to the storage shards to update its sending window.
   *
   * @param shard       target Shard ID.
   * @param window_low  the smallest LSN in current sliding window.
   * @param window_high the largest LSN in current sliding window.
   */
  virtual int sendWindowMessage(ShardID shard,
                                lsn_t window_low,
                                lsn_t window_high);

  /**
   * Call the application-supplied callback to deliver a record.
   */
  virtual bool recordCallback(std::unique_ptr<DataRecord>& record) {
    return record_callback_ ? record_callback_(record) : true;
  }

  /**
   * Call the application-supplied callback to report a gap.
   */
  virtual bool gapCallback(const GapRecord& gap) {
    return gap_callback_ ? // may not be specified by the application
        gap_callback_(gap)
                         : true;
  }

  /**
   * Call the application-supplied callback to report we are done with reading.
   */
  virtual void doneCallback(logid_t logid) {
    if (done_callback_) { // may not be specified by the application
      done_callback_(logid);
    }
  }

  /**
   * Call the application-supplied callback to report client read stream health
   * change.
   */
  virtual void healthCallback(bool is_healthy) {
    if (health_callback_) {
      health_callback_(is_healthy);
    }
  }

  /**
   * Called when we receive a valid copy of a record from a storage shard.
   * Note that unlike recordCallback() and gapCallback() this is not called
   * sequentially, it's called as soon as we receive a record from a storage
   * shard (in onDataRecord()), so these callbacks can come out of order
   * and have duplicate LSNs.
   */
  virtual void recordCopyCallback(ShardID from,
                                  const DataRecordOwnsPayload* record) {
    if (record_copy_callback_) {
      record_copy_callback_(from, record);
    }
  }

  /**
   * Disposes of the ClientReadStream by calling
   * AllClientReadStreams::erase().  ClientReadStream itself calls this when
   * it has reached the end it has no more reason to live.  In tests, this is
   * stubbed since there may not even be a AllClientReadStreams instance.
   */
  virtual void dispose();

  // Utility methods to create a BackoffTimer
  virtual std::unique_ptr<BackoffTimer>
  createBackoffTimer(std::chrono::milliseconds initial_delay,
                     std::chrono::milliseconds max_delay);
  virtual std::unique_ptr<BackoffTimer> createBackoffTimer(
      const chrono_expbackoff_t<std::chrono::milliseconds>& settings);

  void updateBackoffTimerSettings(
      std::unique_ptr<BackoffTimer>& timer,
      const chrono_expbackoff_t<std::chrono::milliseconds>& settings);

  void updateBackoffTimerSettings(std::unique_ptr<BackoffTimer>& timer,
                                  std::chrono::milliseconds initial_delay,
                                  std::chrono::milliseconds max_delay);

  // Utility method to create a Timer.
  virtual std::unique_ptr<Timer>
  createTimer(std::function<void()> cb = nullptr);

  /**
   * Returns the callback that resolves a ClientReadStream id to an instance
   */
  virtual std::function<ClientReadStream*(read_stream_id_t)>
  getStreamByIDCallback();

  virtual const Settings& getSettings() const;

  std::chrono::milliseconds computeGapGracePeriod() const;

  virtual ShardAuthoritativeStatusMap getShardStatus() const;

  /**
   * Call refreshClusterStateAsync() on this Worker's ClusterState.
   */
  virtual void refreshClusterState();

  /**
   * update the epoch metadata cache with the epoch metadata read from the
   * metadata log
   */
  virtual void updateEpochMetaDataCache(epoch_t epoch,
                                        epoch_t until,
                                        const EpochMetaData& metadata,
                                        MetaDataLogReader::RecordSource source);

  read_stream_id_t getReadStreamID() const {
    return read_stream_id_;
  }

  const std::string& getClientSessionID() const {
    return client_session_id_;
  }

  // Gets the protocol version of an outgoing socket (if the socket exists).
  virtual folly::Optional<uint16_t>
      getSocketProtocolVersion(node_index_t) const;

  virtual ClientID getOurNameAtPeer(node_index_t) const;

  virtual ~ClientReadStreamDependencies();

  virtual bool hasMemoryPressure() const;

 private:
  read_stream_id_t read_stream_id_;
  logid_t log_id_;
  std::string client_session_id_;
  record_cb_t record_callback_;
  gap_cb_t gap_callback_;
  done_cb_t done_callback_;
  // If our owner requested health updates, this is the callback.
  health_cb_t health_callback_;

  record_copy_cb_t record_copy_callback_;

  // pointer to the cache in logdevice client which stores the metadata log
  // results. the cache must outlive the read stream
  EpochMetaDataCache* const metadata_cache_{nullptr};

  // store metadata fetched from the cache
  std::unique_ptr<EpochMetaData> metadata_cached_;

  // When metadata is fetched from cache, we start a zero-timeout timer
  // to deliver the metadata on the next iteration of the event loop.
  // metadata_cached_ owns the fetched metadata that will be delivered on
  // the timer.
  std::unique_ptr<Timer> delivery_timer_;

  // currently running NodeSetFinder
  std::unique_ptr<NodeSetFinder> nodeset_finder_;
};

/**
 * Descriptor for a record/gap marker stored in the ClientReadStreamBuffer
 * (see ClientReadStream::buffer_ below). Linked to a particular LSN.
 */
struct ClientReadStreamRecordState {
  /**
   * Pointer to the record received from storage shard.
   */
  std::unique_ptr<DataRecordOwnsPayload> record{};

  /**
   * List of storage shards for which one of the records the shard delivered
   * has this LSN, or which sent a gap message for this LSN.
   *
   * Each ClientReadStreamSenderState is contained in at most one of these
   * lists. This list is used to maintain |S_G| (see docblock for
   * updateGapStat() in ClientReadStream.cpp) in O(1).
   */
  folly::IntrusiveList<ClientReadStreamSenderState,
                       &ClientReadStreamSenderState::list_hook_>
      list{};

  /**
   * True if this LSN is equal to end_gap + 1 of a gap reported by some
   * storage shard. This property is used to distinguish between a case
   * where the record was actually received (record != nullptr) and the
   * case where only a GAP message was received (e.g. a trim gap or an
   * epoch bump).
   */
  bool gap{false};

  /**
   * True if this record is filtered out by server and we received a Gap
   * message with GapType::FILTERED_OUT for this lsn. [Experimental feature]
   */
  bool filtered_out{false};

  void reset();
};

class ClientReadStream : boost::noncopyable {
 private:
  using SenderState = ClientReadStreamSenderState;
  using RecordState = ClientReadStreamRecordState;
  using GapState = SenderState::GapState;

 public:
  using GapFailureDomain =
      FailureDomainNodeSet<SenderState::GapState,
                           HashEnum<SenderState::GapState>>;

  ClientReadStream(read_stream_id_t id,
                   logid_t log_id,
                   lsn_t start_lsn,
                   lsn_t until_lsn,
                   double flow_control_threshold,
                   ClientReadStreamBufferType buffer_type,
                   size_t buffer_capacity,
                   std::unique_ptr<ClientReadStreamDependencies>&& deps,
                   std::shared_ptr<UpdateableConfig> config,
                   ReaderBridge* reader = nullptr,
                   const ReadStreamAttributes* attrs = nullptr);

  ~ClientReadStream();

  std::string getDebugInfoStr() const;

  void getDebugInfo(InfoClientReadStreamsTable& table) const;

  /**
   * Contacts storage shards to start delivering.
   */
  void start();

  // Continuation of the start() method after logs config data is received.
  void startContinuation(std::shared_ptr<LogsConfig::LogGroupNode> log_config);

  /*
   * Attempt to go to epoch >= 1 without actually retrieving metadata for
   * EPOCH_INVALID.
   */
  void
  ensureSkipEpoch0(const std::shared_ptr<LogsConfig::LogGroupNode> log_config);

  /**
   * Sends START message to one sender, updating the state as necessary.
   */
  void sendStart(ShardID shard, SenderState& state);

  /**
   * Called when the outcome of sending a START message is known.  If the
   * message was successfully sent, we infer that there is an established
   * connection.  Otherwise, start a timer to try again later.
   */
  void onStartSent(ShardID shard, Status status);

  /**
   * Called by a worker thread when a RECORD message is received from a
   * storage shard.
   */
  void onDataRecord(ShardID shard,
                    std::unique_ptr<DataRecordOwnsPayload> record);

  /**
   * Called by a worker thread when a STARTED message is received from a
   * storage shard. Updates rebuilding_nodes_ if needed.
   */
  void onStarted(ShardID from, const STARTED_Message& msg);

  /**
   * Called by a worker thread when a GAP message is received from a server.
   */
  void onGap(ShardID shard, const GAP_Message& msg);

  /**
   * Helper method, sends a WINDOW message to a single storage shard.  This is
   * called as part of a window update (when the update is broadcast to all
   * shards) and when retrying a previously failed send.
   */
  void sendWindowMessage(SenderState&);

  /**
   * Add these flags to START messages sent out. See code below for possible
   * values. See START_Message for descriptions.
   */
  void addStartFlags(START_flags_t flags) {
    const START_flags_t mask = START_Header::INCLUDE_EXTRA_METADATA |
        START_Header::DIRECT | START_Header::NO_PAYLOAD |
        START_Header::CSI_DATA_ONLY | START_Header::PAYLOAD_HASH_ONLY |
        START_Header::INCLUDE_BYTE_OFFSET;
    ld_check((flags & mask) == flags);
    additional_start_flags_ |= flags;
  }

  void setNoPayload() {
    addStartFlags(START_Header::NO_PAYLOAD);
  }

  /**
   * Require all shards in the read set to chime in before reporting a gap.
   * Shards in REBUILDING or EMPTY state count as chiming in.
   *
   * By default, this class waits for an f-majority of shards.  (Because all
   * records are supposed to be replicated `r` times, if fewer than `r` shards
   * have not responded, we can make progress without waiting for them.)
   *
   * When this option is used, a gap will not be reported until all shards that
   * can chime in do chime in, ie the only accepted f-majority results are
   * AUTHORITATIVE_COMPLETE and NON_AUTHORITATIVE, the read stream will not make
   * progress with AUTHORITATIVE_INCOMPLETE.
   *
   * This is used by tools such as the checker utility which needs to know what
   * records each of the shards in the storage set have.
   */
  void requireFullReadSet() {
    require_full_read_set_ = true;
  }

  /**
   * See START_Header::IGNORE_RELEASED_STATUS.
   */
  void ignoreReleasedStatus() {
    ignore_released_status_ = true;
  }

  /*
   * Do not use the Single Copy Delivrey optimization even if it is available on
   * that log.
   * This must not be called after start().
   */
  void forceNoSingleCopyDelivery() {
    ld_check(!started_);
    force_no_scd_ = true;
  }

  /**
   * When reading a partially trimmed section of the log, instead of eagerly
   * reporting partially trimmed gaps, deliver whatever records are still
   * available. This results in an interleaved stream of records and TRIM gaps.
   */
  void doNotSkipPartiallyTrimmedSections() {
    do_not_skip_partially_trimmed_sections_ = true;
  }

  /**
   * Ship records even if their contents do not match their checksum.
   * If the user is interested in receiving all copies that have a bad checksum,
   * waitForAllCopies() should be used alongside this option. Otherwise, if some
   * copies have a bad checksum and some copies have a good checksum, only
   * the copy that arrived first from a storage shard will be delivered.
   */
  void shipCorruptedRecords() {
    ship_corrupted_records_ = true;
  }

  /**
   * Request approximate amount of data written to log. Byte offset will be
   * send along with record once it is available on storage shards.
   */
  void includeByteOffset() {
    addStartFlags(START_Header::INCLUDE_BYTE_OFFSET);
  }

  /**
   * By default the read stream will attempt to read from and update the
   * epoch metadata cache when fetching epoch metadata if possbile. This
   * function turns off the optimization.
   */
  void disableEpochMetaDataCache() {
    use_epoch_metadata_cache_ = false;
  }

  /**
   * Hole plugs and bridge records are stored and transmitted as records, but
   * don't represent any real user-provided records. By default they're not
   * reported as records: hole plugs are reported as gaps, and bridge records
   * are not reported at all.
   * This method changes this behavior and makes ClientReadStream pass these
   * records to callbacks just like normal records. This is used by tools
   * like checker and meta-fixer.
   */
  void shipPseudorecords() {
    ship_pseudorecords_ = true;
  }

  /**
   * Instead of reporting record as soon as we can, treat them as if they were
   * gaps: only call recordCallback() when an f-majority of shards chimed in
   * (or all shards if requireFullReadSet() was called).
   * This is useful if the reader wants a list of all copies of the record
   * in the cluster. checker utility uses it to check data consistency.
   *
   * ClientReadStreamDependencies::recordCopyCallback() can be used to get
   * a list of copies of a record.
   *
   * Incompatible with SCD.
   */
  void waitForAllCopies() {
    wait_for_all_copies_ = true;
  }

  /**
   * Called when a client has consumed enough records from the queue for us to
   * ask storage shards for more data (slide the window).
   */
  void onReaderProgress();

  /**
   * Called when configuration has changed. If there is a valid read set,
   * update the storage shards in read set using the current epoch metadata
   * and the new cluster configuration (see updateCurrentReadSet());
   */
  void noteConfigurationChanged();

  /**
   * Called when settings are updated. Re-apply authoritative status and re-run
   * gap detection.
   */
  void onSettingsUpdated();

  /**
   * Called when we know of a change in authoritativeness of a shard either
   * because the shard sent a message STARTED(E::REBUILDING) or because we were
   * notified that Worker::shard_status_state::shard_status_map_ was updated
   * through a call to applyShardStatus().
   */
  void setShardAuthoritativeStatus(SenderState& state,
                                   AuthoritativeStatus status);

  /**
   * Looks at the current content of
   * Worker::shard_status_state::shard_status_map_ and updates shard
   * states accordingly.  Doesn't affect shards in READING state - rebuilding
   * status received from the shard itself takes precedence over information
   * from event log since the event log reader can fall behind.  Called when
   * Worker::shard_status_state::shard_status_map_ changes and when sending a
   * START message; the latter covers all cases when we're failing to contact a
   * shard and may want to proceed without it if it's rebuilding.
   *
   * @param context  Human-readable string identifying the reason for this call.
   * @param state  The shard to update. If folly::none, update all shards.
   * @param try_make_progress
   *    If true, applyShardStatus() will call findGapsAndRecords() and
   *    disposeIfDone(); this means *this can be destroyed.
   *    If false, you need to call findGapsAndRecords() and disposeIfDone()
   *    after applyShardStatus() before returning to event loop.
   */
  void applyShardStatus(const char* context,
                        folly::Optional<SenderState*> state = folly::none,
                        bool try_make_progress = true);

  /**
   * Called when the previous request of fetching epoch metadata has completed.
   * If success, epoch metadata is delivered through the function.
   */
  void onEpochMetaData(Status st, MetaDataLogReader::Result result);

  /**
   * Called when fetched metadata contains a storage set that is effectively
   * empty (i.e. none of the shards in the storage set are in the config).
   */
  void handleEmptyStorageSet(const MetaDataLogReader::Result& result,
                             epoch_t until);

  /**
   * Called when the socket for a storage shard was closed.
   */
  void onSocketClosed(SenderState& state, Status st);

  /**
   * Hook called by ClientReadStreamSenderState instances whenever the
   * isHealthy() for a shard may have changed.  Prompts ClientReadStream to
   * recalculate overall health.
   */
  void onConnectionHealthChange(ShardID shard, bool healthy);

  read_stream_id_t getID() const {
    return id_;
  }

  const std::shared_ptr<UpdateableConfig>& getConfig() const {
    return config_;
  }

  ClientReadStreamDependencies& getDeps() {
    return *deps_;
  }

  const EpochMetaData* getCurrentEpochMetadata() const {
    return current_metadata_.get();
  }

  lsn_t getNextLSNToDeliver() const {
    return next_lsn_to_deliver_;
  }

  /**
   * Returns our current lower bound of the trim point. It's the maximum high
   * LSN of TRIM gaps we got from storage shards so far. Not necessarily a good
   * estimate of trim point: e.g. it'll stay LSN_INVALID if we never encounter
   * a TRIM gap.
   */
  lsn_t getTrimPointLowerBound() const {
    return trim_point_;
  }

  /**
   * Exposes the data structure used for f-majority detection.
   * Used by replication checker to find which shards are known to be
   * missing data (AuthoritativeStatus::UNDERREPLICATION
   * or GapState::UNDER_REPLICATED).
   */
  const GapFailureDomain& getGapFailureDomain() const {
    return *gap_failure_domain_;
  }

  // Should be called in places where we expect the state to be consistent, ie
  // |S_G|, gap_failure_domain_ and scd_->gap_shards_filtered_out_ to
  // have been updated to reflect reality. Verifies that the values are correct.
  void checkConsistency() const;

  /**
   * Resume delivery immediately without waiting for redelivery_timer_ to
   * trigger redelivery. No-op if redelivery_timer_ is not active.
   */
  void resumeReading();

  /**
   * Attempts to deliver a NOTINCONFIG Gap record to the client. When
   * successful, this ReadStream is destroyed. If it is not, we will try to
   * issue that gap the next time the redelivery timer triggers.
   */
  void deliverNoConfigGapAndDispose();

  /**
   * @return a concise description of the list of state of all senders / shards.
   */
  std::string senderStatePretty() const;

  /**
   * @return a concise description of the list of shards that are known to be
   * unavailable either because their authoritative state is UNDERREPLICATION or
   * AUTHORITATIVE_EMPTY, or because we don't have a connection to them, or
   * because they are in persistent error state.
   */
  std::string unavailableShardsPretty() const;

  /**
   * @return Concise description of grace counters. Mostly used to detect
   * whether we have shards that appear to be healthy but failed to send records
   * and/or gaps for `grace_counter` number of grace periods.
   */
  std::string graceCountersPretty() const;

  /**
   * @return A string representation of healthy_node_set_ if
   * last_epoch_with_metadata_ is valid.
   */
  std::string getStorageSetHealthStatusPretty() const;

  size_t getBytesBuffered() const;

  bool isStreamStuckFor(std::chrono::milliseconds time);

  struct ClientReadStreamDebugInfo {
    explicit ClientReadStreamDebugInfo(const read_stream_id_t stream_id_)
        : stream_id(stream_id_) {}

    logid_t log_id;
    const read_stream_id_t stream_id;
    /* Next lsn to deliver */
    lsn_t next_lsn;
    lsn_t window_high;
    lsn_t until_lsn;
    /* Read set size */
    uint64_t set_size;
    /* Gap end outside window */
    lsn_t gap_end;
    lsn_t trim_point;
    std::string gap_shards_next_lsn;
    std::string unavailable_shards;
    /* Connection health */
    folly::Optional<std::string> health;
    /* Redelivery In-progress */
    bool redelivery;
    filter_version_t::raw_type filter_version;
    std::string shards_down;
    std::string shards_slow;
    folly::Optional<int64_t> bytes_lagged;
    folly::Optional<int64_t> timestamp_lagged;
    folly::Optional<std::chrono::milliseconds> last_lagging;
    folly::Optional<std::chrono::milliseconds> last_stuck;
    folly::Optional<std::string> last_report;
    folly::Optional<std::string> last_tail_info;
    folly::Optional<std::string> lag_record;
  };

  /**
   * @return ClientReadStreamDebugInfo with all the data for debug
   */
  ClientReadStreamDebugInfo getClientReadStreamDebugInfo() const;

  /**
   * sample the ClientReadStreamDebugInfo
   */
  void sampleDebugInfo(const ClientReadStreamDebugInfo&) const;

 private:
  /**
   * @return True if there are records we can ship right now to the application,
   * ie the front of `buffer_` contains a record.
   */
  bool canDeliverRecordsNow() const;

  /**
   * Delivers the record and pops it from buffer, updating state accordingly.
   *
   * @return 0 if the record was shipped, -1 if deliverGap() failed.
   */
  int handleRecord(RecordState* rstate,
                   bool grace_period_expired,
                   bool filter_out = false);

  /**
   * Called when the read stream encounters a bridge record written by log
   * recovery in its front slot. Deliver a bridge gap to the end of epoch
   * without involing the gap detection algorithm.
   * The parameters are only used for asserts.
   *
   * @return   0 if the bridge gap was shipped, and the read stream has advanced
   *           to the next epoch
   *           -1 if the read stream is unable to make further progress
   */
  int handleBridgeRecord(lsn_t bridge_record_lsn, RecordState* rstate);

  // @see checkFMajority().
  enum ProgressDecision {
    WAIT,                  // We did not hear from enough shards to be able to
                           // make a decision.
    WAIT_FOR_GRACE_PERIOD, // We heard from enough shards but advise waiting
                           // for a grace period to give more time for other
                           // shards to chime in in case there was
                           // underreplicated data.
    ISSUE_GAP,             // We heard from enough shards, we can make a
                           // decision to issue a gap.
    ADD_TO_KNOWN_DOWN_AND_REWIND // Blacklist shards that reported
                                 // underreplication and rewind.
  };

  /**
   * Look at the state of all storage shards and make a decision on whether or
   * not we should make progress.
   */
  ProgressDecision checkFMajority(bool grace_period_expired) const;

  /**
   * Checks if we can declare a gap that starts at next_lsn_to_deliver_
   * and handles the gap.
   *
   * @return 0 if some progress was made and some records or gaps could have
   *         become shippable. -1 if we should wait for some of:
   *          - gap/record message from more shards,
   *          - SCD failover,
   *          - grace period (if `grace_period_expired` = false),
   *          - epoch metadata,
   *          - redelivery timer.
   */
  int detectGap(bool grace_period_expired);

  /**
   * Attempts to deliver a single record.
   *
   * @return 0 on success, -1 on failure (downstream didn't accept)
   */
  int deliverRecord(std::unique_ptr<DataRecordOwnsPayload>& record);

  /**
   * Attempts to delivers parameter gap record.
   *
   * @return 0 on success, -1 on failure (downstream didn't accept)
   */
  int deliverGap(GapType type, lsn_t lo, lsn_t hi);

  /**
   * Attempts to deliver access Gap record to the client. When successful, this
   * ReadStream is destroyed. If it is not, all out going data records are
   * blocked and redelivery is attempted until successful.
   */
  void deliverAccessGapAndDispose();

  /**
   * Decides if a record can be accepted.  It is accepted if it can be
   * immediately delivered (lsn == next_lsn_to_deliver_), was already
   * delivered (lsn < next_lsn_to_deliver_) or it fits in the buffer.
   */
  bool canAcceptRecord(lsn_t lsn) const;

  /**
   * Flow control: asks any senders to slide their flow-control window.
   * next_lsn_to_deliver_ will also be fast-forwarded if needed.
   *
   * This will be called whenever the client delivers data to the application.
   *
   * Broadcast WINDOW message only when:
   * (1) next_lsn_to_slide_window_ is reached, and
   * (2) until_lsn_ is not reached.
   *
   * If delivering records via callbacks, WINDOW messages will immediately be
   * sent. Otherwise, a flag will be set and onReaderProgress() will notify
   * storage shards to update the window.
   *
   * @return true if the window was slid, false if nothing changed.
   */
  bool slideSenderWindows();

  bool canSkipPartiallyTrimmedSection() const;

  /**
   * Immediately try to deliver a gap up to `next_lsn-1` to the client and on
   * success rotate the buffer and move `next_lsn_to_deliver_` to `next_lsn`.
   * This function must be used when wishing to bypass the gap detection
   * mechanism.
   *
   * @param type     Type of the gap to be issued.
   * @param next_lsn New head of the buffer.
   *
   * @return 0 on success or -1 if the user cannot accept the gap for now. In
   * that case, it expected that redeliver() will be called when the user is
   * ready to accept more data.
   */
  int deliverFastForwardGap(GapType type, lsn_t next_lsn);

  /**
   * Compute the right-side of the window
   */
  void calcWindowHigh();

  /**
   * Calculates the LSN that triggers flow control and assigns the value to
   * next_lsn_to_slide_window_.
   */
  void calcNextLSNToSlideWindow();

  /**
   * Updates server_window_ to [next_lsn_to_deliver_, window_high_].
   */
  void updateServerWindow();

  /**
   * Evaluates current conditions and update the size of the next window if
   * needed. This does not change the current window.
   */
  void updateWindowSize();

  /**
   * Set shard's connection state. Used in tests only.
   */
  void
  overrideConnectionState(ShardID shard,
                          ClientReadStreamSenderState::ConnectionState state);

  /**
   * Simulate the reconnect timer of a shard calling its callback.
   * Used in tests only.
   */
  void reconnectTimerCallback(ShardID shard);

  /**
   * Check that the reconnect timer of a shard is active.
   * Used in tests only.
   */
  bool reconnectTimerIsActive(ShardID shard) const;

  /**
   * Simulate the started timer of a shard calling its callback.
   * Used in tests only.
   */
  void startedTimerCallback(ShardID shard);

  /**
   * Called when we are unable to read from a storage shards because either:
   * - sendStartMessage() returned -1;
   * - onStartSent() is called with status != E::OK;
   * - onStarted() is called with msg.header_.status == E::AGAIN (or an
   *   unexpected error);
   * - onSocketClosed() is called.
   *
   * Reset the appropriate timers and:
   * - if SCD is active: notify the ClientReadStreamScd module of the failure so
   *   it can perform SCD failover;
   * - if SCD is not active: activate the reconnect timer.
   */
  void onConnectionFailure(SenderState& state, Status st);

  /**
   * Change the GapState of a sender.
   *
   * @param sender Sender for which to change the gap state.
   * @param stateA New GapState for the sender.
   */
  void setSenderGapState(SenderState& sender, GapState state);

  /**
   * This method maintains |S_G|, which is the set of storage shards for which
   * the smallest record can deliver is strictly larger than
   * next_lsn_to_deliver. It's called when a GAP mesage is received or when a
   * record is received with lsn > next_lsn_to_deliver_.
   */
  void updateGapState(lsn_t next_lsn, SenderState& state);

  /**
   * Called whe we found a DATALOSS gap. Returns true if we should rewind
   * because when SCD is in use, there is a possible race condition that causes
   * an f-majority of storage shards to not send a record.
   */
  bool shouldRewindWhenDataLoss();

  /**
   * Add shards with GapState::UNDER_REPLICATED to scd known down list and
   * schedule rewind.
   */
  void promoteUnderreplicatedShardsToKnownDown();

  /**
   * Called when a gap [next_lsn_to_deliver_, gap_lsn) is detected. This method
   * calls an application-specified callback, adjusts buffer_ and slides
   * windows.
   *
   * @return 0 on success, -1 on failure (downstream didn't accept)
   */
  int handleGap(lsn_t gap_lsn);

  /**
   * As long as next_lsn_to_deliver_ either has a record or starts a gap,
   * ships records and handles gaps.
   *
   * Doesn't handle gaps if SCD is active because we never deliver gaps when
   * in SCD mode. We always go through failing over to all send all mode first
   * when we suspect that there is no data for some lsns.
   *
   * @param grace_period_expired  if true, reports a gap immediately as long as
   *                              an f-majority of shards don't have a record
   * @param redelivery_in_progress if true, indicates that the call is in the
   *                               context of a redelivery. This is used, for
   *                               instance, to prevent fast forwarding due to
   *                               TRIM gap.
   */
  void findGapsAndRecords(bool grace_period_expired = false,
                          bool redelivery_in_progress = false);

  /**
   * Called when read stream is likely at the beginning of the epoch or just
   * started reading. Performs a forward search on the read stream buffer for
   * a record with EPOCH_BEGIN flag or at ESN_MIN.
   *
   * @return         LSN_INVALID if no record with EPOCH_BEGIN or with ESN_MIN
   *                 has been detected; otherwise, return the lsn of the epoch
   *                 begin record
   */
  lsn_t checkEpochBegin() const;

  /**
   * Fast forward the read stream to the first record of an epoch, delivering
   * a bridge gap to the user. Like handling of bridge record, this is
   * performed regardless of the gap status of shards in the storage set.
   * In other words, gap detection algorithm is not used here.
   *
   * @return  0 if the bridge gap is delivered. -1 the read stream is unable
   *          to make progress.
   */
  int handleEpochBegin(lsn_t epoch_begin);

  /**
   * Moves gap information forward from the record state, updating gap
   * detection parameters as needed. After the call sender gap states reflect
   * the situation at this LSN.
   */
  void unlinkRecordState(lsn_t, RecordState&);

  /**
   * Removes any data/gap information for the specified LSN and moves it
   * forward.  Helper method used to advance the buffer.  Expected to be called
   * in increasing order of LSNs as it moves gap information forward.
   */
  void clearRecordState(lsn_t lsn, RecordState& rstate) {
    unlinkRecordState(lsn, rstate);
    rstate.reset();
  }

  /**
   * @return   number of storage shards in the storage set whose GapState is
   *           st regardless of their authoritative status.
   */
  size_t numShardsInState(SenderState::GapState st) const;

  /**
   * @return number of storage shards that have the given authoritative status.
   */
  size_t numShardsInAuthoritativeStatus(AuthoritativeStatus st) const;

  /**
   * @return number of shards whose authoritative status is not
   * AuthoritativeStatus::FULLY_AUTHORITATIVE.
   */
  size_t numShardsNotFullyAuthoritative() const;

  /**
   * Initializes a new ClientReadStreamSenderState for the given ShardID and
   * inserts it into storage_set_states_.
   */
  SenderState& createStateForShard(ShardID shard_id);

  /**
   * Called by onEpochMetaData(), update the current epoch metadata for data
   * logs using the result from the metadata log reader callback
   *
   * return 0 for success, -1 on error
   */
  int updateCurrentMetaData(Status st,
                            epoch_t epoch,
                            epoch_t until,
                            std::unique_ptr<EpochMetaData> metadata);

  /**
   * Called by onEpochMetaData(), update last_epoch_with_metadata_ for data
   * logs using the result from the metadata log reader callback
   */
  void updateLastEpochWithMetaData(Status st,
                                   epoch_t epoch,
                                   epoch_t until,
                                   MetaDataLogReader::RecordSource source);

  /**
   * Update the storage_set_states_ according to current epoch metadata
   * configuration and cluster configuration. Called when the read stream
   * receives new epoch metadata or cluster configuration changes.
   *
   * Specifically, it deletes shards from the readset if the shard is either no
   * longer in the current storage set, or no longer in cluster configuration.
   * On the other hand, it adds a shard not previously in the readset if the
   * shard is both present in the current storage set and in cluster config as a
   * valid storage shard.
   */
  void updateCurrentReadSet();

  /**
   * Update the status of SCD mode based on its current status and current
   * log configuration.
   */
  void updateScdStatus();

  // Continuation of updateScdStatus after log configuration data is received.
  void updateScdStatusContinuation(
      std::shared_ptr<LogsConfig::LogGroupNode> log_config);
  /**
   * Called when (re)sending START to a shard (e.g. after reconnecting) or when
   * a shard is no longer present in the config. Updates GapState for storage
   * shards if necessary.
   */
  void resetGapParametersForSender(SenderState& state);

  /**
   * Returns the gap type for gap_lsn based on next_lsn_to_deliver_ and
   * trim_point_.
   */
  GapType determineGapType(lsn_t gap_lsn);

  bool done() const {
    return next_lsn_to_deliver_ > until_lsn_;
  }

  // This should be called at the end of all public methods that have the
  // potential to advance next_lsn_to_deliver_ beyond until_lsn_.  If we are
  // done(), we should never yield to the event loop and stay alive.
  void disposeIfDone() {
    if (done()) {
      deps_->dispose();
    }
  }

  // Callback for redelivery_timer_
  void redeliver();

  // Adjust the redelivery timer based on the success of the just-attempted
  // delivery of a record or gap to the application
  void adjustRedeliveryTimer(bool delivery_success);

  void activateRedeliveryTimer();
  void resetRedeliveryTimer();
  void cancelRedeliveryTimer();

  /**
   * Number of shards we are trying to read from.
   */
  size_t readSetSize() const {
    return storage_set_states_.size();
  }

  // current epoch which the read stream is working on
  epoch_t currentEpoch() const {
    return lsn_to_epoch(next_lsn_to_deliver_);
  }

  // Request epoch metadata for a certain epoch
  // @param require_consistent_from_cache   if true, only allows consistent
  //                                        metadata records delivered from
  //                                        the metadata cache
  void requestEpochMetaData(epoch_t epoch,
                            bool require_consistent_from_cache = true);

  // updated last_released_ based on what we got from storage shards.
  void updateLastReleased(lsn_t last_released_lsn);

  // Activate metadata fetch retry timer
  void activateMetaDataRetryTimer();

  // Bump up the grace counters of all shards that were supposed to send a
  // record before the grace period timer fired
  void updateGraceCounters();

  // Check whether checkFMajority() should enable grace period timer and wait
  // for grace period.
  bool shouldWaitForGracePeriod(FmajorityResult fmajority_result,
                                bool grace_period_expired) const;

  void handleStartPROTONOSUPPORT(ShardID);

  /**
   * Schedule a rewind of this stream.
   */
  void scheduleRewind(std::string reason);

  /**
   * @return true if a rewind has been scheduled.
   */
  bool rewindScheduled() const {
    return rewind_scheduler_->isScheduled();
  }

  /**
   * Clear buffer_ and ask storage nodes to rewind to next_lsn_to_deliver_
   * by sending them a new START message.
   */
  void rewind(std::string reason);

  /**
   * Called to set gap state to be FILTERED_OUT from start_lsn to end_lsn.
   * This method first verifies start_lsn, end_lsn, state. If we should
   * proceed, then set up GapState to FILTERED_OUT and update SenderState.
   *
   * @return  -1   fail to set up GapState FILTERED_OUT
   *           0   success
   */
  int setGapStateFilteredOut(lsn_t start_lsn,
                             lsn_t end_lsn,
                             SenderState& state);

  bool started_{false};

  /**
   * @see forceNoSingleCopyDelivery()
   */
  bool force_no_scd_{false};

  /**
   * Set to true when log uses scd. Changes when configuration is updated.
   */
  bool log_uses_scd_{false};

  /**
   * Set to true when log uses local scd. Changes when configuration is updated.
   */
  bool log_uses_local_scd_{false};

  /**
   * @see useEpochMetaDataCache()
   */
  bool use_epoch_metadata_cache_{true};

  /**
   * When we are retrying reading the metadata we already read, we temporarily
   * disable caching. This flag stores the previous value of
   * use_epoch_metadata_cache_, so we can restore it after we get valid
   * metadata.
   */
  bool reenable_cache_on_valid_metadata_{false};

  /**
   * Current filter version for this read stream. When we receive a data record
   * or a gap record from a storage shard, we discard it if that storage shard
   * did not already respond to the START message we sent it with that filter
   * version.
   *
   * This fixes a race condition where we decide to switch from single copy
   * delivery mode to all send all and thus send new START messages to storage
   * shards but before these storage shards see the START message they already
   * sent some records that were filtered according to single copy delivery. We
   * want to discard these records instead of using them for gap detection,
   * otherwise we will detect dataloss thinking these records were sent as part
   * of all send all mode.
   */
  filter_version_t filter_version_{1};

  // ID of this read stream, uniquely identifying it among read streams
  // created by a Client object
  read_stream_id_t id_;

  // ID of log we are reading from
  logid_t log_id_;

  /**
   * Log group name to be retrieved at start.
   */
  std::string log_group_name_ = "<WAITING LOGSCONFIG>";

  /**
   * LSN from which the client started reading.
   */
  lsn_t start_lsn_;

  /**
   * LSN of the next record we expect to deliver to the application.
   */
  lsn_t next_lsn_to_deliver_;

  /**
   * LSN at which reading stops.
   */
  lsn_t until_lsn_;

  /**
   * Higest lsn of data record received from storage shards
   */
  lsn_t highest_record_lsn_{LSN_INVALID};

  /**
   * last released lsn of the log maintained by the readstream. Updated when
   * the read stream receives STARTED or record/gap from storage shards. May not
   * be accurate and fall behind of the actual last released lsn. Used for
   * optimizing metadata fetching.
   */
  lsn_t last_released_{LSN_INVALID};

  // size of the sequencer's sliding window. Currently used to determine data
  // loss at the beginning of the epoch. Obtained when the read stream starts.
  size_t sequencer_window_size_{0};

  /**
   * Controls how often we send WINDOW messages to senders for them
   * to slide their windows. A smaller value means more often.
   * This value is multiplied by the buffer capacity to find the LSN at which
   * we broadcast.
   *
   * For example, suppose the buffer capacity is 100 and flow_control_threshold_
   * is 0.5. When reading starts, we ask senders for the first 100 records.
   * After we get the first 50 (100 * 0.5) records from senders, we'll
   * slide senders' windows.
   */
  double flow_control_threshold_;

  /**
   * This member is employed to avoid doing the math every time we call
   * slideSenderWindows().
   *
   * When next_lsn_to_deliver_ > next_lsn_to_slide_window_,
   * the Client broadcasts WINDOW messages to storage shards,
   * and at the same time updates:
   *
   * next_lsn_to_slide_window_ =  next_lsn_to_deliver_ +
   *                              flow_control_threshold_ * buffer_->capacity();
   */
  lsn_t next_lsn_to_slide_window_;

  /**
   * A ClientReadStreamBuffer object that holds records and gap markers
   * received from storage shards but not yet released to the application.
   * Contains slots for records with LSNs between next_lsn_to_deliver_+1
   * and next_lsn_to_deliver_+buffer_->capacity().
   */
  std::unique_ptr<ClientReadStreamBuffer> buffer_;

  /**
   * Size of the window in terms of number of records. The window size may be
   * dynamically scaled to accommodate constraints such as memory pressure.
   * The window size is bounded by the client_read_buffer_size which limits the
   * maximum number of records that can be buffered at any given time.
   */
  size_t window_size_;

  /**
   * The largest LSN in sender's sliding window. This member variable
   * is employed to avoid doing the math every time we call
   * slideSenderWindows().
   */
  lsn_t window_high_;

  // This is the map where we keep per-server state, such as the state of the
  // connection and other goodies.  Shards appear in this map if we are trying
  // to read from them.
  std::unordered_map<ShardID, SenderState, ShardID::Hash> storage_set_states_;

  std::unique_ptr<ClientReadStreamDependencies> deps_;

  std::shared_ptr<UpdateableConfig> config_;

  // latest epoch metadata received
  std::unique_ptr<EpochMetaData> current_metadata_;

  // highest epoch which we have confirmed is using current_metadata_
  epoch_t last_epoch_with_metadata_;

  // highest epoch whose metadata is ever requested by the read stream
  folly::Optional<epoch_t> epoch_metadata_requested_;

  // this is the epoch of the last released LSN at the time when the read stream
  // issued the last epoch metadata request. Used to determine the `effective
  // until epoch' of record gotten from metadata logs
  epoch_t last_released_epoch_when_metadata_requested_{EPOCH_INVALID};

  // Track the GapState of storage shards participating in reading. Used to
  // determine whether the subset of storage shards with certain GapState is a
  // f-majority set taking into account of location-based failure domain
  // information.
  std::unique_ptr<GapFailureDomain> gap_failure_domain_;

  /**
   * Smallest end_gap+1 > window_high_ across all gap messages received.
   * Used to determine the right endpoint of a gap interval when buffer_
   * is empty (e.g. in case of an epoch bump).
   */
  lsn_t gap_end_outside_window_;

  /**
   * Client's view of a trim point for a log. Updated when trim gap messages
   * are received by storage shards. Used by handleGap() to determine whether
   * the gap was caused by log trimming.
   *
   * At least one storage shard reported that it trimmed LSNs up to this point
   * (inclusive).  If doNotSkipPartiallyTrimmedSections() was not called,
   * we inform the client of this as soon as possible and skip ahead in the log.
   */
  lsn_t trim_point_;

  // grace period timer; used to wait for more shards to report back before
  // deciding there was a gap; nullptr if grace period is zero/disabled
  std::unique_ptr<BackoffTimer> grace_period_;

  // backoff timer used to retry reading metadata log if the metadata log is
  // found empty when reading started
  std::unique_ptr<BackoffTimer> retry_read_metadata_;

  // Timer used to retry delivery when it fails
  std::unique_ptr<BackoffTimer> redelivery_timer_;

  // If the client starts at an lsn with epoch 0, we issue a bridge gap to
  // epoch 1. If the client rejects that gap, we will reattempt to deliver
  std::unique_ptr<BackoffTimer> reattempt_start_timer_;

  // @see shipPseudorecords
  bool ship_pseudorecords_ = false;

  // @see requireFullReadSet
  bool require_full_read_set_ = false;

  // @see waitForAllCopies
  bool wait_for_all_copies_ = false;

  // @see addStartFlags
  START_flags_t additional_start_flags_ = 0;

  // @see ignoreReleasedStatus
  bool ignore_released_status_ = false;

  // @see doNotSkipPartiallyTrimmedSections
  bool do_not_skip_partially_trimmed_sections_ = false;

  // @see shipCorruptedRecords
  bool ship_corrupted_records_ = false;

  // Reader object to deliver to when not using callbacks.  Null if using
  // callbacks
  ReaderBridge* reader_;

  // Sliding window boundaries sent to storage shards. Updated to match client's
  // window either immediately (when callbacks are used) or after Reader makes
  // some progress.
  Boundaries server_window_;

  // The expected largest protocol supported by *all* servers in the storage
  // set.  This allows seamless rollout of features that require the servers to
  // be in sync, e.g. copyset reordering.
  //
  // Here is the flow for how this is tracked and used:
  // (1) ClientReadStream optimistically initialises this to
  // MAX_PROTOCOL_SUPPORTED.
  // (2) ClientReadStream::sendStart() uses the current `coordinated_proto_'
  // value to decide which features servers should use.  (Initially this is
  // all features due to the above optimism.)
  // (3) If any START messages fail to go out with PROTONOSUPPORT errors,
  // `coordinated_proto_' gets downgraded, the stream gets rewound and we go
  // to step 2 again.
  //
  // Note that this may not 100% reflect the exact protocol versions the
  // servers support, since we only care about protocol version boundaries
  // concerning features involving ClientReadStream.
  uint16_t coordinated_proto_;
  bool any_start_sent_ = false;

  // Indicates that onReaderProgress() should update sender()window_ and send
  // WINDOW messages to ask storage shards for more data. Used only if reader_
  // is not null.
  bool window_update_pending_;

  // a Throttled tracelogger to report gaps
  std::unique_ptr<ClientGapTracer> gap_tracer_;
  // a Sampled tracelogger for tracing reads
  std::unique_ptr<ClientReadTracer> read_tracer_;
  // a Throttled tracelogger for tracing tailer reads
  std::unique_ptr<ClientReadersFlowTracer> readers_flow_tracer_;
  // a Throttled tracelogger for tracing events
  std::unique_ptr<ClientReadStreamTracer> events_tracer_;

  // Tracks the health of the connection, ie whether we have a healthy TCP
  // connection to an f-majority of shards in the read set. Uses a tracer to log
  // debug information when the read stream does not have a good connection
  // health. Also calls the connection health callback provided by the user.
  // Finally, this bumps various counters that the operator can use to alert
  // when readers do not have a good connection.
  std::unique_ptr<ClientReadStreamConnectionHealth> connection_health_tracker_;

  // time taken to get log config information from config
  std::chrono::microseconds logid_request_latency_{0};
  // time when we requested epoch metadata
  std::chrono::steady_clock::time_point epoch_metadata_request_time_;
  // time it took to fetch epoch metadata
  std::chrono::microseconds epoch_metadata_request_latency_{0};
  // A string factory function object that calculates epoch_metadata to be used
  // in the client reads tracer code
  std::function<std::string()> epoch_metadata_str_factory_ = std::bind([&]() {
    return current_metadata_ ? current_metadata_->toString() : "[nullptr]";
  });

  // A string factory function object for getting debug information about nodes
  // that are not available for reads.
  std::function<std::string()> unavailable_shards_str_factory_ = [this]() {
    return unavailableShardsPretty();
  };

  // A string factory function object for getting debug information about nodes
  // that are not available for reads.
  std::function<std::string()> storage_set_health_status_str_factory_ =
      [this]() { return getStorageSetHealthStatusPretty(); };

  // In debug builds, we keep track of when we are executing a callback in
  // this variable to catch errors like indirectly deleting this object in a
  // callback.
  bool inside_callback_ = false;

  // When the clientReadStream receives a STARTED_Message with an E::ACCESS
  // error from any storage shard in the current readset, permission_denied_ is
  // set to true indicating that all outgoing data/gap records (if any) will
  // not be delivered. Instead an ACCESS gap to until_lsn_ will be delivered.
  bool permission_denied_ = false;

  // Set to true if the log was removed from the config. When this happens we
  // want to deliver a NOTINCONFIG gap to until_lsn_. This flag is useful only
  // if we failed to deliver that gap and we need redeliver() to be aware of it.
  bool log_removed_from_config_ = false;

  using HealthyNodeSet = FailureDomainNodeSet<bool>;

  // Track the set of storage nodes that participate in reading. Used to
  // determine whether the overall connection is healthy by checking if it's
  // an f-majority subset.
  std::unique_ptr<HealthyNodeSet> healthy_node_set_;

  std::unique_ptr<ClientReadStreamScd> scd_;

  // Log offsets of next_lsn_to_deliver_. Will be delivered to client with
  // next record once clientReadStream receive at least one valid OffsetMap
  // value along with record form storage shard. Later it can conclude next
  // offsets based on payload sizes of next records unless there were any data
  // loss gaps.
  OffsetMap accumulated_offsets_;

  // Counters of the number of records and bytes delivered. Used to track
  // reading speed.
  size_t num_records_delivered_{0};
  size_t num_bytes_delivered_{0};

  // Counter of the size (in bytes) of the current ReadStream.
  size_t bytes_buffered_{0};

  /**
   * When we are in all send all mode but SCD is in use on the log, there is a
   * race condition that can cause erroneous data loss reporting. We fix this by
   * rewinding the stream once when we see a gap in the numbering sequence.
   *
   * When this happens, we set this to next_lsn_to_deliver_. Each time we are
   * seeing a dataloss gap, we compare next_lsn_to_deliver_ with this value in
   * order to know if we already retried or not. See T5849538.
   */
  lsn_t last_dataloss_gap_retried_{LSN_INVALID};

  // LSN of last record delivered to client
  lsn_t last_delivered_lsn_{LSN_INVALID};
  // The timestamp of last delivered (the timestamp at which
  // this record was acknowledged by sequencer at the time of write)
  std::chrono::milliseconds last_in_record_ts_{0};
  // The timestamp when last record was received by client
  std::chrono::milliseconds last_received_ts_{0};

  // @see scheduleRewind().
  std::unique_ptr<RewindScheduler> rewind_scheduler_;

  ReadStreamAttributes attrs_ = ReadStreamAttributes();

  friend class ClientReadStreamTest;
  friend class ClientReadStreamScd;
  friend class ClientReadStreamSenderState;
  friend class ClientReadersFlowTracer;
  friend class ClientReadStreamConnectionHealth;
  friend class RewindScheduler;
};

}} // namespace facebook::logdevice
