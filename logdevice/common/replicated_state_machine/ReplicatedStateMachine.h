/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <zstd.h>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/IntrusiveList.h>
#include <folly/Memory.h>
#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

/**
 * ReplicatedStateMachine is a utility for building a distributed replicated
 * state machine backed by two logs, a snapshot log which contains serialized
 * versioned snapshots of the state, and a delta log which contains ordered
 * serialized deltas to be applied on a snapshot. The state is versioned, and
 * that version is the LSN of the last delta record that was applied.
 *
 * Users can subscribe to this state machine to get notified when the state
 * changes. On startup, the state machine performs a "replay" procedure which
 * consists in syncing the local state with the current state. This is
 * effectively done by reading the last snapshot record in the snapshot log and
 * then reading the backlog of delta records from the delta log, starting from
 * the version of that snapshot. Once the replay procedure is complete,
 * subscribers get notified of the initial state, and new updates are
 * streamed to subscribers in real-time as they arrive.
 *
 * Users can mutate the state by writing deltas. Deltas are unconditionally
 * appended to the delta log, but may or may not be applied as they are read.
 * There are two modes of writing deltas. With the first mode
 * (CONFIRM_APPEND_ONLY), the user does not wish to be informed whether or not
 * the delta is actually applied after appended. The user gets a confirmation
 * immediately after the append succeeds. With the second mode
 * (CONFIRM_APPLIED), the user gets a confirmation after the local state is
 * synced to a version past that delta, and is notified whether the delta was
 * applied or not. This way of handling concurrent writes works well for
 * state machines that have a low update rate.
 *
 * This class is templated by two types. T is the state type. The user is
 * required to provide serialization and deserialization methods for it by
 * overriding the `deserializeState` and `serializeState` methods. D is the type
 * of the delta. The user is required to provide a deserialization method by
 * overriding `deserializeDelta` as well as `applyDelta` which mutates a state
 * of type T with a delta of type D. `applyDelta` may fail if the delta is
 * stale, in that case the delta is skipped and the state is not mutated.
 *
 * This class provides a `snapshot` method which the user can use to serialize
 * the current local state onto a new snapshot record and append it to the
 * snapshot log. That method cannot be used while we are replaying the backlog.
 * This class maintains some metadata about the delta log such as the number of
 * delta records seen as well as the number of bytes seen. This metadata is also
 * included in snapshots so it is never lost and used to estimate the amount of
 * records/bytes that are in the delta log but have never been snapshotted. The
 * user relies on this information to decide when to create a new snapshot.
 *
 * If this state machine misses data when reading the delta log (because it sees
 * a DATALOSS or TRIM gap), it will stall until a snapshot record is received
 * for a version past the higher end of that gap. If the gap was a TRIM gap,
 * hopefully another entity decided to create a snapshot and then trimmed the
 * delta log so it is reasonnable to expect a snapshot will be received shortly.
 * If the gap was a DATALOSS gap and no one ever wrote a snapshot that accounted
 * for the lost deltas, the state machine will stall until a human manually
 * writes a snapshot.
 *
 * Limitations and TODOs:
 *
 * TODO(T13475700): If recovery completed and we were able to retrieve the
 * last_released_real_lsn, the current replay mechanism only tries to read the
 * last snapshot record from the snapshot log. It is theoretically possible to
 * have unordered snapshot record, ie that the last snapshot is not the most
 * recent one. This is not a big deal if the delta log is not trimmed
 * aggresively.  If we want to trim the delta log aggressively after creating a
 * snapshot, we might need to add some logic to make this safer.
 *
 * TODO(T13475700): the replay mechanism depends on recovery completing.
 * We may consider relaxing this and allowing streaming updates to subscribers
 * when the state machine is past the current last released lsn instead of the
 * curren tail.
 * It is still unclear if we'd rather give a stale version or nothing at all
 * (and stall) when recovery has not completed.
 */

namespace facebook { namespace logdevice {

template <typename T, typename D>
class ReplicatedStateMachine {
 public:
  explicit ReplicatedStateMachine(RSMType rsm_type,
                                  logid_t delta_log_id,
                                  logid_t snapshot_log_id = LOGID_INVALID);

  virtual ~ReplicatedStateMachine() {}

  /**
   * Stop reading (equivalent to calling stop()) once the tail of the snapshot
   * and delta logs have been reached. The tails of these logs are the LSNs of
   * the last records appended to them prior to start() being called, tail LSNs
   * are computed when the user calls start() and never change. This means that
   * any delta/snapshot appended after a call to start() may be missed.
   *
   * This option can be used when the user only wishes to get the
   * current value of the state but does not wish to get streaming updates.
   * The subscribers' callback will be called only once when the initial state
   * is retrieved, and then the state machine will stop.
   */
  void stopAtTail() {
    stop_at_tail_ = true;
  }

  /**
   * Do not wait until we reached the tail of the snapshot and delta logs to
   * start delivering individual updates.
   */
  void deliverWhileReplaying() {
    deliver_while_replaying_ = true;
  }

  /**
   * By default, if there is evidence that some deltas were lost (because we
   * received a DATALOSS gap in the delta log for instance), this state machine
   * will stall until it sees a snapshot record that accounted for all these
   * lost deltas. Use this option if you wish to not wait for such a snapshot,
   * but the data may be inconsistent depending on what deltas were lost.
   */
  void doNotStallIfDataLoss() {
    stall_if_data_loss_ = false;
  }

  /**
   * If `doNotStallIfDataLoss()` was not called, wait for this amount of time
   * before bumping the "num_replicated_state_machines_stalled" stat.
   */
  void considerStalledAfterGracePeriod(std::chrono::milliseconds period) {
    stalled_grace_period_ = period;
  }

  /**
   * Each delta record normally contains a header (@see DeltaHeader).
   * However, such a header has not always been present, and old binaries may
   * not expect it. This option should be used if it is expected that there are
   * still old binaries that cannot undarstand that header. The counterpart is
   * that if the header is not written, we cannot use the CONFIRM_APPLIED option
   * when writting a delta.
   *
   * More concretely, this option should be used while we are migrating all
   * servers and clients to using EventLogStateMachine which uses this utility
   * and there are still binaries using the old EventLogReader utility which
   * does not expect a header. Once all binaries (servers and clients) are
   * migrated, this option should be removed and everyone should use the
   * header.
   */
  void doNotWriteDeltaHeader() {
    write_delta_header_ = false;
  }

  /**
   * When the user calls writeDelta() with the CONFIRM_APPLIED flag, we insert
   * it into a queue so we can match it with deltas read from the delta log.
   * Use this function to change the capacity of that queue. When that queue is
   * full, calls to writeDelta() with the CONFIRM_APPLIED flag will fail with
   * E::NOBUFS.
   */
  void setMaxDeltasPendingConfirmation(size_t max) {
    max_pending_confirmation_ = max;
  }

  /*
   * @param Timeout to use for AppendRequest on the delta log.
   */
  void setDeltaAppendTimeout(std::chrono::milliseconds timeout) {
    delta_append_timeout_ = timeout;
  }

  /*
   * @param Timeout to use for AppendRequest on the snapshot log.
   */
  void setSnapshotAppendTimeout(std::chrono::milliseconds timeout) {
    snapshot_append_timeout_ = timeout;
  }

  /*
   * After the replay procedure completes, and when we are now in tailing mode,
   * applying a new snapshot "fast-forwards" the state machine to the version of
   * that snapshots, potentially skipping many intermediary deltas. This is
   * something we may want to avoid as this jeopardizes the opportunity for the
   * user to get notified about individual delta updates as well as the capacity
   * to confirm deltas that were written with the CONFIRM_APPLIED flag.
   *
   * We use a grace period to give more chance for individual deltas to come
   * around before we actually apply the snapshot. Unless the delta log is
   * unreadable, it should never happen that this grace period expires because
   * we don't try to trim the delta log aggresively after applying a snapshot.
   *
   * @param Period to use.
   */
  void setFastForwardGracePeriod(std::chrono::milliseconds period) {
    fast_forward_grace_period_ = period;
  }

  void setSnapshottingGracePeriod(std::chrono::milliseconds period) {
    snapshotting_grace_period_ = period;
  }
  /**
   * @param timeout Timeout following a successful append of a delta with the
   *                E::CONFIRM_APPLIED after which we give up waiting for
   *                confirmation of that delta and we call the user-provide
   *                callback with E::TIMEDOUT.
   */
  void setConfirmTimeout(std::chrono::milliseconds timeout) {
    confirm_timeout_ = timeout;
  }

  void enableSnapshotCompression(bool val) {
    snapshot_compression_ = val;
  }

  void allowSkippingBadSnapshots(bool val) {
    can_skip_bad_snapshot_ = val;
  }

  /**
   * Start reading the snapshot and delta logs.
   * Must be called on a worker thread.
   */
  void start();

  /**
   * Stop this state machine. Once called, the user should do nothing but
   * destroy this state machine.
   */
  void stop();

  /**
   * Block until this state machine completes either because someone called
   * stop() or the stopAtTail() option was provided and the state machine
   * reached the tail.
   *
   * This method is thread safe.
   *
   * @return true if this state machine finished reading, false if this method
   * returned because the timeout expired.
   */
  bool wait(std::chrono::milliseconds timeout);

  enum class WriteMode { CONFIRM_APPEND_ONLY, CONFIRM_APPLIED };

  /**
   * Write an update to the replicated state machine.
   *
   * @param delta Payload of the delta.
   * @param cb    callback called on completion. On success, `st` is E::OK and
   *              `version` is the state machine's version after applying the
   *              delta. On error, `st` is one of the error status returned by
   *              `serializeDelta()` if serialization failed, or `applyDelta()`
   *              if deserialization or applying the delta failed and mode is
   *              CONFIRM_APPLIED. `st` can also be E::TIMEDOUT if it could not
   *              be determined whether or not applying the individual delta
   *              succeeded because the delta log was snapshotted and trimmed(
   *              unlikely). When E::TIMEDOUT is supplied, version can either be
   *              LSN_INVALID, in which case we don't know for sure if the delta
   *              was written to the delta log, or a valid LSN in which case we
   *              know for sure that the delta was appended but could not verify
   *              it was applied.
   * @param mode  If CONFIRM_APPEND_ONLY, call cb once the AppendRequest request
   *              completes. If CONFIRM_APPLIED, sync the local copy of the
   *              state machine after appending the delta to check if it was
   *              actually applied or not (stale delta) before calling the
   *              callback.
   * @param base_version If specified, the delta will only be written providing
   *                     that the current state version matches the
   *                     base_version. Otherwise, the callback will be executed
   *                     with status E::STALE.
   */
  void writeDelta(
      std::string payload,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  using update_cb_t =
      std::function<void(const T& state, const D* delta, lsn_t version)>;

  class SubscriptionHandle {
   public:
    ~SubscriptionHandle();
    SubscriptionHandle(SubscriptionHandle&& other) = default;
    SubscriptionHandle& operator=(SubscriptionHandle&& other) = default;
    SubscriptionHandle() = default; // invalid handle
   private:
    SubscriptionHandle(ReplicatedStateMachine<T, D>* owner,
                       typename std::list<update_cb_t>::iterator it);
    ReplicatedStateMachine<T, D>* owner_{nullptr};
    typename std::list<update_cb_t>::iterator it_;
    friend class ReplicatedStateMachine<T, D>;
  };

  /**
   * Register a callback to be called each time the state changes.
   * @param cb Callback to be called. The callback provides the new state and
   * its version.
   * Note: calling stop() from inside the callback is not supported.
   *
   * @return Handle to be used for unsubscribing. Destroying that handle
   * automaticall unsubscribes.
   */
  std::unique_ptr<SubscriptionHandle> subscribe(update_cb_t cb);

  /**
   * Unsubscribe for updates.
   *
   * @param h Handle that was returned by subscribe().
   */
  void unsubscribe(SubscriptionHandle& h);

  /**
   * Create a snapshot of the state machine and write it to the snapshot log.
   *
   * @param cb Called once the snapshot has been successfully written.
   *           `st` is set to:
   *           - E::OK on success;
   *           - E::INPROGRESS if there is already a snapshot being appended;
   *           - E::AGAIN if this state machine is still recovering the initial
   *             state;
   *           - or any error status that can be returned in the callback of
   *             AppendRequest.
   */
  virtual void snapshot(std::function<void(Status st)> cb);

  /**
   * An estimate of the number of delta records that have not been included in a
   * snapshot yet. The user can use this to decide when to create snapshots().
   */
  size_t numDeltaRecordsSinceLastSnapshot() const {
    if (delta_log_offset_ < last_snapshot_offset_) {
      // We can be here if we fast forwarded the state machine when receiving a
      // snapshot.
      return 0;
    }
    return delta_log_offset_ - last_snapshot_offset_;
  }

  size_t numBytesSinceLastSnapshot() const {
    if (delta_log_byte_offset_ < last_snapshot_byte_offset_) {
      // We can be here if we fast forwarded the state machine when receiving a
      // snapshot.
      return 0;
    }
    return delta_log_byte_offset_ - last_snapshot_byte_offset_;
  }

  // Called once we received a new snapshot record. If we are syncing the
  // snapshot log, buffer the record but do not process it immediately as we may
  // receive a more recent snapshot. If we are not syncing the snapshot or if
  // we are syncing and the record's lsn is >= snapshot_sync_, process it now.
  bool onSnapshotRecord(std::unique_ptr<DataRecord>& record);

  // Process a new snapshot record. Update `data_` with the content of the
  // snapshot if that snapshot is for a version greater than `version_`. Calls
  // the user-provided `deserializeState()` method to deserialize the state
  // contained in that snapshot.
  bool processSnapshot(std::unique_ptr<DataRecord>& record);

  // Called when we receive a gap in the snapshot log.
  bool onSnapshotGap(const GapRecord& gap);

  // Called when we received a record from the delta log. Use the user-provided
  // `applyDelta()` method to deserialize and apply the delta onto the current
  // state.
  //
  // If the delta is actually a delta that the user appended with a call
  // to `writeDelta()` and the user is still waiting for confirmation whether or
  // not the delta could be applied, notify the user now.
  bool onDeltaRecord(std::unique_ptr<DataRecord>& record);

  // Called when we received a gap from the delta log.
  bool onDeltaGap(const GapRecord& gap);

  // Called once the tail lsn of the delta log was retrieved. Create a read
  // stream to read the delta log starting from `delta_log_start_lsn_`.
  void onGotDeltaLogTailLSN(Status st, lsn_t lsn);

  // Called when the delta log client read stream switches to being unhealthy
  // or healthy again
  void onDeltaLogReadStreamHealthChange(bool is_healthy);

  // Called once the tail lsn of the snapshot log was retrieved. Create a read
  // stream to read the snapshot log.
  void onGotSnapshotLogTailLSN(Status st, lsn_t start, lsn_t lsn);

  // Create a payload for a snapshot. The payload includes `data` serialized as
  // well as the version of that snapshot.
  std::string createSnapshotPayload(const T& data,
                                    lsn_t version,
                                    bool rsm_include_read_pointer_in_snapshot);

  // Some metadata included inside delta records.
  struct DeltaHeader {
    uint32_t checksum{0};
    uint32_t header_sz{0};
    uint32_t format_version{0}; // unused, might be handy in the future.
    uint32_t flags{0};          // unused, might be handy in the future.

    boost::uuids::uuid uuid = boost::uuids::nil_generator()();
  };
  // Prefer a static assert rather than using __attribute__((__packed__)) to
  // prevent inadvertent creation of unaligned fields that trigger undefined
  // behavior.
  static_assert(sizeof(DeltaHeader) == 32, "wrong expected size");
  static constexpr size_t MIN_DELTA_HEADER_SZ = 32;

  // Create the payload of a delta record. This payload contains a header
  // followed by a checksum and then the user payload.
  std::string createDeltaPayload(std::string user_payload, DeltaHeader header);

  static bool deserializeDeltaHeader(const Payload& payload,
                                     DeltaHeader& header);

  // Returns the current state object
  const T& getState() const {
    return *data_;
  }

  // Used for debugging. Doesn't populate "Propagated version" column.
  void getDebugInfo(InfoReplicatedStateMachineTable& table) const;

  // Used by tests to find which UUID was assigned during the last call to
  // writeDelta().
  boost::uuids::uuid getLastUUID() const {
    return last_uuid_;
  }

  // Called when we time out trying to confirm an appended delta. Can be called
  // by tests.
  void onDeltaConfirmationTimeout(boost::uuids::uuid uuid);

  // return current version of the state
  lsn_t getVersion() const {
    return version_;
  }

 protected:
  // These functions may be overridden by tests.

  // Start a SyncSequencerRequest to find the tail lsn of the snapshot log.
  virtual void getSnapshotLogTailLSN();

  // Called once we have retrieved the base snapshot to apply delta onto. Issues
  // a SyncSequencerRequest to find the tail lsn of the delta log.
  virtual void getDeltaLogTailLSN();

  // Utility function for creating a read stream for the delta and snapshot
  // logs.
  virtual read_stream_id_t
  createBasicReadStream(logid_t logid,
                        lsn_t start_lsn,
                        lsn_t until_lsn,
                        ClientReadStreamDependencies::record_cb_t on_record,
                        ClientReadStreamDependencies::gap_cb_t on_gap,
                        ClientReadStreamDependencies::health_cb_t health_cb);

  // Resume reading a read stream on which we may have been pushing back.
  virtual void resumeReadStream(read_stream_id_t id);

  // These functions must be overridden by the user.

  /**
   * Create a default state. This will be the initial state if the snapshot log
   * is empty.
   *
   * @param version Version of the state.
   *
   * @return default state.
   */
  virtual std::unique_ptr<T> makeDefaultState(lsn_t version) const = 0;

  /**
   * Deserialize a snapshot record.
   *
   * @param payload Payload to deserialize from.
   * @param version Version of the state contained in the snapshot.
   *
   * @return State extracted from the snapshot or nullptr if the snapshot could
   * not be deserialized, in that case err is expected to be set to an error
   * status.
   */
  virtual std::unique_ptr<T>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const = 0;

  /**
   * Informs the subclass that we've finished retrieving the base state
   * will start applying deltas to it. The base state can come from a snapshot
   * or be empty if there are no snapshots.
   */
  virtual void gotInitialState(const T& /*state*/) const {}

  /**
   * Deserialize a delta payload.
   *
   * @param payload Payroad to deserialize from.
   *
   * @returned deserialized delta or nullptr if the delta could not be
   * deserialized, in that case err is expected to be set to an error status.
   */
  virtual std::unique_ptr<D> deserializeDelta(Payload payload) = 0;

  /**
   * Apply a delta to the given state.
   *
   * @param delta   Delta to be applied.
   * @param state   State to apply the delta onto.
   * @param version Version of the state with the delta applied.
   *
   * @return 0 on success, or -1 and err is populated with an error code.
   */
  virtual int applyDelta(const D& delta,
                         T& state,
                         lsn_t version,
                         std::chrono::milliseconds timestamp,
                         std::string& failure_reason) = 0;

  /**
   * Serialize a state onto a buffer.
   * This utility asserts that this function never fails.
   *
   * @param state    State to be serialized.
   * @param buf      Buffer to serialize onto.
   * @param buf_size Size of the buffer.
   *
   * @return size of the serialized payload or -1 on error. On error, `err` is
   * expected to be populated with an error code.
   */
  virtual int serializeState(const T& state, void* buf, size_t buf_size) = 0;

  // Post a request on a worker. Mocked by tests.
  virtual void postRequestWithRetrying(std::unique_ptr<Request>& rq);

  // Create and post an AppendRequest, mocked by unit tests.
  virtual void postAppendRequest(logid_t logid,
                                 std::string payload,
                                 std::chrono::milliseconds timeout,
                                 std::function<void(Status st, lsn_t lsn)> cb);

  virtual void activateGracePeriodForFastForward();
  virtual void cancelGracePeriodForFastForward();
  virtual bool isGracePeriodForFastForwardActive();

  virtual void activateStallGracePeriod();
  virtual void cancelStallGracePeriod();
  virtual void activateConfirmTimer(boost::uuids::uuid uuid);

  virtual void activateGracePeriodForSnapshotting();
  virtual void cancelGracePeriodForSnapshotting();
  virtual bool isGracePeriodForSnapshottingActive();

  virtual bool shouldCreateSnapshot() const = 0;
  virtual bool canSnapshot() const = 0;
  virtual void onSnapshotCreated(Status st, size_t snapshotSize) = 0;

  const RSMType rsm_type_;
  const logid_t delta_log_id_;
  const logid_t snapshot_log_id_;

  // Used by tests.
  boost::uuids::uuid last_uuid_;

  boost::uuids::random_generator uuid_gen_;

  // Options.
  bool stop_at_tail_{false};
  bool deliver_while_replaying_{false};
  bool stall_if_data_loss_{true};
  size_t max_pending_confirmation_{1000};
  std::chrono::milliseconds delta_append_timeout_{std::chrono::seconds{5}};
  std::chrono::milliseconds snapshot_append_timeout_{std::chrono::seconds{5}};
  std::chrono::milliseconds fast_forward_grace_period_{
      std::chrono::seconds{10}};
  std::chrono::milliseconds confirm_timeout_{std::chrono::seconds{5}};
  bool snapshot_compression_{false};
  bool can_skip_bad_snapshot_{false};
  std::chrono::milliseconds stalled_grace_period_{std::chrono::seconds{30}};
  std::chrono::milliseconds snapshotting_grace_period_{
      std::chrono::minutes{10}};
  bool write_delta_header_{true};
  bool snapshot_in_flight_{false};

 private:
  // Deserialize a record in the snapshot log. Expect a RSMSnapshotHeader
  // followed by the payload which the user provides deserialization for through
  // `deserializeState()`.
  int deserializeSnapshot(const DataRecord& record,
                          std::unique_ptr<T>& out,
                          RSMSnapshotHeader& header) const;

  // Called by `onSnapshotRecord()` or `onSnapshotGap()` when we have reached
  // the tail lsn `snapshot_sync_` of the snapshot log. When this
  // function is called, we have the base snapshot to apply deltas onto, so it's
  // time to read the delta log.
  // Calls getDeltaLogTailLSN().
  void onBaseSnapshotRetrieved();

  // When we are tailing, we may receive a new snapshot record. Applying that
  // snapshot "fast-forwards" the state machine to the version of that snapshot,
  // which means we will discard all the deltas with smaller version that will
  // later arrive. Ideally, we'd prefer making progress using these deltas
  // instead of fast-forwarding for two reasons:
  //
  // 1/ Some users prefer being notified of changes on a per-delta basis;
  // 2/ We might be waiting for some deltas to be confirmed
  //    (pending_confirmation_ not empty).
  //
  // We start a grace period to push back on applying this snapshot and give a
  // chance for the deltas to arrive. In practice the deltas should not have
  // been trimmed because we try to not too aggressively trim the delta log
  // after creating a new snapshot.
  bool canFastForward(lsn_t lsn);

  // Called when we receive a delta record. Deserialize its header and payload.
  int deserializeDelta(const DataRecord& record,
                       std::unique_ptr<D>& out,
                       DeltaHeader& header);

  // Called by `onDeltaRecord()` or `onDeltaGap()` once we have reached the tail
  // lsn `delta_sync_` of the record log. When this function is
  // called, we consider that we have finished reading the backlog of deltas and
  // we notify subscribers of the initial state. As new updates come in either
  // via deltas or new snapshots, subscribers will from now on be updated
  // immediately.
  void onReachedDeltaLogTailLSN();

  // Notify all subscribers of a new version of the state. Called when we
  // receive a delta record or for notifying of the initial state on
  // initiatization.
  // `delta` is populated if we are notifying the subscribers of a change due to
  // applying a new delta. It is nullptr if the change is because we read a new
  // snapshot.
  void notifySubscribers(const D* delta = nullptr);

  // Discard any entry in pending_confirmation_ that have lsn <=
  // version_ and that we could not confirm either because we fast forwarded
  // with a snapshot or we skipped the delta when we read it because we could
  // not parse its header (and thus its uuid).
  void discardSkippedPendingDeltas();

  // Used to proxy callbacks for requests enqueued in a different worker thread
  // back to this worker thread.
  WorkerCallbackHelper<ReplicatedStateMachine<T, D>> callbackHelper_;

  // LSN of the tail of the snapshot log computed on startup. We use this to
  // define which record in the snapshot log can be considered the most recent
  // base to apply deltas on. Once we have found that record (if any), we start
  // reading the delta log.
  lsn_t snapshot_sync_{LSN_OLDEST};

  // onSnapshotRecord() may buffer the record here if we are syncing the
  // snapshot log and that record is not yet known to be the most recent
  // snapshot. This record is processed once the read stream that reads the
  // snapshot log reaches snapshot_sync_.
  std::unique_ptr<DataRecord> last_snapshot_record_;

  // LSN of the tail of the delta log computed once we have found the base
  // snapshot (if any). We notify subscribers of the initial state once we have
  // read the delta log past this lsn.
  lsn_t delta_sync_{LSN_OLDEST};

  // LSN of the last record or gap read from the delta log.
  lsn_t delta_read_ptr_{LSN_INVALID};

  // version of the last record from the snapshot log.
  lsn_t last_snapshot_version_{LSN_INVALID};

  // Delta log read pointer as marked by the last snapshot (this allows us to
  // skip reading deltas already applied and skip gaps).
  lsn_t last_snapshot_last_read_ptr_{LSN_OLDEST};

  // Current state of the process of syncing our local version to the last
  // version prior to this state machine starting.
  // SYNC_SNAPSHOT: we are issuing a SyncSequencerRequest to find the tail LSN
  //                for the snapshot log and reading our initial snapshot up
  //                to that tail;
  // SYNC_DELTAS:   we are issuing a SyncSequencerRequest to find the tail LSN
  //                for the delta log and reading our initial deltas up to
  //                that tail. Upon completion of this stage, subscribers are
  //                notified of the initial state;
  // TAILING:       we are now notifying subscribers as new deltas/snapshots
  //                are received. Writing deltas with CONFIRM_APPLIED flag as
  //                well as writing snapshots is now permitted.
  enum SyncState { SYNC_SNAPSHOT, SYNC_DELTAS, TAILING };
  SyncState sync_state_{SyncState::SYNC_SNAPSHOT};

  // Local copy of the state. Updated when we receive a new snapshot with
  // version greater than `version_` or when we receive a new delta and that
  // delta can be applied.
  std::unique_ptr<T> data_;

  // Version of the local copy.
  lsn_t version_{LSN_OLDEST};

  // Ids of the read streams for reading the snapshot and delta logs.
  read_stream_id_t snapshot_log_rsid_{0};
  read_stream_id_t delta_log_rsid_{0};

  struct DeltaPendingConfirmation {
    // If LSN_INVALID, the append is still in flight, otherwise, this is the
    // LSN of the record.
    lsn_t lsn{LSN_INVALID};
    boost::uuids::uuid uuid;
    std::function<void(Status, lsn_t, const std::string&)> cb;
    // After the append completes, we activate this timer. If the timer expires
    // before we can confirm the delta, we confirm the delta with E::TIMEDOUT.
    std::unique_ptr<Timer> timer;
  };

  // List of delta appends in flight that were written with
  // WriteMode::CONFIRM_APPLIED. We keep them in this queue until they are read
  // locally so we can inform the user whether or not the delta was actually
  // applied. Appends should be in LSN order since we are appending from the
  // same thread.
  // Using std::list because removing an entry does not invalidate iterators of
  // other entries.
  std::list<DeltaPendingConfirmation> pending_confirmation_;

  // We may read deltas before the AppendRequest completes. This map makes it
  // possible to look up the corresponding DeltaPendingConfirmation object by
  // uuid since we cannot do it by LSN.
  std::unordered_map<boost::uuids::uuid,
                     typename std::list<DeltaPendingConfirmation>::iterator,
                     boost::hash<boost::uuids::uuid>>
      pending_confirmation_by_uuid_;

  size_t delta_appends_in_flight_{0};

  // List of callbacks to call when the local state gets updated.
  std::list<update_cb_t> subscribers_;

  // Used for debugging.
  bool stopped_{false};

  // The following counters are used to provide an estimate to users of the
  // number of records and bytes in the delta log that have not been taken into
  // account in a snapshot. @see numDeltaRecordsSinceLastSnapshot() and
  // numBytesSinceLastSnapshot():
  // - Offset of the last record read from the delta log.
  size_t delta_log_offset_{0};
  // - Offset of the last delta considered in the last retrieved snapshot.
  size_t last_snapshot_offset_{0};
  // - Byte offset of the last delta record read.
  //   Can be used to estimate the size in bytes of the delta log past the last
  //   snrapshot. This is included inside the snapshot record payload.
  uint64_t delta_log_byte_offset_{0};
  // - Byte offset of the last delta log record that was accounted for in a
  //   snapshot.
  uint64_t last_snapshot_byte_offset_{0};

  // If != LSN_INVALID, do not make progress reading the delta log until we have
  // read a snapshot with at least this version. This is set if we see a
  // DATALOSS or TRIM gap in the delta log. This is a safety mechanism because
  // such a gap means we skipped some data, we don't want to apply a delta onto
  // a stale version. If the gap that caused this is a TRIM gap, hopefully the
  // delta log was trimmed because a snapshot was written. If the gap that
  // caused this is a DATALOSS gap, hopefully a snapshot was taken before the
  // data was lost, otherwise we expect the oncall will eventually fix things
  // and unstall this state machine by manually writing a snapshot.
  lsn_t waiting_for_snapshot_{LSN_INVALID};
  // If waiting_for_snapshot_ != LSN_INVALID, set to true once the
  // `stallGracePeriodTimer_` expired and we bumped a stat to notify the
  // oncall. Used so we know we have to decrease the stat counter when the
  // situation gets resolved.
  bool bumped_stalled_stat_{false};
  // keeps track of whether the delta read stream is healthy or not. We may
  // still receive deltas and deliver them while it is unhealthy, however we
  // may stall if a gap is encountered. In any case, until the read stream
  // becomes healthy again, the state machine will decline writing deltas
  // with CONFIRMED_APPLIED flag.
  bool delta_read_stream_is_healthy_{true};

  // See comment in onSnapshotRecord().
  Timer fastForwardGracePeriodTimer_;
  lsn_t allow_fast_forward_up_to_{LSN_INVALID};

  // See commen considerStalledAfterGracePeriod().
  Timer stallGracePeriodTimer_;

  // Used by wait() method.
  Semaphore sem_;

  // timestamp of last snapshot
  // we make new snapshot if:
  //  - grace period passed after last snapshot
  //  - there are new deltas since last snapshot
  std::chrono::milliseconds snapshot_log_timestamp_{0};
  Timer snapshotting_timer_;
};

template <typename T>
class StartReplicatedStateMachineRequest : public Request {
 public:
  explicit StartReplicatedStateMachineRequest(T* processor)
      : processor_(processor) {}
  Execution execute() override {
    processor_->start();
    return Execution::COMPLETE;
  }
  int getThreadAffinity(int /*nthreads*/) override {
    return 0;
  }

 private:
  T* processor_;
};

template <typename T>
class StopReplicatedStateMachineRequest : public Request {
 public:
  explicit StopReplicatedStateMachineRequest(T* processor)
      : processor_(processor) {}
  Execution execute() override {
    processor_->stop();
    return Execution::COMPLETE;
  }
  int getThreadAffinity(int /*nthreads*/) override {
    return 0;
  }

 private:
  T* processor_;
};

template <typename T>
class DestroyReplicatedStateMachineRequest : public Request {
 public:
  explicit DestroyReplicatedStateMachineRequest(T* processor,
                                                std::function<void()> cb)
      : processor_(processor), cb_(cb) {}
  Execution execute() override {
    delete processor_;
    cb_();
    return Execution::COMPLETE;
  }
  int getThreadAffinity(int /*nthreads*/) override {
    return 0;
  }

 private:
  T* processor_;
  std::function<void()> cb_;
};

template <typename T>
class CreateSnapshotRequest : public Request {
 public:
  explicit CreateSnapshotRequest(T* processor,
                                 std::function<void(Status st)> cb)
      : processor_(processor), cb_(std::move(cb)) {}

  Execution execute() override {
    processor_->snapshot(cb_);
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return 0;
  }

 private:
  T* processor_;
  std::function<void(Status st)> cb_;
};

template <typename T>
class WriteDeltaRequest : public Request {
 public:
  explicit WriteDeltaRequest(
      T* processor,
      const std::string& delta_blob,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb)
      : processor_(processor), blob_(delta_blob), cb_(std::move(cb)) {}

  Execution execute() override {
    processor_->writeDelta(blob_, cb_, T::WriteMode::CONFIRM_APPLIED);
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /* unused */) override {
    return 0;
  }

 private:
  T* processor_;
  std::string blob_;
  std::function<
      void(Status st, lsn_t version, const std::string& failure_reason)>
      cb_;
};
}} // namespace facebook::logdevice

#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-inl.h"
