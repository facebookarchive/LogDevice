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

#include <boost/intrusive/set.hpp>
#include <folly/AtomicIntrusiveLinkedList.h>
#include <folly/IntrusiveList.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Priority.h"
#include "logdevice/common/SCDCopysetReordering.h"
#include "logdevice/common/ServerRecordFilter.h"
#include "logdevice/common/ShapingContainer.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/strong_typedef.h"
#include "logdevice/include/types.h"
#include "logdevice/server/RealTimeRecordBuffer.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/ReadIoShapingCallback.h"

namespace facebook { namespace logdevice {

class CatchupQueue;
class IteratorCache;
class StatsHolder;
class EpochRecordCacheEntry;

// Useful macro that can be used for debugging a stream's lifetime when
// in debug loglevel.
#define stream_ld_debug(_stream_, _name_, ...)                               \
  ld_debug(_name_ " - client_id=%s, id=%lu, log_id=%lu, "                    \
                  "read_ptr=%s, window_high=%s, until_lsn=%s, version=%lu, " \
                  "digest=%s, traffic_class=%s",                             \
           ##__VA_ARGS__,                                                    \
           (_stream_).client_id_.toString().c_str(),                         \
           (_stream_).id_.val_,                                              \
           (_stream_).log_id_.val_,                                          \
           lsn_to_string((_stream_).getReadPtr().lsn).c_str(),               \
           lsn_to_string((_stream_).getWindowHigh()).c_str(),                \
           lsn_to_string((_stream_).until_lsn_).c_str(),                     \
           (_stream_).version_.val_,                                         \
           (_stream_).digest_ ? "true" : "false",                            \
           trafficClasses()[(_stream_).trafficClass()].c_str());

LOGDEVICE_STRONG_TYPEDEF(uint64_t, server_read_stream_version_t);

/**
 * @file State and logic for a read stream for a single log and client, from a
 *       server's perspective.
 *
 *       The class is not thread-safe; all calls are assumed to happen on a
 *       single worker thread.
 */

class ServerReadStream : boost::noncopyable {
 public:
  ServerReadStream(read_stream_id_t id,
                   ClientID client_id,
                   logid_t log_id,
                   shard_index_t shard,
                   StatsHolder* stats = nullptr,
                   std::shared_ptr<std::string> log_group_path = nullptr);

  ~ServerReadStream();

  /**
   * Notify that SCD should be enabled.
   *
   * @param known_down List of shards in the blacklist;
   * @param my_node_index, used to maintain stats about whether this server read
   * stream sees itself in the known down list.
   */
  void enableSingleCopyDelivery(const small_shardset_t& known_down,
                                node_index_t my_node_index);

  void disableSingleCopyDelivery();

  void enableLocalScd(const std::string& client_location);

  void disableLocalScd();

  /**
   * @return True if this stream is catching up, ie it is currently enqueued in
   * CatchupQueue.
   *
   * "Catching up" means that there are records that the storage node needs to
   * send to the client.  There are three reasons why a read stream may be
   * catching up:
   *
   * (1) The read stream was started and we need to send a backlog of existing
   *     records to the client.  For example, if a client starts reading at
   *     LSN 1 and we already have LSNs up to 1000000 in the local log store,
   *     we need to ship all those records to the client.
   *
   * (2) The read stream was up to date but then a new record in the log was
   *     released for delivery.
   *
   * (3) A WINDOW message moved the sliding window (by increasing window_high_
   *     to a larger value).
   */
  bool isCatchingUp() const;

  /**
   * @return True if this stream reached the end of the client provided window
   *         (read_ptr.lsn > window_high_).
   */
  bool isPastWindow() const;

  /**
   * @return True if this stream's current position is in a region where
   *         records may have been lost.
   *
   * This is used to control the type of no-records gap delievered to clients.
   */
  bool isInUnderreplicatedRegion() const;

  /**
   * Set the read pointer for this stream.
   * @param lsn  Next LSN to be read;
   */
  void setReadPtr(lsn_t lsn);
  void setReadPtr(LocalLogStoreReader::ReadPointer read_ptr);

  const LocalLogStoreReader::ReadPointer& getReadPtr() const;

  /**
   * Change the client provided window.
   */
  void setWindowHigh(lsn_t window_high);

  lsn_t getWindowHigh() const {
    return window_high_;
  }

  bool scdEnabled() const {
    return scd_enabled_;
  }
  bool localScdEnabled() const {
    return local_scd_enabled_;
  }
  TrafficClass trafficClass() const {
    return traffic_class_;
  }

  void setSCDCopysetReordering(SCDCopysetReordering val,
                               uint64_t csid_hash_pt1_ = 0,
                               uint64_t csid_hash_pt2_ = 0) {
    ld_check(val < SCDCopysetReordering::MAX);
    scd_copyset_reordering_ = val;

    if (scd_copyset_reordering_ ==
        SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED) {
      csid_hash_pt1 = csid_hash_pt1_;
      csid_hash_pt2 = csid_hash_pt2_;
    }
  }

  SCDCopysetReordering scdCopysetReordering() const {
    return scd_copyset_reordering_;
  }

  bool isThrottled() {
    return is_throttled_;
  }

  void csidHash(uint64_t& pt1, uint64_t& pt2) const {
    pt1 = csid_hash_pt1;
    pt2 = csid_hash_pt2;
  }

  void setPriority(Priority rp) {
    rpriority_ = rp;
  }

  Priority getReadPriority() const {
    return rpriority_;
  }

  void setTrafficClass(TrafficClass c);

  const small_shardset_t& getKnownDown() const {
    return known_down_;
  }

  std::string toString() const;

  void getDebugInfo(InfoReadersTable& table) const;

  logid_t getLogId() {
    return log_id_;
  }

  // These update per traffic class stats. setTrafficClass() has to unupdate
  // all these stats for the old class and update them for the new class.
  // Make sure to keep stats logic in setTrafficClass() in sync with these.
  void adjustStatWhenCatchingUpChanged();
  void adjustStatWhenWindowChanged();

  WeakRef<ServerReadStream> createRef() {
    return ref_holder_.ref();
  }

  void addReleasedRecords(const std::shared_ptr<ReleasedRecords>& ptr);

  std::vector<std::shared_ptr<ReleasedRecords>> giveReleasedRecords() {
    // Note: the move constructor here doesn't guarantee that released_records_
    // will be empty.  In fact, the optimizer may elide the construction of a
    // new vector altogether.
    std::vector<std::shared_ptr<ReleasedRecords>> ret{
        std::move(released_records_)};
    released_records_.clear();
    released_records_.shrink_to_fit();
    return ret;
  }

  void markThrottled(bool throttled) {
    is_throttled_ = throttled;
    if (throttled) {
      throttling_start_time_ = std::chrono::steady_clock::now();
      STAT_INCR(stats_, read_throttling_num_streams_throttled);
    } else {
      STAT_INCR(stats_, read_throttling_num_streams_unthrottled);
    }
  }

  std::chrono::steady_clock::time_point getThrottlingStartTime() {
    return throttling_start_time_;
  }

  void addCQRef(WeakRef<CatchupQueue> cq) {
    read_shaping_cb_.addCQRef(cq);
  }

  size_t getCurrentMeterLevel() const {
    auto w = Worker::onThisThread();
    ShapingContainer& read_container = w->readShapingContainer();
    auto& flow_group = read_container.getFlowGroup(NodeLocationScope::NODE);
    return flow_group.level(getReadPriority());
  }

  // Id of this read stream.
  read_stream_id_t id_;

  // Client this read stream is for.
  ClientID client_id_;

  // Log this read stream is for.
  logid_t log_id_;

  // Id of the shard we should be reading from on this node.
  shard_index_t shard_;

  // The LSN the client started reading from.
  // Used for stream validations and debugging.
  lsn_t start_lsn_;

  // How far the client asked to read (inclusive)
  lsn_t until_lsn_;

  // LSN of the last data record we sent to the client. Most often this is
  // exactly equal to read_ptr_.lsn - 1 but copyset filtering may cause the read
  // pointer to advance without actually sending to the client and updating
  // last_delivered_record_.
  // Currently only used for debugging.
  lsn_t last_delivered_record_;

  // Last lsn we delivered to the client. Either the lsn of a record or the
  // higher bound of a gap.
  lsn_t last_delivered_lsn_;

  // Used to remember end lsn for filtered out gap when processing records.
  // Then we can merge filterted out gaps into one interval and reset this
  // to LSN_INVALID.
  lsn_t filtered_out_end_lsn_ = LSN_INVALID;

  // Cached last_known_good lsn for the epoch the stream is currently reading
  // from. Avoids asking the store for mutable per-epoch metadata on every
  // single record when reading past the last_released_lsn.
  lsn_t last_known_good_ = LSN_INVALID;

  // If true, consider last_delivered_lsn_ equal to -1. This is used to handle
  // the case when client sets start_lsn to LSN_INVALID. We want to make sure
  // we send a gap that covers start_lsn.
  bool need_to_deliver_lsn_zero_ = false;

  // Set to true if the STARTED message must be issued prior to sending
  // any additional records for this stream. This happens the first time
  // the stream is started and for every subsequent, client initiated,
  // rewind of the stream.
  //
  // Note: If the STARTED message is discarded due to traffic shaping,
  //       this field will be reset to true so STARTED is issued when
  //       transmission is retried.
  bool needs_started_message_ = false;

  /**
   * If true, indicates that we should not serve records or gaps for this read
   * stream because our shard for log `log_id_` is being rebuilt. We sent
   * STARTED(status=E::REBUILDING) to the reader. Once rebuilding of the shard
   * is complete, AllServerReadStreams::onShardRebuilt() will wake up this read
   * stream.
   */
  bool rebuilding_;

  /**
   * If true, indicates that the last read of this stream intersected with
   * a log region where records may have been lost.
   */
  bool in_under_replicated_region_;

  /**
   * ServerReadStream is versioned to avoid a class of concurrency issues where:
   *
   * (1) A ReadStorageTask is created given the current ServerReadStream state.
   *
   * (2) While the ReadStorageTask is processing on a storage thread, the
   *     ServerReadStream state is altered, e.g. by a WINDOW message from the
   *     client or because a new record is released.
   *
   * (3) Based on the (now outdated) properties of the storage task, the
   *     storage thread determines that the read stream is caught up.
   *     However, we should not mark the ServerReadStream as caught up because
   *     of (2).
   */
  server_read_stream_version_t version_;

  /*
   * Used to provide any created ReadStorageTask with a weak reference to this
   * ServerReadStream so that the task can know whether this ServerReadStream
   * has been destroyed while it was in flight.
   */
  WeakRefHolder<ServerReadStream> ref_holder_;

  /**
   * ServerReadStream has another version number to avoid a class of
   * concurrencly issues with clients where:
   *
   * (1) We are in single copy delivery mode and sent some records over the
   *     wire.
   *
   * (1) The client (ClientReadStream) performs failover to all send all mode by
   *     sending us a new START message.
   *
   * (2) The client receives the record that we had sent still when in single
   *     copy delivery mode. It thinks it is in all send all mode and thus
   *     performs normal gap detection on these records that were actually
   *     filtered according to the single copy delivery mode.
   *
   * In order to deal with that, we version the read stream for its filtering
   * mode. Each time the client rewinds by sending us a START message, it will
   * bump its internal filter version and pass it to the header of that message.
   * The storage node guarantees that it will not enqueue RECORD messages for
   * that new filter version before it enqueues the STARTED message for it.
   * ClientReadStream can rely on that and discard RECORD messages that are
   * received in between the time it rewinded and the time it receives the
   * STARTED message it is waiting for.
   */
  filter_version_t filter_version_;

  /**
   * Support for verifying stream invariants as the message layer confirms
   * messages are sent.
   *
   * On each start/rewind, a new OnSentState is pushed onto the back of
   * sent_state. As STARTED, RECORD, and GAP responses are transmitted,
   * verification is performed to ensure that the lsn sequence is always
   * increasing.
   */
  struct OnSentState {
    OnSentState(filter_version_t fv, lsn_t start)
        : filter_version(fv),
          start_lsn(start),
          min_next_lsn(LSN_INVALID),
          started(false) {}

    std::string toString() const;

    filter_version_t filter_version;
    lsn_t start_lsn;
    lsn_t min_next_lsn;
    lsn_t last_record_lsn;
    lsn_t last_lsn;
    bool started;
  };
  std::list<OnSentState> sent_state;

  /**
   * When sending RECORD messages, should they include ExtraMetadata
   * structures?  The client requests this.
   */
  bool include_extra_metadata_;

  /**
   * When reading from the local log store, should we let it fill its cache?
   */
  bool fill_cache_;

  /**
   * Should we not take released status into account when reading?
   * Currently used for log recovery only.
   */
  bool ignore_released_status_;

  /**
   * Is this read stream used by epoch recovery code for building a
   * record digest?
   */
  bool digest_;

  /*
   * Shoud we not send payload to clients?
   */
  bool no_payload_;

  /*
   * Try to deliver only data contained in the copyset index
   */
  bool csi_data_only_;

  /*
   * Should we replace payload with its hash?
   */
  bool payload_hash_only_ = false;

  /**
   * Indicate that storage node should try to send byte offset of log up to
   * current reading record if record contain offset_within_epoch. Byte offset
   * can be unavailable if epoch_offset is not set in PerEpochLogMetadata.
   * In this case GetSeqStateRequest should be send to get epoch_offset without
   * blocking reading.
   */
  bool include_byte_offset_ = false;

  /**
   * true if the reader is a logdevice server. This means the stream is probably
   * important for the cluster to work properly - e.g. it may be a recovery
   * digest stream, or a server tailing event log, or a server reading config
   * log on startup. Such streams are prioritized over others.
   */
  bool is_internal_ = false;

  /**
   * Indicate that EpochOffsetTask was sent and still in process.
   * This flag helps to prevent creating more than one task at the time.
   */
  bool epoch_task_in_flight = false;

  // Iterators used for this read stream. Created once and reused. Periodically
  // invalidated (if unused) to avoid pinning resources.
  std::shared_ptr<IteratorCache> iterator_cache_;

  // Protocol used to communicate with the client.
  uint16_t proto_;

  // Time this stream was created (first START message received)
  SteadyTimestamp created_;

  // Last time this stream saw a START message
  SteadyTimestamp last_rewind_time_;

  // Last time where the stream was adding in the CatchupQueue for catchup.
  // Used for stats and debugging.
  SteadyTimestamp last_enqueued_time_;

  // Last time CatchupOneStream started reading a batch. Used for stats and
  // debugging.
  SteadyTimestamp last_batch_started_time_;

  // When artificial latency is injected for this read stream, we keep track
  // here of the next time we can read a batch.
  SteadyTimestamp next_read_time_;

  // Useful for displaying stream's current throttling state via admin command
  bool is_throttled_{false};

  std::chrono::steady_clock::time_point throttling_start_time_;

  ReadIoShapingCallback read_shaping_cb_;

  // Whether there is currently a storage task in flight for this stream, in
  // which case the stream should be at the top of CatchupQueue.
  bool storage_task_in_flight_;

  // Status of the last batch. Used for debugging only.
  // Pointer to a string literal, so that it's fast to assign.
  const char* last_batch_status_ = "no batches";

  // Replication factor this client expects for records.
  // This value helps the storage node figure out that the copy it has is
  // actually an extra so it can ship it if SCD is active.
  // Set to 0 if replication is unknown.
  uint16_t replication_;

  // Hook for CatchupQueue::queue_.
  folly::IntrusiveListHook queue_hook_;

  // Hook for CatchupQueue::queue_delayed_.
  using set_member_hook_type = boost::intrusive::set_member_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
  set_member_hook_type queue_delayed_hook_;

  folly::Optional<std::pair<epoch_t, OffsetMap>> epoch_offsets_ = folly::none;

  // ServerRecordFilter used to filter out record. It will be constructed
  // by ServerRecordFilterFactory.
  std::unique_ptr<ServerRecordFilter> filter_pred_;

  // The location of the client reader.
  // Only used if local_scd_enabled_ is set to true.
  std::string client_location_;

  // Cached log range value, used for per-log-stats in the RECORD_Message's
  // onSent handler
  std::shared_ptr<std::string> log_group_path_;

  enum class RecordSource { REAL_TIME, NON_BLOCKING, BLOCKING, MAX };

  static const SimpleEnumMap<RecordSource, const char*> names;

  void noteSent(StatsHolder* stats, RecordSource, size_t msg_size_bytes_approx);

 private:
  // LSN to read from next time we try to read a batch of records
  // from the log store
  LocalLogStoreReader::ReadPointer read_ptr_;

  /**
   * This field is used for flow control. It is initialized by START message
   * and can be updated by WINDOW message.
   *
   * Sender should never send records with lsn greater than window_high_, and
   * window_high_ should not be greater than until_lsn_.
   */
  lsn_t window_high_;

  StatsHolder* stats_;

  /**
   * Traffic classification for this reader.
   *
   * The class may be dynamically changed based on observed read behavior.
   * For example, if reads are detected to be coming from disk, the class
   * may be lowered to READ_BACKLOG.
   */
  TrafficClass traffic_class_ = TrafficClass::MAX;

  // TODO: hardcoded for now, need to change priority based on categories later
  // on.
  Priority rpriority_ = Priority::MAX;

  /**
   * Set to true if single copy delivery mode is enabled.
   * @see doc/single-copy-delivery.md for more information about scd.
   */
  bool scd_enabled_;

  /**
   * Set to true if local scd is enabled.
   */
  bool local_scd_enabled_;

  /**
   * Set to true if this ServerReadStream sees itself in the known down list,
   * which indicates that the client thinks the shard is down or too slow.
   */
  bool self_in_known_down_ = false;

  /**
   * Copyset reordering approach to use in SCD, specified by the client.  See
   * SCDCopysetReordering.h and LocalLogStoreReader.{h,cpp} for details.
   */
  SCDCopysetReordering scd_copyset_reordering_{SCDCopysetReordering::NONE};
  /*
   * Hashed session id (128 bits in total) of client initiating the read.
   * Relevant for specific SCDCopysetReordering values.
   */
  uint64_t csid_hash_pt1;
  uint64_t csid_hash_pt2;

  /**
   * Set of nodes currently considered down by the client.
   * Only used if scd_enabled_ is set to true.
   */
  small_shardset_t known_down_;

  // ServerReadStream are destroyed in Worker::finishWorkAndCloseSockets()
  std::vector<std::shared_ptr<ReleasedRecords>> released_records_;

  folly::Optional<RecordSource> last_sent_source_;
};

inline std::ostream& operator<<(std::ostream& os,
                                const ServerReadStream::RecordSource source) {
  return os << ServerReadStream::names[source];
}

}} // namespace facebook::logdevice
