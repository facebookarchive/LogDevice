/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/small_vector.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/CopySetSelectorFactory.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/NodeAvailabilityChecker.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Random.h"
#include "logdevice/common/RebuildingTracer.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/include/types.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

/**
 * @file Base class that provides functionality for rebuilding a single record.
 */

namespace facebook { namespace logdevice {

class CopySetSelector;
struct RebuildingSet;

using NodeIndexServerInstancePair = std::pair<node_index_t, ServerInstanceId>;

struct NodeIndexServerInstancePairKeyHasher {
  size_t operator()(const NodeIndexServerInstancePair& key) const {
    return folly::hash::hash_combine(key.first, key.second);
  }
};

using FlushTokenMap = std::unordered_map<NodeIndexServerInstancePair,
                                         FlushToken,
                                         NodeIndexServerInstancePairKeyHasher>;

struct ReplicationScheme {
  EpochMetaData epoch_metadata;
  std::shared_ptr<NodeSetState> nodeset_state;
  std::unique_ptr<CopySetSelector> copysetSelector;
  NodeID sequencer_node_id; // this is put in STORE messages
  bool relocate_local_records = false;

  ReplicationScheme() {} // only used in tests

  ReplicationScheme(
      logid_t logid,
      EpochMetaData _epoch_metadata,
      const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
      folly::Optional<NodeID> my_node_id,
      const logsconfig::LogAttributes* log_attrs,
      const Settings& settings,
      bool relocate,
      NodeID sequencer_node)
      : epoch_metadata(std::move(_epoch_metadata))
        // not excluding zero-weight nodes
        ,
        nodeset_state(std::make_shared<NodeSetState>(
            epoch_metadata.shards,
            logid,
            NodeSetState::HealthCheck::DISABLED)),
        sequencer_node_id(sequencer_node),
        relocate_local_records(relocate) {
    // Copyset selector initialization may need an RNG (e.g. to shuffle
    // racks). Use a deterministic PRNG seeded with log ID, to make sure that
    // different donors would select the same copyset for the same block.
    XorShift128PRNG init_rng(folly::hash::twang_mix64(logid.val_),
                             folly::hash::twang_mix64(logid.val_ + 1));
    copysetSelector = CopySetSelectorFactory::create(logid,
                                                     epoch_metadata,
                                                     nodeset_state,
                                                     nodes_configuration,
                                                     my_node_id,
                                                     log_attrs,
                                                     settings,
                                                     init_rng);
  }
};

// RecordRebuildingAmendState contains all the information necessary to
// start a RecordRebuildingAmend state machine. We have a separate struct
// to hold this info instead of just creating RecordRebuildingAmend state
// machine as soon as RecordRebuildingStore state machine completes is
// becasue of the memory footprint of RecordRebuildingAmend state machines.
// In order to reduce the memory usage while waiting for stores
// to be durable, we use this struct to hold necessary info and create a
// RecordRebuildingAmendState machine only when it is required.
using RebuildingStoreChain = folly::small_vector<StoreChainLink, 6>;
using RebuildingCopyset = folly::small_vector<ShardID, 6>;
using RebuildingLegacyCopyset = folly::small_vector<node_index_t, 6>;
struct RecordRebuildingAmendState {
 public:
  explicit RecordRebuildingAmendState(
      lsn_t lsn,
      std::shared_ptr<ReplicationScheme> replication,
      STORE_Header storeHeader,
      LocalLogStoreRecordFormat::flags_t flags,
      copyset_t newCopyset,
      copyset_t amendRecipients,
      uint32_t rebuildingWave)
      : lsn_(lsn),
        replication_(replication),
        storeHeader_(storeHeader),
        flags_(flags),
        newCopyset_(newCopyset),
        amendRecipients_(amendRecipients),
        rebuildingWave_(rebuildingWave) {}
  lsn_t lsn_;
  std::shared_ptr<ReplicationScheme> replication_;
  STORE_Header storeHeader_;
  LocalLogStoreRecordFormat::flags_t flags_;
  copyset_t newCopyset_;
  copyset_t amendRecipients_;
  uint32_t rebuildingWave_;
};

// Target of callbacks from RecordRebuilding.
// Also provides RecordRebuilding with some information about the rebuilding.
// LogRebuilding and ChunkRebuilding implement this interface.
class RecordRebuildingOwner {
 public:
  virtual ~RecordRebuildingOwner() = default;

  virtual const RebuildingSet& getRebuildingSet() const = 0;
  virtual logid_t getLogID() const = 0;
  virtual lsn_t getRebuildingVersion() const = 0;
  virtual lsn_t getRestartVersion() const = 0;
  virtual log_rebuilding_id_t getLogRebuildingId() const = 0;
  virtual ServerInstanceId getServerInstanceId() const = 0;
  virtual UpdateableSettings<RebuildingSettings>
  getRebuildingSettings() const = 0;

  virtual void
  onAllStoresReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) = 0;
  virtual void onCopysetInvalid(lsn_t lsn) = 0;
  virtual void
  onAllAmendsReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) = 0;
};

class RecordRebuildingBase : public RecordRebuildingInterface {
 public:
  class AmendSelfStorageTask : public WriteStorageTask {
   public:
    explicit AmendSelfStorageTask(const RecordRebuildingBase& owner);

    void onDone() override;
    void onDropped() override;
    ThreadType getThreadType() const override {
      return ThreadType::FAST_STALLABLE;
    }
    size_t getNumWriteOps() const override {
      return 1;
    }
    size_t getWriteOps(const WriteOp** write_ops,
                       size_t write_ops_len) const override {
      if (write_ops_len > 0) {
        write_ops[0] = &writeOp_;
        return 1;
      } else {
        return 0;
      }
    }
    bool allowIfStoreIsNotAcceptingWrites(Status status) const override {
      return status == E::NOSPC;
    }

    const logid_t logid;
    const lsn_t lsn;
    const lsn_t restartVersion;
    const uint32_t rebuildingWave;

   private:
    std::string recordHeaderBuf_;
    std::string copySetIndexEntryBuf_;
    PutWriteOp writeOp_;
    // Keep a weak reference to the owning RecordRebuilding.
    WeakRefHolder<RecordRebuildingBase>::Ref ref_;
    PutWriteOp createWriteOp(const RecordRebuildingBase& owner);
    void onDoneOrDropped(Status status);
  };

  /**
   * @param lsn                   LSN of the record
   * @param shard                 Local shard for which this state machine is.
   * @param owner                 Object that owns this RecordRebuilding object.
   * @param replication           How copysets should be selected for the
   *                              current epoch.
   * @param node_availability     Object to use for checking which nodes
   *                              are alive.
   */
  RecordRebuildingBase(lsn_t lsn,
                       shard_index_t shard,
                       RecordRebuildingOwner* owner,
                       std::shared_ptr<ReplicationScheme> replication,
                       const NodeAvailabilityChecker* node_availability =
                           NodeAvailabilityChecker::instance());

  ~RecordRebuildingBase() override;

  /**
   * @return offset of shard in provided copyset or -1 if not found.
   */
  static copyset_off_t findCopysetOffset(RebuildingCopyset& copyset,
                                         ShardID shard);

  /**
   * Start this state machine.
   *
   * @param read_only If true, actually do not rebuild anything and complete
   *                  immediately. Used for testing.
   */
  virtual void start(bool read_only = false) = 0;

  lsn_t getLsn() const {
    return lsn_;
  }

  void onStored(const STORED_Header& header,
                ShardID from,
                lsn_t rebuilding_version,
                uint32_t rebuilding_wave,
                log_rebuilding_id_t rebuilding_id,
                ServerInstanceId serverInstanceId = ServerInstanceId_INVALID,
                FlushToken flush_token = FlushToken_INVALID) override;

  void onStoreSent(Status st,
                   const STORE_Header& header,
                   ShardID to,
                   lsn_t rebuilding_version,
                   uint32_t rebuilding_wave) override;

  void onConnectionClosed(ShardID shard);

  void onAmendedSelf(Status status,
                     FlushToken flush_token = FlushToken_INVALID);

  struct RecipientNode {
    class ResendStoreCallback : public BWAvailableCallback {
     public:
      explicit ResendStoreCallback(RecordRebuildingBase& rr)
          : record_rebuilding_(&rr) {}

      void operator()(FlowGroup&, std::mutex&) override;
      void cancelled(Status) override;

      void setMessage(std::unique_ptr<Message> msg, ShardID shard) {
        msg_ = std::move(msg);
        shard_ = shard;
      }

     private:
      RecordRebuildingBase* record_rebuilding_;
      ShardID shard_;
      std::unique_ptr<Message> msg_;
    };

    class SocketClosedCallback : public SocketCallback {
     public:
      explicit SocketClosedCallback(RecordRebuildingBase& rr, ShardID shard)
          : record_rebuilding_(&rr), shard_(shard) {}

      // called when a connection to a storage node gets closed after we've
      // sent a message to it
      void operator()(Status st, const Address& name) override;

     private:
      RecordRebuildingBase* record_rebuilding_;
      ShardID shard_;
    };

    explicit RecipientNode(RecordRebuildingBase& rr, ShardID shard)
        : on_bw_avail(rr), on_socket_close(rr, shard), shard_(shard) {}

    bool isStoreInFlight() const {
      return on_socket_close.active();
    }

    void deactivateCallbacks() {
      if (on_bw_avail.active()) {
        on_bw_avail.deactivate();
        on_bw_avail.cancelled(E::OK);
      }
      on_socket_close.deactivate();
    }

    // Active when we're waiting for bandwidth to become available.
    ResendStoreCallback on_bw_avail;
    // Active if we've successfully sent a STORE to this node and haven't heard
    // back yet.
    SocketClosedCallback on_socket_close;

    ShardID shard_;
    bool succeeded = false;
    FlushToken flush_token = FlushToken_INVALID;
    ServerInstanceId server_instance_id = ServerInstanceId_INVALID;
  };

  struct StageRecipients {
    enum class Type { STORE, AMEND };

    Type type;
    folly::small_vector<RecipientNode, 2> recipients;

    StageRecipients() {}
    explicit StageRecipients(Type type) : type(type) {}
  };

 private:
  virtual void traceEvent(const char* event_type, const char* status) = 0;
  StageRecipients* curStageRecipient_;

  ExponentialBackoffTimer retryTimer_;
  ExponentialBackoffTimer storeTimer_;
  std::unique_ptr<Timer> deferredCompleteTimer_;
  bool amendSelfStorageTaskInFlight_ = false;

  // On failure returns -1 and sets err to:
  //  E::CBREGISTERED  The store was deferred by traffic shaping and will be
  //                   retried soon.
  //  E::NOSPC  The node is marked as out of space.
  //            Need to pick a different copyset.
  //  E::AGAIN  Failed to send for some other reason, e.g. the node is down.
  //            Need to keep trying.
  int sendStore(StageRecipients::Type type, RecipientNode& recipient);

  // Send a message that was deferred due to traffic shaping.
  void sendDeferredStore(std::unique_ptr<Message>, ShardID);
  void onDeferredStoreCancelled(std::unique_ptr<Message>, ShardID, Status);

  void amendSelf();
  std::unique_ptr<STORE_Message> buildStoreMessage(ShardID shard, bool amend);

  RecipientNode* findRecipient(ShardID shard);
  void onRecipientSuccess(RecipientNode* r,
                          ServerInstanceId s,
                          FlushToken flush_token);
  void onRecipientTransientFailure(RecipientNode* r);

 protected:
  size_t curStageRepliesExpected_ = 0;
  size_t deferredStores_ = 0;
  bool started_ = false;
  copyset_t newCopyset_;
  copyset_t existingCopyset_;
  copyset_t amendRecipients_;
  const NodeAvailabilityChecker* nodeAvailability_;
  lsn_t lsn_;
  shard_index_t shard_;
  RecordRebuildingOwner* owner_;
  RebuildingTracer tracer_;
  // time when the record builder was created, used to calculate the latency
  std::chrono::steady_clock::time_point creation_time_;
  std::shared_ptr<ReplicationScheme> replication_;
  std::unique_ptr<SenderBase> sender_;
  WeakRefHolder<RecordRebuildingBase> refHolder_;
  LocalLogStoreRecordFormat::flags_t recordFlags_;
  STORE_Header storeHeader_;
  OffsetMap offsets_within_epoch_;
  std::map<KeyType, std::string> optional_keys_;
  uint32_t rebuildingWave_ = 1;

  int sendStage(StageRecipients* stage, bool resend_inflight_stores);

  std::string stageDebugInfo(StageRecipients* s) const;

  ShardID getMyShardID() const;
  logid_t getLogID() const;
  lsn_t getRestartVersion() const;

  // Can be overridden in tests.
  virtual node_index_t getMyNodeIndex() const;
  virtual bool isStorageShardInConfig(ShardID shard) const;
  virtual const Settings& getSettings() const;
  virtual void activateRetryTimer();
  virtual void resetRetryTimer();
  virtual void activateStoreTimer();
  virtual bool isStoreTimerActive();
  virtual void resetStoreTimer();
  // Start a new wave. If immediate==true, start it immediately. Otherwise,
  // activate the timer and start the new wave when it triggers.
  void startNewWave(bool immediate);
  // Schedule onComplete() to be called on a later event loop iteration.
  // Used in case we want to call it when in start(), to not burden the upper
  // layer with handling recursive calls to onComplete() from start().
  virtual void deferredComplete();
  virtual uint32_t getStoreTimeoutMs() const;
  virtual void putAmendSelfTask(std::unique_ptr<AmendSelfStorageTask> task);

  /**
   * Called when this state machine completes. Inform the owner
   * that we completed. This might cause `this` to be deleted.
   */
  virtual void onComplete() = 0;
  virtual void onStoreFailed() = 0;

  virtual void onRetryTimeout() = 0;
  virtual void onStoreTimeout() = 0;
  virtual void onStageComplete() = 0;

  virtual const std::map<KeyType, std::string>& getKeys() {
    return optional_keys_;
  };
  // Returns false if current copyset contains a node that's not in config
  // anymore. We need to pick a new copyset in this case.
  bool checkEveryoneStillInConfig();

  virtual std::shared_ptr<PayloadHolder> getPayloadHolder() const {
    return nullptr;
  }
};

}} // namespace facebook::logdevice
