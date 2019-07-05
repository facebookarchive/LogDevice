/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

/**
 * @file Mutator is a state machine that performs mutation for one single
 *       record on the set of nodes from which EpochRecovery receives its
 *       digest. The main purpose of mutation is to ensure that the particular
 *       esn slot is in a consistent state across all storage nodes in the
 *       digest set. It mainly does the following:
 *          1) amend record metadata stored on the nodes in amend_metadata
 *          2) overwrite record stored on the nodes in conflict_copies
 *          3) replicate the consensus record to other nodes until the set of
 *             nodes that stored the consensus record (including nodes in
 *             amend_metadata and conflict_copies) satisfy the replication
 *             requirement for this epoch.
 *
 *       In addition, currently Mutator requires all stores and amends must be
 *       done in the same wave. This is to guarantee that there are at least
 *       a set of nodes in the mutation set that end up with the a consistent
 *       and accurate copyset (copyset contains exactly the nodes fully stored
 *       and is the same across the set of nodes). This is not strictly
 *       required by log recovery, but is currently crucial to rebuilding
 *       for ensuring the record gets properly re-replicated.
 *
 *       All Mutators are owned by EpochRecovery objects and reside in
 *       EpochRecovery::mutation_ maps. When a mutator is done, it must call
 *       onMutationComplete() of its containing EpochRecovery object, which
 *       will remove the Mutator from the map and destroy it.
 */

class EpochRecovery;

class Mutator {
 public:
  /**
   * Constructs a new Mutator.
   *
   * @param header       STORE message header prepared by epoch recovery, must
   *                     have STORE_Header::WRITTEN_BY_RECOVERY set.
   *
   * @param extra        STORE message extra prepared by epoch recovery,
   *                     contains recovery id, seal epoch number,
   *                     offset_with_epoch, etc.
   *
   * @param payload      payload of the record to be stored, can be empty if
   *                     the record is a hole plug
   *
   * @param mutation_set  set of nodes to perform mutation, it is the mutation
   *                      set in epoch recovery
   *
   * @param replication              replication property of the record
   *
   * @param amend_meadata            the set of nodes whose record metadata
   *                                 needs to amended
   * @param conflict_copies          the set of nodes whose record needs to be
   *                                 overwritten
   *
   * @param epoch_recovery           EpochRecovery object that owns this Mutator
   */
  Mutator(const STORE_Header& header,
          const STORE_Extra& extra,
          Payload payload,
          StorageSet mutation_set,
          ReplicationProperty replication,
          std::set<ShardID> amend_metadata,
          std::set<ShardID> conflict_copies,
          EpochRecovery* epoch_recovery);

  virtual ~Mutator() {}

  /**
   * Start performing a mutation.
   */
  virtual void start();

  /**
   * Called by EpochRecovery::onMutated() when a MUTATED message for
   * this mutation is received.
   *
   * @param from  the node that sent the message
   * @param header MUTATED msg header
   */
  void onStored(ShardID from, const MUTATED_Header& header);

  /**
   * Called by STORE_Message::onSent() when a STORE message for this mutation
   * is sent.
   *
   * @param to        recipient of the STORE message
   * @param st        onSent() status
   * @param header    STORE_Header of the message
   */
  void onMessageSent(ShardID to, Status st, const STORE_Header& header);

  /**
   *  If the Muator was preempted by another epoch recovery instance, store
   *  the seal of the preemptor. Only valid if Muator is preempted.
   */
  Seal getPreemptedSeal() const {
    return preempted_seal_;
  }

  /**
   * What to print to the log if this mutation times out.
   */
  std::string describeState() const;

  /**
   * Enables collection and printing of a trace of what this Mutator has done.
   * See NodeSetAccessor::getDebugTrace().
   */
  void printDebugTraceWhenComplete();

  //// expose some internal members for testing

  const STORE_Header& getStoreHeader() const {
    return header_;
  }

  const STORE_Extra& getStoreExtra() const {
    return store_extra_;
  }

  const std::set<ShardID>& getAmendSet() const {
    return amend_metadata_;
  }

  const std::set<ShardID>& getConflictSet() const {
    return conflict_copies_;
  }

  const StorageSet& getMutationSet() const {
    return nodeset_;
  }

 protected:
  virtual std::unique_ptr<StorageSetAccessor> createStorageSetAccessor(
      logid_t log_id,
      EpochMetaData epoch_metadata_with_mutation_set,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      StorageSetAccessor::ShardAccessFunc node_access,
      StorageSetAccessor::CompletionFunc completion,
      StorageSetAccessor::Property property);

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual AuthoritativeStatus getNodeAuthoritativeStatus(ShardID shard) const;

  virtual chrono_interval_t<std::chrono::milliseconds>
  getMutationTimeout() const;

  virtual void finalize(Status status, ShardID node_to_reseal = ShardID());

  std::unique_ptr<SenderBase> sender_;

 private:
  // callback used to detect when a connection to one of the nodes in the
  // mutation set was closed and optionally restart recovery (if node was
  // replaced)
  struct SocketClosedCallback : public SocketCallback {
    explicit SocketClosedCallback(Mutator* mutator, ShardID shard)
        : mutator_(mutator), shard_(shard) {}
    void operator()(Status st, const Address& address) override;
    Mutator* mutator_;
    ShardID shard_;
  };

  struct NodeBWAvailable : public BWAvailableCallback {
    explicit NodeBWAvailable(Mutator* mutator, ShardID shard)
        : mutator_(mutator), shard_(shard) {}
    void operator()(FlowGroup&, std::mutex&) override;

    void setMessage(std::unique_ptr<STORE_Message> msg) {
      msg_ = std::move(msg);
    }

    Mutator* mutator_;
    ShardID shard_;
    std::unique_ptr<STORE_Message> msg_;
  };

  // maintain per-node state not tracked by StorageSetAccessor
  struct NodeState {
    NodeState(Mutator* mutator, ShardID shard)
        : socket_callback_(mutator, shard),
          bw_available_callback_(mutator, shard) {}

    SocketClosedCallback socket_callback_;
    NodeBWAvailable bw_available_callback_;
  };

  STORE_Header header_;
  STORE_Extra store_extra_;
  const Payload payload_;
  const StorageSet nodeset_;
  const ReplicationProperty replication_;

  std::set<ShardID> amend_metadata_;
  std::set<ShardID> conflict_copies_;

  EpochRecovery* const epoch_recovery_;

  // for determine whether mutation is completed as well as handling retries
  std::unique_ptr<StorageSetAccessor> nodeset_accessor_;

  std::unordered_map<ShardID, NodeState, ShardID::Hash> node_state_;

  // If set to something other than E::OK, indicates that a critical error (e.g.
  // a node in the mutation set was replaced, or a storage node replied with
  // E::PREEMPTED) had occurred and that this Mutator should be aborted.
  Status mutation_status_{E::UNKNOWN};

  // If a storageNode sent a MUTATED_Message with E::PREEMPTED status, this
  // will be set to the seal record which preempted current recovery
  Seal preempted_seal_;

  // When message sending fails with E::NOTINCONFIG, this will be set to the id
  // of the shard which was removed. This is passed to EpochRecovery's
  // onMutationComplete() method, causing it to remove the node from recovery
  // set.
  // Default-initiliazed to an invalid ShardID.
  ShardID shard_not_in_config_;

  // TODO 11866467: this is a workaround for pretending all MUTATED replies are
  // from the latest wave, remove this once we include wave in MUTATED messages
  uint32_t current_wave_{0};

  bool print_debug_trace_when_complete_ = false;

  bool done() const {
    return mutation_status_ != E::UNKNOWN;
  }

  void finalizeIfDone();

  void applyShardAuthoritativeStatus();

  StorageSetAccessor::SendResult
  sendSTORE(ShardID shard, const StorageSetAccessor::WaveInfo& wave_info);

  void sendDeferredSTORE(ShardID shard, std::unique_ptr<STORE_Message> msg);

  // called by SocketClosedCallback when a connection to `node' has been closed
  void onConnectionClosed(ShardID shard, Status status);

  void onStorageSetAccessorComplete(Status status);

  // maximum delay used for store and retry timers
  static const std::chrono::milliseconds MUTATION_MAX_DELAY;
};

}} // namespace facebook::logdevice
